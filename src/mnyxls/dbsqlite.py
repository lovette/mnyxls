from __future__ import annotations

import logging
import re
import sqlite3
from collections import defaultdict
from datetime import date, timedelta
from typing import TYPE_CHECKING

import pandas as pd
from mysqlstmt import Insert, Select, Stmt, Update
from pandas._libs.tslibs.parsing import DateParseError  # pyright: ignore[reportPrivateImportUsage]

from .currencydecimal import CurrencyDecimal, currency_to_value
from .dbcore import db_execute_stmt
from .dbschema import (
    TABLE_ACCOUNT_BALANCES,
    TABLE_ACCOUNTS,
    TABLE_CATEGORIES,
    TABLE_CATEGORY_BALANCES,
    TABLE_ERAS,
    TABLE_LOANS,
    TABLE_PAYEES,
    TABLE_SCHEMAS,
    TABLE_TXNS,
    TABLE_TXNS_INV,
    table_schema_columns,
)
from .dbviews import VIEW_SCHEMAS, VIEW_TXNS_WITHTYPEANDCLASS
from .mysqlstmt_upsert import Upsert
from .report import (
    TRANSFER_CATEGORIES,
    AccountCategory,
    AccountClassification,
    ReportType,
    TxnClass,
    TxnType,
)
from .report_balances import MoneyReportAccountBalances
from .report_balancesdetails import MoneyReportAccountBalancesWithDetails
from .report_spending import MoneyReportIncomeAndSpending
from .rewritetxns import rewrites_apply_txns
from .shared import MnyXlsConfigError, MnyXlsRuntimeError, config_warning, parse_yyyymmdd_flex, pd_read_sql, split_category_pair

if TYPE_CHECKING:
    from .configtypes import ConfigAccountCategoriesT, ConfigAccountsT, ConfigAccountT, ConfigErasT, ConfigEraT, MainConfigFileT
    from .report import MoneyReport

# SQLite creates and manages these indexes
SQLITE_INDEX_PREFIX = "sqlite_"

# Use a named logger instead of root logger
logger = logging.getLogger("mnyxls")


######################################################################
# Contants


# INSERT INTO Txns ...
# pd.to_sql with method="multi" has a limit to how many values can be parameterized in a single query.
# When this limit is exceeded, an exception will occur: "sqlite3.OperationalError: too many SQL variables"
#
# Per https://www.sqlite.org/limits.html the
# "Maximum Number Of Host Parameters In A Single SQL Statement" is 32,766 (as of 2025)
#
# To reduce the total number of INSERT statements executed, estimate how many rows can be included
# in each INSERT batch/chunk based on the number of columns.
DB_TXNS_CHUNKSIZE = int(32766 / len(TABLE_SCHEMAS[TABLE_TXNS]["columns"]))
DB_TXNSINV_CHUNKSIZE = int(32766 / len(TABLE_SCHEMAS[TABLE_TXNS_INV]["columns"]))


######################################################################
# Helpers


def _db_trace_log_sql(sql: str) -> None:
    """Set as `set_trace_callback` function to log SQL statements.

    Args:
        sql (str): SQL statement.
    """

    def _sql_trunc_insert(sql: str) -> str:
        index = sql.find("VALUES")
        if index == -1:
            return sql
        return sql[:index] + "VALUES (?,?,?,...),(...),..."

    if sql.startswith((f'INSERT INTO "{TABLE_TXNS}" (', f'INSERT INTO "{TABLE_TXNS_INV}" (')):
        # Txns INSERT statements are voluminous, no need to overflow the terminal!
        sql = _sql_trunc_insert(sql)

    logger.debug(f"> {sql}")


def _conn_execute_count_rows(conn: sqlite3.Connection, q_select: Select) -> int:
    """Execute a Select query against a SQLite database connection and return row count.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        q_select (Select): Select statement.

    Returns:
        int
    """
    q_select.column_expr("COUNT(*)", named="count")

    row = db_execute_stmt(conn, q_select).fetchone()

    assert row is not None
    assert isinstance(row[0], int)

    return row[0]


def _list_account_classifications(conn: sqlite3.Connection) -> dict[str, list[str]]:
    """List classifications and categories assigned to accounts.

    Args:
        conn (sqlite3.Connection): SQLite connection.

    Returns:
        dict[classification] = list[category]
    """
    q_select = Select(TABLE_ACCOUNTS)
    q_select.columns("AccountClassification")
    q_select.columns("AccountCategory")
    q_select.where_value("AccountClassification", None, "<>")
    q_select.group_by(("AccountClassification", "AccountCategory"))
    q_select.order_by(("AccountClassification", "AccountCategory"))

    categories_by_classification: dict[str, list[str]] = defaultdict(list)

    df_categories = pd_read_sql(conn, q_select)

    if not df_categories.empty:
        for row in df_categories.itertuples(index=False):
            assert isinstance(row.AccountClassification, str)
            assert isinstance(row.AccountCategory, str)
            categories_by_classification[str(row.AccountClassification)].append(row.AccountCategory)

    return dict(categories_by_classification)


def _db_defer_foreign_keys(conn: sqlite3.Connection) -> None:
    """Disable foreign key constraints.

    `defer_foreign_keys` is implicitly turned OFF after each transaction or rollback.
    https://www.sqlite.org/pragma.html#pragma_defer_foreign_keys

    Args:
        conn (sqlite3.Connection): SQLite connection.
    """
    if False:
        cursor = conn.cursor()
        cursor.execute("PRAGMA defer_foreign_keys")
        row = cursor.fetchone()
        status = row[0] if row else 0

        logger.debug(f"Foreign keys {'WERE NOT' if status else 'WERE'} enforced; turning OFF now")

    conn.execute("PRAGMA defer_foreign_keys = ON")


def _db_enable_foreign_keys(conn: sqlite3.Connection, enable: bool = True, raise_err: bool = False) -> None:  # noqa: C901
    """Enable foreign keys and check foreign key constraints.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        enable (bool): Enable or disable foreign keys.
        raise_err (bool): Raise error on foreign key constraint violation.

    Raises:
        sqlite3.IntegrityError
    """
    # Enabling `foreign_keys` does not check constraints for records already inserted.
    # Constraints are enforced for operations performed after foreign keys are turned ON.
    # https://www.sqlite.org/pragma.html#pragma_foreign_key_check

    # Cache foreign keys so we don't have to query them multiple times.
    foreign_key_lists: dict[str, dict[int, dict[str, str]]] = defaultdict(dict)

    def _get_fk_info(child_table: str, child_fkid: int) -> dict[str, str] | None:
        if child_table in foreign_key_lists:
            return foreign_key_lists[child_table].get(child_fkid)

        child_fks = conn.execute(f"PRAGMA foreign_key_list({child_table})")
        if not child_fks:
            return None

        for row in child_fks.fetchall():
            assert len(row) == 8  # noqa: PLR2004
            fkid, fkseq, parent_table, child_key, parent_key, on_update, on_delete, match = row

            if fkid in foreign_key_lists[child_table]:
                # Multi-column key
                child_key = ",".join([foreign_key_lists[child_table][fkid]["child_key"], child_key])
                parent_key = ",".join([foreign_key_lists[child_table][fkid]["parent_key"], parent_key])

            foreign_key_lists[child_table][fkid] = {
                "child_key": child_key,
                "parent_table": parent_table,
                "parent_key": parent_key,
            }

        assert child_table in foreign_key_lists
        assert child_fkid in foreign_key_lists[child_table]

        return foreign_key_lists[child_table].get(child_fkid)

    def _get_fk_violations() -> dict[str, int] | None:
        violations = defaultdict(int)

        for row in conn.execute("PRAGMA foreign_key_check").fetchall():
            assert len(row) == 4  # noqa: PLR2004
            child_table, child_rowid, parent_table, child_fkid = row

            fkinfo = _get_fk_info(child_table, child_fkid)
            assert fkinfo is not None

            violations[f"{child_table}({fkinfo['child_key']}) -> {parent_table}({fkinfo['parent_key']})"] += 1

        return violations if violations else None

    def _check_violations() -> None:
        violations = _get_fk_violations()

        if violations:
            logger.error("The following FOREIGN KEY constraints have violations:")
            for vref, vcount in violations.items():
                logger.error(f"- {vref} [{vcount} rows]")
            if raise_err:
                raise sqlite3.IntegrityError("FOREIGN KEY key constraint violations")

    conn.execute(f"PRAGMA foreign_keys = {'ON' if enable else 'OFF'}")

    if enable:
        _check_violations()


def _db_drop_table(conn: sqlite3.Connection, table_name: str) -> None:
    """Drop a table.

    Changes are not made within a transaction, invoke `with conn` to ensure atomicity.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        table_name (str): Table name.
    """
    _db_delete_table_indexes(conn, table_name)
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")


def _db_drop_view(conn: sqlite3.Connection, view_name: str) -> None:
    """Drop a view if it exists.

    Changes are not made within a transaction, invoke `with conn` to ensure atomicity.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        view_name (str): View name
    """
    conn.execute(f"DROP VIEW IF EXISTS {view_name}")


def _db_create_table(
    conn: sqlite3.Connection,
    schema_table_name: str,
    table_name: str | None = None,
    temp: bool = False,
) -> None:
    """Create table using defined schema.

    Changes are not made within a transaction, invoke `with conn` to ensure atomicity.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        schema_table_name (str): Table name to use for schema.
        table_name (str): Table name to create, defaults to `schema_table_name`.
        temp (bool): Create a temporary table.
    """
    table_props = TABLE_SCHEMAS[schema_table_name]
    columns = table_props["columns"]
    primary_key = table_props.get("primary_key") or None
    foreign_keys = table_props.get("foreign_keys") or []
    table_name = table_name or schema_table_name

    # (column name, type, options)
    table_schema = [" ".join(col_name_type_opts) for col_name_type_opts in columns]

    if primary_key:
        if not isinstance(primary_key, str):
            primary_key = ", ".join(primary_key)
        table_schema.append(f"PRIMARY KEY ({primary_key})")

    for foreign_key_parts in foreign_keys:
        if len(foreign_key_parts) == 2:  # noqa: PLR2004
            child_fields, parent_table = foreign_key_parts
            parent_fields = child_fields
        elif len(foreign_key_parts) == 3:  # noqa: PLR2004
            child_fields, parent_table, parent_fields = foreign_key_parts
        else:
            raise ValueError(f"Table {table_name}: Invalid foreign key definition: '{foreign_key_parts}'")

        if not isinstance(child_fields, str):
            child_fields = ", ".join(child_fields)
        if not isinstance(parent_fields, str):
            parent_fields = ", ".join(parent_fields)

        table_schema.append(f"FOREIGN KEY ({child_fields}) REFERENCES {parent_table}({parent_fields})")

    table_schema = ", ".join(table_schema)

    create_table = " ".join(
        [
            "CREATE",
            "TEMPORARY" if temp else "",
            "TABLE",
            f"{table_name}",
            f"({table_schema})",
        ]
    ).rstrip()

    conn.execute(create_table)


def _db_create_tables(conn: sqlite3.Connection) -> None:
    """Create SQLite database schema with proper column definitions.

    Args:
        conn (sqlite3.Connection): SQLite connection.
    """
    with conn:
        for table_name in TABLE_SCHEMAS:
            _db_drop_table(conn, table_name)
            _db_create_table(conn, table_name)


def _db_create_indexes(conn: sqlite3.Connection) -> None:
    """Create table indexes.

    Args:
        conn (sqlite3.Connection): SQLite connection.
    """
    table_indexes = {}

    for table_name, table_props in TABLE_SCHEMAS.items():
        table_index_defs = table_props.get("indexes")
        if table_index_defs:
            table_indexes[table_name] = table_index_defs

    with conn:
        for table_name, indexes in table_indexes.items():
            for idx_col_names, idx_unique in indexes:
                idx_col_names = idx_col_names if isinstance(idx_col_names, str) else ", ".join(idx_col_names)  # noqa: PLW2901
                idx_name = "_".join(["idx", table_name, re.sub(r"[, ]+", "_", idx_col_names)])

                create_index_sql = " ".join(
                    [
                        "CREATE",
                        "UNIQUE INDEX" if idx_unique else "INDEX",
                        idx_name.lower(),
                        "ON",
                        table_name,
                        f"({idx_col_names})",
                    ]
                )

                conn.execute(create_index_sql)


def _db_delete_table_indexes(conn: sqlite3.Connection, table_name: str) -> None:
    """Delete user-created indexes for a table.

    Changes are not made within a transaction, invoke `with conn` to ensure atomicity.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        table_name (str): Table name.
    """
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA index_list('{table_name}')")

    indexes = [index[1] for index in cursor.fetchall() if not index[1].startswith(SQLITE_INDEX_PREFIX)]

    for index_name in indexes:
        cursor.execute(f"DROP INDEX IF EXISTS '{index_name}'")


def _db_resort_table(conn: sqlite3.Connection, table_name: str, order_by: str) -> None:
    """Resort a table by a column.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        table_name (str): Table name.
        order_by (str): Column name to sort by.
    """
    assert table_name in TABLE_SCHEMAS

    temp_table = f"{table_name}_temp"
    columns = table_schema_columns(table_name)

    q_insert_temp = Insert(temp_table).columns(columns)
    q_insert_temp.select(Select(table_name).columns(columns))

    q_insert_sorted = Insert(table_name).columns(columns)
    q_insert_sorted.select(Select(temp_table).columns(columns).order_by(order_by))

    with conn:
        _db_create_table(conn, table_name, temp_table, temp=True)
        db_execute_stmt(conn, q_insert_temp)
        conn.execute(f"DELETE FROM `{table_name}`")  # noqa: S608
        db_execute_stmt(conn, q_insert_sorted)
        _db_drop_table(conn, temp_table)


def _db_insert_txns(conn: sqlite3.Connection, reports: dict[ReportType, MoneyReport]) -> None:
    """Insert transactions into transaction table.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        reports (dict[ReportType, MoneyReport]): Reports by type.
    """
    report = reports.get(ReportType.ACCOUNT_TRANSACTIONS)
    if not report:
        raise ValueError("Transactions report is required.")

    if not report.empty_report:
        assert report.df_report is not None

        with conn:
            # INSERT INTO Txns ...
            report.df_report.to_sql(
                TABLE_TXNS,
                conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=DB_TXNS_CHUNKSIZE,
            )


def _db_insert_txns_inv(conn: sqlite3.Connection, reports: dict[ReportType, MoneyReport]) -> None:
    """Insert transactions into transaction table.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        reports (dict[ReportType, MoneyReport]): Reports by type.
    """
    report = reports.get(ReportType.INV_ACCOUNT_TRANSACTIONS)

    if report and not report.empty_report:
        assert report.df_report is not None
        with conn:
            # INSERT INTO TxnsInv ...
            report.df_report.to_sql(
                TABLE_TXNS_INV,
                conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=DB_TXNSINV_CHUNKSIZE,
            )


def _db_update_txns_xfers(conn: sqlite3.Connection) -> None:
    """Set XferAccount field for account transfer transactions.

    Transfers are identified by the Category being one of the transfer categories.

    Args:
        conn (sqlite3.Connection): SQLite connection.
    """
    q_update = Update(TABLE_TXNS)
    q_update.set_raw_value("XferAccount", "`Subcategory`")
    q_update.where_value("Category", TRANSFER_CATEGORIES, "IN")

    q_update2 = Update(TABLE_TXNS)
    q_update2.set_value("Subcategory", None)
    q_update2.where_value("XferAccount", None, "<>")

    with conn:
        db_execute_stmt(conn, q_update)
        db_execute_stmt(conn, q_update2)


def _db_insert_loans(conn: sqlite3.Connection, reports: dict[ReportType, MoneyReport]) -> None:
    """Initialize Loans table with loan accounts and details.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        reports (dict[ReportType, MoneyReport]): Reports by type.
    """
    report = reports.get(ReportType.LOAN_TERMS)

    if report and not report.empty_report:
        assert report.df_report is not None

        # Only include columns that are in the table schema
        table_columns = table_schema_columns(TABLE_LOANS)

        with conn:
            # INSERT INTO Loans ...
            report.df_report[table_columns].to_sql(
                TABLE_LOANS,
                conn,
                if_exists="append",
                index=False,
                method="multi",
            )


def _db_insert_accounts(conn: sqlite3.Connection, reports: dict[ReportType, MoneyReport]) -> None:  # noqa: C901, PLR0915
    """Initialize Accounts table with unique accounts from Txns.

    Must be done *after*:
    - _db_insert_txns
    - _db_insert_txns_inv
    - _db_insert_loans

    Args:
        conn (sqlite3.Connection): SQLite connection.
        reports (dict[ReportType, MoneyReport]): Reports by type.
    """
    q_accountnames = Select(TABLE_ACCOUNTS)
    q_accountnames.columns("Account")

    def _insert_txn_accounts() -> None:
        # Accounts directly referenced in Txns

        insert_cols = (  # must match SELECT order!
            "Account",
            "TxnDateMin",
            "TxnDateMax",
        )

        q_select = Select(TABLE_TXNS)
        q_select.columns("Account")
        q_select.column_expr("MIN(Date)", named="TxnDateMin")
        q_select.column_expr("MAX(Date)", named="TxnDateMax")

        q_select.where_value("Account", None, "<>")
        q_select.where_select("Account", q_accountnames, "NOT IN")

        q_select.group_by("Account")
        q_select.order_by("Account")

        q_insert = Insert(TABLE_ACCOUNTS, select_allow_placeholders=True)
        q_insert.columns(insert_cols)
        q_insert.select(q_select)

        db_execute_stmt(conn, q_insert)

    def _insert_txninv_accounts() -> None:
        # Accounts directly referenced in TxnsInv

        insert_cols = (  # must match SELECT order!
            "Account",
            "AccountClassification",
            "AccountCategory",
            "TxnDateMin",
            "TxnDateMax",
        )

        q_select = Select(TABLE_TXNS_INV)
        q_select.columns("Account")
        q_select.column_expr(AccountClassification.ASSET.value, named="AccountClassification", quote=True)
        q_select.column_expr(AccountCategory.INVESTMENT.value, named="AccountCategory", quote=True)
        q_select.column_expr("MIN(Date)", named="TxnDateMin")
        q_select.column_expr("MAX(Date)", named="TxnDateMax")

        q_select.where_select("Account", q_accountnames, "NOT IN")

        q_select.group_by("Account")
        q_select.order_by("Account")

        q_insert = Insert(TABLE_ACCOUNTS, select_allow_placeholders=True)
        q_insert.columns(insert_cols)
        q_insert.select(q_select)

        db_execute_stmt(conn, q_insert)

    def _insert_xfer_accounts() -> None:
        # Transactions for investment accounts and loans are not included in the "Account transactions report"
        # but they are referenced as transfers so we can include them in the Accounts table.
        # (There is no report that includes loan transactions!)

        q_accounts_notxns = Select(TABLE_ACCOUNTS)
        q_accounts_notxns.columns("Account")
        q_accounts_notxns.where_value("TxnDateMin", None)

        q_select = Select(TABLE_TXNS, where_predicate="AND")
        q_select.columns("XferAccount")
        q_select.column_expr("MIN(Date)", named="TxnDateMin")
        q_select.column_expr("MAX(Date)", named="TxnDateMax")

        # Look for accounts referenced in any transaction transfer category that were not
        # referenced in any reports or were referenced in a report other than
        # an 'Account transactions' report.
        q_select.where_value("XferAccount", None, "<>")
        q_select.where_or()
        q_select.where_select("XferAccount", q_accountnames, "NOT IN")
        q_select.where_select("XferAccount", q_accounts_notxns, "IN")

        q_select.group_by("XferAccount")
        q_select.order_by("XferAccount")

        for row in pd_read_sql(
            conn,
            q_select,
            date_cols=[
                "TxnDateMin",
                "TxnDateMax",
            ],
        ).itertuples(index=False):
            assert isinstance(row.XferAccount, str)
            assert isinstance(row.TxnDateMin, date)
            assert isinstance(row.TxnDateMax, date)

            q_upsert = Upsert(TABLE_ACCOUNTS, select_allow_placeholders=True)
            q_upsert.on_conflict("Account")
            q_upsert.set_value("Account", row.XferAccount)
            q_upsert.set_value("XferOnly", 1)
            q_upsert.set_value("TxnDateMin", row.TxnDateMin)
            q_upsert.set_value("TxnDateMax", row.TxnDateMax)

            db_execute_stmt(conn, q_upsert)

    def _insert_loan_accounts() -> None:
        # Accounts referenced in loan terms report

        insert_cols = (  # must match SELECT order!
            "Account",
            "AccountClassification",
            "AccountCategory",
        )

        q_select = Select(TABLE_LOANS)
        q_select.columns("Loan")  # AS "Account"
        q_select.column_expr(AccountClassification.LIABILITY.value, named="AccountClassification", quote=True)
        q_select.column_expr(AccountCategory.LOAN.value, named="AccountCategory", quote=True)
        q_select.where_select("Loan", q_accountnames, "NOT IN")
        q_select.order_by("Loan")

        q_insert = Insert(TABLE_ACCOUNTS, select_allow_placeholders=True)
        q_insert.columns(insert_cols)
        q_insert.select(q_select)

        db_execute_stmt(conn, q_insert)

    def _insert_report_account_balances() -> None:
        # Insert accounts included in the account balances report.

        report = reports.get(ReportType.ACCOUNT_BALANCES)
        if not report:
            return

        assert report.df_report is not None

        for row in report.df_report.itertuples(index=False):
            assert isinstance(row.Account, str)

            q_upsert = Upsert(TABLE_ACCOUNTS)
            q_upsert.set_value("Account", row.Account)
            q_upsert.on_conflict("Account").do_nothing()  # ignore if already exists

            db_execute_stmt(conn, q_upsert)

    def _insert_report_balances_details() -> None:
        # Insert accounts included in the account balances with details report.

        report = reports.get(ReportType.ACCOUNT_BALANCES_DETAILS)
        if not report:
            return

        assert report.df_report is not None

        for row in report.df_report.itertuples(index=False):
            assert isinstance(row.Account, str)

            q_upsert = Upsert(TABLE_ACCOUNTS)
            q_upsert.set_value("Account", row.Account)
            q_upsert.on_conflict("Account").do_nothing()  # ignore if already exists

            db_execute_stmt(conn, q_upsert)

    for fn_insert in (
        _insert_txninv_accounts,  # before _insert_txn_accounts because it sets classification
        _insert_loan_accounts,  # before _insert_txn_accounts because it sets classification
        _insert_txn_accounts,
        _insert_report_account_balances,
        _insert_report_balances_details,
        _insert_xfer_accounts,  # after _insert_txn_accounts so XferOnly is set correctly
    ):
        with conn:
            fn_insert()

    # Accounts are inserted based on what reports they are referenced in.
    # For asthetics, I'd like the table sorted as if they were all inserted at once :)
    _db_resort_table(conn, TABLE_ACCOUNTS, "Account")


def _db_update_accounts_reports(conn: sqlite3.Connection, reports: dict[ReportType, MoneyReport]) -> None:
    """Update accounts with metadata from reports.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        reports (dict[ReportType, MoneyReport]): Reports by type.
    """

    def _update_account_types() -> None:
        # Update account properties included in the account balances report.
        # Accounts that are not in Txns will be inserted.

        report = reports.get(ReportType.ACCOUNT_BALANCES)
        if not report:
            return

        assert isinstance(report, MoneyReportAccountBalances)

        for account_type_subtype, accounts in report.accounts_by_type.items():
            account_type, account_subtype = account_type_subtype

            for account_name in accounts:
                q_update = Update(TABLE_ACCOUNTS)
                q_update.set_value("AccountClassification", account_type)  # Asset, Liability
                q_update.set_value("AccountCategory", account_subtype)  # Bank, Cash, Investment, Credit Card, Loan, etc.
                q_update.where_value("Account", account_name)

                db_execute_stmt(conn, q_update)

    def _update_account_details() -> None:
        # Update account properties included in the account balances with details report.

        report = reports.get(ReportType.ACCOUNT_BALANCES_DETAILS)
        if not report:
            return

        assert isinstance(report, MoneyReportAccountBalancesWithDetails)

        df_nonan = report.df_report_nonan

        # itertuples() does not work for us because column names contain spaces.
        for _, row in df_nonan.iterrows():
            q_update = Update(TABLE_ACCOUNTS)
            q_update.set_value("OpeningBalance", currency_to_value(row["Opening Balance"]))
            q_update.set_value("Abbreviation", row["Abbreviation"] if "Abbreviation" in df_nonan.columns else None)
            q_update.set_value("BankName", row["Bank Name"] if "Bank Name" in df_nonan.columns else None)
            q_update.set_value("AccountNumber", row["AccountNumber"] if "AccountNumber" in df_nonan.columns else None)
            q_update.set_value("AccountLimit", currency_to_value(row["Limit"]) if "Limit" in df_nonan.columns else None)

            db_execute_stmt(conn, q_update)

    for fn_update in (
        _update_account_types,
        _update_account_details,
    ):
        with conn:
            fn_update()


def _db_insert_account_balances(conn: sqlite3.Connection, reports: dict[ReportType, MoneyReport]) -> None:
    """Update account balances included in the account balances report.

    Must be done *after*:
        - _db_update_accounts_config

    Args:
        conn (sqlite3.Connection): SQLite connection.
        reports (dict[ReportType, MoneyReport]): Reports by type.
    """
    accounts_warned = set()

    def _warn_account(account_name: str, reason: str) -> bool:
        name_reason = f"{account_name}:{reason}"
        if name_reason in accounts_warned:
            return False
        accounts_warned.add(name_reason)
        return True

    report = reports.get(ReportType.ACCOUNT_BALANCES)
    if not report:
        return

    assert isinstance(report, MoneyReportAccountBalances)

    select_dates = ["OpenedDate", "TxnDateMin"]

    q_select = Select(TABLE_ACCOUNTS).columns(("Account", *select_dates))

    df_accounts = pd_read_sql(conn, q_select, date_cols=select_dates)

    df_accounts = df_accounts.dropna(subset=select_dates, how="all")  # OpenedDate=NULL AND TxnDateMin=NULL

    # min() cannot compare `date` with `NaT` (NULL) but can with `datetime` :/
    df_accounts["OpenedDate"] = pd.to_datetime(df_accounts["OpenedDate"])
    df_accounts["TxnDateMin"] = pd.to_datetime(df_accounts["TxnDateMin"])

    df_accounts["TxnDateMin"] = df_accounts[select_dates].min(axis=1).dt.date  # date(min(OpenedDate, TxnDateMin))

    firsttxn_dates = df_accounts.set_index("Account")["TxnDateMin"].to_dict()

    # There is a column for each year in the report.
    for col_name, col_date in report.asof_total_columns_map(report).items():
        df_annual = report.df_report_nonan[["Account", col_name]].dropna(subset=[col_name])

        for row in df_annual.itertuples(index=False):
            # Account balance as of the report date.
            # We have to index by number because the date column name
            # contains "/" which is not a valid identifier.
            account_name = row[0]
            balance = row[-1]

            assert isinstance(balance, CurrencyDecimal)

            firsttxn_date = firsttxn_dates.get(account_name)

            # My balance reports include accounts that were open and have no transactions
            # prior to the report as-of date. I don't know if this is a bug or corruption in my data.
            # At minimum, we can ignore accounts that have no transactions prior to the report date.
            if not firsttxn_date:
                if _warn_account(account_name, "notxnbalance"):
                    logger.warning(
                        f"'{account_name}': Account balance report shows a {balance} balance as of {col_date} but account has no transactions."
                    )
            elif col_date < firsttxn_date:
                continue

            q_insert = Insert(TABLE_ACCOUNT_BALANCES)
            q_insert.set_value("Account", account_name)
            q_insert.set_value("Date", col_date)
            q_insert.set_value("Balance", currency_to_value(balance))

            db_execute_stmt(conn, q_insert)


def _db_update_accounts_classification(conn: sqlite3.Connection, config: MainConfigFileT) -> None:  # noqa: C901
    """Set classification and category for accounts based on configuration file.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        config (dict): Configuration file settings.
    """

    def _get_category_classification_map() -> dict[str, str]:
        # Return dict[category] = classification of classifications and categories
        # assigned to accounts merged with `account_categories` from configuration file.

        # Account classifications and categories based on reports
        categories_by_classification = _list_account_classifications(conn)

        user_classifications: ConfigAccountCategoriesT | None = config.get("account_categories")

        if user_classifications:
            if not isinstance(user_classifications, dict):
                raise MnyXlsConfigError("Directive must be a set of key/value pairs.", config, "account_categories")

            # Merge with classifications from configuration file.
            for classification, categories in user_classifications.items():
                if classification not in categories_by_classification:
                    categories_by_classification[classification] = categories
                else:
                    categories_by_classification[classification].extend(categories)

            # Remove duplicate categories
            for classification, categories in categories_by_classification.items():
                categories_by_classification[classification] = sorted(set(categories))

        return {category: classification for classification, categories in categories_by_classification.items() for category in categories}

    def _set_accounts_classification(classification: str, category: str, accounts: list[str]) -> None:
        # Set the classification and category for each account in the list.

        with conn:
            for account in accounts:
                q_update = Update(TABLE_ACCOUNTS)
                q_update.set_value("AccountClassification", classification)
                q_update.set_value("AccountCategory", category)
                q_update.where_value("Account", account)

                db_execute_stmt(conn, q_update)

    config_accounts: ConfigAccountsT | None = config.get("accounts", {})
    category_classification = _get_category_classification_map()
    classifications = set(category_classification.values())
    category_accounts: dict[str, list[str]] = defaultdict(list)

    if not isinstance(config_accounts, dict):
        raise MnyXlsConfigError("Directive must be a set of key/value pairs.", config, "accounts")

    for name, account_details in config_accounts.items():
        if not isinstance(account_details, dict):
            raise MnyXlsConfigError("Directive must be a set of key/value pairs.", config, ("accounts", name))

        user_category = account_details.get("category")
        if user_category:
            category_accounts[user_category].append(name)

    for user_category, accounts in category_accounts.items():
        if ":" in user_category:
            classification, category = user_category.split(":", 1)
            classification = classification.rstrip()
            category = category.lstrip()
        else:
            category = user_category
            classification = category_classification.get(category)

        if not classification:
            config_warning(f"Account category '{user_category}' is not associated with a classification.", config, "accounts")
        elif classification not in classifications:
            config_warning(f"Account category '{user_category}' does not reference a known classification.", config, "accounts")
        else:
            _set_accounts_classification(classification, category, accounts)


def _db_update_accounts_config(conn: sqlite3.Connection, config: MainConfigFileT) -> None:
    def _update_opened_dates(account_name: str, account_details: ConfigAccountT) -> None:
        # Update account opened dates from configuration file.

        assert account_details is not None
        if not isinstance(account_details, dict):
            raise MnyXlsConfigError("Directive must be a set of key/value pairs.", config, ("accounts", account_name))

        opened_date = account_details.get("opened_date")
        closed_date = account_details.get("closed_date")

        if opened_date:
            try:
                opened_date = pd.to_datetime(opened_date).date()
            except DateParseError as err:
                config_warning(f"{err}", config, ("accounts", account_name, "opened_date"))

        if closed_date:
            try:
                closed_date = pd.to_datetime(closed_date).date()
            except DateParseError as err:
                config_warning(f"{err}", config, ("accounts", account_name, "closed_date"))

        if opened_date or closed_date:
            q_update = Update(TABLE_ACCOUNTS)
            q_update.set_value("OpenedDate", opened_date)
            q_update.set_value("ClosedDate", closed_date)
            q_update.where_value("Account", account_name)

            db_execute_stmt(conn, q_update)

    config_accounts: ConfigAccountsT | None = config.get("accounts")

    if config_accounts:
        with conn:
            for account_name, account_details in config_accounts.items():
                _update_opened_dates(account_name, account_details)


def _db_update_accounts_defaults(conn: sqlite3.Connection) -> None:
    """Update accounts with defaults.

    Args:
        conn (sqlite3.Connection): SQLite connection.
    """


def _db_insert_payees(conn: sqlite3.Connection) -> None:
    """Initialize Payees table with unique payees from Txns.

    Must be done *after* transactions are inserted.

    Args:
        conn (sqlite3.Connection): SQLite connection.
    """
    cols = ["Payee"]

    q_select = Select(TABLE_TXNS)
    q_select.columns(cols)
    q_select.column_expr("MIN(Date)", named="TxnDateMin")
    q_select.column_expr("MAX(Date)", named="TxnDateMax")
    q_select.where_value("Payee", None, "<>")
    q_select.group_by("Payee")
    q_select.order_by("Payee")

    q_insert = Insert(TABLE_PAYEES)
    q_insert.columns([*cols, "TxnDateMin", "TxnDateMax"])
    q_insert.select(q_select)

    with conn:
        db_execute_stmt(conn, q_insert)


def _db_insert_categories(conn: sqlite3.Connection) -> None:
    """Initialize Categories table with unique categories from Txns.

    Must be done *after* transactions are inserted.
    Transfer categories are not included.

    Args:
        conn (sqlite3.Connection): SQLite connection.
    """
    cols = ["Category", "Subcategory"]

    q_select = Select(TABLE_TXNS)
    q_select.columns(cols)
    q_select.column_expr("MIN(Date)", named="TxnDateMin")
    q_select.column_expr("MAX(Date)", named="TxnDateMax")
    q_select.where_value("Category", TRANSFER_CATEGORIES, "NOT IN")
    q_select.group_by(["Category", "Subcategory"])
    q_select.order_by(["Category", "Subcategory"])

    q_insert = Insert(TABLE_CATEGORIES, select_allow_placeholders=True)
    q_insert.columns([*cols, "TxnDateMin", "TxnDateMax"])
    q_insert.select(q_select)

    with conn:
        db_execute_stmt(conn, q_insert)


def _db_update_categories_reports(conn: sqlite3.Connection, reports: dict[ReportType, MoneyReport]) -> None:
    """Update category properties included in the income and spending and monthly income reports.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        reports (dict[ReportType, MoneyReport]): Reports by type.
    """

    def _update_category_types(conn: sqlite3.Connection, report: MoneyReport) -> None:
        # These reports group categories by "type"

        assert isinstance(report, MoneyReportIncomeAndSpending)

        for txn_type, categories in report.categories_by_type().items():
            q_update = Update(TABLE_CATEGORIES)
            q_update.set_value("TxnType", txn_type)  # Income, Expense
            q_update.where_value("Category", categories, "IN")

            db_execute_stmt(conn, q_update)

    for report_type in (ReportType.INCOME_SPENDING, ReportType.MONTHLY_INCOME_EXPENSES):
        report = reports.get(report_type)
        if report:
            with conn:
                _update_category_types(conn, report)


def _db_update_categories_config(conn: sqlite3.Connection, config: MainConfigFileT) -> None:  # noqa: C901
    """Update categories with metadata from user configuration file.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        config (dict): Configuration file settings.
    """

    def _set_category_field(table_field: str, field_categories: dict[str, list[str]] | None) -> None:
        field_set_categories: defaultdict[str, list[str]] = defaultdict(list)
        field_set_subcategories: defaultdict[str, list[tuple[str, str]]] = defaultdict(list)

        if field_categories is not None:
            for txn_type, category_list in field_categories.items():
                for category_pair in category_list:
                    category, subcategory = split_category_pair(category_pair)
                    if subcategory:
                        # Applies to specific subcategory of given category
                        field_set_subcategories[txn_type].append((category, subcategory))
                    else:
                        # Applies to all subcategories of given category
                        field_set_categories[txn_type].append(category)

        with conn:
            for txn_type, categories in field_set_categories.items():
                q_update = Update(TABLE_CATEGORIES)
                q_update.set_value(table_field, txn_type)
                q_update.where_value("Category", categories, "IN")

                db_execute_stmt(conn, q_update)

        with conn:
            for txn_type, category_pairs in field_set_subcategories.items():
                for category, subcategory in category_pairs:
                    q_update = Update(TABLE_CATEGORIES)
                    q_update.set_value(table_field, txn_type)
                    q_update.where_value("Category", category)
                    q_update.where_value("Subcategory", subcategory)

                    db_execute_stmt(conn, q_update)

    category_types: dict[str, list[str]] | None = config.get("category_types")
    category_classes: dict[str, list[str]] | None = config.get("category_classes")

    if category_types and not isinstance(category_types, dict):
        raise MnyXlsConfigError("Directive must be a set of key/value pairs.", config, "category_types")
    if category_classes and not isinstance(category_classes, dict):
        raise MnyXlsConfigError("Directive must be a set of key/value pairs.", config, "category_classes")

    _set_category_field("TxnType", category_types)
    _set_category_field("TxnClass", category_classes)


def _db_update_categories_defaults(conn: sqlite3.Connection, config: MainConfigFileT) -> None:
    """Update categories with defaults from user configuration file.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        config (dict): Configuration file settings.
    """

    def _set_default_txntype() -> None:
        # Categories without a TxnType are considered expenses (by default)
        q_update = Update(TABLE_CATEGORIES)
        q_update.set_value("TxnType", config.get("category_type_default", TxnType.EXPENSE.value))
        q_update.where_value("TxnType", None)

        with conn:
            db_execute_stmt(conn, q_update)

    def _set_default_txnclass() -> None:
        # TxnClass for non-expense categories defaults to its TxnType
        q_update = Update(TABLE_CATEGORIES)
        q_update.set_raw_value("TxnClass", "`TxnType`")
        q_update.where_value("TxnType", TxnType.EXPENSE.value, "<>")
        q_update.where_value("TxnClass", None)

        with conn:
            db_execute_stmt(conn, q_update)

        # Categories without a TxnClass are considered discretionary (by default)
        q_update = Update(TABLE_CATEGORIES)
        q_update.set_value("TxnClass", config.get("category_class_default", TxnClass.DISCRETIONARY.value))
        q_update.where_value("TxnClass", None)

        with conn:
            db_execute_stmt(conn, q_update)

    _set_default_txntype()
    _set_default_txnclass()


def _db_insert_category_balances(conn: sqlite3.Connection, reports: dict[ReportType, MoneyReport]) -> None:
    """Insert category balances from 'Income and spending' and 'Monthly income and expenses' reports.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        reports (dict[ReportType, MoneyReport]): Reports by type.
    """

    def _insert_report(report: MoneyReport) -> None:
        # MoneyReportMonthlyIncomeAndExpenses is derived from MoneyReportIncomeAndSpending
        assert isinstance(report, MoneyReportIncomeAndSpending)

        # There is a column for each year in the report.
        for col_name, col_date in report.asof_total_columns_map(report).items():
            df_annual = report.df_report_nonan[["Category", "TxnType", col_name]].dropna(subset=[col_name])

            for row in df_annual.itertuples(index=False):
                category = row.Category
                txn_type = row.TxnType

                # Category balance as of the report date.
                # Index by number because the date column name contains "/" which is not a valid identifier.
                balance = row[-1]

                assert isinstance(category, str)
                assert isinstance(txn_type, str)
                assert isinstance(balance, CurrencyDecimal)

                if txn_type == TxnType.TRANSFER.value:
                    continue  # Transfers refer to accounts not categories

                # Both reports include the same information so it's ok if there are dup
                q_upsert = Upsert(TABLE_CATEGORY_BALANCES)
                q_upsert.set_value("Category", category)
                q_upsert.set_value("Date", col_date)
                q_upsert.set_value("Balance", currency_to_value(balance))
                q_upsert.on_conflict(("Category", "Date")).do_nothing()

                try:
                    db_execute_stmt(conn, q_upsert)
                except sqlite3.IntegrityError as err:
                    logger.debug(f"'{category}': {err}")
                    raise

    for report_type in (ReportType.INCOME_SPENDING, ReportType.MONTHLY_INCOME_EXPENSES):
        report = reports.get(report_type)
        if report:
            with conn:
                _insert_report(report)


def _db_insert_eras_config(conn: sqlite3.Connection, config: MainConfigFileT) -> None:  # noqa: C901, PLR0915
    """Insert Eras from user configuration file.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        config (dict): Configuration file settings.
    """

    def _cmp_era(a: tuple[str, ConfigEraT]) -> tuple[date, date]:
        date_from = a[1].get("date_from")
        date_to = a[1].get("date_to")

        assert isinstance(date_from, (date, type(None)))
        assert isinstance(date_to, (date, type(None)))  # date or None

        # Ideally, eras are sorted by their start/end time ranges.
        return date_from or date.min, date_to or date.max

    def _insert_era(era_name: str, era_config: ConfigEraT) -> None:
        date_from = era_config.get("date_from")
        date_to = era_config.get("date_to")

        q_insert = Insert(TABLE_ERAS)
        q_insert.set_value("EraName", era_name)

        if date_from:
            q_insert.set_value("EraDateFrom", date_from)
        if date_to:
            q_insert.set_value("EraDateTo", date_to)

        with conn:
            db_execute_stmt(conn, q_insert)

    def _validate_and_sort_eras(config_eras: ConfigErasT) -> ConfigErasT:
        # Configure eras.
        # Eras were roughly validated in `_validate_main_config`.

        valid_eras: ConfigErasT = {}
        prev_date_to: date | None = None

        for era_name, era_config in config_eras.items():
            config_keys = ("eras", era_name)
            date_from_spec = era_config.get("date_from")
            date_to_spec = era_config.get("date_to")
            date_from = None
            date_to = None

            if date_from_spec is not None:
                if isinstance(date_from_spec, str):
                    date_from = parse_yyyymmdd_flex(date_from_spec, first_day=True)
            elif prev_date_to is not None:
                # Start after the previous era's end date
                date_from = prev_date_to + timedelta(days=1)

            if isinstance(date_to_spec, str) and date_to_spec != "...":
                date_to = parse_yyyymmdd_flex(date_to_spec, first_day=False)

            if not (date_from or date_to):
                raise MnyXlsConfigError("'date_from' or 'date_to' is required.", config, config_keys)

            if date_from and date_to and date_to < date_from:
                date_from, date_to = date_to, date_from

            insert_era_config: ConfigEraT = {}

            if date_from is not None:
                insert_era_config["date_from"] = date_from
            if date_to is not None:
                insert_era_config["date_to"] = date_to
                prev_date_to = date_to

            valid_eras[era_name] = insert_era_config

        return dict(sorted(valid_eras.items(), key=_cmp_era))

    def _validate_era_ranges(valid_eras: ConfigErasT) -> None:
        # Ensure date ranges do not overlap

        era_names = tuple(valid_eras.keys())

        for idx in range(len(era_names) - 1):
            era_a_key = era_names[idx]
            era_b_key = era_names[idx + 1]

            era_a = valid_eras[era_a_key]
            era_b = valid_eras[era_b_key]

            # `date_to` of each era must be less than `date_from` of next era
            date_a = era_a.get("date_to") or date.max
            date_b = era_b.get("date_from") or date.min

            assert isinstance(date_a, date)
            assert isinstance(date_b, date)

            if date_b < date_a:
                raise MnyXlsConfigError(
                    f"Eras '{era_a_key}' and '{era_b_key}' have overlapping date ranges.",
                    config,
                    ("eras",),
                )

    config_eras: ConfigErasT | None = config.get("eras")
    if not config_eras:
        return

    insert_eras = _validate_and_sort_eras(config_eras)

    _validate_era_ranges(insert_eras)

    for era_name, era_config in insert_eras.items():
        _insert_era(era_name, era_config)


def _db_update_txns_eras(conn: sqlite3.Connection) -> None:
    """Update Txns to set Eras.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        config (dict): Configuration file settings.
    """

    def _apply_era(era_name: str, date_from: date | None, date_to: date | None) -> None:
        # Update Txns with the given era

        q_update = Update(TABLE_TXNS)
        q_update.set_value("EraName", era_name)

        if date_from and date_to:
            q_update.where_raw_value("Date", "? AND ?", "BETWEEN", value_params=(date_from, date_to))
        elif date_from is not None:
            q_update.where_value("Date", date_from, ">=")
        elif date_to is not None:
            q_update.where_value("Date", date_to, "<=")

        db_execute_stmt(conn, q_update)

    def _warn_era_unassigned() -> None:
        q_select = Select(TABLE_TXNS)
        q_select.where_value("EraName", None)

        unassigned_count = _conn_execute_count_rows(conn, q_select)

        if unassigned_count > 0:
            logger.warning(f"{unassigned_count} transactions do not have an era assigned.")

    df_eras = pd_read_sql(
        conn,
        Select(TABLE_ERAS),
        date_cols=("EraDateFrom", "EraDateTo"),
    )

    if not df_eras.empty:
        for row in df_eras.itertuples(index=False):
            with conn:
                assert isinstance(row.EraName, str)
                assert isinstance(row.EraDateFrom, date)  # NULL will be 'NaT' which is a 'date'
                assert isinstance(row.EraDateTo, date)  # NULL will be 'NaT' which is a 'date'

                date_from = row.EraDateFrom if pd.notna(row.EraDateFrom) else None
                date_to = row.EraDateTo if pd.notna(row.EraDateTo) else None

                assert date_from or date_to

                _apply_era(row.EraName, date_from, date_to)

        _warn_era_unassigned()


def _recommend_reports(conn: sqlite3.Connection) -> None:
    """Recommend reports user can run to create a more complete snapshot.

    Args:
        conn (sqlite3.Connection): SQLite connection.
    """

    def _check_account_classifications() -> None:
        q_select = Select(TABLE_ACCOUNTS)
        q_select.columns("Account")
        q_select.columns("TxnDateMin")
        q_select.columns("TxnDateMax")
        q_select.where_value("AccountClassification", None)
        q_select.group_by("Account")
        q_select.order_by("Account")

        df_accounts = pd_read_sql(
            conn,
            q_select,
            date_cols=["TxnDateMin", "TxnDateMax"],
        )

        if not df_accounts.empty:
            logger.info("The following accounts are not assigned a classification.")
            logger.info("Classifications can be assigned by importing an 'Account balances' report that")
            logger.info("includes the account transaction period or assigned in the configuration file.")

            for row in df_accounts.itertuples(index=False):
                if pd.notna(row.TxnDateMin) and pd.isna(row.TxnDateMax):
                    assert isinstance(row.TxnDateMin, date)
                    assert isinstance(row.TxnDateMax, date)
                    txn_range = f"{row.TxnDateMin} to {row.TxnDateMax}"
                else:
                    txn_range = "No transactions"

                logger.info(f"- '{row.Account}' ({txn_range})")

    _check_account_classifications()


def _db_create_views(conn: sqlite3.Connection) -> None:
    """Create database views.

    Args:
        conn (sqlite3.Connection): SQLite connection.
    """
    with conn:
        for view_name, sql in VIEW_SCHEMAS.items():
            _db_drop_view(conn, view_name)

            if isinstance(sql, Stmt):
                db_execute_stmt(conn, sql)
            else:
                conn.execute(sql, [])


def _db_verify_views(conn: sqlite3.Connection) -> None:
    """Sanity check that views are correct."""
    q_select = Select(TABLE_TXNS)
    txns_count = _conn_execute_count_rows(conn, q_select)

    q_select = Select(VIEW_TXNS_WITHTYPEANDCLASS)
    view_count = _conn_execute_count_rows(conn, q_select)

    assert txns_count == view_count


def _db_check_tables(conn: sqlite3.Connection) -> None:
    """Check that all tables exist.

    Args:
        conn (sqlite3.Connection): SQLite connection.
    """
    q_select = Select("sqlite_master").columns("name")
    q_select.where_value("type", "table")

    df_tables = pd_read_sql(conn, q_select)
    tables = set(df_tables["name"].tolist())

    if not tables:
        raise MnyXlsRuntimeError("Database does not have any tables; run again without '--no-reports' to initialize.")

    for table in TABLE_SCHEMAS:
        if table not in tables:
            raise MnyXlsRuntimeError(f"Database is missing required table '{table}'; run again without '--no-reports' to rebuild.")


######################################################################
# Module public


def db_create(
    conn: sqlite3.Connection,
    reports: dict[ReportType, MoneyReport] | None,
    config: MainConfigFileT,
) -> None:
    """Create database using data from Money reports.

    Args:
        conn (sqlite3.Connection): Database connection.
        reports (dict[ReportType, MoneyReport]): Money report for each report type.
            Can be None to use previously imported report data.
        config (ConfigFileT): Configuration options.
    """
    debug_sql = config.get("debug_sql", False)

    if debug_sql:
        conn.set_trace_callback(_db_trace_log_sql)

    # Let SQLite know how to convert CurrencyDecimal objects to string
    sqlite3.register_adapter(CurrencyDecimal, currency_to_value)

    if reports:
        _db_enable_foreign_keys(conn, enable=False)  # OFF by default, but just to be explicit...

        _db_create_tables(conn)
        _db_create_views(conn)

        _db_insert_txns(conn, reports)
        _db_insert_txns_inv(conn, reports)

        _db_create_indexes(conn)  # after _db_insert_txns for better INSERT performance

        _db_update_txns_xfers(conn)  # after _db_create_indexes for performance

        rewrites_apply_txns(conn, config)  # after _db_create_indexes and before _db_insert_payees, etc.

        _db_insert_loans(conn, reports)
        _db_insert_accounts(conn, reports)  # after _db_insert_loans
        _db_insert_payees(conn)
        _db_insert_categories(conn)

        _db_update_accounts_reports(conn, reports)
        _db_update_accounts_config(conn, config)
        _db_update_accounts_classification(conn, config)
        _db_update_accounts_defaults(conn)

        _db_update_categories_reports(conn, reports)
        _db_update_categories_config(conn, config)
        _db_update_categories_defaults(conn, config)

        _db_insert_eras_config(conn, config)
        _db_update_txns_eras(conn)

        _db_insert_account_balances(conn, reports)  # after _db_update_accounts_config
        _db_insert_category_balances(conn, reports)
    else:
        _db_check_tables(conn)

    _db_enable_foreign_keys(conn, enable=True)  # after primary tables are populated

    _db_verify_views(conn)

    # Err on the side of caution and do a quick integrity check
    conn.execute("PRAGMA quick_check")

    if config.get("opt_recommend_reports", True):
        _recommend_reports(conn)


def db_list_distinct(conn: sqlite3.Connection, table: str, column: str) -> list[str]:
    """List distinct column values in the given table.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        table (str): Table name.
        column (str): Column name.

    Returns:
        list[str]
    """
    q_select = Select(table).distinct().column(column).order_by(column)

    df_select = pd_read_sql(conn, q_select)

    return df_select[column].tolist() if not df_select.empty else []


def db_list_accounts(conn: sqlite3.Connection) -> list[str]:
    """List names in the Accounts table.

    Args:
        conn (sqlite3.Connection): SQLite connection.

    Returns:
        list[str]
    """
    return db_list_distinct(conn, TABLE_ACCOUNTS, "Account")


def db_list_eras(conn: sqlite3.Connection) -> list[str]:
    """List Eras ordered by date range.

    Args:
        conn (sqlite3.Connection): SQLite connection.

    Returns:
        list[str]
    """
    q_select = Select(TABLE_ERAS)
    q_select.column("EraName")
    q_select.order_by("rowid")

    df_select = pd_read_sql(conn, q_select)

    return df_select["EraName"].tolist() if not df_select.empty else []


def db_list_txn_yyyy(conn: sqlite3.Connection) -> list[int]:
    """List Date years in the Txns table.

    Args:
        conn (sqlite3.Connection): SQLite connection.

    Returns:
        list[int]
    """
    q_select = Select(TABLE_TXNS)
    q_select.column_expr("CAST(strftime('%Y', `Date`) AS INTEGER)", named="yyyy")
    q_select.group_by("yyyy")
    q_select.order_by("yyyy")

    df_select = pd_read_sql(conn, q_select)

    return df_select["yyyy"].tolist() if not df_select.empty else []

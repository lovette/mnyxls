from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

from mysqlstmt import Select

from .dbschema import TABLE_ACCOUNTS, TABLE_ERAS
from .dbsqlite import db_get_txndates
from .dbviews import VIEW_TXNS_WITHTYPEANDCLASS
from .mysqlstmt_selectand import SelectAnd  # type: ignore[reportMissingImport]
from .shared import (
    config_select_allow,
    config_select_remove,
    get_date_relative_to,
    get_select_values,
    get_select_values_and_cond,
    parse_yyyymmdd_flex,
    pd_read_sql,
    pluralize,
    split_category_pair,
)
from .worksheet import MoneyWorksheet

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Container
    from datetime import date

    from .configtypes import WorkbookConfigSelectT, WorksheetConfigSelectT, WorksheetConfigT
    from .workbook import MoneyWorkbook


######################################################################
# Transactions worksheet


class MoneyWorksheetTxnsBase(MoneyWorksheet):
    """Base class common to "txns:*" worksheets."""

    ######################################################################
    # Class methods for expanding `foreach` directives

    @classmethod
    def expand_foreach_txntype(
        cls,
        workbook: MoneyWorkbook,
        conn: sqlite3.Connection,
        worksheet_key: str,
        worksheet_base_config: WorksheetConfigT,
    ) -> int:
        """Expand `foreach` directive and add sheets to workbook.

        foreach: "txntype"

        Args:
            workbook (MoneyWorkbook): Workbook.
            conn (sqlite3.Connection): SQLite connection.
            worksheet_key (str): Worksheet configuration key (typically the sheet name.)
            worksheet_base_config (WorksheetConfigT): Worksheet configuration.

        Returns:
            int: Number of worksheets added. If 0, base worksheet will be added.
        """
        worksheet_count = 0

        def _list_txntypes() -> list[str]:
            q_select = SelectAnd(VIEW_TXNS_WITHTYPEANDCLASS).distinct().column("TxnType").order_by("TxnType")
            cls.apply_txns_select_where(conn, q_select, worksheet_base_config, allow_directives=("txntype",))
            df_select = pd_read_sql(conn, q_select)
            return df_select["TxnType"].tolist() if not df_select.empty else []

        for txn_type_value in _list_txntypes():
            txntypeplural = pluralize(txn_type_value)

            sheet_name = cls.render_sheet_name(
                worksheet_key,
                {"foreach": txntypeplural},
                txntypeplural,
            )

            # Create a worksheet configuration unique to each worksheet.
            worksheet_config = cls.copy_worksheet_config(worksheet_base_config)
            assert "select" in worksheet_config

            # Set conditions that will be applied to the SELECT query.
            # This *overwrites* any existing condition.
            worksheet_config["select"]["txntype"] = txn_type_value

            workbook.add_worksheet(cls(workbook, sheet_name, worksheet_config))
            worksheet_count += 1

        return worksheet_count

    @classmethod
    def expand_foreach_txnclass(
        cls,
        workbook: MoneyWorkbook,
        conn: sqlite3.Connection,
        worksheet_key: str,
        worksheet_base_config: WorksheetConfigT,
    ) -> int:
        """Expand `foreach` directive and add sheets to workbook.

        foreach: "txnclass"

        Args:
            workbook (MoneyWorkbook): Workbook.
            conn (sqlite3.Connection): SQLite connection.
            worksheet_key (str): Worksheet configuration key (typically the sheet name.)
            worksheet_base_config (WorksheetConfigT): Worksheet configuration.

        Returns:
            int: Number of worksheets added. If 0, base worksheet will be added.
        """

        def _list_txnclasses() -> list[str]:
            q_select = SelectAnd(VIEW_TXNS_WITHTYPEANDCLASS).distinct().column("TxnClass").order_by("TxnClass")
            cls.apply_txns_select_where(conn, q_select, worksheet_base_config, allow_directives=("txnclass",))
            df_select = pd_read_sql(conn, q_select)
            return df_select["TxnClass"].tolist() if not df_select.empty else []

        worksheet_count = 0

        for txn_class_value in _list_txnclasses():
            txnclassplural = pluralize(txn_class_value)

            sheet_name = cls.render_sheet_name(
                worksheet_key,
                {"foreach": txnclassplural},
                txnclassplural,
            )

            # Create a worksheet configuration unique to each worksheet.
            worksheet_config = cls.copy_worksheet_config(worksheet_base_config)
            assert "select" in worksheet_config

            # Set conditions that will be applied to the SELECT query.
            # This *overwrites* any existing condition.
            worksheet_config["select"]["txnclass"] = txn_class_value

            workbook.add_worksheet(cls(workbook, sheet_name, worksheet_config))
            worksheet_count += 1

        return worksheet_count

    @classmethod
    def expand_foreach_era(
        cls,
        workbook: MoneyWorkbook,
        conn: sqlite3.Connection,
        worksheet_key: str,
        worksheet_base_config: WorksheetConfigT,
    ) -> int:
        """Expand `foreach` directive and add sheets to workbook.

        foreach: "era"

        Args:
            workbook (MoneyWorkbook): Workbook.
            conn (sqlite3.Connection): SQLite connection.
            worksheet_key (str): Worksheet configuration key (typically the sheet name.)
            worksheet_base_config (WorksheetConfigT): Worksheet configuration.

        Returns:
            int: Number of worksheets added. If 0, base worksheet will be added.
        """
        worksheet_count = 0

        q_select = SelectAnd(TABLE_ERAS).column("EraName").order_by("rowid")
        cls.apply_txns_select_where(conn, q_select, worksheet_base_config, allow_directives=("era",))

        df_select = pd_read_sql(conn, q_select)

        for row in df_select.itertuples(index=False):
            era_name = row.EraName

            assert isinstance(era_name, str)

            sheet_name = cls.render_sheet_name(
                worksheet_key,
                {"foreach": era_name},
                era_name,
            )

            # Create a worksheet configuration unique to each worksheet.
            worksheet_config = cls.copy_worksheet_config(worksheet_base_config)
            assert "select" in worksheet_config

            # Set conditions that will be applied to the SELECT query.
            # This *overwrites* any existing condition.
            worksheet_config["select"]["era"] = era_name

            workbook.add_worksheet(cls(workbook, sheet_name, worksheet_config))
            worksheet_count += 1

        return worksheet_count

    @classmethod
    def expand_foreach_account(
        cls,
        workbook: MoneyWorkbook,
        conn: sqlite3.Connection,
        worksheet_key: str,
        worksheet_base_config: WorksheetConfigT,
    ) -> int:
        """Expand `foreach` directive and add sheets to workbook.

        foreach: "account"

        Args:
            workbook (MoneyWorkbook): Workbook.
            conn (sqlite3.Connection): SQLite connection.
            worksheet_key (str): Worksheet configuration key (typically the sheet name.)
            worksheet_base_config (WorksheetConfigT): Worksheet configuration.

        Returns:
            int: Number of worksheets added. If 0, base worksheet will be added.
        """

        def _list_accounts() -> list[str]:
            q_select = SelectAnd(TABLE_ACCOUNTS).distinct().column("Account").order_by("Account")
            cls.apply_txns_select_where(conn, q_select, worksheet_base_config, allow_directives=("account",))
            df_select = pd_read_sql(conn, q_select)
            return df_select["Account"].tolist() if not df_select.empty else []

        worksheet_count = 0
        worksheet_config_clean = cls.copy_worksheet_config(worksheet_base_config)
        assert "select" in worksheet_config_clean

        # Remove selection criteria we explicitly set for each sheet.
        config_select_remove(worksheet_config_clean["select"], ("account",))

        for account_name in _list_accounts():
            sheet_name = cls.render_sheet_name(
                worksheet_key,
                {"foreach": account_name},
                account_name,
            )

            # Create a worksheet configuration unique to each worksheet.
            worksheet_config = cls.copy_worksheet_config(worksheet_config_clean)
            assert "select" in worksheet_config

            # Set conditions that will be applied to the SELECT query.
            # This *overwrites* any existing condition.
            worksheet_config["select"]["account"] = account_name

            workbook.add_worksheet(cls(workbook, sheet_name, worksheet_config))
            worksheet_count += 1

        return worksheet_count

    @classmethod
    def expand_foreach_account_category(
        cls,
        workbook: MoneyWorkbook,
        conn: sqlite3.Connection,
        worksheet_key: str,
        worksheet_base_config: WorksheetConfigT,
    ) -> int:
        """Expand `foreach` directive and add sheets to workbook.

        foreach: "account_category"

        Args:
            workbook (MoneyWorkbook): Workbook.
            conn (sqlite3.Connection): SQLite connection.
            worksheet_key (str): Worksheet configuration key (typically the sheet name.)
            worksheet_base_config (WorksheetConfigT): Worksheet configuration.

        Returns:
            int: Number of worksheets added. If 0, base worksheet will be added.
        """

        def _list_account_categories() -> list[str]:
            q_select = SelectAnd(TABLE_ACCOUNTS).distinct().column("AccountCategory").order_by("AccountCategory")
            cls.apply_txns_select_where(conn, q_select, worksheet_base_config, allow_directives=("account_category",))
            df_select = pd_read_sql(conn, q_select)
            return df_select["AccountCategory"].tolist() if not df_select.empty else []

        worksheet_count = 0
        worksheet_config_clean = cls.copy_worksheet_config(worksheet_base_config)
        assert "select" in worksheet_config_clean

        # Remove selection criteria we explicitly set for each sheet.
        config_select_remove(worksheet_config_clean["select"], ("account_category",))

        for account_category in _list_account_categories():
            sheet_name = cls.render_sheet_name(
                worksheet_key,
                {"foreach": account_category},
                account_category or "No category",
            )

            # Create a worksheet configuration unique to each worksheet.
            worksheet_config = cls.copy_worksheet_config(worksheet_config_clean)
            assert "select" in worksheet_config

            # Set conditions that will be applied to the SELECT query.
            # This *overwrites* any existing condition.
            worksheet_config["select"]["account_category"] = account_category

            workbook.add_worksheet(cls(workbook, sheet_name, worksheet_config))
            worksheet_count += 1

        return worksheet_count

    @classmethod
    def expand_foreach_account_classification(
        cls,
        workbook: MoneyWorkbook,
        conn: sqlite3.Connection,
        worksheet_key: str,
        worksheet_base_config: WorksheetConfigT,
    ) -> int:
        """Expand `foreach` directive and add sheets to workbook.

        foreach: "account_classification"

        Args:
            workbook (MoneyWorkbook): Workbook.
            conn (sqlite3.Connection): SQLite connection.
            worksheet_key (str): Worksheet configuration key (typically the sheet name.)
            worksheet_base_config (WorksheetConfigT): Worksheet configuration.

        Returns:
            int: Number of worksheets added. If 0, base worksheet will be added.
        """

        def _list_account_classifications() -> list[str]:
            q_select = SelectAnd(TABLE_ACCOUNTS).distinct().column("AccountClassification").order_by("AccountClassification")
            cls.apply_txns_select_where(conn, q_select, worksheet_base_config, allow_directives=("account_classification",))
            df_select = pd_read_sql(conn, q_select)
            return df_select["AccountClassification"].tolist() if not df_select.empty else []

        worksheet_count = 0
        worksheet_config_clean = cls.copy_worksheet_config(worksheet_base_config)
        assert "select" in worksheet_config_clean

        # Remove selection criteria we explicitly set for each sheet.
        config_select_remove(worksheet_config_clean["select"], ("account_classification",))

        for account_classification in _list_account_classifications():
            sheet_name = cls.render_sheet_name(
                worksheet_key,
                {"foreach": account_classification},
                account_classification or "No classification",
            )

            # Create a worksheet configuration unique to each worksheet.
            worksheet_config = cls.copy_worksheet_config(worksheet_config_clean)
            assert "select" in worksheet_config

            # Set conditions that will be applied to the SELECT query.
            # This *overwrites* any existing condition.
            worksheet_config["select"]["account_classification"] = account_classification

            workbook.add_worksheet(cls(workbook, sheet_name, worksheet_config))
            worksheet_count += 1

        return worksheet_count

    @classmethod
    def expand_foreach_yyyy(
        cls,
        workbook: MoneyWorkbook,
        conn: sqlite3.Connection,
        worksheet_key: str,
        worksheet_base_config: WorksheetConfigT,
    ) -> int:
        """Expand `foreach` directive and add sheets to workbook.

        foreach: "yyyy"

        Args:
            workbook (MoneyWorkbook): Workbook.
            conn (sqlite3.Connection): SQLite connection.
            worksheet_key (str): Worksheet configuration key (typically the sheet name.)
            worksheet_base_config (WorksheetConfigT): Worksheet configuration.

        Returns:
            int: Number of worksheets added. If 0, base worksheet will be added.
        """

        def _list_txn_yyyy() -> list[int]:
            q_select = SelectAnd(VIEW_TXNS_WITHTYPEANDCLASS)
            q_select.column_expr("CAST(strftime('%Y', `Date`) AS INTEGER)", named="yyyy")
            q_select.group_by("yyyy")
            q_select.order_by("yyyy")
            cls.apply_txns_select_where(conn, q_select, worksheet_base_config, allow_directives=("yyyy", "date_from", "date_to"))
            df_select = pd_read_sql(conn, q_select)
            return df_select["yyyy"].tolist() if not df_select.empty else []

        worksheet_count = 0

        for yyyy in _list_txn_yyyy():
            sheet_name = cls.render_sheet_name(
                worksheet_key,
                {"foreach": str(yyyy)},
                f"{worksheet_key} {yyyy}",
            )

            # Create a worksheet configuration unique to each worksheet.
            worksheet_config = cls.copy_worksheet_config(worksheet_base_config)
            assert "select" in worksheet_config

            # Set conditions that will be applied to the SELECT query.
            # This *overwrites* any existing condition.
            worksheet_config["select"]["yyyy"] = yyyy

            workbook.add_worksheet(cls(workbook, sheet_name, worksheet_config))
            worksheet_count += 1

        return worksheet_count

    @classmethod
    def expand_foreach_10y(
        cls,
        workbook: MoneyWorkbook,
        conn: sqlite3.Connection,
        worksheet_key: str,
        worksheet_base_config: WorksheetConfigT,
    ) -> int:
        """Expand `foreach` directive and add sheets to workbook.

        foreach: "10y"

        Args:
            workbook (MoneyWorkbook): Workbook.
            conn (sqlite3.Connection): SQLite connection.
            worksheet_key (str): Worksheet configuration key (typically the sheet name.)
            worksheet_base_config (WorksheetConfigT): Worksheet configuration.

        Returns:
            int: Number of worksheets added. If 0, base worksheet will be added.
        """

        def _list_txn_yyyy() -> list[int]:
            q_select = SelectAnd(VIEW_TXNS_WITHTYPEANDCLASS)
            q_select.column_expr("CAST(strftime('%Y', `Date`) AS INTEGER)", named="yyyy")
            q_select.group_by("yyyy")
            q_select.order_by("yyyy")
            cls.apply_txns_select_where(conn, q_select, worksheet_base_config, allow_directives=("yyyy", "date_from", "date_to"))
            df_select = pd_read_sql(conn, q_select)
            return df_select["yyyy"].tolist() if not df_select.empty else []

        worksheet_count = 0

        decades: dict[int, list[int]] = defaultdict(list)
        for yyyy in _list_txn_yyyy():
            decades[yyyy // 10 * 10].append(yyyy)

        for decade, decade_years in decades.items():
            sheet_name = cls.render_sheet_name(
                worksheet_key,
                {"foreach": f"{decade}-{decade + 9}"},
                f"{worksheet_key} {decade}-{decade + 9}",
            )

            # Create a worksheet configuration unique to each worksheet.
            worksheet_config = cls.copy_worksheet_config(worksheet_base_config)
            assert "select" in worksheet_config

            # Set conditions that will be applied to the SELECT query.
            # This *overwrites* any existing condition.
            worksheet_config["select"]["yyyy"] = decade_years

            workbook.add_worksheet(cls(workbook, sheet_name, worksheet_config))
            worksheet_count += 1

        return worksheet_count

    ######################################################################
    # Instance methods

    @classmethod
    def apply_txns_select_where(  # noqa: C901, PLR0912
        cls,
        conn: sqlite3.Connection,
        q_select: Select,
        worksheet_config: WorksheetConfigT,
        allow_directives: Container[str] | None = None,
    ) -> None:
        """Apply sheet configuration 'select' as where conditions to SELECT query.

        Query must include table `TABLE_ACCOUNTS` or `VIEW_TXNS_WITHTYPEANDCLASS`.

        Args:
            conn (sqlite3.Connection): SQLite connection.
            q_select (Select): Select query.
            worksheet_config (WorksheetConfigT): Worksheet configuration.
            allow_directives (Container[str] | None, optional): Allowable directives. Defaults to None.
                If None, all directives are allowed.
        """
        config_select = worksheet_config.get("select", {})

        if config_select and allow_directives:
            # Only evaluate allowed directives
            config_select = config_select_allow(config_select, allow_directives)

        if not config_select:
            return  # nothing to do

        where_value_simple = {
            "account": "Account",
            "era": "T.`EraName`",
            "payee": "Payee",
            "txnclass": "TxnClass",
            "txntype": "TxnType",
        }

        for select_key, table_column in where_value_simple.items():
            values, cond = get_select_values_and_cond(select_key, config_select)
            if values:
                q_select.where_value(table_column, values, cond)

        categories, cond = get_select_values_and_cond("category", config_select)
        if categories:
            # Nest conditions in an "... OR ..." or "NOT (... OR ...)" clause depending on negation
            category_cond = q_select.where_cond.where_or(negate=cond == "<>")

            for category_pair in categories:
                if category_pair.endswith(":"):
                    # OR (C=... AND S IS NULL)
                    c = category_cond.where_and()
                    c.where_value("Category", category_pair[:-1])
                    c.where_value("Subcategory", None)
                elif ":" in category_pair:
                    # OR (C=... AND S=...)
                    category, subcategory = split_category_pair(category_pair)
                    c = category_cond.where_and()
                    c.where_value("Category", category)
                    c.where_value("Subcategory", subcategory)
                else:
                    # OR C = ...
                    category_cond.where_value("Category", category_pair)

        account_classifications, cond = get_select_values_and_cond("account_classification", config_select)
        if account_classifications:
            q_select_accounts = Select(TABLE_ACCOUNTS).columns("Account")
            q_select_accounts.where_value("AccountClassification", account_classifications, cond)
            q_select.where_select("Account", q_select_accounts, "IN")

        account_categories, cond = get_select_values_and_cond("account_category", config_select)
        if account_categories:
            q_select_accounts = Select(TABLE_ACCOUNTS).columns("Account")
            q_select_accounts.where_value("AccountCategory", account_categories, cond)
            q_select.where_select("Account", q_select_accounts, "IN")

        yyyy, cond = get_select_values_and_cond("yyyy", config_select)  # list of years
        if yyyy:
            q_select.where_value("CAST(strftime('%Y', `Date`) AS INTEGER)", yyyy, cond)
        else:
            date_from, date_to = cls._get_select_dates(conn, config_select)

            if date_from and date_to:
                q_select.where_raw_value("Date", "? AND ?", "BETWEEN", value_params=(date_from, date_to))
            elif date_from:
                q_select.where_value("Date", date_from, ">=")
            elif date_to:
                q_select.where_value("Date", date_to, "<=")

    @classmethod
    def _get_select_dates(
        cls,
        conn: sqlite3.Connection,
        select_config: WorkbookConfigSelectT | WorksheetConfigSelectT,
    ) -> tuple[date | None, date | None]:
        """Get date range from select configuration.

        It's ok if dates are not within the transaction date range.

        Args:
            conn (sqlite3.Connection): SQLite connection.
            select_config (WorksheetConfigT): Worksheet configuration.

        Returns:
            tuple[date | None, date | None]
        """

        def _spec_to_date(date_spec: str, txndate_max: date, first_day: bool) -> date | None:
            relative_d = get_date_relative_to(date_spec, txndate_max, first_day)
            if relative_d is not None:
                return relative_d
            return parse_yyyymmdd_flex(date_spec, first_day)

        spec_date_from = get_select_values("date_from", select_config)
        spec_date_to = get_select_values("date_to", select_config)

        if not (spec_date_from or spec_date_to):
            return None, None

        txndate_min, txndate_max = db_get_txndates(conn)

        assert txndate_min is not None
        assert txndate_max is not None

        date_from = None
        date_to = None

        if spec_date_from:
            assert isinstance(spec_date_from[0], str)
            date_from = _spec_to_date(spec_date_from[0], txndate_max, first_day=True)

        if spec_date_to:
            assert isinstance(spec_date_to[0], str)
            date_to = _spec_to_date(spec_date_to[0], txndate_max, first_day=False)

        if date_from and date_to and date_to < date_from:
            date_from, date_to = date_to, date_from

        return date_from, date_to

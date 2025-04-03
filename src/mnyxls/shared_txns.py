from __future__ import annotations

import re
import typing
from datetime import date
from decimal import DecimalException

from mysqlstmt import Select, Update

from .currencydecimal import currency_from_value
from .dbschema import TABLE_ACCOUNTS
from .shared import (
    MnyXlsConfigError,
    config_select_allow,
    get_date_relative_to,
    get_select_values,
    get_select_values_and_cond,
    parse_yyyymmdd_flex,
    pd_read_sql,
    split_category_pair,
)

if typing.TYPE_CHECKING:
    import sqlite3
    from collections.abc import Container

    from .configtypes import ConfigSelectUnionT
    from .currencydecimal import CurrencyDecimal

# Regular expression to match `select` amount specifications such as "<MM.NN".
RE_AMOUNT = re.compile(r"([<>=!]+)\s*(-?[\d,.]+)")


######################################################################
# Helper functions


def _get_select_dates(conn: sqlite3.Connection, select_config: ConfigSelectUnionT) -> tuple[date | None, date | None]:
    """Get date range from select configuration.

    It's ok if dates are not within the transaction date range.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        select_config (ConfigSelectUnionT): Worksheet configuration.

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

    txndate_min, txndate_max = get_txndates(conn)

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


######################################################################
# Module public


def get_txndates(conn: sqlite3.Connection, account_name: str | None = None) -> tuple[date | None, date | None]:
    """Get the min and max transaction dates across accounts or for an individual account.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        account_name (str): Account name.

    Returns:
        tuple[date | None, date | None]
    """
    q_select = Select(TABLE_ACCOUNTS)
    q_select.column("TxnDateMin")
    q_select.column("TxnDateMax")
    q_select.where_value("TxnDateMin", None, "<>")

    if account_name:
        q_select.where_value("Account", account_name)

    df_select = pd_read_sql(conn, q_select, date_cols=["TxnDateMin", "TxnDateMax"])

    if df_select.empty:
        return None, None

    # Using min() is odd but shorter than:
    # > pd.to_datetime(df_select["TxnDateMin"].to_numpy()[0]).date(),

    df_dates = df_select[df_select["TxnDateMin"].notna()]
    min_date = df_dates["TxnDateMin"].min()
    assert isinstance(min_date, date)

    df_dates = df_select[df_select["TxnDateMax"].notna()]
    max_date = df_dates["TxnDateMax"].min()
    assert isinstance(max_date, date)

    return (min_date, max_date)


def apply_txns_select_where(  # noqa: C901, PLR0912, PLR0915
    conn: sqlite3.Connection,
    q_select: Select | Update,
    select_config: ConfigSelectUnionT,
    allow_directives: Container[str] | None = None,
) -> None:
    """Apply 'select' fields as WHERE conditions to a SELECT or UPDATE query.

    Query must include table `TABLE_ACCOUNTS` or `VIEW_TXNS_WITHTYPEANDCLASS`.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        q_select (Select): Select query.
        select_config (ConfigSelectUnionT): Config with `select` criteria.
        allow_directives (Container[str] | None, optional): Allowable directives. Defaults to None.
            If None, all directives are allowed.
    """

    def _amount_currency(value: str) -> CurrencyDecimal:
        try:
            currency = currency_from_value(value)
        except DecimalException:
            currency = None

        if currency is None:
            raise MnyXlsConfigError(f"'select': 'amount': '{value}': Amount is not a decimal number.")

        return currency

    # Criteria are in the `select` field for WorkbookConfigT and WorksheetConfigT
    select_config = select_config.get("select", select_config)

    if select_config and allow_directives:
        # Only evaluate allowed directives
        select_config = config_select_allow(select_config, allow_directives)

    if not select_config:
        return  # nothing to do

    where_value_simple = {
        "account": "Account",
        "era": "T.`EraName`",
        "payee": "Payee",
        "memo": "Memo",
        "txnclass": "TxnClass",
        "txntype": "TxnType",
    }

    for select_key, table_column in where_value_simple.items():
        values, cond = get_select_values_and_cond(select_key, select_config)
        if values:
            q_select.where_value(table_column, values, cond)

    categories, cond = get_select_values_and_cond("category", select_config)
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

    account_classifications, cond = get_select_values_and_cond("account_classification", select_config)
    if account_classifications:
        q_select_accounts = Select(TABLE_ACCOUNTS).columns("Account")
        q_select_accounts.where_value("AccountClassification", account_classifications, cond)
        q_select.where_select("Account", q_select_accounts, "IN")

    account_categories, cond = get_select_values_and_cond("account_category", select_config)
    if account_categories:
        q_select_accounts = Select(TABLE_ACCOUNTS).columns("Account")
        q_select_accounts.where_value("AccountCategory", account_categories, cond)
        q_select.where_select("Account", q_select_accounts, "IN")

    yyyy, cond = get_select_values_and_cond("yyyy", select_config)  # list of years
    if yyyy:
        q_select.where_value("CAST(strftime('%Y', `Date`) AS INTEGER)", yyyy, cond)
    else:
        date_from, date_to = _get_select_dates(conn, select_config)

        if date_from and date_to:
            q_select.where_raw_value("Date", "? AND ?", "BETWEEN", value_params=(date_from, date_to))
        elif date_from:
            q_select.where_value("Date", date_from, ">=")
        elif date_to:
            q_select.where_value("Date", date_to, "<=")

    amount = get_select_values("amount", select_config)
    if amount:
        if amount[0].startswith("<>"):
            if len(amount) != 3:  # noqa: PLR2004
                raise MnyXlsConfigError("'select': 'amount': operator requires 3 values ['<>, <lower>, <upper>']")

            a, b = _amount_currency(amount[1]), _amount_currency(amount[2])
            if b < a:
                a, b = b, a

            q_select.where_raw_value("Amount", "? AND ?", "BETWEEN", value_params=(a.nofmt(), b.nofmt()))
        else:
            if len(amount) == 1:
                match = RE_AMOUNT.fullmatch(amount[0])
                op, value = match.groups() if match else ("=", amount[0])
            else:
                op, value = amount[0], amount[1]

            if op not in ("=", "!", "!=", "<", "<=", ">", ">="):
                raise MnyXlsConfigError(f"'select': 'amount': '{op}': Invalid operator.")

            if op.startswith("!"):
                op = "<>"

            value = _amount_currency(value)

            q_select.where_value("Amount", value.nofmt(), op)

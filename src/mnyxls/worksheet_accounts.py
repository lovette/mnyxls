from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING

from mysqlstmt import Select

from .configtypes import WorksheetConfigSelectAccountsT
from .dbschema import TABLE_ACCOUNTS, table_schema_columns
from .mysqlstmt_selectand import SelectAnd
from .shared import config_select_allow, config_select_remove, get_select_values_and_cond, pd_read_sql, validate_config_typed_dict
from .worksheet import WORKSHEET_COLWIDTH_MAX, MoneyWorksheet

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Container, Sequence

    import pandas as pd

    from .configtypes import WorksheetConfigSelectT, WorksheetConfigT
    from .workbook import MoneyWorkbook


######################################################################
# Transactions worksheet


class MoneyWorksheetAccounts(MoneyWorksheet):
    """Worksheet for account details."""

    SHEET_TYPE = "accounts"

    # Keep column widths within reasonable bounds
    WORKSHEET_COLWIDTH_BOUNDS = MappingProxyType(
        {
            "Account": (20, WORKSHEET_COLWIDTH_MAX),
            "AccountClassification": (21, WORKSHEET_COLWIDTH_MAX),
            "Abbreviation": (12, 12),
            "BankName": (10, WORKSHEET_COLWIDTH_MAX),
            "AccountNumber": (15, WORKSHEET_COLWIDTH_MAX),
            "AccountLimit": (12, 15),
            "OpeningBalance": (15, 15),
            "TxnDateMin": (12, 12),
            "TxnDateMax": (12, 12),
            "OpenedDate": (12, 12),
            "ClosedDate": (12, 12),
        }
    )

    def __init__(self, *args, **kwargs) -> None:  # noqa: D107
        super().__init__(*args, **kwargs)

    ######################################################################
    # Class methods

    @classmethod
    def validate_config_select(cls, workbook: MoneyWorkbook, select_config: WorksheetConfigSelectT, config_keys: str | Sequence[str]) -> None:
        """Validate worksheet configuration `select` directive.

        Args:
            workbook (MoneyWorkbook): Workbook.
            select_config (WorksheetConfigSelectT): Select configuration.
            config_keys (str | Sequence[str]): Configuration keys for reporting errors.

        Raises:
            MnyXlsConfigError: Invalid configuration.
        """
        MoneyWorksheet.validate_config_select(workbook, select_config, config_keys)

        validate_config_typed_dict(select_config, WorksheetConfigSelectAccountsT, workbook.config, config_keys)

    ######################################################################
    # Class methods for expanding `foreach` directives

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
            cls._apply_select_where(q_select, worksheet_base_config, allow_directives=("account_category",))
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
            cls._apply_select_where(q_select, worksheet_base_config, allow_directives=("account_classification",))
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

    ######################################################################
    # Instance methods

    def get_sheet_data(self, conn: sqlite3.Connection) -> pd.DataFrame:
        """Query database and return data to write to worksheet.

        Args:
            conn (sqlite3.Connection): SQLite connection.
        """
        assert self.workbook is not None
        assert self.workbook.config is not None

        drop_cols = ["XferOnly"]  # Internal field
        select_config: WorksheetConfigSelectAccountsT = self.worksheet_config.get("select", {})
        select_columns = table_schema_columns(TABLE_ACCOUNTS)

        # Columns that are dates will be formatted as dates in Excel
        self.date_cols = ("TxnDateMin", "TxnDateMax", "OpenedDate", "ClosedDate")

        # Let `prepare_to_excel` know we want to avoid Excel marking currency cells
        # with "Number Stored as Text" (best we can.)
        self.currency_cols = ("AccountLimit", "OpeningBalance")

        # Don't include columns that are redundant with the sheet name
        for select_key, table_column in {
            "account_category": "AccountCategory",
            "account_classification": "AccountClassification",
        }.items():
            if select_key in select_config:
                drop_cols.append(table_column)

        q_select = Select(TABLE_ACCOUNTS)

        q_select.column([col for col in select_columns if col not in drop_cols])

        self._apply_select_where(q_select, self.worksheet_config)

        q_select.order_by("Account")

        return pd_read_sql(conn, q_select, date_cols=self.date_cols, currency_cols=self.currency_cols)

    def format_sheet(self) -> None:
        """Format worksheet after sheet is created."""
        super().format_sheet()

        assert self.pyxl_worksheet is not None

        if self.currency_cols:
            for header in self.currency_cols:
                self.set_column_number_format(self.amount_number_format, header=header, raise_missing_header=False)

        if self.date_cols:
            for header in self.date_cols:
                self.set_column_number_format(self.date_number_format, header=header, raise_missing_header=False)

    ######################################################################
    # Helper methods

    @classmethod
    def _apply_select_where(
        cls,
        q_select: Select,
        worksheet_config: WorksheetConfigT,
        allow_directives: Container[str] | None = None,
    ) -> None:
        """Apply sheet configuration 'select' as where conditions to SELECT query.

        Applies to `foreach` directive selects too.

        Args:
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

        accounts, cond = get_select_values_and_cond("account", config_select)
        if accounts:
            q_select.where_value("Account", accounts, cond)

        account_classifications, cond = get_select_values_and_cond("account_classification", config_select)
        if account_classifications:
            q_where = Select(TABLE_ACCOUNTS).columns("Account")
            q_where.where_value("AccountClassification", account_classifications)
            q_select.where_select("Account", q_where, "IN" if cond == "=" else "NOT IN")

        account_categories, cond = get_select_values_and_cond("account_category", config_select)
        if account_categories:
            q_where = Select(TABLE_ACCOUNTS).columns("Account")
            q_where.where_value("AccountCategory", account_categories)
            q_select.where_select("Account", q_where, "IN" if cond == "=" else "NOT IN")


######################################################################
# Register worksheet class so it can be referenced in the configuration file.

MoneyWorksheetAccounts.register_sheet_type()

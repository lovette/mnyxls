from __future__ import annotations

import logging
from types import MappingProxyType
from typing import TYPE_CHECKING

from .configtypes import WorksheetConfigSelectTxnsT
from .dbschema import TABLE_TXNS, table_schema_columns
from .dbviews import VIEW_TXNS_WITHTYPEANDCLASS
from .mysqlstmt_selectand import SelectAnd
from .report import TxnType
from .shared import config_warning, get_values_and_cond, pd_read_sql, validate_config_typed_dict
from .worksheet import WORKSHEET_COLWIDTH_MAX, MoneyWorksheet
from .worksheet_txns_base import MoneyWorksheetTxnsBase

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Sequence

    import pandas as pd

    from .configtypes import WorksheetConfigSelectT
    from .workbook import MoneyWorkbook

# This is the order of columns in the worksheet.
TXNS_WORKSHEET_COLUMNS = (
    "N",
    "Num",
    "Date",
    "DateYYYYMM",  # consolidate
    "Account",
    "Payee",
    "Category",
    "Subcategory",
    "XferAccount",
    "Amount",
    "TxnType",
    "TxnClass",
    "Memo",
    "Split",
    "C",
)

# Use a named logger instead of root logger
logger = logging.getLogger("mnyxls")


######################################################################
# Transactions worksheet


class MoneyWorksheetTxns(MoneyWorksheetTxnsBase):
    """Worksheet for non-investment account transactions."""

    SHEET_TYPE = "txns"

    # Keep column widths within reasonable bounds
    WORKSHEET_COLWIDTH_BOUNDS = MappingProxyType(
        {
            "N": (5, 15),
            "Num": (7, 15),
            "Date": (10, 12),
            "Account": (20, WORKSHEET_COLWIDTH_MAX),
            "Payee": (20, WORKSHEET_COLWIDTH_MAX),
            "Category": (20, WORKSHEET_COLWIDTH_MAX),
            "Subcategory": (20, WORKSHEET_COLWIDTH_MAX),
            "Amount": (10, 15),
            "TxnType": (10, WORKSHEET_COLWIDTH_MAX),
            "TxnClass": (10, WORKSHEET_COLWIDTH_MAX),
            "Memo": (20, WORKSHEET_COLWIDTH_MAX),
            "Split": (5, 5),
            "C": (2, 5),
            "XferAccount": (20, WORKSHEET_COLWIDTH_MAX),
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

        validate_config_typed_dict(select_config, WorksheetConfigSelectTxnsT, workbook.config, config_keys)

    ######################################################################
    # Instance methods

    def get_sheet_data(self, conn: sqlite3.Connection) -> pd.DataFrame:  # noqa: C901, PLR0912, PLR0915
        """Query database and return data to write to worksheet.

        Args:
            conn (sqlite3.Connection): SQLite connection.
        """
        assert self.workbook is not None
        assert self.workbook.config is not None

        date_col = "Date"
        consolidate = self.worksheet_config.get("consolidate")
        select_config: WorksheetConfigSelectTxnsT = self.worksheet_config.get("select", {})
        select_columns = [*table_schema_columns(TABLE_TXNS), "TxnType", "TxnClass"]  # VIEW_TXNS_WITHTYPEANDCLASS
        drop_cols = []
        maybe_drop_cols = []

        # Let `prepare_to_excel` know we want to avoid Excel marking currency cells
        # with "Number Stored as Text" (best we can.)
        self.currency_cols = ["Amount"]

        if "txntype" in select_config:
            if select_config["txntype"] == TxnType.TRANSFER.value:
                drop_cols.append("Subcategory")  # Transfers don't have a subcategory
            if select_config["txntype"] != TxnType.TRANSFER.value:
                drop_cols.append("XferAccount")  # Only transfers have a transfer account

        # Don't include columns that are redundant with the sheet name
        for select_key, table_column in {
            "account": "Account",
            "account_category": "AccountCategory",
            "account_classification": "AccountClassification",
            "txnclass": "TxnClass",
            "txntype": "TxnType",
        }.items():
            if select_key in select_config:
                maybe_drop_cols.append(table_column)

        q_select = SelectAnd(VIEW_TXNS_WITHTYPEANDCLASS)

        if consolidate is not None:
            if consolidate == "yyyymm":
                date_col = "DateYYYYMM"
                drop_cols.extend(("Num", "Date", "Amount", "Memo", "Split", "C"))  # these don't make sense for txn groups
                self.date_number_format = "mmm-yyyy"
                q_select.column_expr("strftime('%Y-%m', `Date`)", named=f"`{date_col}`")
                q_select.column_expr("SUM(`Amount`)", named="Amount")
                q_select.group_by([f"`{date_col}`", "Payee", "Category", "Subcategory", "TxnType", "TxnClass"])
            else:
                config_warning("Unrecognized option.", self.workbook.config, (*self.config_keys, "consolidate", consolidate))

        q_select.column([col for col in select_columns if col not in drop_cols])

        self.apply_txns_select_where(conn, q_select, self.worksheet_config)

        q_select.order_by((f"`{date_col}`", "Category", "Subcategory", "Payee", "Account", "N"))

        df_worksheet = pd_read_sql(
            conn,
            q_select,
            date_cols=[date_col],
            currency_cols=self.currency_cols,
        )

        # Include or exclude columns as defined in the configuration file.
        config_columns = self.filter_df_columns(df_worksheet)
        if config_columns:
            df_worksheet = df_worksheet[config_columns]

        # Drop columns that are redundant with the sheet name only if they only have one value.
        # (This prevents having a sheet named "Checking expenses" with columns "Account: Checking" and "Txn Type: Expenses".)
        for col in maybe_drop_cols:
            if col in df_worksheet.columns:
                # https://docs.astral.sh/ruff/rules/pandas-nunique-constant-series-check/
                # > df_worksheet[col].nunique() == 1
                arr = df_worksheet[col].to_numpy()
                if arr.shape[0] == 0 or (arr[0] == arr).all():
                    df_worksheet = df_worksheet.drop(columns=[col])

        if date_col != "Date":
            # Date column may be renamed when consolidating
            df_worksheet = df_worksheet.rename(columns={date_col: "Date"})

        unexpectedcolumns = set(df_worksheet.columns) - set(TXNS_WORKSHEET_COLUMNS)
        if unexpectedcolumns:
            logger.debug(f"TXNS_WORKSHEET_COLUMNS does not include columns: {', '.join(unexpectedcolumns)}")

        # Replace the "N" column from the database with the internal sequential index.
        # (We don't simply name the index "N" because ExcelWriter applies formatting to the index column if index=True.)
        df_worksheet.index += 1  # 0...N -> 1...N
        df_worksheet = df_worksheet.drop("N", axis=1)
        df_worksheet = df_worksheet.reset_index(names="N")

        # Order columns in consistent order, preserving columns in reused worksheet when possible
        # Dicts are ordered in Python 3.7+
        ordered_columns = {}
        if self.preserve_column_headers:
            ordered_columns.update({col: col for col in self.preserve_column_headers if col in df_worksheet.columns})
        ordered_columns.update({col: col for col in TXNS_WORKSHEET_COLUMNS if col in df_worksheet.columns})
        ordered_columns.update({col: col for col in df_worksheet.columns})

        return df_worksheet.reindex(columns=list(ordered_columns.values()))

    def format_sheet(self) -> None:  # noqa: D102
        super().format_sheet()

        self.set_column_number_format(self.date_number_format, header="Date")
        self.set_column_number_format(self.amount_number_format, header="Amount")


######################################################################
# Register worksheet class so it can be referenced in the configuration file.

MoneyWorksheetTxns.register_sheet_type()

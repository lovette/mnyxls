from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING

from mysqlstmt import Select

from .configtypes import WorksheetConfigSelectCategoriesT
from .dbschema import TABLE_CATEGORIES
from .shared import get_select_values, pd_read_sql, validate_config_typed_dict
from .worksheet import WORKSHEET_COLWIDTH_MAX, MoneyWorksheet

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Sequence

    import pandas as pd

    from .configtypes import WorksheetConfigSelectT
    from .workbook import MoneyWorkbook


######################################################################
# Categories worksheet


class MoneyWorksheetCategories(MoneyWorksheet):
    """Worksheet for categories details."""

    SHEET_TYPE = "categories"

    # Keep column widths within reasonable bounds
    WORKSHEET_COLWIDTH_BOUNDS = MappingProxyType(
        {
            "Category": (20, WORKSHEET_COLWIDTH_MAX),
            "Subcategory": (20, WORKSHEET_COLWIDTH_MAX),
            "TxnType": (10, WORKSHEET_COLWIDTH_MAX),
            "TxnClass": (10, WORKSHEET_COLWIDTH_MAX),
            "TxnDateMin": (12, 12),
            "TxnDateMax": (12, 12),
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

        validate_config_typed_dict(select_config, WorksheetConfigSelectCategoriesT, workbook.config, config_keys)

    ######################################################################
    # Instance methods

    def get_sheet_data(self, conn: sqlite3.Connection) -> pd.DataFrame:
        """Query database and return data to write to worksheet.

        ```
        "Review subcategories":
            sheet_type: "categories"
            select:
                ...
        ```

        Args:
            conn (sqlite3.Connection): SQLite connection.
        """
        assert self.workbook is not None
        assert self.workbook.config is not None

        q_select = Select(TABLE_CATEGORIES)
        self._apply_select_where(q_select)
        q_select.order_by(("Category", "Subcategory"))

        return pd_read_sql(conn, q_select)

    ######################################################################
    # Helper methods

    def _apply_select_where(self, q_select: Select) -> None:
        """Apply sheet configuration 'select' as where conditions to SELECT query.

        Args:
            q_select (Select): Select query.
        """
        where_value_simple = {
            "txnclass": "TxnClass",
            "txntype": "TxnType",
        }

        for select_key, table_column in where_value_simple.items():
            values = get_select_values(select_key, self.worksheet_config)
            if values:
                q_select.where_value(table_column, values)


######################################################################
# Register worksheet class so it can be referenced in the configuration file.

MoneyWorksheetCategories.register_sheet_type()

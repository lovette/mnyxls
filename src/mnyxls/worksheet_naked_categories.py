from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING

from mysqlstmt import Select

from .configtypes import WorksheetConfigSelectCategoriesT
from .dbschema import TABLE_CATEGORIES
from .dbviews import VIEW_TXNS_WITHTYPEANDCLASS
from .shared import pd_read_sql, validate_config_typed_dict
from .worksheet import WORKSHEET_COLWIDTH_MAX, MoneyWorksheet

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Sequence

    import pandas as pd

    from .configtypes import WorksheetConfigSelectT
    from .workbook import MoneyWorkbook


######################################################################
# Categories worksheet


class MoneyWorksheetCategoriesNaked(MoneyWorksheet):
    """Worksheet for naked categories."""

    SHEET_TYPE = "categories:naked"

    # Keep column widths within reasonable bounds
    WORKSHEET_COLWIDTH_BOUNDS = MappingProxyType(
        {
            "Category": (20, WORKSHEET_COLWIDTH_MAX),
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

        # Categories with subcategories
        q_categories_with_subs = Select(TABLE_CATEGORIES)
        q_categories_with_subs.column("Category").distinct()
        q_categories_with_subs.where_value("Subcategory", None, "<>")

        q_select = Select(VIEW_TXNS_WITHTYPEANDCLASS)
        q_select.column("Category").distinct()
        q_select.where_select("Category", q_categories_with_subs, "IN")
        q_select.where_value("Subcategory", None)
        q_select.order_by("Category")

        df_worksheet = pd_read_sql(conn, q_select)

        # Rename header
        return df_worksheet.rename(
            columns={
                df_worksheet.columns[0]: "These categories have transactions not assigned a subcategory",
            }
        )


######################################################################
# Register worksheet class so it can be referenced in the configuration file.

MoneyWorksheetCategoriesNaked.register_sheet_type()

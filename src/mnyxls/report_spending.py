from __future__ import annotations

import logging
from collections import defaultdict
from types import MappingProxyType
from typing import TYPE_CHECKING

import pandas as pd

from .report import MoneyReport, ReportType, TxnType
from .report_mixinasofcolumns import MoneyReportMixinAsOfColumns

if TYPE_CHECKING:
    from .currencydecimal import CurrencyDecimal

CategoriesByTypeT = dict[str, list[str]]


# Use a named logger instead of root logger
logger = logging.getLogger("mnyxls")

######################################################################
# Income and spending report
#
# Required export options:
# - Rows: Categories
# - Columns: Years
#
# We can set these account properties with this report:
# - Category types (Income, Expenses)
# - Transfers categorized as expense?
# - Annual category totals that can be used to validate import.
#
# Report format:
#
# REPORT TITLE
# Converted to base currency: US dollar
# MM/DD/YYYY through MM/DD/YYYY
# ,MM/DD/YYYY -,MM/DD/YYYY -,MM/DD/YYYY -,MM/DD/YYYY -,MM/DD/YYYY -,MM/DD/YYYY -,MM/DD/YYYY -,MM/DD/YYYY -,MM/DD/YYYY -,MM/DD/YYYY -,
# Category,MM/DD/YYYY,MM/DD/YYYY,MM/DD/YYYY,MM/DD/YYYY,MM/DD/YYYY,MM/DD/YYYY,MM/DD/YYYY,MM/DD/YYYY,MM/DD/YYYY,MM/DD/YYYY,Total
# Income Categories,,,,,,,,,,,
# ACCOUNT,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN
# ...
# Total Income Categories,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN
# Expense Categories,,,,,,,,,,,
# ACCOUNT,,,,,,,,,MM.NNNN,,MM.NNNN
# ...
# Total Expense Categories,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN
# Transfers,,,,,,,,,,,
# ACCOUNT,,,,,,-MM.NNNN,-MM.NNNN,,,,-MM.NNNN
# ...
# Total Transfers,,,,-MM.NNNN,-MM.NNNN,-MM.NNNN,-MM.NNNN,-MM.NNNN,-MM.NNNN,-MM.NNNN,-MM.NNNN
# Grand Total,MM.NNNN,MM.NNNN,-MM.NNNN,-MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN


class MoneyReportIncomeAndSpending(MoneyReportMixinAsOfColumns, MoneyReport):
    """Parser for income and spending report."""

    report_type = ReportType.INCOME_SPENDING
    header_row_limit = 5
    required_columns = (
        "Category",  # Sorted by this column
        "Total",
    )
    currency_columns = ("Total",)
    footer_row_value = "Grand Total"

    # Section headers differ slightly between "Income and spending" and "Monthly income and expenses" reports.
    section_header_txntype = MappingProxyType(
        {
            "Income Categories": TxnType.INCOME,
            "Expense Categories": TxnType.EXPENSE,
            "Transfers": TxnType.TRANSFER,
        }
    )

    extra_df_columns = ("TxnType",)

    def __init__(  # noqa: D107
        self,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    ######################################################################
    # Abstract methods

    def parse_report(self) -> None:  # noqa: D102
        assert len(self.report_paths) == 1

        logger.info(f"Parsing '{self.report_paths[0]}'")
        logger.debug(f"> Type: {self.report_name} report")

        drop_categories: list[str] = []

        # Read one line of report to get names of the annual columns
        df_report_temp = self.read_report_csv(nrows=1)

        # Set annual columns *before* reading the report so they are included in expected_columns()!
        annual_columns = self.asof_total_columns_map(df_report_temp)

        if not annual_columns:
            self.fail_parse("Failed to find any annual columns in report.")

        # All columns except Category are currency values
        self.currency_columns = tuple(sorted(annual_columns.keys()) + list(self.currency_columns))

        df_report = self.read_report_csv()

        # Capture grand total then drop.
        grand_total_row = df_report[df_report["Category"] == self.footer_row_value]
        if not grand_total_row.empty:
            report_grand_total = grand_total_row["Total"].iloc[0]
            df_report = df_report.drop(grand_total_row.index)
        else:
            report_grand_total = None

        # Associate each category with its type (based on its section header)
        df_report["TxnType"] = self._get_column_txntypes(df_report, drop_categories)

        # Drop rows for Account types and "Total *"
        df_report = df_report[~df_report["Category"].isin(drop_categories)]

        # Sanity check
        if not df_report[df_report["Total"].isna()].empty:
            self.fail_parse("'Total' column has an Category without a value?")

        annual_columns_date_range = list(annual_columns.values())

        logger.debug(f"> Dates: {annual_columns_date_range[0]} to {annual_columns_date_range[-1]}")
        logger.debug(f"> Categories: {len(df_report)}")

        if report_grand_total is not None:
            self._check_totals(df_report, report_grand_total)

        # Drop the "Total" column since it's not needed anymore
        df_report = df_report.drop("Total", axis=1)

        dataframe_columns = [*self.required_columns, *self.extra_df_columns, *annual_columns.keys()]
        dataframe_columns.remove("Total")

        # Reorder columns
        df_report = df_report.reindex(columns=dataframe_columns)

        df_report = self._apply_filter(df_report)

        if not df_report.empty:
            self.df_report = df_report.sort_values(self.required_columns[0], ignore_index=True)

    ######################################################################
    # Instance methods

    def combine(self, report2: MoneyReport) -> None:  # noqa: D102
        super().combine(report2)

        assert isinstance(report2, type(self))
        assert report2.df_report is not None

        if self.empty_report:
            self.df_report = report2.df_report.copy()
            return

        df_report = self.df_report.merge(
            report2.df_report,
            on=self.required_columns[0],
            how="outer",
            suffixes=("_df1", "_df2"),
        )

        # Merge overlapping columns
        overlapping_columns = [col.removesuffix("_df1") for col in df_report.columns if col.endswith("_df1")]
        for col_name in overlapping_columns:
            # https://github.com/pandas-dev/pandas/issues/57734
            with pd.option_context("future.no_silent_downcasting", True):  # noqa: FBT003
                df_report[col_name] = df_report[f"{col_name}_df2"].fillna(df_report[f"{col_name}_df1"])

        # Sort by category name
        df_report = df_report.sort_values(self.required_columns[0], ignore_index=True)

        annual_columns = self.asof_total_columns_map(df_report)
        dataframe_columns = [*self.required_columns, *self.extra_df_columns, *annual_columns.keys()]
        dataframe_columns.remove("Total")  # in `required_columns` but we dropped it in `parse_report()`

        # Reorder columns by ascending dates
        self.df_report = df_report.reindex(columns=dataframe_columns)

    def drop_unexpected_columns(self, df_report: pd.DataFrame) -> pd.DataFrame:  # noqa: D102
        annual_columns = self.asof_total_columns_map(df_report)
        expected_columns = self.expected_columns() | set(annual_columns.keys())

        # Drop unexpected columns
        drop_columns = sorted(set(df_report.columns) - expected_columns)
        if drop_columns:
            logger.debug(f"> Ignoring columns: {', '.join(drop_columns)}")
            df_report = df_report.drop(drop_columns, axis=1)

        return df_report

    def categories_by_type(self) -> CategoriesByTypeT:
        """Return a dictionary of categories by `TxnType.value`.

        Returns:
            CategoriesByTypeT
        """
        categories_by_type: CategoriesByTypeT = defaultdict(list)

        for row in self.df_report.itertuples(index=False):
            category, txn_type = row.Category, row.TxnType
            assert isinstance(category, str)
            assert isinstance(txn_type, str)
            categories_by_type[txn_type].append(category)

        return dict(categories_by_type)

    ######################################################################
    # Helper methods

    def _get_column_txntypes(self, df_report: pd.DataFrame, drop_categories: list[str]) -> pd.Series:
        """Group accounts by type and subtype as defined in the report.

        Args:
            df_report (pd.DataFrame): Report data.
            drop_categories (list[str]): Rows to drop. (modified in-place)

        Returns:
            list[str|None]: TxnType for each row in report.

        Raises:
            MnyXlsParseError
        """
        section_header: str | None = None  # Income Categories, Expense Categories, Transfers
        txn_type: TxnType | None = None
        column_txntypes: list[str | None] = []

        # Assign account types to accounts
        for _, row in df_report.iterrows():
            category_or_type, total = row["Category"], row["Total"]
            total_for = category_or_type.split(" ", 1)[-1] if category_or_type.startswith("Total") else None

            if pd.isna(total):
                if not section_header:
                    # Header is always first
                    assert isinstance(category_or_type, str)
                    section_header = category_or_type
                    txn_type = self.section_header_txntype.get(section_header)
                    if txn_type is None:
                        self.fail_parse(f"Unrecognized 'Category' value; did not expect '{category_or_type}'")
                    drop_categories.append(category_or_type)
                else:
                    self.fail_parse(f"Unrecognized 'Category' value; did not expect '{category_or_type}'")
            else:
                if not section_header:
                    self.fail_parse(f"Unrecognized 'Category' value; did not expect '{category_or_type}'")

                if total_for and total_for == section_header:
                    drop_categories.append(category_or_type)
                    section_header = None
                    txn_type = None
                elif not section_header:
                    self.fail_parse(f"Unrecognized 'Category' value; did not expect '{category_or_type}'")

            column_txntypes.append(txn_type.value if txn_type is not None else None)

        assert len(column_txntypes) == len(df_report)

        return pd.Series(column_txntypes)

    def _check_totals(self, df_report: pd.DataFrame, report_grand_total: CurrencyDecimal) -> None:
        """Check that totals are correct."""
        total_income: CurrencyDecimal = df_report[df_report["TxnType"] == TxnType.INCOME.value]["Total"].sum()
        total_expenses: CurrencyDecimal = df_report[df_report["TxnType"] == TxnType.EXPENSE.value]["Total"].sum()
        total_transfers: CurrencyDecimal = df_report[df_report["TxnType"] == TxnType.TRANSFER.value]["Total"].sum()

        grand_total = (total_income - total_expenses) + total_transfers

        if self.config.get("opt_check_total", True) and report_grand_total != grand_total:
            self.log_warning(
                "Report grand total does not match (Income - Expenses) + Transfers: "
                f"{report_grand_total} != ({total_income} - {total_expenses}) + {total_transfers}",
            )

        logger.debug(f"> {self.footer_row_value}: {grand_total}")

    def _apply_filter(self, df_report: pd.DataFrame) -> pd.DataFrame:
        """Apply date filter to report.

        Args:
            df_report (pd.DataFrame): Report data.

        Returns:
            pd.DataFrame
        """
        drop_columns = []
        annual_columns = self.asof_total_columns_map(df_report)
        import_date_from, import_date_to = self.import_date_range

        for col_name, col_date in annual_columns.items():
            if (import_date_from and col_date < import_date_from) or (import_date_to and import_date_to < col_date):
                drop_columns.append(col_name)

        # Drop columns outside of date range
        if drop_columns:
            if len(drop_columns) < len(annual_columns):
                logger.debug(f"> Ignoring {len(drop_columns)} columns outside of date range")
                df_report = df_report.drop(drop_columns, axis=1)
            else:
                # Return empty dataframe if all annual columns were dropped
                logger.debug("> Ignoring report; outside of date range")
                return pd.DataFrame()

        return df_report


######################################################################
# Register report class so it can be included in search list.

MoneyReportIncomeAndSpending.register_report()

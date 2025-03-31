from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING

import pandas as pd

from .report import MoneyReport, ReportType
from .shared import pd_split_category_pair

if TYPE_CHECKING:
    from pathlib import Path

# Use a named logger instead of root logger
logger = logging.getLogger("mnyxls")


######################################################################
# Investment ccount transactions report
#
# Provides:
# - Investment ransactions
#
# Report format:
#
# REPORT TITLE
# All Investment Accounts
# MM/DD/YYYY through MM/DD/YYYY
# Date,Account,Investment,Activity,C,Quantity,Price,Commission,Total,Memo,Transfer Account,Category
# ...


class MoneyReportInvAccountTxns(MoneyReport):
    """Parser for investment account transaction report."""

    report_type = ReportType.INV_ACCOUNT_TRANSACTIONS
    header_row_limit = 5
    required_columns = (
        "Date",
        "Account",
        "Investment",
        "Activity",
        "Quantity",
        "Price",
        "Commission",
        "Total",
        "Transfer Account",
        "Category",
    )

    optional_columns = (
        "C",
        "Memo",
    )

    currency_columns = ("Amount", "Price", "Commission", "Total")

    txn_report_date = "%Y-%m-%d"  # Date format in transaction report

    ######################################################################
    # Helper methods

    def _apply_import_date_range(self, df_report: pd.DataFrame) -> pd.DataFrame:
        """Filter transactions by date range.

        Args:
            df_report (pd.DataFrame): Report data.

        Returns:
            pd.DataFrame
        """
        import_date_from, import_date_to = self.import_date_range

        if import_date_from:
            assert isinstance(import_date_from, date)
            df_skipped = df_report[df_report["Date"] < import_date_from]

            if not df_skipped.empty:
                logger.debug(f"> Ignoring {len(df_skipped)} transactions with dates before {import_date_from}")
                df_report = df_report[df_report["Date"] >= import_date_from]

        if import_date_to:
            assert isinstance(import_date_to, date)
            df_skipped = df_report[df_report["Date"] > import_date_to]

            if not df_skipped.empty:
                logger.debug(f"> Ignoring {len(df_skipped)} transactions with dates after {import_date_to}")
                df_report = df_report[df_report["Date"] <= import_date_to]

        return df_report

    @classmethod
    def _sort_and_reindex_df(cls, df_report: pd.DataFrame) -> pd.DataFrame:
        """Sort by Date while preserving report order, then reset index.

        Args:
            df_report (pd.DataFrame): Report data.

        Returns:
            pd.DataFrame
        """
        df_report = df_report.sort_values(by=["Date", "N"]).reset_index(drop=True)
        df_report.index.name = "N"

        return df_report

    ######################################################################
    # Class methods

    @classmethod
    def is_report_type(cls, report_path: Path) -> ReportType | None:
        """Determine if format of report data file is recognized as this report type.

        Args:
            report_path (Path): Comma-delimited report path.

        Returns:
            ReportType: Report type or None if format is unrecognized.
        """
        # This is the only report that has a "Commission" column
        if any(",Commission," in line_text for _, line_text in cls.iter_report_lines(report_path, cls.header_row_limit)):
            return cls.report_type
        return None

    ######################################################################
    # Abstract methods

    @property
    def report_date_range(self) -> tuple[date | None, date | None]:  # noqa: D102
        if self.df_report is not None:
            df_dates = self.df_report[self.df_report["Date"].notna()]
            return df_dates["Date"].min(), df_dates["Date"].max()
        return None, None

    def parse_report(self) -> None:
        """Read account transaction report into a DataFrame.

        Raises:
            MnyXlsParseError

        Returns:
            DataFrame
        """
        assert len(self.report_paths) == 1

        logger.info(f"Parsing '{self.report_paths[0]}'")
        logger.debug(f"> Type: {self.report_name} report")

        # Can't use `parse_dates` and `date_format` because "Grand Total" row does not have a date.
        df_report = self.read_report_csv()

        # Index will capture transaction order in the data file
        df_report.index.name = "N"

        df_report["Date"] = pd.to_datetime(df_report["Date"], format=self.txn_report_date, errors="raise").dt.date

        # Remove the space in "Transfer Account" column name
        df_report = df_report.rename(columns={"Transfer Account": "XferAccount"})

        txn_count = len(df_report)
        accounts_count = df_report["Account"].nunique()
        df_dates = df_report[df_report["Date"].notna()]

        min_date = df_dates["Date"].min()
        max_date = df_dates["Date"].max()

        assert isinstance(min_date, date)
        assert isinstance(max_date, date)

        logger.debug(f"> Dates: {min_date} to {max_date}")
        logger.debug(f"> Accounts: {accounts_count}")
        logger.debug(f"> Transactions: {txn_count}")

        # Split Category into Category and Subcategory
        categories_split = df_report["Category"].apply(pd_split_category_pair)
        df_report["Category"] = categories_split.str[0]
        df_report["Subcategory"] = categories_split.str[1]

        df_report = self._apply_import_date_range(df_report)

        if not df_report.empty:
            self.df_report = self._sort_and_reindex_df(df_report)

    ######################################################################
    # Instance methods

    def combine(self, report2: MoneyReport) -> None:  # noqa: D102
        super().combine(report2)

        assert isinstance(report2, type(self))
        assert report2.df_report is not None

        if self.empty_report:
            self.df_report = report2.df_report.copy()
            return

        # Combine in order of earliest date to minimize overall sorting
        if self.df_report["Date"].iloc[0] < report2.df_report["Date"].iloc[0]:
            concat_dfs = [self.df_report, report2.df_report]
        else:
            concat_dfs = [report2.df_report, self.df_report]

        self.df_report = pd.concat(concat_dfs)

        self.df_report = self._sort_and_reindex_df(self.df_report)


######################################################################
# Register report class so it can be included in search list.

MoneyReportInvAccountTxns.register_report()

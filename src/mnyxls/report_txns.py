from __future__ import annotations

import logging
import typing
from datetime import date
from typing import TYPE_CHECKING

import pandas as pd

from .report import MoneyReport, ReportType
from .shared import pd_split_category_pair

if TYPE_CHECKING:
    from pathlib import Path

    from .currencydecimal import CurrencyDecimal

# Use a named logger instead of root logger
logger = logging.getLogger("mnyxls")


######################################################################
# Account transactions report
#
# Provides:
# - Transactions
#
# Report format:
#
# REPORT TITLE
# All accounts
# Converted to base currency: US dollar
# MM/DD/YYYY through MM/DD/YYYY
# Num,Date,Payee,C,Account,Memo,Category,Amount,Running Balance
# ,YYYY-MM-DD,PAYEE,CLEARED,ACCOUNT,MEMO,CATEGORY,-MM.NNNN,-MM.NNNN
# ...
# Grand Total,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN


class MoneyReportAccountTxns(MoneyReport):
    """Parser for account transaction report."""

    report_type = ReportType.ACCOUNT_TRANSACTIONS
    header_row_limit = 5
    required_columns = (
        "Num",
        "Date",
        "Payee",
        "Account",
        "Category",
        "Amount",
    )

    optional_columns = (
        "C",
        "Memo",
    )

    footer_row_value = "Grand Total"
    currency_columns = ("Amount",)

    txn_report_date = "%Y-%m-%d"  # Date format in transaction report

    ######################################################################
    # Helper methods

    def _fix_splits(self, df_txn: pd.DataFrame) -> pd.DataFrame:
        """Mark split transactions.

        Args:
            df_txn (pd.DataFrame): Transaction data.

        Raises:
            MnyXlsParseError:

        Returns:
            pd.DataFrame
        """
        # Modify a copy (to protect caller)
        df_txn = df_txn.copy()

        idxprev = None  # Index of previous transaction
        idxsplit = None  # Index of the transaction that is split

        for idxtxn, txn in df_txn.iterrows():
            # I spent HOURS trying to find a way to satisfy type checking for the `.loc[]` calls below!
            # None of the tutorials, examples, or documentation I could find addressed the issue :(
            # This is my partial solution... the other is to simply `ignore` type checking.
            idxtxn = typing.cast("int", idxtxn)

            if pd.notna(txn["Date"]):
                idxsplit = None
                idxprev = idxtxn
                continue

            if idxsplit is None:
                # Found a split transaction!
                assert idxprev is not None
                idxsplit = idxprev
                df_txn.loc[idxsplit, "Split"] = "X"

            # Splits have an Amount, Memo and Category but no Date, Payee or Account
            update_cols = ["Split", "Num", "Date", "Payee", "Account"]
            if pd.isna(txn["Memo"]):
                update_cols.append("Memo")

            update_vals = df_txn.loc[idxsplit, update_cols].tolist()  # pyright: ignore[reportCallIssue, reportArgumentType]
            update_vals[0] = "Y"

            df_txn.loc[idxtxn, update_cols] = update_vals

        # Exclude split parents from transactions
        return df_txn[df_txn["Split"] != "X"]

    def _check_totals(self, df_report: pd.DataFrame, report_grand_total: CurrencyDecimal) -> None:
        """Check that totals are correct."""
        report_total = df_report["Amount"].sum()

        if self.config.get("opt_check_total", True) and report_total != report_grand_total:
            logger.warning(
                f"'{self.report_path_name}': Report 'Grand Total' does not match sum of transaction amounts: {report_total} != {report_grand_total}"
            )

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
        # We don't check the footer row because this is the only report that has a "Num" column
        # so there's no need to read the entire file to determine the report type.
        if cls.has_line_that_startswith(report_path, f"{cls.required_columns[0]},", cls.header_row_limit):
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

        grand_total_row = df_report[df_report["Num"] == self.footer_row_value]
        if grand_total_row.empty:
            self.fail_parse("Expected to find a row named 'Grand Total'")

        # Capture grand total then drop.
        if not grand_total_row.empty:
            report_grand_total = grand_total_row["Amount"].iloc[0]
            df_report = df_report.drop(grand_total_row.index)
        else:
            report_grand_total = None

        # Convert dates *after* dropping "Grand Total"
        df_report["Date"] = pd.to_datetime(df_report["Date"], format=self.txn_report_date, errors="raise").dt.date

        # Count before deleting void and splits
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

        # Set fields for splits
        df_report["Split"] = pd.NA
        df_report = self._fix_splits(df_report)

        # Exclude void transactions (delete *after* splits)
        df_report = df_report[df_report["Num"] != "**VOID**"]

        # Check *before* filtering dates
        if report_grand_total is not None:
            self._check_totals(df_report, report_grand_total)

        # Filter *after* setting dates for splits
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

MoneyReportAccountTxns.register_report()

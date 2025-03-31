from __future__ import annotations

import logging
import re
from datetime import datetime

import pandas as pd

from .report import MoneyReport, ReportType
from .report_mixinasofcolumns import MoneyReportMixinAsOfColumns

# Use a named logger instead of root logger
logger = logging.getLogger("mnyxls")


######################################################################
# Account balances with details report
#
# We can set these account properties with this report:
# - Bank name
# - Account number
# - Account limit
# - Opening balance
#
# This report does not denote the report date in the export.
# We use the file modification time by default.
# The report can also be defined by including the following header line:
# "As of MM/DD/YYYY"
#
# Report format:
#
# REPORT TITLE
# Converted to base currency: US dollar
# ,,,,Account,Opening,Current
# Account,Abbreviation,Bank Name,Account Number,Limit,Balance,Balance
# ACCOUNT,,,,,,MM.NNNN
# ...


class MoneyReportAccountBalancesWithDetails(MoneyReportMixinAsOfColumns, MoneyReport):
    """Parser for account balances with details report."""

    report_type = ReportType.ACCOUNT_BALANCES_DETAILS
    header_row_limit = 5

    # Column names span two rows in the report but we start the import
    # from the second row so there are two columns named "Balance".
    required_columns = (
        "Account",  # Sorted by this column
        "Balance",  # Opening balance
        "Balance.1",  # Current balance (renamed to report date)
    )

    optional_columns = (
        "Abbreviation",
        "Bank Name",
        "Account Number",
        "Limit",
    )

    currency_columns = (
        "Limit",
        "Balance",  # Opening balance
        "Balance.1",  # Current balance (column name at parse time)
    )

    ######################################################################
    # Abstract methods

    def parse_report(self) -> None:  # noqa: D102
        assert len(self.report_paths) == 1

        logger.info(f"Parsing '{self.report_paths[0]}'")
        logger.debug(f"> Type: {self.report_name} report")

        as_of_date = None

        # Find report date (with a user defined header)
        for _, line_text in self.iter_report_lines(self.report_paths[0], self.header_row_limit):
            m = re.match(r"As of ([\d/]+)", line_text)
            if m:
                as_of_date = self.parse_mmddyyyy(m.group(1))
                break

        if not as_of_date:
            mtime = self.report_paths[0].stat().st_mtime
            as_of_date = datetime.fromtimestamp(mtime).date()  # noqa: DTZ006
            logger.debug(f"> Using file modification time as report date: {as_of_date}")

        import_date_from, import_date_to = self.import_date_range

        if import_date_from and as_of_date < import_date_from:
            logger.debug(f"> Skipping report; Report date is before {import_date_from}")
            return

        if import_date_to and import_date_to < as_of_date:
            logger.debug(f"> Skipping report; Report date is after {import_date_to}")
            return

        df_report = self.read_report_csv()

        grand_total = df_report["Balance.1"].sum()

        # Rename "Current Balance" column so report can be combined with others.
        df_report = df_report.rename(
            columns={
                "Balance": "Opening Balance",
                "Balance.1": as_of_date.strftime(self.report_date_format),
            }
        )

        logger.debug(f"> As of: {as_of_date}")
        logger.debug(f"> Grand total (calculated): {grand_total}")

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

        # Merge accounts and balances
        self.df_report = self.df_report.merge(
            report2.df_report,
            on=self.required_columns[0],
            how="outer",
            suffixes=("_df1", "_df2"),
        )

        # Merge overlapping columns
        overlapping_columns = [col.removesuffix("_df1") for col in self.df_report.columns if col.endswith("_df1")]
        for col_name in overlapping_columns:
            # https://github.com/pandas-dev/pandas/issues/57734
            with pd.option_context("future.no_silent_downcasting", True):  # noqa: FBT003
                self.df_report[col_name] = self.df_report[f"{col_name}_df2"].fillna(self.df_report[f"{col_name}_df1"])

        # Sort by account name
        self.df_report = self.df_report.sort_values(self.required_columns[0], ignore_index=True)

        # Drop redundant columns and reorder annual columns by ascending dates
        self.df_report = self.df_report.reindex(
            columns=[
                "Account",
                "Opening Balance",  # self.required_columns["Balance"]
                *self.optional_columns,
                *self.asof_total_columns_names(self.df_report),
            ]
        )


######################################################################
# Register report class so it can be included in search list.

MoneyReportAccountBalancesWithDetails.register_report()

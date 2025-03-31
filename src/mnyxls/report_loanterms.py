from __future__ import annotations

import logging
import re
from collections import defaultdict
from datetime import datetime
from types import MappingProxyType

import pandas as pd

from .report import MoneyReport, ReportType
from .report_mixinasofcolumns import MoneyReportMixinAsOfColumns
from .shared import split_category_pair

PayeeCategoryByLoanT = dict[
    str,  # loan
    dict[str, tuple[str, str | None]],  # [payee] = (category, subcategory)
]

# Use a named logger instead of root logger
logger = logging.getLogger("mnyxls")


######################################################################
# Loan terms
#
# We can set loan account properties with this report.
#
# This report does not denote the report date in the export.
# We use the file modification time by default.
# The report can also be defined by including the following header line:
# "As of MM/DD/YYYY"
#
# Report format:
#
# REPORT TITLE
# ,,Loan,Loan,Interest,,Principal +,,Balloon,Interest,,,Other,,Payment,
# Loan,Abbreviation,Number,Amount,Rate,Length,Interest,Frequency,Amount,category,Payee,Payment,Fees,Type,Interest,Balance
# ACCOUNT,,,MM.NN,MM%,TERM,MM.NN,Monthly,MM.NN,Bank Charges : Interest,PAYEE,MM.NN,MM.NN,Borrowed,When due,MM.NN
# ,,,,,,,,,Principal Transfer : PAYEE,Principal,,,,,
# ,,,,,,,,,Bank Charges : Interest,Interest,,,,,
# ...


class MoneyReportLoanTerms(MoneyReportMixinAsOfColumns, MoneyReport):
    """Parser for loan terms report."""

    report_type = ReportType.LOAN_TERMS
    header_row_limit = 4

    # Column names span two rows in the report but we start the import
    # from the second row so there are two columns for some fields.
    required_columns = (
        "Loan",  # Account; Sorted by this column
        "Amount",  # Loan amount
        "Rate",
        "Length",
        "Interest",  # Principal + Interest
        "Frequency",
        "Amount.1",  # Balloon amount
        "category",  # "Category : Subcategory" related to Payee
        "Payee",
        "Payment",
        "Fees",
        "Type",
        "Interest.1",  # Interest is calculated... (not a number)
        "Balance",
    )

    optional_columns = (
        "Abbreviation",
        "Number",
    )

    currency_columns = (
        "Amount",
        "Rate",
        "Interest",
        "Amount.1",
        "Payment",
        "Fees",
        "Balance",  # Current balance (column name at parse time)
    )

    rename_columns = MappingProxyType(
        {
            "Number": "AccountNumber",
            "Amount": "LoanAmount",
            "Rate": "InterestRate",
            "Interest": "PrincipalInterest",
            "Amount.1": "BalloonAmount",
            "category": "Category",
            "Interest.1": "InterestWhen",
        }
    )

    def __init__(  # noqa: D107
        self,
        payee_category_by_loan: PayeeCategoryByLoanT | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.payee_category_by_loan = payee_category_by_loan or {}

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

        logger.debug(f"> As of: {as_of_date}")

        import_date_from, import_date_to = self.import_date_range

        if import_date_from and as_of_date < import_date_from:
            logger.debug(f"> Skipping report; Report date is before {import_date_from}")
            return

        if import_date_to and import_date_to < as_of_date:
            logger.debug(f"> Skipping report; Report date is after {import_date_to}")
            return

        df_report = self.read_report_csv()

        cur_loan = None
        payee_category_by_loan = defaultdict(dict)

        # Each loan has multiple associated payees and categories
        for row in df_report.itertuples(index=False):
            if pd.notna(row.Loan):
                cur_loan = str(row.Loan)

            assert cur_loan is not None

            payee_category_by_loan[cur_loan][str(row.Payee)] = split_category_pair(str(row.category))

        self.payee_category_by_loan = dict(payee_category_by_loan)

        # Drop rows defining categories and payees
        df_report = df_report.dropna(subset=[self.required_columns[0]])

        # This report has two row column headers so some adjustments are necessary,
        # and "Balance" must be renamed so report can be combined with others.
        df_report = df_report.rename(
            columns={
                **self.rename_columns,
                "Balance": as_of_date.strftime(self.report_date_format),
            }
        )

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

        # Sort by account name
        df_report = df_report.sort_values(self.required_columns[0], ignore_index=True)

        # Drop redundant columns and reorder annual columns by ascending dates
        df_report = df_report.reindex(
            columns=[
                *[
                    self.rename_columns.get(col, col)
                    for col in [
                        *self.required_columns,
                        *self.optional_columns,
                    ]
                    if col not in ("Balance",)  # included as mm/dd/yyyy
                ],
                *self.asof_total_columns_names(df_report),
            ]
        )

        self.df_report = df_report
        self.payee_category_by_loan = {*self.payee_category_by_loan, *report2.payee_category_by_loan}


######################################################################
# Register report class so it can be included in search list.

MoneyReportLoanTerms.register_report()

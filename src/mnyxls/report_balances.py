from __future__ import annotations

import logging
import re
from collections import defaultdict
from typing import TYPE_CHECKING

import pandas as pd

from .report import MoneyReport, ReportType
from .report_mixinasofcolumns import MoneyReportMixinAsOfColumns

if TYPE_CHECKING:
    from .currencydecimal import CurrencyDecimal

AccountsByTypeT = dict[tuple[str, str], list[str]]  # (Account type, Account subtype) -> [Account names]

# Use a named logger instead of root logger
logger = logging.getLogger("mnyxls")


######################################################################
# Account balances report
#
# We can set these account properties with this report:
# - Account type (Bank, Investment, Asset, Liability, etc.)
#
# I'm not sure if this is a bug or caused by data corruption, but my
# balance reports include accounts that were opened *after* the report as-of date.
#
# Report format:
#
# REPORT TITLE
# Converted to base currency: US dollar
# As of MM/DD/YYYY
# Account,Total
# Assets,
# Bank and Cash Accounts
# ACCOUNT,MM.NNNN
# ...
# Total Bank and Cash Accounts,MM.NNNN
# Other Assets
# ACCOUNT,MM.NNNN
# ...
# Total Other Assets,MM.NNNN
# Investment Accounts
# ACCOUNT,MM.NNNN
# ...
# Total Investment Accounts,MM.NNNN
# Total Assets,MM.NNNN
# Liabilities,
# Credit Cards
# ACCOUNT,MM.NNNN
# ...
# Total Credit Cards,MM.NNNN
# Other Liabilities
# ACCOUNT,MM.NNNN
# ...
# Total Other Liabilities,MM.NNNN
# Loans
# ACCOUNT,MM.NNNN
# ...
# Total Loans,MM.NNNN
# Total Liabilities,MM.NNNN
# Net Worth,MM.NNNN


class MoneyReportAccountBalances(MoneyReportMixinAsOfColumns, MoneyReport):
    """Parser for account balances report."""

    report_type = ReportType.ACCOUNT_BALANCES
    header_row_limit = 4
    required_columns = (
        "Account",  # Sorted by this column
        "Total",
    )
    currency_columns = ("Total",)
    footer_row_value = "Net Worth"

    def __init__(  # noqa: D107
        self,
        accounts_by_type: AccountsByTypeT | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.accounts_by_type: AccountsByTypeT = accounts_by_type or {}

    ######################################################################
    # Abstract methods

    def parse_report(self) -> None:  # noqa: C901, D102
        assert len(self.report_paths) == 1

        logger.info(f"Parsing '{self.report_paths[0]}'")
        logger.debug(f"> Type: {self.report_name} report")

        as_of_date = None

        # Find report date
        for _, line_text in self.iter_report_lines(self.report_paths[0], self.header_row_limit):
            m = re.match(r"As of ([\d/]+)", line_text)
            if m:
                as_of_date = self.parse_mmddyyyy(m.group(1))
                break

        if not as_of_date:
            self.fail_parse("Failed to find report date; looking for 'As of MM/DD/YYYY'")

        import_date_from, import_date_to = self.import_date_range

        if import_date_from and as_of_date < import_date_from:
            logger.debug(f"> Skipping report; Report date is before {import_date_from}")
            return

        if import_date_to and import_date_to < as_of_date:
            logger.debug(f"> Skipping report; Report date is after {import_date_to}")
            return

        df_report = self.read_report_csv()

        net_worth_row = df_report[df_report["Account"] == "Net Worth"]
        if net_worth_row.empty:
            self.fail_parse("Expected to find a row named 'Net Worth'")

        # Capture grand total "Net Worth" then drop.
        if not net_worth_row.empty:
            report_net_worth = net_worth_row["Total"].iloc[0]
            df_report = df_report.drop(net_worth_row.index)
        else:
            report_net_worth = None

        drop_account_names: list[str] = []

        self.accounts_by_type = self._get_accounts_by_type(df_report, drop_account_names)

        # Drop rows for Account types and "Total *"
        df_report = df_report[~df_report["Account"].isin(drop_account_names)]

        if not df_report[df_report["Total"].isna()].empty:
            self.fail_parse("'Total' column has an Account without a value?")

        if report_net_worth is not None:
            self._check_totals(df_report, report_net_worth)

        # Rename "Total" column so report can be combined with others.
        df_report = df_report.rename(
            columns={
                "Total": as_of_date.strftime(self.report_date_format),
            }
        )

        logger.debug(f"> As of: {as_of_date}")
        logger.debug(f"> Accounts: {len(df_report)}")

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
            self.accounts_by_type = report2.accounts_by_type.copy()
            return

        self.df_report = self.df_report.merge(report2.df_report, on=self.required_columns[0], how="outer")

        # Sort by account name
        self.df_report = self.df_report.sort_values(self.required_columns[0], ignore_index=True)

        # Reorder columns to ascending dates
        asof_total_col_names = self.asof_total_columns_names(self.df_report)
        self.df_report = self.df_report.reindex(columns=[*self.required_columns, *asof_total_col_names])

        combined_accounts_by_type: AccountsByTypeT = {}

        # Combine account names by type
        for account_type_subtype in sorted(set(self.accounts_by_type.keys()) | set(report2.accounts_by_type.keys())):
            account_names_1 = self.accounts_by_type.get(account_type_subtype, [])
            account_names_2 = report2.accounts_by_type.get(account_type_subtype, [])

            combined_accounts_by_type[account_type_subtype] = sorted(set(account_names_1 + account_names_2))

        self.accounts_by_type = combined_accounts_by_type

    ######################################################################
    # Helper methods

    def _get_accounts_by_type(self, df_report: pd.DataFrame, drop_account_names: list[str]) -> AccountsByTypeT:
        """Group accounts by type and subtype as defined in the report.

        Args:
            df_report (pd.DataFrame): Report data.
            drop_account_names (list[str]): Rows to drop. (modified in-place)

        Raises:
            MnyXlsParseError
        """
        account_type: str | None = None  # Assets, Liabilities
        account_subtype: str | None = None  # Bank and Cash Accounts, Credit Cards
        accounts_by_type: AccountsByTypeT = defaultdict(list)

        # Assign account types to accounts
        for _, row in df_report.iterrows():
            account_name, total = row["Account"], row["Total"]
            total_for = account_name.split(" ", 1)[-1] if account_name.startswith("Total") else None

            if pd.isna(total):
                if not account_type:
                    # Type is always first
                    account_type = account_name
                    account_subtype = None
                    drop_account_names.append(account_name)
                elif not account_subtype:
                    # Type is always followed by a subtype
                    account_subtype = account_name
                    drop_account_names.append(account_name)
                else:
                    self.fail_parse(f"Unrecognized 'Account' value; did not expect '{account_name}'")
            else:
                if not account_type:
                    self.fail_parse(f"Unrecognized 'Account' value; did not expect '{account_name}'")

                if total_for and total_for == account_type:
                    drop_account_names.append(account_name)
                    account_type = None
                    account_subtype = None
                elif total_for and total_for == account_subtype:
                    drop_account_names.append(account_name)
                    account_subtype = None
                elif account_subtype:
                    accounts_by_type[(account_type, account_subtype)].append(account_name)
                else:
                    self.fail_parse(f"Unrecognized 'Account' value; did not expect '{account_name}'")

        return accounts_by_type

    def _check_totals(self, df_report: pd.DataFrame, report_net_worth: CurrencyDecimal) -> None:
        """Check that totals are correct."""
        asset_account_names = [
            account_name
            for account_type_subtype, account_names in self.accounts_by_type.items()
            for account_name in account_names
            if account_type_subtype[0] == "Assets"
        ]

        liability_account_names = [
            account_name
            for account_type_subtype, account_names in self.accounts_by_type.items()
            for account_name in account_names
            if account_type_subtype[0] == "Liabilities"
        ]

        report_total = df_report["Total"].sum()
        total_assets = df_report[df_report["Account"].isin(asset_account_names)]["Total"].sum()
        total_liabilities = df_report[df_report["Account"].isin(liability_account_names)]["Total"].sum()

        # Liabilities are positive values in the report
        grand_total = total_assets + total_liabilities
        net_worth = total_assets - total_liabilities

        if self.config.get("opt_check_total", True):
            if report_total != grand_total:
                self.log_warning(
                    f"Sum of Account balances does not match assets plus liabilities: {report_total} != {total_assets} + {total_liabilities}",
                )

            if report_net_worth != net_worth:
                self.log_warning(
                    f"Report 'Net worth' does not match assets minus liabilities: {report_net_worth} != {total_assets} - {total_liabilities}",
                )

        logger.debug(f"> Calculated net worth: {net_worth}")


######################################################################
# Register report class so it can be included in search list.

MoneyReportAccountBalances.register_report()

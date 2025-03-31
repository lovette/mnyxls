from __future__ import annotations

from types import MappingProxyType

from .report import ReportType, TxnType
from .report_spending import MoneyReportIncomeAndSpending

######################################################################
# Monthly income and expenses report
#
# Required export options:
# - Rows: Categories
# - Columns: Years
#
# We can set these account properties with this report:
# - Same as "Income and spending" report but with more transfers
#
# Report format:
#
# REPORT TITLE
# MM/DD/YYYY through MM/DD/YYYY
# ,MM/DD/YYYY -,MM/DD/YYYY -,MM/DD/YYYY -,MM/DD/YYYY -,MM/DD/YYYY -,MM/DD/YYYY -,MM/DD/YYYY -,MM/DD/YYYY -,MM/DD/YYYY -,MM/DD/YYYY -,
# Category,MM/DD/YYYY,MM/DD/YYYY,MM/DD/YYYY,MM/DD/YYYY,MM/DD/YYYY,MM/DD/YYYY,MM/DD/YYYY,MM/DD/YYYY,MM/DD/YYYY,MM/DD/YYYY,Total
# Income,,,,,,,,,,,
# ACCOUNT,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN
# ...
# Total Income,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN
# Expenses,,,,,,,,,,,
# ACCOUNT,,,,,,,,,MM.NNNN,,MM.NNNN
# ...
# Total Expenses,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN,MM.NNNN
# Transfers,,,,,,,,,,,
# ACCOUNT,,,,,-MM.NNNN,,,,,,-MM.NNNN
# ...
# Total Transfers,,,,-MM.NNNN,-MM.NNNN,-MM.NNNN,-MM.NNNN,-MM.NNNN,-MM.NNNN,MM.NNNN,-MM.NNNN
# Income less Expenses,MM.NNNN,MM.NNNN,-MM.NNNN,-MM.NNNN,MM.NNNN,-MM.NNNN,MM.NNNN,MM.NNNN,-MM.NNNN,MM.NNNN,MM.NNNN


class MoneyReportMonthlyIncomeAndExpenses(MoneyReportIncomeAndSpending):
    """Parser for monthly income and expenses report."""

    report_type = ReportType.MONTHLY_INCOME_EXPENSES
    footer_row_value = "Income less Expenses"

    # Section headers differ slightly between "Income and spending" and "Monthly income and expenses" reports.
    section_header_txntype = MappingProxyType(
        {
            "Income": TxnType.INCOME,
            "Expenses": TxnType.EXPENSE,
            "Transfers": TxnType.TRANSFER,
        }
    )


######################################################################
# Register report class so it can be included in search list.

MoneyReportMonthlyIncomeAndExpenses.register_report()

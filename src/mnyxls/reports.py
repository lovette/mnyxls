from __future__ import annotations

import logging
from collections import defaultdict
from functools import reduce
from typing import TYPE_CHECKING

from .report import MoneyReport

# Import all report classes so they register themselves with `register_report`.
from .report_balances import MoneyReportAccountBalances  # noqa: F401
from .report_balancesdetails import MoneyReportAccountBalancesWithDetails  # noqa: F401
from .report_investmenttxns import MoneyReportInvAccountTxns  # noqa: F401
from .report_loanterms import MoneyReportLoanTerms  # noqa: F401
from .report_monthly import MoneyReportMonthlyIncomeAndExpenses  # noqa: F401
from .report_spending import MoneyReportIncomeAndSpending  # noqa: F401
from .report_txns import MoneyReportAccountTxns  # noqa: F401

if TYPE_CHECKING:
    from pathlib import Path

    import click

    from .configtypes import MainConfigFileT
    from .report import ReportType


# https://docs.python.org/3/library/pathlib.html#pattern-language
GLOBCHARS = ("*", "?", "[", "]")

# Use a named logger instead of root logger
logger = logging.getLogger("mnyxls")


######################################################################
# Module public


def gather_reports(
    ctx: click.Context,
    report_paths: tuple[Path, ...],
    config: MainConfigFileT,
) -> dict[ReportType, list[MoneyReport]]:
    """Gather and determine report format for set of comma-delimited reports.

    Will invoke `ctx.fail()` if any of the reports are not found or are not files.

    Args:
        ctx (click.Context): Click context.
        report_paths (list[Path]): Set of comma-delimited report path.
        config (ConfigFileT): Configuration options.

    Returns:
        dict[ReportType, MoneyReport] | None: Dictionary of parsed reports by type; None if no reports were parsed.
    """
    reports_by_type: dict[ReportType, list[MoneyReport]] = defaultdict(list)

    # Expand globs
    if any(True for report_path in report_paths if any(char in report_path.name for char in GLOBCHARS)):
        new_report_paths = []

        for report_path in report_paths:
            if "*" in report_path.name:
                new_report_paths.extend(report_path.parent.glob(report_path.name))
            else:
                new_report_paths.append(report_path)

        report_paths = tuple(new_report_paths)

    report_paths = tuple(sorted(set(report_paths)))

    logger.debug(f"Parsing {len(report_paths)} report paths")

    # Gather reports
    for report_path in report_paths:
        if not report_path.exists():
            ctx.fail(f"'{report_path}': No such file.")
        elif not report_path.is_file():
            ctx.fail(f"'{report_path}': Is not a file.")

        report_cls = MoneyReport.get_report_class(report_path)

        if not report_cls:
            ctx.fail(f"'{report_path.name}': Unrecognized report format.")

        report = report_cls(report_path=report_path, config=config)
        assert report.report_type is not None
        reports_by_type[report.report_type].append(report)

    assert reports_by_type, "No reports found."

    return reports_by_type


def parse_reports(
    reports_by_type: dict[ReportType, list[MoneyReport]],
    config: MainConfigFileT,
) -> dict[ReportType, MoneyReport] | None:
    """Parse set of comma-delimited reports of various types.

    Args:
        reports_by_type: dict[ReportType, list[MoneyReport]]: Money reports by type.
        config (MainConfigFileT): Configuration options.

    Returns:
        dict[ReportType, MoneyReport] | None: Dictionary of parsed reports by type; None if no reports were parsed.
    """
    report_for_type: dict[ReportType, MoneyReport] = {}

    # Parse reports
    for reports_for_type in reports_by_type.values():
        for report in reports_for_type:
            report.parse_report()

    # Combine reports into one for each type
    for report_type, reports_for_type in reports_by_type.items():
        report = reports_for_type[0]

        if len(reports_for_type) > 1:
            cls = type(report)
            report = reduce(cls.combine_reports, reports_for_type)

        if not report.empty_report:
            report_for_type[report_type] = report

    # Save interim reports for review
    if config.get("save_interim_reports", False):
        for report in report_for_type.values():
            report.save_interim_report()

    return report_for_type or None

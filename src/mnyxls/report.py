from __future__ import annotations

import logging
import typing
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum, auto, unique
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn

import numpy as np
import pandas as pd

from .currencydecimal import currency_from_value
from .shared import MnyXlsRuntimeError, resolve_rel_path

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable, Iterator, Sequence
    from datetime import date

    from pandas._typing import Dtype

    from .configtypes import MainConfigFileT


# Money reports can contain smart quotes that exist in Windows-1252 encoding.
# Using utf-8 will result in UnicodeDecodeError: 'utf8' codec can't decode byte 0x92 ...
REPORT_ENCODING = "cp1252"


@unique
class TxnType(Enum):
    """Transaction types.

    User-defined categories are not included.
    Categories not assigned a TxnType are set to EXPENSE (user configurable).
    """

    INCOME = "Income"
    EXPENSE = "Expense"
    TRANSFER = "Transfer"
    INVESTMENT = "Investment"


@unique
class TxnClass(Enum):
    """Transaction classifications.

    User-defined categories are not included.
    TxnClass for transfers are not user configurable.
    TxnClass for income categories defaults to its TxnType.
    Categories not assigned a TxnClass are set to DISCRETIONARY (user configurable).
    """

    INCOME = TxnType.INCOME.value
    TRANSFER = TxnType.TRANSFER.value
    DISCRETIONARY = "Discretionary"
    ESSENTIAL = "Essential"


@unique
class AccountClassification(Enum):
    """Predefined account classifications.
    User-defined classifications are not included.
    """

    # Classifications Money uses in reports.
    ASSET = "Assets"
    LIABILITY = "Liabilities"

    # Internal
    UNDEFINED = "Undefined"


@unique
class AccountCategory(Enum):
    """Predefined account categories.
    User-defined categories are not included.
    """

    # Categories Money uses in reports.
    BANK = "Bank and Cash Accounts"
    CREDITCARD = "Credit Cards"
    INVESTMENT = "Investment Accounts"
    LOAN = "Loans"
    OTHER_ASSETS = "Other Assets"
    OTHER_LIABILITIES = "Other Liabilities"

    # Internal
    UNDEFINED = "Undefined"


# When adding new report type make sure you add to:
# `get_report_class`
# `MoneyReport.report_name`
@unique
class ReportType(Enum):
    """Format of Money report imported."""

    ACCOUNT_BALANCES = auto()
    ACCOUNT_BALANCES_DETAILS = auto()
    ACCOUNT_TRANSACTIONS = auto()
    INCOME_SPENDING = auto()
    INV_ACCOUNT_TRANSACTIONS = auto()
    LOAN_TERMS = auto()
    MONTHLY_INCOME_EXPENSES = auto()


# Categories that Money assigns for account transfers.
TRANSFER_CATEGORIES = (
    "Buy Investment/CD",  # Internal to Money
    "Credit Card Payment",
    "Principal Transfer",  # Referenced in Loan terms report
    "Transfer From",
    "Transfer To",
)

# List of MoneyReport subclasses.
_MONEYREPORTS_CLS: list[type[MoneyReport]] = []

# Use a named logger instead of root logger
logger = logging.getLogger("mnyxls")


######################################################################
# Reports base class


class MoneyReport(ABC):
    """Report parser base class."""

    # Defined by subclasses
    report_type: ReportType | None = None  # Report type
    required_columns: Sequence[str] = ()  # Required header column values
    header_row_limit: int = 10  # Number of header rows to search for header columns (1-based)
    footer_row_value: str | None = None  # Value to find in footer row
    currency_columns: Sequence[str] = ()  # Columns with currency values
    optional_columns: Sequence[str] = ()  # Optional columns

    def __init__(
        self,
        report_path: Path | None = None,
        report_paths: Sequence[Path] | None = None,
        df_report: pd.DataFrame | None = None,
        config: MainConfigFileT | None = None,
    ) -> None:
        """Constructor.

        Args:
            report_path (Path): Path parsed for this report; defaults to None.
            report_paths (Sequence[Path]): Paths combined to make up this report; defaults to None.
            df_report (pd.DataFrame): Report data; defaults to None.
            config (MainConfigFileT): Configuration options; defaults to None.
        """
        if report_path is not None:
            assert report_paths is None
            self.report_paths = (report_path,)
        elif report_paths is not None:
            self.report_paths = tuple(report_paths)
        else:
            self.report_paths = ()

        self.df_report = df_report
        self.config = config or typing.cast("MainConfigFileT", {})

    def __repr__(self) -> str:
        """Return a string representation of the object for debugging purposes.

        Returns:
            str
        """
        report_paths = "' | '".join([str(p.name) for p in self.report_paths])
        report_file = f"'{report_paths}'" if self.report_paths else "No report"

        return f"{self.__class__.__name__}({report_file})"

    ######################################################################
    # Class methods

    @classmethod
    def register_report(cls) -> None:
        """Register report class."""
        _MONEYREPORTS_CLS.append(cls)

    @classmethod
    def get_report_class(cls, report_path: Path) -> type[MoneyReport] | None:
        """Get report class for given report path.

        Args:
            report_path (Path): Path to report.

        Returns:
            type[MoneyReport] | None: Report class or None if not found.
        """
        assert len(_MONEYREPORTS_CLS) > 0

        for reportcls in _MONEYREPORTS_CLS:
            if reportcls.is_report_type(report_path):
                return reportcls

        return None

    @classmethod
    def iter_report_lines(cls, path: Path, max_lines: int = 0) -> Iterator[tuple[int, str]]:
        """Yields line number and text of non-blank lines in a report.

        Args:
            path (Path): Report path.
            max_lines (int): Number of non-blank lines to search. Line numbers are 1-based.

        Yields:
            tuple[int, str]: (Line number, Line text)
        """
        lineno = 0

        with path.open(encoding=REPORT_ENCODING) as f:
            for line in f:
                line_text = line.rstrip()
                if line_text:
                    lineno += 1
                    if max_lines and max_lines < lineno:
                        break
                    yield lineno, line_text

    @classmethod
    def has_line_that_startswith(cls, path: Path, text: str, max_lines: int = 0) -> bool:
        """Return True if file contains a line that starts with given text.

        Args:
            path (Path):  File path.
            text (str): Text to search for.
            max_lines (int): Number of lines to search.

        Returns:
            bool
        """
        return any(line_text.startswith(text) for _, line_text in cls.iter_report_lines(path, max_lines))

    def expected_columns(self) -> set[str]:
        """Return set of expected columns.

        Returns:
            set[str]
        """
        return set(self.required_columns) | set(self.optional_columns or [])

    @classmethod
    def is_report_type(cls, report_path: Path) -> ReportType | None:
        """Determine if format of report data file is recognized as this report type.

        Args:
            report_path (Path): Comma-delimited report path.

        Returns:
            ReportType: Report type or None if format is unrecognized.
        """
        if all(
            (
                cls.has_line_that_startswith(report_path, f"{cls.required_columns[0]},", cls.header_row_limit),
                cls.has_line_that_startswith(report_path, f"{cls.footer_row_value},") if cls.footer_row_value else True,
            )
        ):
            return cls.report_type

        return None

    @classmethod
    def combine_reports(cls, report1: MoneyReport, report2: MoneyReport) -> MoneyReport:
        """Combine two reports.

        If one or both reports are empty, the other report is returned.

        Args:
            report1 (MoneyReport): First report.
            report2 (MoneyReport): Second report.

        Returns:
            MoneyReport
        """
        # If one or both are empty, there's nothing to combine!
        if report1 is None or report1.empty_report:
            return report2
        if report2 is None or report2.empty_report:
            return report1

        merged_report = cls()

        merged_report.combine(report1)
        merged_report.combine(report2)

        return merged_report

    ######################################################################
    # Instance properties

    @cached_property
    def report_name(self) -> str:
        """Report name.

        Returns:
            str
        """
        titles = {
            ReportType.ACCOUNT_BALANCES_DETAILS: "Account balances with details",
            ReportType.ACCOUNT_BALANCES: "Account balances",
            ReportType.ACCOUNT_TRANSACTIONS: "Account transactions",
            ReportType.INCOME_SPENDING: "Income and spending",
            ReportType.INV_ACCOUNT_TRANSACTIONS: "Investment account transactions",
            ReportType.LOAN_TERMS: "Loan terms",
            ReportType.MONTHLY_INCOME_EXPENSES: "Monthly income and expenses",
        }

        return titles[self.report_type] if self.report_type else "Report type not set"

    @property
    def empty_report(self) -> bool:
        """Return True if report is empty.

        Returns:
            bool
        """
        return self.df_report.empty if self.df_report is not None else True

    @property
    def import_date_range(self) -> tuple[date | None, date | None]:
        """Return date range for filtering report data.

        Returns:
            tuple[date | None, date | None]: Start date, End date
        """
        assert self.config is not None

        import_date_range = self.config.get("import_date_range", [None, None])
        assert len(import_date_range) == 2  # noqa: PLR2004
        import_date_from, import_date_to = import_date_range

        # We only compare dates when filtering and you can't compare `datetime` with `date`!
        if isinstance(import_date_from, datetime):
            import_date_from = import_date_from.date()
        if isinstance(import_date_to, datetime):
            import_date_to = import_date_to.date()

        return import_date_from, import_date_to

    @property
    def df_report_nonan(self) -> pd.DataFrame:
        """Return report data with NaN values replaced with None for SQL queries.

        Returns:
            pd.DataFrame
        """
        if self.df_report is None:
            raise ValueError("parse_report must be called first.")

        return self.df_report.replace(np.nan, None)

    @property
    def report_path_name(self) -> str:
        """Return report path name.

        Returns:
            str
        """
        if len(self.report_paths) == 1:
            return str(self.report_paths[0].name)
        return self.default_path_stem

    @property
    def default_path_stem(self) -> str:
        """Return path stem report can be saved as.

        Valid only for reports that have been parsed.

        Returns:
            str
        """
        min_date, max_date = self.report_date_range

        if not (min_date or max_date):
            raise ValueError("Report has no date range.")

        if min_date and max_date:
            if min_date == max_date:
                return f"{self.report_name} {min_date}"
            return f"{self.report_name} {min_date} to {max_date}"

        if not min_date:
            raise ValueError("Report should not have 'min' date range.")

        if max_date:
            return f"{self.report_name} {max_date}"

        return "Unnamed report"

    ######################################################################
    # Instance methods

    def log_warning(self, message: str) -> None:
        """Log parsing warning message.

        Args:
            message (str): Warning message.
        """
        message_parts = [
            f"'{self.report_path_name}'",
            message,
        ]

        logger.warning(": ".join([part for part in message_parts if part]))

    def fail_parse(self, message: str) -> NoReturn:
        """Log parsing warning message.

        Args:
            message (str): Warning message.

        Raises:
            MnyXlsParseError
        """
        raise MnyXlsParseError(message, report=self)

    def read_report_csv(
        self,
        **kwargs,
    ) -> pd.DataFrame:
        """Read report data from CSV file.

        Args:
            **kwargs: Additional arguments to pass to `pd.read_csv`.

        Returns:
            pd.DataFrame: _description_
        """
        header_row_lineno = None
        dtypes: dict[Hashable, Dtype] = {}
        converters: dict[str, Callable] = {}
        currency_columns = self.currency_columns or []

        assert len(self.report_paths) == 1

        # Find report header
        for lineno, line_text in self.iter_report_lines(self.report_paths[0], self.header_row_limit):
            if line_text.startswith(f"{self.required_columns[0]},"):
                header_row_lineno = lineno
                break

        if not header_row_lineno:
            self.fail_parse(f"Failed to find header row; looking for '{self.required_columns[0]}'")

        # Data types and custom converters for specific columns.
        # Converters require the entire column is formatted correctly!
        # dtypes and converters are mutex.
        for col in self.expected_columns():
            if col in currency_columns:
                converters[col] = currency_from_value
            else:
                dtypes[col] = str

        # Reports with annual columns haven't set expected_columns yet.
        for col in self.currency_columns:
            converters[col] = currency_from_value

        df_report: pd.DataFrame = pd.read_csv(
            self.report_paths[0],
            encoding=REPORT_ENCODING,
            header=header_row_lineno - 1,
            skip_blank_lines=True,
            dtype=dtypes,
            converters=converters,
            **kwargs,
        )

        df_report = self.drop_unexpected_columns(df_report)

        missing_columns = set(self.required_columns) - set(df_report.columns)
        if missing_columns:
            self.fail_parse(f"Missing columns: {', '.join(missing_columns)}")

        # Drop rows with all NaN
        return df_report.dropna(how="all")

    def save_interim_report(self) -> None:
        """Save interim report data to CSV file."""
        assert self.df_report is not None

        data_dir = self.config.get("data_dir", Path.cwd())
        saveas_name = Path(f"Interim {self.default_path_stem}").with_suffix(".csv")
        saveas_path = resolve_rel_path(saveas_name, data_dir)

        if not self.empty_report:
            logger.debug(f"Saving interim report to '{saveas_path}'")
            self.df_report.to_csv(saveas_path, index=False)
        else:
            logger.info(f"Interim {self.report_name} report is empty, nothing to save.")

    def drop_unexpected_columns(self, df_report: pd.DataFrame) -> pd.DataFrame:
        """Invoked to drop unexpected columns from report data.

        Args:
            df_report (pd.DataFrame): Report data.

        Returns:
            pd.DataFrame: Report data with unexpected columns removed.
        """
        drop_columns = sorted(set(df_report.columns) - self.expected_columns())

        if drop_columns:
            logger.debug(f"> Ignoring columns: {', '.join(drop_columns)}")
            df_report = df_report.drop(drop_columns, axis=1)

        return df_report

    def combine(self, report2: MoneyReport) -> None:
        """Combine a report into this one.

        Combine reports using `combine_reports`, do not invoke this method directly.

        Args:
            report2 (MoneyReport): Second report.
        """
        self.report_paths = tuple(sorted(list(self.report_paths) + list(report2.report_paths)))
        self.config = report2.config  # both have same config

    ######################################################################
    # Abstract methods that derived reports must implement

    @abstractmethod
    def parse_report(self) -> None:
        """Parse comma-delimited Money report data file.

        Raises:
            MnyXlsParseError
        """

    @property
    @abstractmethod
    def report_date_range(self) -> tuple[date | None, date | None]:
        """Report date range.

        If report is for a single date, both values will be the same.

        Returns:
            tuple[date | None, date | None]
        """


######################################################################
# Report parser error class


class MnyXlsParseError(MnyXlsRuntimeError):
    """Error raised if `parse_report` fails."""

    def __init__(self, message: str, report: MoneyReport) -> None:
        """Constructor.

        Args:
            message (str): Error message.
            report (MoneyReport): Report that failed.
        """
        super().__init__(message)
        self.report = report

    def format_message(self) -> str:
        """Format reported error message.

        Returns:
            str
        """
        message = super().format_message()

        message_parts = [
            f"'{self.report.report_path_name}'" if self.report else None,
            message,
        ]

        return ": ".join([part for part in message_parts if part])

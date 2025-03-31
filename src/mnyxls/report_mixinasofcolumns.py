from __future__ import annotations

from datetime import datetime
from operator import itemgetter
from typing import TYPE_CHECKING

from .report import MoneyReport

if TYPE_CHECKING:
    from datetime import date

    import pandas as pd


class MoneyReportMixinAsOfColumns:
    """A mixin class that provides methods to handle 'as of' total columns in a Money report.

    This mixin includes methods to map, retrieve names, and retrieve dates of annual balance columns
    from a given report or DataFrame. It also provides a cached property to get the date range of the report.
    """

    report_date_format = "%m/%d/%Y"

    # This class property is set to satisfy type checking.
    # The actual instance property is set in the derived class.
    df_report = None

    @classmethod
    def asof_total_columns_map(cls, report_or_df: MoneyReport | pd.DataFrame) -> dict[str, date]:
        """Return a mapping of annual balance columns to dates for a report or DataFrame.

        Args:
            report_or_df (MoneyReport | pd.DataFrame): Report or data frame.

        Returns:
            dict[str, date]
        """
        account_asof_total_cols = {}

        df_report = report_or_df.df_report if isinstance(report_or_df, MoneyReport) else report_or_df

        if df_report is None:
            raise ValueError("df_report is None.")

        # Find columns that can be parsed as dates
        for col in df_report.columns:
            dt = cls.parse_mmddyyyy(col)
            if dt:
                account_asof_total_cols[col] = dt

        return dict(sorted(account_asof_total_cols.items(), key=itemgetter(1)))

    @classmethod
    def asof_total_columns_names(cls, report_or_df: MoneyReport | pd.DataFrame) -> list[str]:
        """Return a list of annual balance column names for a report or DataFrame.

        Args:
            report_or_df (MoneyReport | pd.DataFrame): Report or data frame.

        Returns:
            list[str]
        """
        return list(cls.asof_total_columns_map(report_or_df).keys())

    @classmethod
    def asof_total_columns_dates(cls, report_or_df: MoneyReport | pd.DataFrame) -> list[date]:
        """Return a list of annual balance column dates for a report or DataFrame.

        Args:
            report_or_df (MoneyReport | pd.DataFrame): Report or data frame.

        Returns:
            list[date]
        """
        return list(cls.asof_total_columns_map(report_or_df).values())

    @classmethod
    def parse_mmddyyyy(cls, val: str) -> date | None:
        """Parse date in MM/DD/YYYY format.

        Does not raise an exception if date is invalid.

        Args:
            val (str): Value to parse.

        Returns:
            date or None if date is invalid.
        """
        try:
            return datetime.strptime(val, cls.report_date_format).date()  # noqa: DTZ007
        except ValueError:
            return None

    @property
    def report_date_range(self) -> tuple[date | None, date | None]:  # noqa: D102
        if self.df_report is None:
            return None, None

        asof_total_cols = self.asof_total_columns_dates(self.df_report)

        return asof_total_cols[0], asof_total_cols[-1]

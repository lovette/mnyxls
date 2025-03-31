from __future__ import annotations

import logging
import math
from decimal import Context, ConversionSyntax, Decimal, DecimalException
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import EllipsisType

# Use a named logger instead of root logger
logger = logging.getLogger("mnyxls")


######################################################################
# CurrencyDecimal class


class CurrencyDecimal(Decimal):
    """Decimal objects that represent a currency value."""

    moneyfmt_places: int = 2
    """Number of places after the decimal point. Defaults to 2."""

    moneyfmt_curr: str = "$"
    """Currency symbol before the sign (may be blank). Defaults to "$"."""

    moneyfmt_sep: str = ","
    """Grouping separator (comma, period, space, or blank). Defaults to ","."""

    moneyfmt_dp: str = "."
    """Point indicator (comma or period). Only specify as blank when places is zero Defaults to "."."""

    moneyfmt_pos: str = ""
    """Sign for positive numbers: '+', space or blank. Defaults to "."."""

    moneyfmt_neg: str = "-"
    """Sign for negative numbers: '-', '(', space or blank. Defaults to "-"."""

    moneyfmt_trailneg: str = ""
    """Trailing minus indicator:  '-', ')', space or blank. Defaults to ""."""

    def __str__(self) -> str:
        """Compute the “informal” string representation of an object.

        Returns:
            str
        """
        return self._moneyfmt(
            places=self.moneyfmt_places,
            curr=self.moneyfmt_curr,
            sep=self.moneyfmt_sep,
            dp=self.moneyfmt_dp,
            pos=self.moneyfmt_pos,
            neg=self.moneyfmt_neg,
            trailneg=self.moneyfmt_trailneg,
        )

    def __repr__(self) -> str:
        """Return a string representation of the object for debugging purposes.

        Returns:
            str
        """
        return f"{self.__class__.__name__}({self.__str__()})"

    def __format__(self, specifier: str, context: Context | EllipsisType | None = ..., /) -> str:
        """Compute formatted string representation of an object.

        Used by f-strings, the format() built-in function and the str.format() method.

        Args:
            specifier (str): Specifies the format of the result.
            context (Context | None, optional): Context for the conversion. Defaults to None.

        Returns:
            str
        """
        return self.__str__()

    def __neg__(self):  # noqa: ANN204, D105
        return CurrencyDecimal(super().__neg__())

    def __add__(self, other):  # noqa: ANN001, ANN204, D105
        return CurrencyDecimal(super().__add__(other))

    def __radd__(self, other):  # noqa: ANN001, ANN204, D105
        return CurrencyDecimal(super().__radd__(other))

    def __sub__(self, other):  # noqa: ANN001, ANN204, D105
        return CurrencyDecimal(super().__sub__(other))

    def __rsub__(self, other):  # noqa: ANN001, ANN204, D105
        return CurrencyDecimal(super().__rsub__(other))

    def __mul__(self, other):  # noqa: ANN001, ANN204, D105
        return CurrencyDecimal(super().__mul__(other))

    def __rmul__(self, other):  # noqa: ANN001, ANN204, D105
        return CurrencyDecimal(super().__rmul__(other))

    def __divmod__(self, other) -> tuple[CurrencyDecimal, CurrencyDecimal]:  # noqa: ANN001, D105
        a, b = super().__divmod__(other)
        return CurrencyDecimal(a), CurrencyDecimal(b)

    def __rdivmod__(self, other) -> tuple[CurrencyDecimal, CurrencyDecimal]:  # noqa: ANN001, D105
        a, b = super().__rdivmod__(other)
        return CurrencyDecimal(a), CurrencyDecimal(b)

    def __floordiv__(self, other):  # noqa: ANN001, ANN204, D105
        return CurrencyDecimal(super().__floordiv__(other))

    def __rfloordiv__(self, other):  # noqa: ANN001, ANN204, D105
        return CurrencyDecimal(super().__rfloordiv__(other))

    def __truediv__(self, other):  # noqa: ANN001, ANN204, D105
        return CurrencyDecimal(super().__truediv__(other))

    def __rtruediv__(self, other):  # noqa: ANN001, ANN204, D105
        return CurrencyDecimal(super().__rtruediv__(other))

    def nofmt(self) -> str:
        """Return string value without currency formatting.

        Returns:
            str
        """
        return super().__str__()

    def _moneyfmt(self, places: int, curr: str, sep: str, dp: str, pos: str, neg: str, trailneg: str) -> str:  # noqa: PLR0913
        """Convert value to an accounting formatted string.

        Source:
            https://docs.python.org/3/library/decimal.html

        Args:
            places (int, optional): Number of places after the decimal point.
            curr (str, optional): Currency symbol before the sign (may be blank).
            sep (str, optional): Grouping separator (comma, period, space, or blank).
            dp (str, optional): Point indicator (comma or period). Only specify as blank when places is zero.
            pos (str, optional): Sign for positive numbers: '+', space or blank.
            neg (str, optional): Sign for negative numbers: '-', '(', space or blank.
            trailneg (str, optional): trailing minus indicator:  '-', ')', space or blank.

        Returns:
            str

        Example:
            >>> d = CurrencyDecimal('-1234567.8901')
            >>> d.moneyfmt(curr='$')
            '-$1,234,567.89'
            >>> d.moneyfmt(places=0, sep='.', dp='', neg='', trailneg='-')
            '1.234.568-'
            >>> d.moneyfmt(curr='$', neg='(', trailneg=')')
            '($1,234,567.89)'
        """
        q = Decimal(10) ** -places  # 2 places --> '0.01'
        sign, digits, _ = self.quantize(q).as_tuple()
        result = []
        digits = list(map(str, digits))
        build, next_digit = result.append, digits.pop

        if sign:
            build(trailneg)
        for _ in range(places):
            build(next_digit() if digits else "0")
        if places:
            build(dp)
        if not digits:
            build("0")

        i = 0
        while digits:
            build(next_digit())
            i += 1
            if i == 3 and digits:  # noqa: PLR2004
                i = 0
                build(sep)

        build(curr)
        build(neg if sign else pos)

        return "".join(reversed(result))


######################################################################
# Helper functions


def currency_from_value(raw_value: str | float | None, default_value: CurrencyDecimal | None = None) -> CurrencyDecimal | None:  # noqa: C901
    """Capture currency as Decimal instead of float64.

    Why use Decimal?
    https://beepscore.com/website/2018/10/12/using-pandas-with-python-decimal.html

    Store falsy values as None instead of NaN to simplify comparison
    https://pandas.pydata.org/docs/user_guide/missing_data.html

    Args:
        raw_value (str|float): Original data value. Panda will invoke with NaN if empty.
        default_value (str): Data value to return if `raw_value` is empty string.

    Returns:
        Decimal or None if it's an empty string.

    Raises:
        ValueError if value contains unexpected characters.
        DecimalException if value cannot be interpreted as a decimal number.
    """

    def _clean_value(raw_value: str) -> str:
        value = raw_value

        if "," in value:
            value = value.replace(",", "")

        if value[-1] == "%":
            value = value[:-1]  # Allow percentages

        while value != "" and not (value[0].isdigit() or value[0] == "-"):
            if value[0].isspace() or value[-1].isspace():
                logger.warning(f"currency_from_value('{raw_value}'): contains whitespace")
                value = value.strip()
            elif value.startswith("(") and value.endswith(")"):
                # Negative number denoted as (xx.yy)
                value = "-" + value[1:-1]
            else:
                # Remove (presumed) currency symbol
                value = value[1:]
                if value == "" or not (value[0].isdigit() or value[0] == "-"):
                    raise ValueError(f"currency_from_value('{raw_value}'): starts with unexpected characters")

        return value

    if raw_value is None:
        return default_value

    if isinstance(raw_value, float):
        logger.warning(f"currency_from_value({raw_value}): float values can lose precision")
        return CurrencyDecimal(raw_value) if not math.isnan(raw_value) else default_value

    if isinstance(raw_value, str):
        if raw_value == "":
            return default_value

        value = _clean_value(raw_value)

        if value == "":
            raise ValueError(f"currency_from_value('{raw_value}'): contains unexpected characters")
    else:
        value = raw_value  # int? Decimal?

    try:
        return CurrencyDecimal(value)

    except ConversionSyntax as err:
        # Decimal exceptions don't have a user friendly "message", so we can override it
        raise DecimalException(f"Amount column value '{raw_value}' is not a decimal number") from err
    except DecimalException as err:
        # Decimal exceptions don't have a user friendly "message", so we can override it
        raise DecimalException(f"Amount column value '{raw_value}' is not a decimal number") from err


def currency_to_value(value: CurrencyDecimal) -> str | None:
    """Adapt CurrencyDecimal to SQLite string representation.

    Args:
        value (CurrencyDecimal): Currency value.

    Returns:
        str or None if value is None or NaN.
    """
    if value is None:
        return None
    if math.isnan(value):
        return None
    return value.nofmt()

from __future__ import annotations

import json
import logging
import re
import types
import typing
from collections.abc import Container, Mapping, Sequence
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, get_origin, get_type_hints, is_typeddict

import numpy as np
import pandas as pd
import yaml
from click import ClickException
from dateutil.relativedelta import relativedelta
from mysqlstmt import Select

from .currencydecimal import currency_from_value

if TYPE_CHECKING:
    import sqlite3
    from io import TextIOWrapper

    from mysqlstmt import Stmt
    from pandas.api.typing import NAType

    from .configtypes import CommonConfigFileT, WorkbookConfigSelectT, WorkbookConfigT, WorksheetConfigSelectT, WorksheetConfigT

ELLIPSIS_LEN = len("...")

UNION_PART_TYPES = {
    "bool": bool,
    "int": int,
    "pathlib.Path": Path,
    "str": str,
}

# Regular expression to match relative date specifications
# such as "-1d", "-2w", "-3m", "-4y" (the "-" is optional).
RE_REL_DATE = re.compile(r"-?(\d+)([dwmy])")

# Use a named logger instead of root logger
logger = logging.getLogger("mnyxls")


######################################################################
# Exceptions


class MnyXlsRuntimeError(ClickException):
    """Generic app exception."""


class MnyXlsConfigError(MnyXlsRuntimeError):
    """Configuration exception."""

    def __init__(
        self,
        message: str,
        config: CommonConfigFileT | None = None,
        directive: str | Sequence[str] | None = None,
    ) -> None:
        """Constructor.

        Args:
            message (str): Error message.
            config (Mapping[str, Any] | None, optional): Top-level configuration options; defaults to None.
            directive (str | Sequence[str] | None, optional): Offending configuration directive(s); defaults to None.
        """
        super().__init__(message)

        self.config = config
        self.config_file: Path | None = config.get("_config_file") if config else None
        self.directive = directive

    def format_message(self) -> str:
        """Format reported error message.

        Returns:
            str
        """
        message = super().format_message()

        directives = []

        if self.config_file:
            directives.append(self.config_file.name)

        if self.directive:
            directives.extend([self.directive] if isinstance(self.directive, str) else self.directive)

        message_parts = [
            ": ".join([f"'{d}'" for d in directives]),
            message,
        ]

        return ": ".join([part for part in message_parts if part])


######################################################################
# YAML UniqueKeyLoader


# Thanks to the conversation at:
# https://gist.github.com/pypt/94d747fe5180851196eb


class UniqueKeyLoader(yaml.SafeLoader):
    """Load YAML and fail if key is duplicated."""

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> dict:
        """Internal mapping function.

        Args:
            node (yaml.MappingNode): ??
            deep (bool): ??

        Raises:
            ValueError: Duplicate key found.

        Returns:
            dict
        """
        mapping = set()

        for key_node, _value_node in node.value:
            if ":merge" not in key_node.tag:
                key = self.construct_object(key_node, deep=deep)
                if key in mapping:
                    raise ValueError(f"{key!r}: Duplicate key found in YAML.")
                mapping.add(key)

        return super().construct_mapping(node, deep)


######################################################################
# Functions


def read_config_file(config_path: Path) -> dict[str, Any]:
    """Open and parse a JSON or YAML configuration file.

    Args:
        config_path (Path): Config file path.

    Raises:
        MnyXlsConfigError: Failed to open or parse file.

    Returns:
        dict; empty if file does not exist or is empty.
    """

    def _read_json_config(fp: TextIOWrapper) -> dict[str, Any]:
        try:
            d = json.load(fp)
        except json.JSONDecodeError as err:
            raise MnyXlsConfigError(f"{err}") from err

        if not isinstance(d, dict):
            raise MnyXlsConfigError("Configuration file must be a JSON set of key/value pairs.")

        return d

    def _read_yaml_config(fp: TextIOWrapper) -> dict[str, Any]:
        try:
            d = yaml.load(fp.read(), Loader=UniqueKeyLoader)  # noqa: S506
        except (yaml.YAMLError, ValueError) as err:
            raise MnyXlsConfigError(f"{err}") from err

        if not isinstance(d, dict):
            raise MnyXlsConfigError("Configuration file must be a YAML set of key/value pairs.")

        return d

    try:
        if config_path.is_file() and config_path.stat().st_size:
            logger.debug(f"Reading {config_path}")
            with config_path.open() as fp:
                return _read_json_config(fp) if config_path.suffix == ".json" else _read_yaml_config(fp)

    except OSError as err:
        raise MnyXlsConfigError(f"{err}") from err

    return {}


def pd_read_sql(  # noqa: C901
    conn: sqlite3.Connection,
    q: Stmt,
    date_cols: Sequence[str] | dict[str, str] | None = None,
    currency_cols: Sequence[str] | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Execute a mysqlstmt query against a SQLite database connection and return a DataFrame.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        q (Stmt): mysqlstmt
        date_cols (Sequence[str] | None): Columns to convert to `datetime.date`.
            Can be a dictionary of columns to `parse_dates` format.
        currency_cols (Sequence[str] | None): Columns to convert to CurrencyDecimal.
        kwargs (kwargs): Additional arguments for `read_sql`.

    Returns:
        DataFrame
    """
    dtypes: dict[str, Any] = {}
    parse_dates = None

    if date_cols:
        assert "parse_dates" not in kwargs
        parse_dates = date_cols

    if currency_cols:
        # Dealing with currency columns in pandas is a bit tricky.
        # In short, we want to create `CurrencyDecimal` objects from the string value to retain
        # precision and therefore do not want pandas to convert currency columns to float.
        #
        # I thought using `dtype=object` was the solution, but it turns out that pandas converts
        # to float *before* applying the dtype and `coerce_float=False` does not prevent this.
        # My solution is to cast the column to a string in the SQL query.
        #
        # Perhaps pandas will add a `converters` parameter in the future to handle this.
        # https://github.com/pandas-dev/pandas/issues/20525
        #
        # Why `dtype=object`? Per Google AI specifying `dtype=str` can lead to unexpected behavior
        # where None values are converted to the string "None" instead of remaining as NaN or None.
        # This occurs because explicitly setting the data type to string forces all values
        # in the column to be represented as strings, including null values. To handle this,
        # it's recommended to use `dtype=object` when reading SQL data with potential null
        # values that you want to preserve. The object dtype allows for mixed data types within
        # a column, accommodating both strings and None values correctly.
        # In pandas 3.0, a dedicated string data type is introduced, aiming to handle missing
        # values consistently using NaN. However, until then, using object as dtype for string
        # columns with potential null values in read_sql is the most reliable way to avoid
        # unintended conversions.
        for col in currency_cols:
            dtypes[col] = object

        if isinstance(q, Select):
            for col in currency_cols:
                select_col = q.get_column(col)
                if select_col is not None:
                    q.remove_column(select_col)
                    q.column(f"CAST({select_col.expr} AS TEXT)", named=col)

    sql, params = q.sql()

    df_sql = pd.read_sql(
        sql,
        conn,
        params=params,  # pyright: ignore[reportCallIssue, reportArgumentType]
        dtype=dtypes,
        parse_dates=parse_dates if isinstance(parse_dates, dict) else typing.cast("list", parse_dates),
        coerce_float=False,
        **kwargs,
    )

    if not df_sql.empty:
        if date_cols:
            for col in date_cols:
                # numpy.datetime64 -> datetime.date
                assert df_sql[col].dtype.type == np.datetime64  # dtype='<M8[ns]'
                df_sql[col] = df_sql[col].dt.date

        if currency_cols:
            for col in currency_cols:
                # numpy.object (str) -> CurrencyDecimal
                assert df_sql[col].dtype == np.object_
                df_sql[col] = df_sql[col].apply(currency_from_value)

    return df_sql


def split_category_pair(category_pair: str) -> tuple[str, str | None]:
    """Split "category : subcategory" field value into category and subcategory parts, trimming whitespace.

    If `category_pair` ends with ":", the subcategory is set to None.
    If subcategory is empty, it is set to None.

    Args:
        category_pair (str): A "Category : Subcategory" pair.

    Returns:
        tuple[Category or None, Subcategory or None]
    """
    if not category_pair:
        return category_pair, None

    parts = category_pair.split(":", 1)
    category = parts[0].rstrip()
    subcategory = parts[1].lstrip() if len(parts) > 1 else None

    return category, subcategory or None


def pd_split_category_pair(category_pair: str) -> tuple[str | NAType, str | NAType]:
    """Split "category : subcategory" DataFrame field value into category and subcategory parts, trimming whitespace.

    Args:
        category_pair (str): A "Category : Subcategory" pair.

    Returns:
        tuple[Category or pd.NA, Subcategory or pd.NA]
    """
    if pd.isna(category_pair):
        return pd.NA, pd.NA

    parts = split_category_pair(category_pair)

    return parts[0] or pd.NA, parts[1] or pd.NA


def resolve_rel_path(name_or_path: Path | str | None, rel_to_dir: Path | None, default_name: str | None = None) -> Path:
    """Resolve the path to a file given a file name and directory.

    Args:
        name_or_path (Path|str): File or directory name, absolute or relative path, "." or None.
        rel_to_dir (Path): Directory 'name_or_path' should be relative to.
        default_name (str|None): Default file name; Can be None if 'name_or_path' is a file name.

    Returns:
        Path: Absolute file or directory path.

    Raises:
        ValueError: If 'name_or_path' is empty or is a directory and 'default_name' is not provided.
    """
    if isinstance(name_or_path, str):
        name_or_path = Path(name_or_path)

    if not name_or_path:
        if not default_name:
            raise ValueError("No default name provided.")
        name_or_path = Path(default_name)
    elif not name_or_path.name:
        # name_or_path is "."
        name_or_path = name_or_path.absolute()

    if name_or_path.is_dir():
        if not default_name:
            raise ValueError("No default name provided.")
        name_or_path = name_or_path / default_name
    elif not name_or_path.is_absolute() and rel_to_dir:
        name_or_path = rel_to_dir / name_or_path

    return name_or_path.resolve() if ".." in name_or_path.parts else name_or_path


def config_warning(message: str, config: CommonConfigFileT, directive: str | Sequence[str] | None) -> None:
    """Log a warning regarding a configuration directive.

    Args:
        message (str): Warning message.
        config (CommonConfigFileT): Top-level configuration options.
        directive (Sequence[str] | str): Offending configuration directive(s); optional.
    """
    assert config is not None

    directives = []
    config_file = config.get("_config_file")

    # Configuration file is optional
    if config_file:
        directives.append(config_file.name)

    if directive:
        if isinstance(directive, str):
            directive = [directive]
        directives.extend(directive)

    message_parts = [
        ": ".join([f"'{d}'" for d in directives]),
        message,
    ]

    logger.warning(": ".join([part for part in message_parts if part]))


def validate_config_typed_dict(  # noqa: C901, PLR0912, PLR0915
    typed_dict: Mapping[str, Any],
    typed_dict_cls: type[object],
    config: CommonConfigFileT,
    config_keys: str | Sequence[str],
) -> None:
    """Validate configuration dictionary against its TypedDict properties.

    Args:
        typed_dict (Mapping[str, Any]): Configuration dictionary defined as a `TypedDict`.
        typed_dict_cls (type[object]): Configuration dictionary type.
        config (CommonConfigFileT): Top-level user configuration directives.
        config_keys (str | Sequence[str]): Key or keys to the configuration dictionary.

    Raises:
        MnyXlsConfigError: Invalid configuration.
    """

    def is_parameterized(vartype) -> bool:  # noqa: ANN001
        return get_origin(vartype) is not None

    def is_required(typed_dict_cls, property_name: str) -> bool:  # noqa: ANN001
        return property_name in typed_dict_cls.__required_keys__

    def _split_union_types(union_types: str) -> list[str]:
        # 'int | str | list[str] | list[str|int]' -> ['int', 'str', 'list[str]', 'list[str|int]']
        union_types_parts = []
        bracket_level = 0
        current_part = []

        for char in union_types:
            if char == "|" and bracket_level == 0:
                union_types_parts.append("".join(current_part).strip())
                current_part = []
            else:
                if char == "[":
                    bracket_level += 1
                elif char == "]":
                    bracket_level -= 1
                current_part.append(char)

        if current_part:
            union_types_parts.append("".join(current_part).strip())

        return union_types_parts

    def _validate_uniontype_val(val: Any, union_types: str | None) -> bool:  # noqa: ANN401
        # get_type_hints(typed_dict_cls).get(typed_dict_key) = 'str | list[str]'
        union_types_parts = _split_union_types(union_types) if union_types else []

        for union_part_spec in union_types_parts:
            if union_part_spec.endswith("]"):
                list_union_part_spec = union_part_spec.split("[", 1)[1].rstrip("]")  # "list[x]"-> "x"

                if is_sequence(val) and all(_validate_uniontype_val(item, list_union_part_spec) for item in val):
                    return True
            else:
                if union_part_spec == "None" and val is None:
                    return True

                # Map a str version of a type to the type itself.
                union_part_type = UNION_PART_TYPES.get(union_part_spec)
                if union_part_type is None:
                    config_warning(f"Unhandled validate config union scalar type '{union_part_spec}'", config, (*config_keys, typed_dict_key))
                    return True

                if isinstance(val, union_part_type):
                    return True

        return False

    def _validate_uniontype(typed_dict_key: str, union_types: types.UnionType | str | None) -> bool:
        if not union_types:
            config_warning(f"Ignoring unexpected union type: '{union_types}'", config, (*config_keys, typed_dict_key))
            return True

        return _validate_uniontype_val(typed_dict[typed_dict_key], str(union_types))

    if isinstance(config_keys, str):
        config_keys = [config_keys]

    if not isinstance(typed_dict, dict):
        raise MnyXlsConfigError("Directive must be a set of key/value pairs.", config, config_keys)

    cls_type_hints = get_type_hints(typed_dict_cls)

    invalid_keys = set(typed_dict) - set(cls_type_hints.keys())
    if invalid_keys:
        for invalid_key in invalid_keys:
            config_warning("Unrecognized directive", config, (*config_keys, invalid_key))

    for typed_dict_key, typed_key_hint in cls_type_hints.items():
        if typed_dict_key in typed_dict:
            if is_parameterized(typed_key_hint):
                # typed_key_hint is `GenericAlias` such as `dict[str, mnyxls.worksheet.WorksheetConfigT]`
                # get_origin(dict[str, WorksheetConfigT]) = <class 'dict>
                # get_args(dict[str, WorksheetConfigT]) = (<class 'str'>, <class 'mnyxls.worksheet.WorksheetConfigT'>)
                # > get_args(typed_key_hint)
                typed_key_hint = get_origin(typed_key_hint)  # noqa: PLW2901

            if typed_key_hint == types.UnionType:
                # > blah: str | list[str]
                if not _validate_uniontype(typed_dict_key, get_type_hints(typed_dict_cls).get(typed_dict_key)):
                    raise MnyXlsConfigError("Directive is not of the correct type.", config, (*config_keys, typed_dict_key))
            elif is_typeddict(typed_key_hint):
                # > class Blah(TypedDict)
                if typed_dict[typed_dict_key] is not None:
                    validate_config_typed_dict(typed_dict[typed_dict_key], typed_key_hint, config, (*config_keys, typed_dict_key))
            elif isinstance(typed_dict[typed_dict_key], typed_key_hint):
                pass  # Ok
            elif typed_key_hint is dict:
                if not isinstance(typed_dict[typed_dict_key], Mapping):
                    raise MnyXlsConfigError("Directive must be a set of key/value pairs.", config, (*config_keys, typed_dict_key))
            elif (typed_key_hint is list) or (typed_key_hint is tuple):
                if not is_sequence(typed_dict[typed_dict_key]):
                    raise MnyXlsConfigError("Directive must be a sequence.", config, (*config_keys, typed_dict_key))
            else:
                raise MnyXlsConfigError("Directive is not of the correct type.", config, (*config_keys, typed_dict_key))

        elif is_required(typed_dict_cls, typed_dict_key):
            raise MnyXlsConfigError("Directive is required.", config, (*config_keys, typed_dict_key))


def truncate_w_ellipsis(text: str, maxlen: int) -> str:
    """Truncate text using ellipsis.

    Args:
        text (str): Text to truncate.
        maxlen (int): Maximum length of text to return.

    Returns:
        str
    """
    if maxlen == ELLIPSIS_LEN:
        return "..."  # truncate entire word

    excess = len(text) - maxlen  # how much to truncate
    if excess <= 0:
        return text

    words = text.split()

    if len(words) > 1:
        # Attempt to truncate an individual word in the middle of the text.
        for i in range(len(words) - 2, -1, -1):
            if len(words[i]) >= excess + ELLIPSIS_LEN:
                words[i] = truncate_w_ellipsis(words[i], len(words[i]) - excess)
                return " ".join(words)

    llen = (maxlen - ELLIPSIS_LEN) // 2
    rlen = maxlen - ELLIPSIS_LEN - llen

    if llen == 0:
        return f"{text[:rlen]}..."  # truncate at the end

    return f"{text[:llen]}...{text[-rlen:]}"


def pluralize(word: str) -> str:
    """Pluralize a word.

    This is a placeholder function that does not actually pluralize the word.

    Args:
        word (str): Singular word.

    Returns:
        str
    """
    return word


def parse_yyyymmdd_flex(val: str, first_day: bool = False) -> date | None:
    """Parse date formatted as "YYYY", "YYYY-MM" or "YYYY-MM-DD".

    Does not raise an exception if date is invalid.

    Args:
        val (str): Value to parse.
        first_day (bool): If True, return the first day of the month if the day is not specified.
            Defaults to False.

    Returns:
        date or None if date is invalid.
    """
    try:
        d = datetime.strptime(val, "%Y-%m-%d")  # noqa: DTZ007
    except ValueError:
        try:
            d = datetime.strptime(val, "%Y-%m").replace(day=1)  # noqa: DTZ007
        except ValueError:
            try:
                d = datetime.strptime(val, "%Y").replace(month=1 if first_day else 12).replace(day=1)  # noqa: DTZ007
            except ValueError:
                return None

        if not first_day:
            # Set to last day of its month
            d = d.replace(day=(d.replace(month=d.month % 12 + 1, day=1) - timedelta(days=1)).day)

    return d.date()


def is_sequence(obj: Any) -> bool:  # noqa: ANN401
    """Check if an object is a sequence.

    Args:
        obj (Any): Object to check.

    Returns:
        bool: True if the object is a sequence, False otherwise.
    """
    return isinstance(obj, Sequence) and not isinstance(obj, str)


def get_select_values(
    select_key: str,
    config: WorkbookConfigT | WorksheetConfigT | WorkbookConfigSelectT | WorksheetConfigSelectT,
) -> Sequence[Any]:
    """Return list of values (even if directive is a scalar value) for a 'select' configuration directive key.

    Args:
        select_key (str): Select directive.
        config (WorkbookConfigT | WorksheetConfigT  WorkbookConfigSelectT | WorksheetConfigSelectT): Workbook or worksheet configuration.

    Returns:
        Sequence[Any]
    """
    config_select = config.get("select", config)
    config_value = config_select.get(select_key, []) if config_select else []
    return config_value if is_sequence(config_value) else [config_value]


def get_select_values_and_cond(
    select_key: str,
    config_select: WorkbookConfigSelectT | WorksheetConfigSelectT,
) -> tuple[Sequence[Any], Literal["=", "<>"]]:
    """Return a value list (even for a single item) and condition.

    Args:
        select_key (str): Select directive.
        config_select (WorkbookConfigSelectT | WorksheetConfigSelectT): Worksheet configuration.

    Returns:
        tuple[Sequence[str], str]: (values, condition)
    """
    values = get_select_values(select_key, config_select)

    if values and isinstance(values[0], str) and values[0].startswith("!"):
        # First value can be "!" or series of "!value"
        values = values[1:] if values[0] == "!" else [value.removeprefix("!") if value.startswith("!") else value for value in values]
        return values, "<>"

    return values, "="


def config_select_allow(
    config_select: WorkbookConfigSelectT | WorksheetConfigSelectT,
    allow_directives: Container | None,
) -> WorkbookConfigSelectT | WorksheetConfigSelectT:
    """Filter `select` configuration to only directives that are allowed.

    Args:
        config_select (WorkbookConfigSelectT | WorksheetConfigSelectT): Config select configuration.
        allow_directives (set|dict|list|tuple): Allowed directives.
            If None, all directives are allowed.

    Returns:
        New config select configuration.
    """
    if allow_directives is None:
        return config_select  # nothing to filter

    assert bool(allow_directives), "If you want to exclude all directives, set to None."

    new_config_select: WorkbookConfigSelectT | WorksheetConfigSelectT = {}

    for k, v in config_select.items():
        if k in allow_directives:
            new_config_select[k] = v

    return new_config_select


def config_select_remove(
    config_select: WorkbookConfigSelectT | WorksheetConfigSelectT,
    remove_directives: Container | None,
) -> None:
    """Remove directives from `select` configuration.

    Argument is modified in place.

    Args:
        config_select (WorkbookConfigSelectT | WorksheetConfigSelectT): Config select configuration.
        remove_directives (set|dict|list|tuple): Directives to remove.
            If None, all directives are allowed.
    """
    if remove_directives is None:
        return  # nothing to filter

    assert bool(remove_directives), "If you want to include all directives, set to None."

    keys_to_remove = [k for k in config_select if k in remove_directives]
    for k in keys_to_remove:
        del config_select[k]


def get_date_relative_to(date_spec: str, relative_to: date, first_day: bool = False) -> date | None:  # noqa: C901, PLR0911
    """Return a date relative to another date given a date specification.

    Args:
        date_spec (str): Date specification formatted as "-N[dmy]".
        relative_to (date): Relative date.
        first_day (bool): If True, return the first day of the month if the day is not specified.
            Defaults to False.

    Returns:
        date or None if spec is invalid.
    """
    if not date_spec:
        return None

    assert isinstance(date_spec, str)

    match = RE_REL_DATE.fullmatch(date_spec)
    if not match:
        return None

    num, char = match.groups()
    num = abs(int(num))

    if char == "d":
        if num == 0:
            return None  # makes no sense
        return relative_to + relativedelta(days=-num)

    if char == "w":
        if num == 0:
            return None  # makes no sense
        return relative_to + relativedelta(weeks=-num)

    d = None

    if char == "m":
        # N months prior; 0 means current month
        d = relative_to.replace(day=1)
        if num > 0:
            d = d + relativedelta(months=-num)

    elif char == "y":
        # N years prior; 0 means current year
        d = relative_to.replace(day=1).replace(month=1 if first_day else 12)
        if num > 0:
            d = d + relativedelta(years=-num)

    if d is None:
        return None

    if not first_day:
        # Set to last day of its month
        d = d.replace(day=(d.replace(month=d.month % 12 + 1, day=1) - timedelta(days=1)).day)

    return d

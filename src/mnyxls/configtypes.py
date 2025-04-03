# This module contains all TypedDicts that describe configuration directives defined
# in the YAML configuration file. They are all defined here together because we cannot
# import annotations from `__future_` as is done in all the other modules.
# This is because `from __future__ import annotations` will break `validate_config_typed_dict`!
# It is a known issue, despite not being widely documented, and will never be "fixed".
# https://github.com/python/cpython/issues/97727

from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import NotRequired, TypedDict

ConfigFileScalarT = str | int | bool
ConfigFileValueT = ConfigFileScalarT | Sequence[ConfigFileScalarT] | Mapping[str, ConfigFileScalarT]
ConfigReportsT = list[str | Path]
ConfigAccountCategoriesT = dict[str, list[str]]  # `account_categories`
YYYYTypeT = str | int | Sequence[str | int]  # !YEAR, YEAR | list[YEAR,...]

# MM.NN or "opMM.NN" or ["op", MM.NN] where `op` is one of "<", ">", "<=", ">=", "<>", "!="
ConfigSelectAmountT = str | float | int | Sequence[str | float | int]

######################################################################
# Typed dictionaries to describe configuration file `select` directives


class WorksheetConfigSelectT(TypedDict):
    """UNION of all `select` configuration directives.

    `workbook.worksheets.*.select`
    Individual worksheet types may use only a subset of these.

    - WorksheetConfigSelectTxnsT
    - WorksheetConfigSelectCategoriesT
    - WorksheetConfigSelectTxnsPivotT
    - WorksheetConfigSelectAccountsT
    - WorkbookConfigSelectT
    """

    account: NotRequired[str | list[str]]
    account_category: NotRequired[str | list[str]]
    account_classification: NotRequired[str | list[str]]
    amount: NotRequired[ConfigSelectAmountT]
    category: NotRequired[str | list[str]]
    era: NotRequired[str | list[str]]
    memo: NotRequired[str | list[str]]
    payee: NotRequired[str | list[str]]
    txnclass: NotRequired[str | list[str]]
    txntype: NotRequired[str | list[str]]
    date_from: NotRequired[str]  # YYYY | YYYY-MM | YYYY-MM-DD
    date_to: NotRequired[str]  # YYYY | YYYY-MM | YYYY-MM-DD
    yyyy: NotRequired[YYYYTypeT]


class WorksheetConfigSelectTxnsT(TypedDict):
    """Structure of `select` configuration directive for `MoneyWorksheetTxns`.

    `workbook.worksheets.*.select`
    """

    account: NotRequired[str | list[str]]
    account_category: NotRequired[str | list[str]]
    account_classification: NotRequired[str | list[str]]
    amount: NotRequired[ConfigSelectAmountT]
    category: NotRequired[str | list[str]]
    era: NotRequired[str | list[str]]
    memo: NotRequired[str | list[str]]
    payee: NotRequired[str | list[str]]
    txnclass: NotRequired[str | list[str]]
    txntype: NotRequired[str | list[str]]
    date_from: NotRequired[str]  # YYYY | YYYY-MM | YYYY-MM-DD
    date_to: NotRequired[str]  # YYYY | YYYY-MM | YYYY-MM-DD
    yyyy: NotRequired[YYYYTypeT]


class WorksheetConfigSelectCategoriesT(TypedDict):
    """Structure of `select` configuration directive for `MoneyWorksheetCategories`.

    `workbook.worksheets.*.select`
    """

    txnclass: NotRequired[str | list[str]]
    txntype: NotRequired[str | list[str]]


class WorksheetConfigSelectTxnsPivotT(WorksheetConfigSelectTxnsT):
    """Structure of `select` configuration directive for `MoneyWorksheetTxnsPivot`.

    `workbook.worksheets.*.select`
    """


class WorksheetConfigSelectAccountsT(TypedDict):
    """Structure of `select` configuration directive for `MoneyWorksheetAccounts`.

    `workbook.worksheets.*.select`
    """

    account: NotRequired[str | list[str]]
    account_category: NotRequired[str | list[str]]
    account_classification: NotRequired[str | list[str]]


class WorkbookConfigSelectT(TypedDict):
    """Structure of workbooko `select` configuration directive.

    `workbook.select`
    """

    account: NotRequired[str | list[str]]
    account_category: NotRequired[str | list[str]]
    account_classification: NotRequired[str | list[str]]
    date_from: NotRequired[str]  # YYYY | YYYY-MM | YYYY-MM-DD
    date_to: NotRequired[str]  # YYYY | YYYY-MM | YYYY-MM-DD
    yyyy: NotRequired[YYYYTypeT]


class ConfigRewriteSelectT(TypedDict):
    """Structure of the `select` directive in `rewrites` configuration."""

    account: NotRequired[str | list[str]]
    amount: NotRequired[ConfigSelectAmountT]
    category: NotRequired[str | list[str]]
    memo: NotRequired[str | list[str]]
    payee: NotRequired[str | list[str]]
    date_from: NotRequired[str]  # YYYY | YYYY-MM | YYYY-MM-DD
    date_to: NotRequired[str]  # YYYY | YYYY-MM | YYYY-MM-DD
    yyyy: NotRequired[YYYYTypeT]


######################################################################
# Typed dictionaries to describe configuration file `options` directives


class WorksheetConfigOptionsT(TypedDict):
    """Combined structure of all `options` configuration directives.

    `workbook.worksheets.*.options`
    Individual worksheet types may use only a subset of these.
    """

    rows: NotRequired[str | list[str]]
    columns: NotRequired[str | list[str]]
    colorful: NotRequired[bool]


class WorksheetConfigOptionsTxnsPivotT(TypedDict):
    """Structure of `options` configuration directive for `MoneyWorksheetTxns`.

    `workbook.worksheets.*.options`
    """

    rows: NotRequired[str | list[str]]
    columns: NotRequired[str | list[str]]


######################################################################
# Typed dictionaries to describe configuration file directives


class WorksheetConfigT(TypedDict):
    """Structure of account configuration directive `workbook.worksheets.*`."""

    sheet_type: str

    # Dependent on `sheet_type`
    autofit: NotRequired[bool]
    columns: NotRequired[str | Sequence[str]]
    consolidate: NotRequired[str]
    foreach: NotRequired[str]
    options: NotRequired[WorksheetConfigOptionsT]
    select: NotRequired[WorksheetConfigSelectT]
    skipempty: NotRequired[bool]
    use_existing: NotRequired[bool]

    # Managed internally
    _config_key: NotRequired[str]


class ConfigEraT(TypedDict):
    """Structure of main configuration directive `eras`."""

    # `date_from` or `date_to` is required
    date_from: NotRequired[str | date]
    date_to: NotRequired[str | date]


ConfigErasT = dict[str, ConfigEraT]

######################################################################
# Typed dictionaries to describe top-level configuration file directives


class WorkbookConfigT(TypedDict):
    """Structure of account configuration directive `workbook`."""

    autofit: NotRequired[bool]
    select: NotRequired[WorkbookConfigSelectT]
    skipempty: NotRequired[bool]
    use_existing: NotRequired[bool]
    worksheets: dict[str, WorksheetConfigT]


class ConfigAccountT(TypedDict):
    """Structure of account configuration directive `accounts`."""

    category: NotRequired[str]
    closed_date: NotRequired[str]
    opened_date: NotRequired[str]


ConfigAccountsT = dict[str, ConfigAccountT]  # `accounts`


class ConfigRewriteT(TypedDict):
    """Structure of main configuration directive `rewrites`."""

    # These fields are applied as the `update` criteria.
    category: NotRequired[str]
    memo: NotRequired[str]
    payee: NotRequired[str]
    txndate: NotRequired[str]  # YYYY-MM-DD

    select: ConfigRewriteSelectT


ConfigSelectUnionT = WorkbookConfigT | WorksheetConfigT | WorkbookConfigSelectT | WorksheetConfigSelectT | ConfigRewriteSelectT

######################################################################
# Typed dictionaries to describe top-level configuration files.


class CommonConfigFileT(TypedDict):
    """Common elements of main and ancillary configuration files."""

    # Managed internally
    _config_file: Path | None


class MainConfigFileT(CommonConfigFileT):
    """Structure of main top-level configuration file."""

    account_categories: NotRequired[ConfigAccountCategoriesT]
    accounts: NotRequired[ConfigAccountsT]
    category_class_default: NotRequired[str]
    category_classes: NotRequired[dict[str, list[str]]]
    category_type_default: NotRequired[str]
    category_types: NotRequired[dict[str, list[str]]]
    data_dir: NotRequired[Path]
    db_file: NotRequired[Path]
    debug_sql: NotRequired[bool]
    eras: NotRequired[ConfigErasT]
    import_date_from: NotRequired[str]
    import_date_range: NotRequired[tuple[date | None, date | None]]  # command line option
    import_date_to: NotRequired[str]
    opt_check_total: NotRequired[bool]  # command line option
    opt_create_db: NotRequired[bool]  # command line option
    opt_create_xls: NotRequired[bool]  # command line option
    opt_no_reports: NotRequired[bool]  # command line option
    opt_recommend_reports: NotRequired[bool]  # command line option
    report_paths: NotRequired[list[Path]]  # command line option
    reports: NotRequired[ConfigReportsT]  # "report_paths" in config file
    rewrites: NotRequired[Sequence[ConfigRewriteT]]
    save_interim_reports: NotRequired[bool]
    verbose: NotRequired[int]  # command line option
    workbook: NotRequired[WorkbookConfigT]
    xls_config: NotRequired[Path]
    xls_file: NotRequired[Path]
    xls_template: NotRequired[Path]


class WorkbookConfigFileT(CommonConfigFileT):
    """Structure of separate workbook configuration file."""

    workbook: WorkbookConfigT

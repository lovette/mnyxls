from __future__ import annotations

import importlib.metadata
import logging
import sqlite3
import sys
import typing
from contextlib import closing
from pathlib import Path
from typing import TYPE_CHECKING

import click
from click.core import ParameterSource
from click_option_group import optgroup

from .configtypes import ConfigReportsT, MainConfigFileT
from .dbsqlite import db_create
from .report import ReportType
from .reports import gather_reports, parse_reports
from .shared import MnyXlsConfigError, MnyXlsRuntimeError, is_sequence, read_config_file, resolve_rel_path, validate_config_typed_dict
from .workbook import DEFAULT_XLS_CONFIG_NAME, DEFAULT_XLS_TEMPLATE_NAME, gather_workbook

if TYPE_CHECKING:
    from datetime import date


VERBOSE_LOGGING_LEVELS = (
    logging.ERROR,  # default
    logging.WARNING,  # -v
    logging.INFO,  # -vv
    logging.DEBUG,  # -vvv
)

LOGGING_FORMAT = "[%(levelname)s] %(message)s"

# Reference version number in pyproject.toml
# (For unexplained reasons, will change "-dev" suffix to "-dev0".)
__version__ = importlib.metadata.version("mnyxls")

# Setup logging
# Runtime verbose level will be set in cli()
logging.basicConfig(level=VERBOSE_LOGGING_LEVELS[0], format=LOGGING_FORMAT)

DEFAULT_DB_STEM = "mnyxls"

# Use a named logger instead of root logger
logger = logging.getLogger("mnyxls")


######################################################################
# Helper functions


def _opt_config_file_callback(ctx: click.Context, param: click.Option, value: Path) -> Path:  # noqa: ARG001
    """Handle `--config` command option.

    Args:
        ctx (click.core.Context): Click context object.
        param (click.Option): Click option object.
        value (str): Option value.

    Returns:
        str: Option value.
    """
    assert value is not None
    assert ctx.default_map is None

    ctx.default_map = {"_config_file": value}  # Capture for error reporting

    if value.is_file():
        try:
            config = read_config_file(value)
        except MnyXlsConfigError as err:
            raise MnyXlsConfigError(f"Failed to read config file {value}: {err}") from err
        else:
            if config:
                ctx.default_map.update(config)

    return value


######################################################################
# Main


@click.command()
@click.option(
    "--config-file",
    "-c",
    default="mnyxls.yaml",
    type=click.Path(
        exists=False,
        dir_okay=False,
        path_type=Path,
        resolve_path=False,
    ),
    callback=_opt_config_file_callback,
    is_eager=True,
    expose_value=False,
    show_default=False,
    help="Read options from configuration FILE.",
)
@click.option(
    "--data-dir",
    "-d",
    type=click.Path(
        exists=True,
        file_okay=False,
        dir_okay=True,
        path_type=Path,
        resolve_path=False,
    ),
    default=None,
    show_default=False,
    help="Set path to directory containing data files.",
)
@click.option(
    "--verbose",
    "-v",
    count=True,
    help="Be more verbose; can specify more than once.",
)
@optgroup.group("REPORTS")
@optgroup.option(
    "import_date_from",
    "--date-start",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Import report transactions after this date (inclusive).",
)
@optgroup.option(
    "import_date_to",
    "--date-end",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Import report transactions before this date (inclusive).",
)
@optgroup.option(
    "opt_no_reports",
    "--no-reports",
    type=bool,
    is_flag=True,
    default=False,
    show_default=False,
    help="Ignore REPORT arguments and use previously imported reports.",
)
@optgroup.option(
    "opt_check_total",
    "--no-check-total",
    type=bool,
    is_flag=True,
    default=True,
    show_default=False,
    help="Do not check reports totals.",
)
@optgroup.option(
    "opt_recommend_reports",
    "--no-recommend-reports",
    type=bool,
    is_flag=True,
    default=True,
    show_default=False,
    help="Do not recommend reports to provide more details.",
)
@optgroup.group("WORKBOOK")
@optgroup.option(
    "opt_create_xls",
    "--xls/--no-xls",
    type=bool,
    is_flag=True,
    default=True,
    show_default=False,
    help="Create Excel workbook. [default: yes]",
)
@optgroup.option(
    "--xls-file",
    type=click.Path(
        exists=False,
        dir_okay=True,  # Save to directory using default file name
        path_type=Path,
        resolve_path=False,
    ),
    default=None,
    show_default=False,
    help="Set name or path to Excel workbook created. [default: REPORT file names]",
)
@optgroup.option(
    "--xls-config",
    type=click.Path(
        exists=False,
        dir_okay=False,
        path_type=Path,
        resolve_path=False,
    ),
    default=DEFAULT_XLS_CONFIG_NAME,
    show_default=True,
    help="Set name or path to Excel workbook configuration FILE.",
)
@optgroup.option(
    "--xls-template",
    type=click.Path(
        exists=False,
        dir_okay=True,  # Save to directory using default file name
        path_type=Path,
        resolve_path=False,
    ),
    default=DEFAULT_XLS_TEMPLATE_NAME,
    show_default=True,
    help="Set name or path to Excel workbook template.",
)
@optgroup.group("DATABASE")
@optgroup.option(
    "opt_create_db",
    "--db/--no-db",
    type=bool,
    is_flag=True,
    default=True,
    show_default=False,
    help="Create SQLite database. [default: yes]",
)
@optgroup.option(
    "--db-file",
    type=click.Path(
        exists=False,
        dir_okay=True,  # Save to directory using default file name
        path_type=Path,
        resolve_path=False,
    ),
    default=None,
    show_default=False,
    help="Set name or path to SQLite database. [default: XLS file name]",
)
@click.version_option(
    version=__version__,
    prog_name="mnyxls",
    message="%(prog)s version %(version)s",
)
@click.argument(
    "report_paths",
    type=click.Path(
        exists=False,  # validate ourself to keep error message consistent
        file_okay=True,
        dir_okay=False,
        path_type=Path,
        resolve_path=False,
    ),
    nargs=-1,
    default=None,
    metavar="[REPORT]...",
)
@click.pass_context
def cli(  # noqa: C901, PLR0912, PLR0913, PLR0915
    ctx: click.Context,
    data_dir: Path | None,
    db_file: Path | None,
    import_date_from: date | None,
    import_date_to: date | None,
    opt_check_total: bool,  # noqa: ARG001
    opt_create_db: bool,
    opt_create_xls: bool,
    opt_no_reports: bool,
    opt_recommend_reports: bool,  # noqa: ARG001
    verbose: int,
    xls_config: Path,
    xls_file: Path,
    xls_template: Path,  # noqa: ARG001
    report_paths: tuple[Path, ...],
) -> None:
    """Create a Microsoft Excel workbook and/or SQLite database from Microsoft Money reports.

    REPORT is a Microsoft Money report saved in comma-delimited format.
    Supported reports: Account transactions, Account balances, Account balances with details,
    Income and spending, Monthly income and expenses.

    If the data directory is not set, data files will be saved to the directory of the
    configuration file or first report.
    """
    # Adjust logging level
    loglevel = VERBOSE_LOGGING_LEVELS[min(verbose, len(VERBOSE_LOGGING_LEVELS) - 1)]
    logging.getLogger().setLevel(loglevel)

    logger.debug(f"Working directory: '{Path.cwd()}'")

    reports = None
    read_reports = not opt_no_reports
    workbook = None

    # Options set in configuration file, set by `config_set_default`
    config = typing.cast("MainConfigFileT", dict(ctx.default_map or {}))

    config_file: Path | None = config.get("_config_file")

    # Error handling expects this key to be present
    assert config_file is not None

    if config_file.is_file():
        logger.debug("Main config file: %s", f"'{config_file.resolve()}'")
    else:
        logger.debug("Main config file: %s: No such file.", f"'{config_file.resolve()}'")

    if not data_dir and config_file:
        # Data files are relative to config directory.
        data_dir = config_file.parent

    # Command line options override config file options (if any)
    for param, param_value in ctx.params.items():
        if ctx.get_parameter_source(param) == ParameterSource.COMMANDLINE:
            if param == "report_paths" and param_value:
                config["reports"] = param_value  # Config file uses directive "reports"; keep it consistent
            else:
                config[param] = param_value

    # Validate config file directives and value types.
    validate_config_typed_dict(config, MainConfigFileT, config, [] if config_file else ["MainConfigFile"])

    debug_sql = config.get("debug_sql", False)

    # Use config file reports if none specified on command line.
    if config and not report_paths:
        config_reports: ConfigReportsT | None = config.get("reports")

        if config_reports:
            # Config file `reports` can be a single path or list of paths.
            if isinstance(config_reports, Path):
                report_paths = (config_reports,)
            elif isinstance(config_reports, str):
                report_paths = (Path(config_reports),)
            else:
                assert is_sequence(config_reports)
                report_paths = tuple([Path(val) for val in config_reports])

    if read_reports:
        if not report_paths:
            ctx.fail("No reports specified; Use 'REPORT' argument or configuration file with `reports` directive.")

        # Resolve report file paths.
        report_paths = tuple([resolve_rel_path(report_path, data_dir or Path.cwd(), report_path.name) for report_path in report_paths])

        if not data_dir:
            # No config file so save data adjacent to report data.
            data_dir = report_paths[0].parent

        # Validate data directory.
        if not data_dir.is_dir():
            ctx.fail(f"'{data_dir}': No such directory.")

        logger.debug(f"Data directory: '{data_dir}'")

        config["data_dir"] = data_dir
        config["import_date_range"] = (import_date_from, import_date_to)

        reports = gather_reports(ctx, report_paths, config)

        if ReportType.ACCOUNT_TRANSACTIONS not in reports:
            ctx.fail("At least one account transactions report must be included.")

        reports = parse_reports(reports, config)

        if not reports:
            if import_date_from or import_date_to:
                ctx.fail("No reports cover given date range.")
            else:
                ctx.fail("No reports parsed.")

        txn_report = reports.get(ReportType.ACCOUNT_TRANSACTIONS)
        if not txn_report:
            ctx.fail("At least one account transactions report must be included.")

        # DB can be derived from XLS file name
        xls_file = resolve_rel_path(xls_file, data_dir, txn_report.default_path_stem).with_suffix(".xlsx")
        db_file = resolve_rel_path(db_file, data_dir, xls_file.stem).with_suffix(".sqlite3")
    else:
        if not opt_create_db:
            ctx.fail("Options --no-reports and --no-db cannot be used together.")
        if not db_file:
            ctx.fail("No database specified; Use '--db-file' option or configuration file with `db_file` directive.")
        if not opt_create_xls:
            logger.warning("Nothing to do when --no-reports and --no-xls are used together.")

        # XLS can be derived from DB file name
        db_file = resolve_rel_path(db_file, data_dir, DEFAULT_DB_STEM).with_suffix(".sqlite3")
        xls_file = resolve_rel_path(xls_file, data_dir, db_file.stem).with_suffix(".xlsx")

        if not db_file.is_file():
            ctx.fail(f"'{db_file}': No such database file.")

        logger.debug("Using previously imported reports.")

    assert data_dir is not None

    xls_config = resolve_rel_path(xls_config, data_dir)

    # Pass along resolved paths
    config["data_dir"] = data_dir
    config["db_file"] = db_file
    config["xls_file"] = xls_file
    config["xls_config"] = xls_config

    if opt_create_xls:
        workbook = gather_workbook(ctx, xls_file, config)

    database = str(db_file) if opt_create_db else ":memory:"

    logger.debug(f"Database path: '{database}'")

    with closing(sqlite3.connect(database)) as conn:
        try:
            db_create(conn, reports, config)

        except sqlite3.OperationalError as err:
            if not debug_sql:
                # Suppress stack trace
                raise MnyXlsRuntimeError(f"SQLite error: {err}") from err
            raise

        if workbook is not None:
            workbook.create_worksheets(conn)
            workbook.write_workbook(conn)

    # (I've never seen this happen, but just in case...)
    # Click's CliRunner.invoke doesn't capture stdout from logging
    # https://github.com/pallets/click/issues/2647
    sys.stderr.flush()


if __name__ == "__main__":
    sys.exit(cli())

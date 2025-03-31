from __future__ import annotations  # noqa: INP001

from typing import TYPE_CHECKING

from tests.testutils import cli_invoke, tmp_path_cwd

if TYPE_CHECKING:
    from pathlib import Path

    import pytest
    from click.testing import CliRunner


from mnyxls.__main__ import cli

######################################################################
# Tests


def test_no_output(runner: CliRunner, resources_dir: Path, caplog: pytest.LogCaptureFixture, tmp_path: Path) -> None:  # noqa: D103
    # Parse txns report
    # Files output: None
    # Configuration file: None (use defaults)

    # These files must be accessible in `runner_cwd`
    tmp_path_locals = []

    cli_args: list[str | Path] = [
        "--no-db",
        "--no-xls",
        resources_dir / "sample_reports/txns_2018_2022.csv",
    ]

    with tmp_path_cwd(tmp_path, tmp_path_locals) as runner_cwd:
        cli_invoke(cli, cli_args, runner, caplog)

        out_files = list(runner_cwd.glob("*.xlsx"))
        assert not out_files, "Expected no .xlsx files"

        out_files = list(runner_cwd.glob("*.sqlite3"))
        assert not out_files, "Expected no .sqlite3 files"

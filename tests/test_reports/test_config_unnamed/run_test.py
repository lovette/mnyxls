from __future__ import annotations  # noqa: INP001

from typing import TYPE_CHECKING

from tests.testutils import assert_compare_xlsx, cli_invoke, tmp_path_cwd

if TYPE_CHECKING:
    import pytest
    from click.testing import CliRunner

from pathlib import Path

from mnyxls.__main__ import cli

######################################################################
# Tests


def test_config_unnamed(  # noqa: D103
    runner: CliRunner,
    resources_dir: Path,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    # Parse txns report
    # Files output: Workbook named "result.xlsx", contents checked
    # Configuration file: Unnamed, so default name `mnyxls.yaml` is used

    test_cwd = Path.cwd()
    result_name = "result.xlsx"

    # These files must be accessible in `runner_cwd`
    tmp_path_locals = ["mnyxls.yaml"]

    cli_args: list[str | Path] = [
        "--no-db",
        "--xls-file",
        result_name,
        resources_dir / "sample_reports/txns_2018_2022.csv",
    ]

    with tmp_path_cwd(tmp_path, tmp_path_locals) as runner_cwd:
        cli_invoke(cli, cli_args, runner, caplog)

        if runner_cwd != test_cwd:
            out_files = list(runner_cwd.glob("*.xlsx"))
            assert len(out_files) == 1, "Expected an .xlsx file"

        result_path = runner_cwd / result_name
        assert result_path.exists(), "Expected an .xlsx file"

        out_files = list(runner_cwd.glob("*.sqlite3"))
        assert not out_files, "Expected no .sqlite3 files"

        assert_compare_xlsx(result_path, test_cwd / "expected_result.xlsx")

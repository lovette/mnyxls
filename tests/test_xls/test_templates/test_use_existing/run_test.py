from __future__ import annotations  # noqa: INP001

from typing import TYPE_CHECKING

from tests.testutils import create_and_compare_workbook

if TYPE_CHECKING:
    from pathlib import Path

    import pytest
    from click.testing import CliRunner


from mnyxls.__main__ import cli

######################################################################
# Tests


def test_templates_use_existing(  # noqa: D103
    runner: CliRunner,
    tmp_path: Path,
    resources_dir: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Generate workbook `result.xlsx` and compare it with `expected_result.xlsx`
    # using the main configuration file `mnyxls_tests.yaml` and
    # local workbook configuration file `mnyxls_workbook.yaml`.
    # These files must be accessible in `runner_cwd`

    tmp_path_locals = ["mnyxls_template.xlsx"]

    create_and_compare_workbook(
        cli,
        runner,
        tmp_path,
        resources_dir,
        caplog,
        tmp_path_locals=tmp_path_locals,
    )

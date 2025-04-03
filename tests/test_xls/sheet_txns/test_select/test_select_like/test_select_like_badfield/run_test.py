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


def test_txns_select_like_badfield(  # noqa: D103
    runner: CliRunner,
    tmp_path: Path,
    resources_dir: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    result = create_and_compare_workbook(
        cli,
        runner,
        tmp_path,
        resources_dir,
        caplog,
        expected_exit_code=1,
        assert_warnings=False,
    )

    assert "'era': Field does not support LIKE condition." in result.stderr

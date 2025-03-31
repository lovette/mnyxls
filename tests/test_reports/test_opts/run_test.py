from __future__ import annotations  # noqa: INP001

from typing import TYPE_CHECKING

from mnyxls.__main__ import cli
from tests.testutils import cli_invoke

if TYPE_CHECKING:
    from pathlib import Path

    import pytest
    from click.testing import CliRunner

######################################################################
# Tests


def test_noreports(runner: CliRunner, caplog: pytest.LogCaptureFixture) -> None:  # noqa: D103
    # No reports specified
    # Files output: None
    # Configuration file: None (use defaults)

    cli_args: list[str | Path] = []

    result = cli_invoke(cli, cli_args, runner, caplog, expected_exit_code=2)

    assert "Error: No reports specified;" in result.stderr

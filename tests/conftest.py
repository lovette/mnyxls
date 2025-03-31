"""Define fixtures accessible to all tests.

https://docs.pytest.org/en/stable/reference/fixtures.html#conftest-py-sharing-fixtures-across-multiple-files
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner

if TYPE_CHECKING:
    from pathlib import Path

######################################################################
# Fixtures invoked prior to each test (autouse=True)


@pytest.fixture(autouse=True)
def set_test_cwd(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Invoked prior to each test to set the current working directory to the test directory.

    We do this as a convenience so each test may rely on relative paths.
    Working directory is restored after the test.

    Credit to:
    https://stackoverflow.com/a/62055409

    Args:
        request (FixtureRequest): The request object.
        monkeypatch (pytest.MonkeyPatch): The monkeypatch object.
    """
    monkeypatch.chdir(request.fspath.dirname)  # type: ignore[reportAttributeAccessIssue]


######################################################################
# Fixtures


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI runner instance for each test.

    Returns:
        CliRunner
    """
    # `mix_stderr` is going away in Click 8.2...
    # https://github.com/pallets/click/pull/2523
    return CliRunner(mix_stderr=False)


@pytest.fixture
def resources_dir(pytestconfig: pytest.Config) -> Path:
    """Return the path to the tests resources directory.

    Returns:
        Path
    """
    return pytestconfig.rootpath / "tests/testresources"

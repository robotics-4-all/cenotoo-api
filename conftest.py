"""
Root conftest: patches Cassandra before any module imports it.

Several utility modules (user_utils, organization_utils, etc.) call
get_cassandra_session() at module level. This must be mocked before
Python collects test files, so we patch in pytest_configure.
"""

from unittest.mock import MagicMock, patch

_mock_session = MagicMock()

_patches = []


def pytest_configure(config):
    """Patch Cassandra session before tests run."""
    del config
    p = patch(
        "utilities.cassandra_connector.get_cassandra_session",
        return_value=_mock_session,
    )
    p.start()
    _patches.append(p)


def pytest_unconfigure(config):
    """Stop all patches after tests finish."""
    del config
    for p in _patches:
        p.stop()
    _patches.clear()

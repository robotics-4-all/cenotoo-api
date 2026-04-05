"""
Root conftest: patches Cassandra and PostgreSQL before any module imports them.

Several utility modules call get_cassandra_session() or postgres_connector
functions at module level. These must be mocked before Python collects test
files, so we patch in pytest_configure.
"""

from unittest.mock import MagicMock, patch

_mock_session = MagicMock()
_mock_pg_pool = MagicMock()
_mock_pg_conn = MagicMock()

_patches = []


def pytest_configure(config):
    """Patch Cassandra and PostgreSQL before tests run."""
    del config
    p1 = patch(
        "utilities.cassandra_connector.get_cassandra_session",
        return_value=_mock_session,
    )
    p1.start()
    _patches.append(p1)

    _mock_pg_pool.getconn.return_value = _mock_pg_conn
    p2 = patch(
        "utilities.postgres_connector.get_postgres_pool",
        return_value=_mock_pg_pool,
    )
    p2.start()
    _patches.append(p2)


def pytest_unconfigure(config):
    """Stop all patches after tests finish."""
    del config
    for p in _patches:
        p.stop()
    _patches.clear()

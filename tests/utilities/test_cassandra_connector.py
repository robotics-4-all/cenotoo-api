"""Tests for utilities/cassandra_connector.py — session management."""

from unittest.mock import MagicMock

from utilities.cassandra_connector import (
    get_cluster,
    get_session,
    set_cluster,
    set_session,
    shutdown_cassandra,
)


class TestShutdownCassandra:
    """Tests for shutdown_cassandra."""

    def test_shutdown_with_active_session_and_cluster(self):
        """Verify shutdown closes active session and cluster."""
        mock_session = MagicMock()
        mock_cluster = MagicMock()
        set_session(mock_session)
        set_cluster(mock_cluster)

        shutdown_cassandra()

        mock_session.shutdown.assert_called_once()
        mock_cluster.shutdown.assert_called_once()
        assert get_session() is None
        assert get_cluster() is None

    def test_shutdown_with_no_session(self):
        """Verify shutdown handles missing session and cluster gracefully."""
        set_session(None)
        set_cluster(None)

        shutdown_cassandra()

        assert get_session() is None
        assert get_cluster() is None

    def test_shutdown_session_only(self):
        """Verify shutdown closes active session when cluster is missing."""
        mock_session = MagicMock()
        set_session(mock_session)
        set_cluster(None)

        shutdown_cassandra()

        mock_session.shutdown.assert_called_once()
        assert get_session() is None
        assert get_cluster() is None

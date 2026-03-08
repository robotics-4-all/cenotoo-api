"""Cassandra database connection management.

This module provides a configured Cassandra session for database operations
with the metadata keyspace.
"""

from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy

from config import settings


def get_cassandra_session():
    """Create and return a configured Cassandra database session.

    Returns:
        Configured Cassandra session connected to the metadata keyspace.
    """
    # Replace with your actual datacenter name
    execution_profile = ExecutionProfile(
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
        request_timeout=60.0,  # Increase the timeout to 60 seconds
        retry_policy=RetryPolicy(),  # Optional: Retry policy for failed requests
    )

    cluster = Cluster(
        [settings.cassandra_contact_points],
        port=settings.cassandra_port,
        protocol_version=5,
        execution_profiles={EXEC_PROFILE_DEFAULT: execution_profile},
    )
    # Make sure the keyspace is set correctly
    session = cluster.connect("metadata")
    return session


_state = {"cluster": None, "session": None}


def get_cluster():
    """Return the current Cassandra cluster instance."""
    return _state["cluster"]


def get_session():
    """Return the current Cassandra session instance."""
    return _state["session"]


def set_cluster(cluster):
    """Set the current Cassandra cluster instance."""
    _state["cluster"] = cluster


def set_session(session):
    """Set the current Cassandra session instance."""
    _state["session"] = session


def shutdown_cassandra():
    """Shutdown the Cassandra session and cluster."""
    if _state["session"]:
        _state["session"].shutdown()
        _state["session"] = None
    if _state["cluster"]:
        _state["cluster"].shutdown()
        _state["cluster"] = None

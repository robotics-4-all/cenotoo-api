"""Cassandra database connection management.

This module provides a configured Cassandra session for database operations
with the metadata keyspace.
"""

import logging
import time

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy

from config import settings

logger = logging.getLogger(__name__)

_MAX_RETRIES = 10
_INITIAL_BACKOFF = 2.0


def get_cassandra_session():
    """Create and return a configured Cassandra database session.

    Retries with exponential backoff if Cassandra is not yet available.

    Returns:
        Configured Cassandra session connected to the metadata keyspace.
    """
    execution_profile = ExecutionProfile(
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=settings.cassandra_dc),
        request_timeout=60.0,
        retry_policy=RetryPolicy(),
    )

    cluster_kwargs = {
        "contact_points": [settings.cassandra_contact_points],
        "port": settings.cassandra_port,
        "protocol_version": 4,
        "execution_profiles": {EXEC_PROFILE_DEFAULT: execution_profile},
    }

    if settings.cassandra_username:
        cluster_kwargs["auth_provider"] = PlainTextAuthProvider(
            username=settings.cassandra_username,
            password=settings.cassandra_password,
        )

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            cluster = Cluster(**cluster_kwargs)
            session = cluster.connect("metadata")
            logger.info("Connected to Cassandra (attempt %d/%d)", attempt, _MAX_RETRIES)
            return session
        except Exception as exc:
            if attempt == _MAX_RETRIES:
                logger.error("Failed to connect to Cassandra after %d attempts", _MAX_RETRIES)
                raise
            backoff = _INITIAL_BACKOFF * (2 ** (attempt - 1))
            logger.warning(
                "Cassandra not ready (attempt %d/%d): %s — retrying in %.0fs",
                attempt,
                _MAX_RETRIES,
                exc,
                backoff,
            )
            time.sleep(backoff)
    raise RuntimeError(f"Failed to connect to Cassandra after {_MAX_RETRIES} attempts")


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

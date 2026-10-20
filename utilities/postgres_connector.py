"""PostgreSQL database connection management.

This module provides a connection pool and helper functions for PostgreSQL
operations using psycopg2 with NamedTupleCursor for attribute-style row access.
"""

import logging
import time

import psycopg2
import psycopg2.extras
import psycopg2.pool

psycopg2.extras.register_uuid()

from config import settings

logger = logging.getLogger(__name__)

_MAX_RETRIES = 10
_INITIAL_BACKOFF = 2.0
_state = {"pool": None}


def get_postgres_pool():
    """Return a cached PostgreSQL connection pool, creating one if needed.

    Uses a lazy singleton with exponential-backoff retry on first call.

    Returns:
        ThreadedConnectionPool connected to PostgreSQL.
    """
    if _state["pool"] is not None:
        return _state["pool"]
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                host=settings.postgres_host,
                port=settings.postgres_port,
                dbname=settings.postgres_db,
                user=settings.postgres_user,
                password=settings.postgres_password,
            )
            logger.info("Connected to PostgreSQL (attempt %d/%d)", attempt, _MAX_RETRIES)
            _state["pool"] = pool
            return pool
        except Exception as exc:
            if attempt == _MAX_RETRIES:
                raise
            backoff = _INITIAL_BACKOFF * (2 ** (attempt - 1))
            logger.warning(
                "PostgreSQL not ready (attempt %d/%d): %s — retrying in %.0fs",
                attempt,
                _MAX_RETRIES,
                exc,
                backoff,
            )
            time.sleep(backoff)
    raise RuntimeError(f"Failed to connect to PostgreSQL after {_MAX_RETRIES} attempts")


def get_pg_conn():
    """Borrow a connection from the pool."""
    return get_postgres_pool().getconn()


def release_pg_conn(conn):
    """Return a connection to the pool."""
    get_postgres_pool().putconn(conn)


def shutdown_postgres():
    """Close all connections in the pool."""
    if _state["pool"]:
        _state["pool"].closeall()
        _state["pool"] = None


def pg_execute(query, params=None):
    """Execute a write query (INSERT/UPDATE/DELETE) and commit.

    Returns:
        List of rows (empty for non-SELECT statements).
    """
    conn = get_pg_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
            cur.execute(query, params)
            conn.commit()
            try:
                return cur.fetchall()
            except psycopg2.ProgrammingError:
                return []
    except Exception:
        conn.rollback()
        raise
    finally:
        release_pg_conn(conn)


def pg_fetchone(query, params=None):
    """Execute a SELECT query and return the first row as a named tuple."""
    conn = get_pg_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
            cur.execute(query, params)
            return cur.fetchone()
    finally:
        release_pg_conn(conn)


def pg_fetchall(query, params=None):
    """Execute a SELECT query and return all rows as named tuples."""
    conn = get_pg_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()
    finally:
        release_pg_conn(conn)

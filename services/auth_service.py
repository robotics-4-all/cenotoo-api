"""Authentication service layer.

This module provides authentication services including password hashing,
JWT token creation and verification, refresh tokens, and token revocation.
"""

import logging
import uuid
from datetime import datetime, timedelta

import bcrypt
import jwt
from fastapi import HTTPException, status

from config import settings

logger = logging.getLogger(__name__)

SECRET_KEY = settings.jwt_secret_key
ALGORITHM = settings.jwt_algorithm
ACCESS_TOKEN_EXPIRE_MINUTES = settings.jwt_expiration_minutes
REFRESH_TOKEN_EXPIRE_DAYS = settings.jwt_refresh_expiration_days

_revoked_tokens: set[str] = set()


def hash_password(password: str) -> str:
    """Hash a plaintext password using bcrypt."""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a plaintext password against a hashed password."""
    return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))


def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    """Create a new JWT access token."""
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire, "type": "access", "jti": str(uuid.uuid4())})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def create_refresh_token(data: dict, expires_delta: timedelta | None = None) -> str:
    """Create a new JWT refresh token."""
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS))
    to_encode.update({"exp": expire, "type": "refresh", "jti": str(uuid.uuid4())})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def verify_jwt_token(token: str) -> str:
    """Verify a JWT access token and return the username."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str | None = payload.get("sub")
        jti: str | None = payload.get("jti")
        token_type: str | None = payload.get("type")

        if username is None:
            raise credentials_exception

        if token_type == "refresh":
            raise credentials_exception

        if jti and is_token_revoked(jti):
            raise credentials_exception

        return username
    except jwt.InvalidTokenError as e:
        raise credentials_exception from e


def verify_refresh_token(token: str) -> str:
    """Verify a JWT refresh token and return the username."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired refresh token",
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str | None = payload.get("sub")
        jti: str | None = payload.get("jti")
        token_type: str | None = payload.get("type")

        if username is None or token_type != "refresh":
            raise credentials_exception

        if jti and is_token_revoked(jti):
            raise credentials_exception

        return username
    except jwt.InvalidTokenError as e:
        raise credentials_exception from e


def revoke_token(token: str) -> None:
    """Revoke a JWT token by adding its JTI to the blocklist."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        jti = payload.get("jti")
        if jti:
            _revoked_tokens.add(jti)
            _persist_revoked_token(jti, payload.get("exp"))
    except jwt.InvalidTokenError:
        pass


def is_token_revoked(jti: str) -> bool:
    """Check if a token JTI is in the revoked list."""
    if jti in _revoked_tokens:
        return True
    return _check_revoked_in_db(jti)


def _persist_revoked_token(jti: str, exp_timestamp: int | None) -> None:
    try:
        from utilities.cassandra_connector import get_cassandra_session

        session = get_cassandra_session()
        query = (
            "INSERT INTO metadata.revoked_tokens (jti, revoked_at, expires_at) VALUES (%s, %s, %s)"
        )
        expires_at = datetime.utcfromtimestamp(exp_timestamp) if exp_timestamp else None
        session.execute(query, (jti, datetime.utcnow(), expires_at))
    except Exception:
        logger.warning("Failed to persist revoked token %s to database", jti)


def _check_revoked_in_db(jti: str) -> bool:
    try:
        from utilities.cassandra_connector import get_cassandra_session

        session = get_cassandra_session()
        query = "SELECT jti FROM metadata.revoked_tokens WHERE jti=%s LIMIT 1"
        result = session.execute(query, (jti,)).one()
        if result:
            _revoked_tokens.add(jti)
            return True
    except Exception:
        pass
    return False

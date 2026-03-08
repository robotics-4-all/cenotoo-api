from datetime import timedelta

import jwt
import pytest

from config import settings
from services.auth_service import (
    create_access_token,
    hash_password,
    verify_jwt_token,
    verify_password,
)


class TestPasswordHashing:
    """Tests for password hashing."""

    def test_hash_produces_bcrypt_output(self):
        """Verify that hashing a password produces a valid bcrypt output string."""
        hashed = hash_password("secret123")
        assert hashed.startswith("$2b$")

    def test_verify_correct_password(self):
        """Verify that a correct password matches its hash."""
        hashed = hash_password("mypassword")
        assert verify_password("mypassword", hashed) is True

    def test_verify_wrong_password(self):
        """Verify that an incorrect password does not match the hash."""
        hashed = hash_password("mypassword")
        assert verify_password("wrongpassword", hashed) is False

    def test_different_hashes_for_same_input(self):
        """Verify that hashing the same password twice produces different hashes due to salting."""
        h1 = hash_password("same")
        h2 = hash_password("same")
        assert h1 != h2  # bcrypt uses random salt


class TestCreateAccessToken:
    """Tests for create_access_token."""

    def test_returns_string(self):
        """Verify that creating an access token returns a string."""
        token = create_access_token(data={"sub": "testuser"})
        assert isinstance(token, str)

    def test_contains_subject_claim(self):
        """Verify that the created access token contains the correct subject claim."""
        token = create_access_token(data={"sub": "alice"})
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
        assert payload["sub"] == "alice"

    def test_contains_expiration_claim(self):
        """Verify that the created access token contains an expiration claim."""
        token = create_access_token(data={"sub": "bob"})
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
        assert "exp" in payload

    def test_custom_expiry(self):
        """Verify that creating an access token with a custom expiry sets the expiration claim."""
        token = create_access_token(
            data={"sub": "charlie"},
            expires_delta=timedelta(minutes=5),
        )
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
        assert "exp" in payload


class TestVerifyJwtToken:
    """Tests for verify_jwt_token."""

    def test_valid_token_returns_username(self):
        """Verify that a valid token returns the correct username."""
        token = create_access_token(data={"sub": "testuser"})
        assert verify_jwt_token(token) == "testuser"

    def test_expired_token_raises(self):
        """Verify that an expired token raises an HTTPException."""
        token = create_access_token(
            data={"sub": "testuser"},
            expires_delta=timedelta(seconds=-1),
        )
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            verify_jwt_token(token)
        assert exc_info.value.status_code == 401

    def test_invalid_token_raises(self):
        """Verify that an invalid token raises an HTTPException."""
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            verify_jwt_token("totally.invalid.token")
        assert exc_info.value.status_code == 401

    def test_missing_sub_claim_raises(self):
        """Verify that a token missing the subject claim raises an HTTPException."""
        token = create_access_token(data={"other": "value"})
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            verify_jwt_token(token)
        assert exc_info.value.status_code == 401

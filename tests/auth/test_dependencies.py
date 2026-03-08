import uuid
from collections import namedtuple
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from dependencies import (
    check_api_key,
    get_organization_id,
    validate_api_key,
    verify_superadmin,
)


class TestGetOrganizationId:
    """Tests for get_organization_id."""

    def test_returns_uuid(self):
        """Verify that getting the organization ID returns a valid UUID object."""
        with patch("dependencies.settings") as mock_settings:
            mock_settings.organization_id = str(uuid.uuid4())
            result = get_organization_id()
        assert isinstance(result, uuid.UUID)


class TestCheckApiKey:
    """Tests for check_api_key."""

    def test_matching_project_and_valid_role(self):
        """Verify that checking an API key passes for a matching project and valid role."""
        pid = uuid.uuid4()
        assert check_api_key("master", pid, pid, ["master", "read"]) is True

    def test_mismatched_project_raises(self):
        """Verify that checking an API key raises an HTTPException for a mismatched project."""
        with pytest.raises(HTTPException) as exc_info:
            check_api_key("master", uuid.uuid4(), uuid.uuid4(), ["master"])
        assert exc_info.value.status_code == 403

    def test_insufficient_role_raises(self):
        """Verify that checking an API key raises an HTTPException for an insufficient role."""
        pid = uuid.uuid4()
        with pytest.raises(HTTPException) as exc_info:
            check_api_key("read", pid, pid, ["master"])
        assert exc_info.value.status_code == 403


class TestValidateApiKey:
    """Tests for validate_api_key."""

    def test_empty_api_key_raises(self, mock_cassandra_session):
        """Verify that validating a None API key raises an HTTPException."""
        del mock_cassandra_session
        with pytest.raises(HTTPException) as exc_info:
            validate_api_key(None, uuid.uuid4())
        assert exc_info.value.status_code == 401

    def test_empty_string_api_key_raises(self, mock_cassandra_session):
        """Verify that validating an empty string API key raises an HTTPException."""
        del mock_cassandra_session
        with pytest.raises(HTTPException) as exc_info:
            validate_api_key("", uuid.uuid4())
        assert exc_info.value.status_code == 401

    def test_valid_key_returns_tuple(self, mock_cassandra_session):
        """Verify that validating a valid API key returns a tuple of key type and project ID."""
        pid = uuid.uuid4()
        KeyRow = namedtuple("KeyRow", ["key_type", "project_id"])
        mock_result = MagicMock()
        mock_result.one.return_value = KeyRow(key_type="master", project_id=pid)
        mock_cassandra_session.execute.return_value = mock_result
        with patch("dependencies.get_cassandra_session", return_value=mock_cassandra_session):
            result = validate_api_key("valid-key-hex", pid)
        assert result == ("master", pid)

    def test_key_not_found_raises(self, mock_cassandra_session):
        """Verify that validating a non-existent API key raises an HTTPException."""
        mock_result = MagicMock()
        mock_result.one.return_value = None
        mock_cassandra_session.execute.return_value = mock_result
        with (
            patch("dependencies.get_cassandra_session", return_value=mock_cassandra_session),
            pytest.raises(HTTPException) as exc_info,
        ):
            validate_api_key("nonexistent", uuid.uuid4())
        assert exc_info.value.status_code == 403

    def test_db_exception_raises_403(self, mock_cassandra_session):
        """Verify that a database exception during API key validation raises a 403 HTTPException."""
        mock_cassandra_session.execute.side_effect = Exception("connection lost")
        with (
            patch("dependencies.get_cassandra_session", return_value=mock_cassandra_session),
            pytest.raises(HTTPException) as exc_info,
        ):
            validate_api_key("some-key", uuid.uuid4())
        assert exc_info.value.status_code == 403


class TestVerifySuperadmin:
    """Tests for verify_superadmin."""

    def test_superadmin_passes(self):
        """Verify that a superadmin user passes the superadmin verification."""
        UserRow = namedtuple("UserRow", ["role", "username"])
        user = UserRow(role="superadmin", username="admin")
        result = verify_superadmin(current_user=user)
        assert result == user

    def test_non_superadmin_raises(self):
        """Verify that a non-superadmin user raises an HTTPException."""
        UserRow = namedtuple("UserRow", ["role", "username"])
        user = UserRow(role="user", username="bob")
        with pytest.raises(HTTPException) as exc_info:
            verify_superadmin(current_user=user)
        assert exc_info.value.status_code == 403

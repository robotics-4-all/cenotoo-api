import uuid
from collections import namedtuple
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from utilities.project_keys_utils import (
    delete_key_by_value,
    delete_keys_by_category,
    fetch_project_keys_by_category,
    generate_key,
    get_project_key_by_value,
    insert_project_key,
    update_project_key,
)

KeyRow = namedtuple("KeyRow", ["id", "api_key", "key_type", "created_at", "project_id"])


class TestGenerateKey:
    """Tests for generate_key."""

    def test_returns_hex_string(self):
        """Verify generate_key returns a 64-character hex string."""
        result = generate_key()

        assert isinstance(result, str)
        assert len(result) == 64

    def test_different_calls_produce_different_keys(self):
        """Verify generate_key produces unique keys on subsequent calls."""
        key1 = generate_key()
        key2 = generate_key()

        assert key1 != key2


class TestInsertProjectKey:
    """Tests for insert_project_key."""

    def test_happy_path(self, mock_cassandra_session):
        """Verify insert_project_key successfully inserts and returns a new key."""
        project_id = uuid.uuid4()

        result = insert_project_key(project_id, "write")

        assert isinstance(result, str)
        assert len(result) == 64
        mock_cassandra_session.execute.assert_called_once()

    def test_exception_raises_http_500(self, mock_cassandra_session):
        """Verify insert_project_key raises HTTP 500 on database error."""
        project_id = uuid.uuid4()
        mock_cassandra_session.execute.side_effect = Exception("DB error")

        with pytest.raises(HTTPException) as exc_info:
            insert_project_key(project_id, "read")

        assert exc_info.value.status_code == 500
        assert "Failed to create project key" in exc_info.value.detail


class TestFetchProjectKeysByCategory:
    """Tests for fetch_project_keys_by_category."""

    def test_all_keys(self, mock_cassandra_session):
        """Verify fetch_project_keys_by_category returns all keys when category is 'all'."""
        project_id = uuid.uuid4()
        keys = [
            KeyRow(
                id=uuid.uuid4(),
                api_key="abc",
                key_type="read",
                created_at="2024-01-01",
                project_id=project_id,
            ),
        ]
        mock_cassandra_session.execute.return_value = MagicMock(all=MagicMock(return_value=keys))

        result = fetch_project_keys_by_category(project_id, "all")

        assert result == keys
        query = mock_cassandra_session.execute.call_args[0][0]
        assert "key_type" not in query.split("WHERE")[1].split("ALLOW")[0]

    def test_specific_category(self, mock_cassandra_session):
        """Verify fetch_project_keys_by_category returns keys for a specific category."""
        project_id = uuid.uuid4()
        keys = [
            KeyRow(
                id=uuid.uuid4(),
                api_key="abc",
                key_type="read",
                created_at="2024-01-01",
                project_id=project_id,
            ),
        ]
        mock_cassandra_session.execute.return_value = MagicMock(all=MagicMock(return_value=keys))

        result = fetch_project_keys_by_category(project_id, "read")

        assert result == keys
        args = mock_cassandra_session.execute.call_args
        assert args[0][1] == (project_id, "read")

    def test_no_keys_raises_404(self, mock_cassandra_session):
        """Verify fetch_project_keys_by_category raises HTTP 404 when no keys are found."""
        project_id = uuid.uuid4()
        mock_cassandra_session.execute.return_value = MagicMock(all=MagicMock(return_value=[]))

        with pytest.raises(HTTPException) as exc_info:
            fetch_project_keys_by_category(project_id, "read")

        assert exc_info.value.status_code == 404

    def test_exception_raises_http_500(self, mock_cassandra_session):
        """Verify fetch_project_keys_by_category raises HTTP 500 on database error."""
        project_id = uuid.uuid4()
        mock_cassandra_session.execute.side_effect = Exception("DB error")

        with pytest.raises(HTTPException) as exc_info:
            fetch_project_keys_by_category(project_id, "all")

        assert exc_info.value.status_code == 500


class TestUpdateProjectKey:
    """Tests for update_project_key."""

    def test_happy_path(self, mock_cassandra_session):
        """Verify update_project_key successfully updates and returns a new key."""
        key_id = uuid.uuid4()

        result = update_project_key(key_id, "old_key_value")

        assert isinstance(result, str)
        assert len(result) == 64
        mock_cassandra_session.execute.assert_called_once()

    def test_exception_raises_http_500(self, mock_cassandra_session):
        """Verify update_project_key raises HTTP 500 on database error."""
        key_id = uuid.uuid4()
        mock_cassandra_session.execute.side_effect = Exception("DB error")

        with pytest.raises(HTTPException) as exc_info:
            update_project_key(key_id, "old_key_value")

        assert exc_info.value.status_code == 500


class TestDeleteKeysByCategory:
    """Tests for delete_keys_by_category."""

    def test_happy_path(self, mock_cassandra_session):
        """Verify delete_keys_by_category successfully deletes keys for a category."""
        project_id = uuid.uuid4()
        keys = [
            KeyRow(
                id=uuid.uuid4(),
                api_key="k1",
                key_type="read",
                created_at="2024-01-01",
                project_id=project_id,
            ),
            KeyRow(
                id=uuid.uuid4(),
                api_key="k2",
                key_type="read",
                created_at="2024-01-01",
                project_id=project_id,
            ),
        ]
        mock_cassandra_session.execute.return_value = MagicMock(all=MagicMock(return_value=keys))

        result = delete_keys_by_category(project_id, "read")

        assert result == {"message": "Keys deleted successfully."}
        # 1 call for fetch + 2 calls for delete
        assert mock_cassandra_session.execute.call_count == 3


class TestDeleteKeyByValue:
    """Tests for delete_key_by_value."""

    def test_happy_path(self, mock_cassandra_session):
        """Verify delete_key_by_value successfully deletes a specific key."""
        key_id = uuid.uuid4()

        delete_key_by_value(key_id)

        mock_cassandra_session.execute.assert_called_once()
        args = mock_cassandra_session.execute.call_args
        assert "DELETE FROM metadata.api_keys" in args[0][0]

    def test_exception_raises_http_500(self, mock_cassandra_session):
        """Verify delete_key_by_value raises HTTP 500 on database error."""
        key_id = uuid.uuid4()
        mock_cassandra_session.execute.side_effect = Exception("DB error")

        with pytest.raises(HTTPException) as exc_info:
            delete_key_by_value(key_id)

        assert exc_info.value.status_code == 500


class TestGetProjectKeyByValue:
    """Tests for get_project_key_by_value."""

    def test_happy_path(self, mock_cassandra_session):
        """Verify get_project_key_by_value returns the expected key row."""
        project_id = uuid.uuid4()
        key = KeyRow(
            id=uuid.uuid4(),
            api_key="abc123",
            key_type="read",
            created_at="2024-01-01",
            project_id=project_id,
        )
        mock_cassandra_session.execute.return_value = MagicMock(one=MagicMock(return_value=key))

        result = get_project_key_by_value("abc123", project_id)

        assert result == key

    def test_not_found_raises_404(self, mock_cassandra_session):
        """Verify get_project_key_by_value raises HTTP 404 when key is not found."""
        project_id = uuid.uuid4()
        mock_cassandra_session.execute.return_value = MagicMock(one=MagicMock(return_value=None))

        with pytest.raises(HTTPException) as exc_info:
            get_project_key_by_value("nonexistent", project_id)

        assert exc_info.value.status_code == 404

    def test_exception_raises_http_500(self, mock_cassandra_session):
        """Verify get_project_key_by_value raises HTTP 500 on database error."""
        project_id = uuid.uuid4()
        mock_cassandra_session.execute.side_effect = Exception("DB error")

        with pytest.raises(HTTPException) as exc_info:
            get_project_key_by_value("abc", project_id)

        assert exc_info.value.status_code == 500

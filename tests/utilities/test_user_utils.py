import uuid
from collections import namedtuple
from unittest.mock import MagicMock, patch

import pytest

from utilities.user_utils import (
    delete_user_from_db,
    get_all_users_in_organization,
    get_user_by_username,
    get_user_by_username_and_org_id,
    insert_user,
    update_user_password_in_db,
)

UserRow = namedtuple("UserRow", ["id", "username", "password", "role", "organization_id"])


class TestGetUserByUsername:
    """Tests for get_user_by_username."""

    def test_returns_user_when_found(self, _patch_postgres):
        """Verify get_user_by_username returns the user row when found."""
        user = UserRow(
            id=uuid.uuid4(),
            username="alice",
            password="hashed",
            role="user",
            organization_id=uuid.uuid4(),
        )

        with patch("utilities.user_utils.pg_fetchone", return_value=user):
            result = get_user_by_username("alice")

        assert result == user

    def test_returns_none_when_not_found(self, _patch_postgres):
        """Verify get_user_by_username returns None when user is not found."""
        with patch("utilities.user_utils.pg_fetchone", return_value=None):
            result = get_user_by_username("nonexistent")

        assert result is None


class TestGetUserByUsernameAndOrgId:
    """Tests for get_user_by_username_and_org_id."""

    @pytest.mark.asyncio
    async def test_returns_result(self, _patch_postgres):
        """Verify get_user_by_username_and_org_id returns the expected user row."""
        org_id = uuid.uuid4()
        row = MagicMock(id=uuid.uuid4())

        with patch("utilities.user_utils.pg_fetchone", return_value=row):
            result = await get_user_by_username_and_org_id("alice", org_id)

        assert result == row


class TestInsertUser:
    """Tests for insert_user."""

    @pytest.mark.asyncio
    async def test_executes_insert(self, _patch_postgres):
        """Verify insert_user executes correct SQL query."""
        user_id = uuid.uuid4()
        org_id = uuid.uuid4()

        with patch("utilities.user_utils.pg_execute") as mock_exec:
            await insert_user(user_id, org_id, "bob", "hashed_pw")

        mock_exec.assert_called_once()
        args = mock_exec.call_args
        assert "INSERT INTO users" in args[0][0]
        assert args[0][1] == (user_id, org_id, "bob", "hashed_pw", "member")


class TestDeleteUserFromDb:
    """Tests for delete_user_from_db."""

    @pytest.mark.asyncio
    async def test_executes_delete(self, _patch_postgres):
        """Verify delete_user_from_db executes correct SQL query."""
        user_id = uuid.uuid4()

        with patch("utilities.user_utils.pg_execute") as mock_exec:
            await delete_user_from_db(user_id)

        mock_exec.assert_called_once()
        args = mock_exec.call_args
        assert "DELETE FROM users" in args[0][0]
        assert args[0][1] == (user_id,)


class TestUpdateUserPasswordInDb:
    """Tests for update_user_password_in_db."""

    @pytest.mark.asyncio
    async def test_executes_update(self, _patch_postgres):
        """Verify update_user_password_in_db executes correct SQL query."""
        user_id = uuid.uuid4()

        with patch("utilities.user_utils.pg_execute") as mock_exec:
            await update_user_password_in_db(user_id, "new_hashed_pw")

        mock_exec.assert_called_once()
        args = mock_exec.call_args
        assert "UPDATE users SET password" in args[0][0]
        assert args[0][1] == ("new_hashed_pw", user_id)


class TestGetAllUsersInOrganization:
    """Tests for get_all_users_in_organization."""

    @pytest.mark.asyncio
    async def test_returns_list_of_users(self, _patch_postgres):
        """Verify get_all_users_in_organization returns a list of users."""
        org_id = uuid.uuid4()
        UserListRow = namedtuple("UserListRow", ["id", "username", "role"])
        rows = [
            UserListRow(id=uuid.uuid4(), username="alice", role="admin"),
            UserListRow(id=uuid.uuid4(), username="bob", role="member"),
        ]

        with patch("utilities.user_utils.pg_fetchall", return_value=rows):
            result = await get_all_users_in_organization(org_id)

        assert result == [
            {"username": "alice", "role": "admin"},
            {"username": "bob", "role": "member"},
        ]

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_users(self, _patch_postgres):
        """Verify get_all_users_in_organization returns an empty list when no users exist."""
        org_id = uuid.uuid4()

        with patch("utilities.user_utils.pg_fetchall", return_value=[]):
            result = await get_all_users_in_organization(org_id)

        assert result == []

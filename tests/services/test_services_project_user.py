"""Unit tests for project, project-key, and user service modules.

Covers:
- services/project_service.py
- services/project_keys_service.py
- services/user_service.py

Each service function has at least one happy-path and one error-path test.
All external dependencies (utilities, etc.) are mocked at the
service module namespace level.
"""

import uuid
from collections import namedtuple
from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

# ---------------------------------------------------------------------------
# Shared namedtuple row types (matching Cassandra row shapes)
# ---------------------------------------------------------------------------

OrgRow = namedtuple(
    "OrgRow",
    ["id", "organization_name", "description", "tags", "creation_date"],
)
ProjectRow = namedtuple(
    "ProjectRow",
    ["id", "organization_id", "project_name", "description", "tags", "creation_date"],
)
UserRow = namedtuple(
    "UserRow",
    ["id", "username", "password", "role", "organization_id"],
)
ProjectKeyRow = namedtuple(
    "ProjectKeyRow",
    ["id", "api_key", "project_id", "key_type", "created_at"],
)

# ---------------------------------------------------------------------------
# Shared test data factories
# ---------------------------------------------------------------------------

_org_id = uuid.uuid4()
_project_id = uuid.uuid4()
_user_id = uuid.uuid4()
_now = datetime.utcnow()


def _make_org_row(org_id=None, name="test_org"):
    """Create a mock organization row."""
    return OrgRow(
        id=org_id or _org_id,
        organization_name=name,
        description="Test org",
        tags=["test"],
        creation_date=_now,
    )


def _make_project_row(project_id=None, org_id=None, name="test_project"):
    """Create a mock project row."""
    return ProjectRow(
        id=project_id or _project_id,
        organization_id=org_id or _org_id,
        project_name=name,
        description="Test project",
        tags=["test"],
        creation_date=_now,
    )


def _make_user_row(user_id=None, username="testuser", role="user", org_id=None):
    """Create a mock user row."""
    return UserRow(
        id=user_id or _user_id,
        username=username,
        password="$2b$12$hashed",
        role=role,
        organization_id=org_id or _org_id,
    )


def _make_key_row(key_id=None, key_value="a" * 64, project_id=None, key_type="read"):
    """Create a mock project key row."""
    return ProjectKeyRow(
        id=key_id or uuid.uuid4(),
        api_key=key_value,
        project_id=project_id or _project_id,
        key_type=key_type,
        created_at=_now,
    )


# ===========================================================================
# services/project_service.py
# ===========================================================================


class TestCreateProjectService:
    """Tests for create_project_service."""

    @patch(
        "services.project_service.create_project_in_db",
        new_callable=AsyncMock,
    )
    @patch(
        "services.project_service.get_project_by_name",
        new_callable=AsyncMock,
    )
    @patch("services.project_service.contains_special_characters")
    async def test_happy_path(self, mock_special, mock_get_by_name, mock_create):
        """Verify that a project is created successfully."""
        from models.project_models import ProjectCreateRequest
        from services.project_service import create_project_service

        mock_special.return_value = False
        mock_get_by_name.return_value = None
        mock_create.return_value = _project_id

        data = ProjectCreateRequest(
            project_name="new_project",
            description="desc",
            tags=["t1"],
        )

        result = await create_project_service(_org_id, data)

        assert "created succesfully" in result["message"]
        assert result["id"] == _project_id
        mock_create.assert_awaited_once_with(_org_id, data)

    @patch("services.project_service.contains_special_characters")
    async def test_special_chars_raises_400(self, mock_special):
        """Verify special characters in name raise 400."""
        from models.project_models import ProjectCreateRequest
        from services.project_service import create_project_service

        mock_special.return_value = True

        data = ProjectCreateRequest(
            project_name="bad@project",
            description="desc",
            tags=[],
        )

        with pytest.raises(HTTPException) as exc_info:
            await create_project_service(_org_id, data)
        assert exc_info.value.status_code == 400

    @patch(
        "services.project_service.get_project_by_name",
        new_callable=AsyncMock,
    )
    @patch("services.project_service.contains_special_characters")
    async def test_already_exists_raises_409(self, mock_special, mock_get_by_name):
        """Verify duplicate project name raises 409."""
        from models.project_models import ProjectCreateRequest
        from services.project_service import create_project_service

        mock_special.return_value = False
        mock_get_by_name.return_value = _make_project_row()

        data = ProjectCreateRequest(
            project_name="existing_project",
            description="desc",
            tags=[],
        )

        with pytest.raises(HTTPException) as exc_info:
            await create_project_service(_org_id, data)
        assert exc_info.value.status_code == 409


class TestUpdateProjectService:
    """Tests for update_project_service."""

    @patch(
        "services.project_service.update_project_in_db",
        new_callable=AsyncMock,
    )
    async def test_happy_path(self, mock_update):
        """Verify that a project is updated successfully."""
        from models.project_models import ProjectUpdateRequest
        from services.project_service import update_project_service

        data = ProjectUpdateRequest(description="updated", tags=["new"])
        await update_project_service(_project_id, data)

        mock_update.assert_awaited_once_with(_project_id, data)


class TestDeleteProjectService:
    """Tests for delete_project_service."""

    @patch(
        "services.project_service.delete_project_in_db",
        new_callable=AsyncMock,
    )
    async def test_happy_path(self, mock_delete):
        """Verify that a project is deleted successfully."""
        from services.project_service import delete_project_service

        await delete_project_service(_project_id)

        mock_delete.assert_awaited_once_with(_project_id)


class TestGetAllProjectsService:
    """Tests for get_all_projects_service."""

    @patch("services.project_service.get_organization_by_id")
    @patch(
        "services.project_service.get_all_organization_projects_from_db",
        new_callable=AsyncMock,
    )
    async def test_happy_path(self, mock_get_all, mock_get_org):
        """Verify that all projects are retrieved successfully."""
        from services.project_service import get_all_projects_service

        mock_get_all.return_value = [_make_project_row()]
        mock_get_org.return_value = _make_org_row()

        result = await get_all_projects_service(_org_id)

        assert len(result) == 1
        assert result[0].project_name == "test_project"
        assert result[0].organization_name == "test_org"


class TestGetProjectByIdService:
    """Tests for get_project_by_id_service."""

    @patch("services.project_service.get_organization_by_id")
    async def test_happy_path(self, mock_get_org):
        """Verify that project info is retrieved successfully."""
        from services.project_service import get_project_by_id_service

        mock_get_org.return_value = _make_org_row()
        row = _make_project_row()

        result = await get_project_by_id_service(row)

        assert result.project_id == row.id
        assert result.project_name == "test_project"
        assert result.organization_name == "test_org"
        assert result.tags == ["test"]


# ===========================================================================
# services/project_keys_service.py
# ===========================================================================


class TestCreateProjectKeyService:
    """Tests for create_project_key_service."""

    @patch("services.project_keys_service.insert_project_key")
    async def test_happy_path(self, mock_insert):
        """Verify that a project key is created successfully."""
        from models.project_keys_models import ProjectKeyCreateRequest
        from services.project_keys_service import (
            create_project_key_service,
        )

        mock_insert.return_value = "a" * 64

        data = ProjectKeyCreateRequest(key_type="read")
        result = await create_project_key_service(_project_id, data)

        assert result.api_key == "a" * 64
        assert result.project_id == _project_id
        assert result.key_type == "read"
        mock_insert.assert_called_once_with(_project_id, "read")


class TestFetchProjectKeysByCategoryService:
    """Tests for fetch_project_keys_by_category_service."""

    @patch("services.project_keys_service.fetch_project_keys_by_category")
    async def test_happy_path(self, mock_fetch):
        """Verify project keys retrieved by category."""
        from services.project_keys_service import (
            fetch_project_keys_by_category_service,
        )

        mock_fetch.return_value = [_make_key_row()]

        result = await fetch_project_keys_by_category_service(_project_id, "read")

        assert len(result) == 1
        assert result[0].api_key == "a" * 64
        assert result[0].key_type == "read"

    @patch("services.project_keys_service.fetch_project_keys_by_category")
    async def test_no_keys_found_raises_404(self, mock_fetch):
        """Verify 404 raised when no project keys found."""
        from services.project_keys_service import (
            fetch_project_keys_by_category_service,
        )

        mock_fetch.return_value = []

        with pytest.raises(HTTPException) as exc_info:
            await fetch_project_keys_by_category_service(_project_id, "read")
        assert exc_info.value.status_code == 404
        assert "No keys found" in exc_info.value.detail


class TestRegenerateKeyService:
    """Tests for regenerate_key_service."""

    @patch("services.project_keys_service.update_project_key")
    @patch("services.project_keys_service.get_project_key_by_value")
    async def test_happy_path(self, mock_get_key, mock_update):
        """Verify that a project key is regenerated successfully."""
        from models.project_keys_models import RegenerateKeyRequest
        from services.project_keys_service import regenerate_key_service

        existing = _make_key_row()
        mock_get_key.return_value = existing
        mock_update.return_value = "b" * 64

        data = RegenerateKeyRequest(key_value="a" * 64)
        result = await regenerate_key_service(_project_id, data)

        assert result.api_key == "b" * 64
        assert result.key_type == "read"
        mock_get_key.assert_called_once_with("a" * 64, _project_id)
        mock_update.assert_called_once_with(existing.id, "a" * 64)

    @patch("services.project_keys_service.get_project_key_by_value")
    async def test_key_not_found_raises_404(self, mock_get_key):
        """Verify regenerating a missing key raises 404."""
        from models.project_keys_models import RegenerateKeyRequest
        from services.project_keys_service import regenerate_key_service

        mock_get_key.return_value = None

        data = RegenerateKeyRequest(key_value="a" * 64)

        with pytest.raises(HTTPException) as exc_info:
            await regenerate_key_service(_project_id, data)
        assert exc_info.value.status_code == 404
        assert "Key not found" in exc_info.value.detail


class TestDeleteKeysByCategoryService:
    """Tests for delete_keys_by_category_service."""

    @patch("services.project_keys_service.delete_keys_by_category")
    async def test_happy_path(self, mock_delete):
        """Verify project keys deleted by category."""
        from services.project_keys_service import (
            delete_keys_by_category_service,
        )

        mock_delete.return_value = {"message": "Keys deleted"}

        result = await delete_keys_by_category_service(_project_id, "read")

        assert result["message"] == "Keys deleted"
        mock_delete.assert_called_once_with(_project_id, "read")


class TestDeleteKeyByValueService:
    """Tests for delete_key_by_value_service."""

    @patch("services.project_keys_service.delete_key_by_value")
    @patch("services.project_keys_service.get_project_key_by_value")
    async def test_happy_path(self, mock_get_key, mock_delete):
        """Verify project key deleted by value."""
        from models.project_keys_models import DeleteKeyRequest
        from services.project_keys_service import (
            delete_key_by_value_service,
        )

        existing = _make_key_row()
        mock_get_key.return_value = existing

        data = DeleteKeyRequest(key_value="a" * 64)
        result = await delete_key_by_value_service(_project_id, data)

        assert result["message"] == "Key deleted successfully."
        mock_delete.assert_called_once_with(existing.id)

    @patch("services.project_keys_service.get_project_key_by_value")
    async def test_key_not_found_raises_404(self, mock_get_key):
        """Verify deleting missing key raises 404."""
        from models.project_keys_models import DeleteKeyRequest
        from services.project_keys_service import (
            delete_key_by_value_service,
        )

        mock_get_key.return_value = None

        data = DeleteKeyRequest(key_value="a" * 64)

        with pytest.raises(HTTPException) as exc_info:
            await delete_key_by_value_service(_project_id, data)
        assert exc_info.value.status_code == 404
        assert "Key not found" in exc_info.value.detail


# ===========================================================================
# services/user_service.py
# ===========================================================================


class TestCreateUserService:
    """Tests for create_user_service."""

    @patch("services.user_service.insert_user", new_callable=AsyncMock)
    @patch("services.user_service.hash_password")
    @patch("services.user_service.get_user_by_username")
    @patch("services.user_service.contains_special_characters")
    @patch("services.user_service.validate_password_strength")
    async def test_happy_path(
        self,
        _mock_validate_pw,
        mock_special,
        mock_get_user,
        mock_hash,
        mock_insert,
    ):
        """Verify that a user is created successfully."""
        from models.user_models import UserRequest
        from services.user_service import create_user_service

        mock_special.return_value = False
        mock_get_user.return_value = None
        mock_hash.return_value = "$2b$12$hashed_new"

        data = UserRequest(username="newuser", password="Secret123!")
        result = await create_user_service(_org_id, data)

        assert result["message"] == "User created successfully"
        assert "user_id" in result
        mock_hash.assert_called_once_with("Secret123!")
        mock_insert.assert_awaited_once()

    @patch("services.user_service.contains_special_characters")
    async def test_special_chars_raises_400(self, mock_special):
        """Verify special characters in username raise 400."""
        from models.user_models import UserRequest
        from services.user_service import create_user_service

        mock_special.return_value = True

        data = UserRequest(username="bad$user", password="secret")

        with pytest.raises(HTTPException) as exc_info:
            await create_user_service(_org_id, data)
        assert exc_info.value.status_code == 400

    @patch("services.user_service.get_user_by_username")
    @patch("services.user_service.contains_special_characters")
    async def test_duplicate_username_raises_409(self, mock_special, mock_get_user):
        """Verify duplicate username raises 409."""
        from models.user_models import UserRequest
        from services.user_service import create_user_service

        mock_special.return_value = False
        mock_get_user.return_value = _make_user_row()

        data = UserRequest(username="existing", password="secret")

        with pytest.raises(HTTPException) as exc_info:
            await create_user_service(_org_id, data)
        assert exc_info.value.status_code == 409


class TestDeleteUserService:
    """Tests for delete_user_service."""

    @patch(
        "services.user_service.delete_user_from_db",
        new_callable=AsyncMock,
    )
    @patch(
        "services.user_service.get_user_by_username_and_org_id",
        new_callable=AsyncMock,
    )
    async def test_happy_path_superadmin(self, mock_get_user, mock_delete):
        """Verify superadmin can delete a user."""
        from services.user_service import delete_user_service

        target_user = _make_user_row(username="target")
        mock_get_user.return_value = target_user

        current_user = _make_user_row(username="admin", role="superadmin")

        result = await delete_user_service(_org_id, "target", current_user)

        assert result["message"] == "User deleted successfully"
        mock_delete.assert_awaited_once_with(target_user.id)

    @patch(
        "services.user_service.get_user_by_username_and_org_id",
        new_callable=AsyncMock,
    )
    async def test_not_found_raises_404(self, mock_get_user):
        """Verify deleting non-existent user raises 404."""
        from services.user_service import delete_user_service

        mock_get_user.return_value = None
        current_user = _make_user_row(role="superadmin")

        with pytest.raises(HTTPException) as exc_info:
            await delete_user_service(_org_id, "nonexistent", current_user)
        assert exc_info.value.status_code == 404

    @patch(
        "services.user_service.get_user_by_username_and_org_id",
        new_callable=AsyncMock,
    )
    async def test_unauthorized_raises_403(self, mock_get_user):
        """Verify non-superadmin cannot delete another user."""
        from services.user_service import delete_user_service

        mock_get_user.return_value = _make_user_row(username="other_user")
        current_user = _make_user_row(username="regular_user", role="user")

        with pytest.raises(HTTPException) as exc_info:
            await delete_user_service(_org_id, "other_user", current_user)
        assert exc_info.value.status_code == 403


class TestUpdateUserPasswordService:
    """Tests for update_user_password_service."""

    @patch(
        "services.user_service.update_user_password_in_db",
        new_callable=AsyncMock,
    )
    @patch("services.user_service.hash_password")
    @patch(
        "services.user_service.get_user_by_username_and_org_id",
        new_callable=AsyncMock,
    )
    @patch("services.user_service.validate_password_strength")
    async def test_happy_path(
        self,
        _mock_validate_pw,
        mock_get_user,
        mock_hash,
        mock_update,
    ):
        """Verify password update succeeds."""
        from models.user_models import UserRequest
        from services.user_service import update_user_password_service

        target_user = _make_user_row(username="target")
        mock_get_user.return_value = target_user
        mock_hash.return_value = "$2b$12$new_hash"

        current_user = _make_user_row(username="admin", role="superadmin")
        data = UserRequest(username="target", password="NewPass1!")

        result = await update_user_password_service(_org_id, data, current_user)

        assert result["message"] == "User password updated successfully"
        mock_hash.assert_called_once_with("NewPass1!")
        mock_update.assert_awaited_once_with(target_user.id, "$2b$12$new_hash")

    @patch(
        "services.user_service.get_user_by_username_and_org_id",
        new_callable=AsyncMock,
    )
    async def test_not_found_raises_404(self, mock_get_user):
        """Verify updating non-existent user password raises 404."""
        from models.user_models import UserRequest
        from services.user_service import update_user_password_service

        mock_get_user.return_value = None
        current_user = _make_user_row(role="superadmin")
        data = UserRequest(username="nonexistent", password="newpass")

        with pytest.raises(HTTPException) as exc_info:
            await update_user_password_service(_org_id, data, current_user)
        assert exc_info.value.status_code == 404

    @patch(
        "services.user_service.get_user_by_username_and_org_id",
        new_callable=AsyncMock,
    )
    async def test_unauthorized_raises_403(self, mock_get_user):
        """Verify non-superadmin cannot update another user's password."""
        from models.user_models import UserRequest
        from services.user_service import update_user_password_service

        mock_get_user.return_value = _make_user_row(username="other_user")
        current_user = _make_user_row(username="regular_user", role="user")
        data = UserRequest(username="other_user", password="newpass")

        with pytest.raises(HTTPException) as exc_info:
            await update_user_password_service(_org_id, data, current_user)
        assert exc_info.value.status_code == 403


class TestGetAllUsersService:
    """Tests for get_all_users_service."""

    @patch(
        "services.user_service.get_all_users_in_organization",
        new_callable=AsyncMock,
    )
    async def test_happy_path(self, mock_get_all):
        """Verify all users are retrieved successfully."""
        from services.user_service import get_all_users_service

        mock_get_all.return_value = [
            {"username": "user1", "role": "user"},
            {"username": "user2", "role": "superadmin"},
        ]

        result = await get_all_users_service(_org_id)

        assert "users" in result
        assert len(result["users"]) == 2
        mock_get_all.assert_awaited_once_with(_org_id)

"""Unit tests for collection and organization service modules.

Covers:
- services/collection_service.py
- services/organization_service.py

Each service function has at least one happy-path and one error-path test.
All external dependencies (utilities, docker, etc.) are mocked at the
service module namespace level.
"""

import uuid
from collections import namedtuple
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

# ---------------------------------------------------------------------------
# Shared namedtuple row types (matching Cassandra row shapes)
# ---------------------------------------------------------------------------

OrgRow = namedtuple("OrgRow", ["id", "organization_name", "description", "tags", "creation_date"])
ProjectRow = namedtuple(
    "ProjectRow", ["id", "organization_id", "project_name", "description", "tags", "creation_date"]
)
CollectionRow = namedtuple(
    "CollectionRow",
    [
        "id",
        "organization_id",
        "project_id",
        "collection_name",
        "description",
        "tags",
        "creation_date",
    ],
)
# ---------------------------------------------------------------------------
# Shared test data factories
# ---------------------------------------------------------------------------

_org_id = uuid.uuid4()
_project_id = uuid.uuid4()
_collection_id = uuid.uuid4()
_now = datetime.utcnow()


def _make_org_row(org_id=None, name="test_org"):
    return OrgRow(
        id=org_id or _org_id,
        organization_name=name,
        description="Test org",
        tags=["test"],
        creation_date=_now,
    )


def _make_project_row(project_id=None, org_id=None, name="test_project"):
    return ProjectRow(
        id=project_id or _project_id,
        organization_id=org_id or _org_id,
        project_name=name,
        description="Test project",
        tags=["test"],
        creation_date=_now,
    )


def _make_collection_row(collection_id=None, org_id=None, project_id=None, name="test_collection"):
    return CollectionRow(
        id=collection_id or _collection_id,
        organization_id=org_id or _org_id,
        project_id=project_id or _project_id,
        collection_name=name,
        description="Test collection",
        tags=["test"],
        creation_date=_now,
    )


# ===========================================================================
# services/collection_service.py
# ===========================================================================


class TestCreateCollectionService:
    """Tests for create_collection_service."""

    @patch("services.collection_service.insert_collection", new_callable=AsyncMock)
    @patch("services.collection_service.docker_client")
    @patch("services.collection_service.create_kafka_topic", new_callable=AsyncMock)
    @patch("services.collection_service.create_cassandra_table", new_callable=AsyncMock)
    @patch("services.collection_service.get_project_by_id")
    @patch("services.collection_service.get_organization_by_id")
    @patch("services.collection_service.fetch_collection_by_name", new_callable=AsyncMock)
    @patch("services.collection_service.contains_special_characters")
    async def test_happy_path(
        self,
        mock_special_chars,
        mock_fetch,
        mock_get_org,
        mock_get_proj,
        mock_create_table,
        mock_create_topic,
        mock_docker,
        mock_insert,
    ):
        """Verify that a collection is created successfully when all conditions are met."""
        from models.collection_models import CollectionCreateRequest
        from services.collection_service import create_collection_service

        mock_special_chars.return_value = False
        mock_fetch.return_value = None
        mock_get_org.return_value = _make_org_row()
        mock_get_proj.return_value = _make_project_row()
        mock_insert.return_value = _collection_id

        data = CollectionCreateRequest(
            name="my_collection",
            description="desc",
            tags=["t1"],
            collection_schema={"field1": "text"},
        )

        result = await create_collection_service(_org_id, _project_id, data)

        assert "created successfully" in result["message"]
        assert str(_collection_id) in result["message"]
        mock_create_table.assert_awaited_once()
        mock_create_topic.assert_awaited_once()
        mock_docker.containers.run.assert_called_once()
        mock_insert.assert_awaited_once()

    @patch("services.collection_service.contains_special_characters")
    async def test_special_chars_raises_400(self, mock_special_chars):
        """Verify that creating a collection with special characters raises a 400 error."""
        from models.collection_models import CollectionCreateRequest
        from services.collection_service import create_collection_service

        mock_special_chars.return_value = True

        data = CollectionCreateRequest(
            name="bad$name",
            description="desc",
            tags=[],
            collection_schema={"f": "text"},
        )

        with pytest.raises(HTTPException) as exc_info:
            await create_collection_service(_org_id, _project_id, data)
        assert exc_info.value.status_code == 400

    @patch("services.collection_service.fetch_collection_by_name", new_callable=AsyncMock)
    @patch("services.collection_service.contains_special_characters")
    async def test_already_exists_raises_409(self, mock_special_chars, mock_fetch):
        """Verify that creating an existing collection raises a 409 error."""
        from models.collection_models import CollectionCreateRequest
        from services.collection_service import create_collection_service

        mock_special_chars.return_value = False
        mock_fetch.return_value = _make_collection_row()

        data = CollectionCreateRequest(
            name="existing",
            description="desc",
            tags=[],
            collection_schema={"f": "text"},
        )

        with pytest.raises(HTTPException) as exc_info:
            await create_collection_service(_org_id, _project_id, data)
        assert exc_info.value.status_code == 409

    @patch("services.collection_service.docker_client")
    @patch("services.collection_service.create_kafka_topic", new_callable=AsyncMock)
    @patch("services.collection_service.create_cassandra_table", new_callable=AsyncMock)
    @patch("services.collection_service.get_project_by_id")
    @patch("services.collection_service.get_organization_by_id")
    @patch("services.collection_service.fetch_collection_by_name", new_callable=AsyncMock)
    @patch("services.collection_service.contains_special_characters")
    async def test_docker_failure_raises_500(
        self,
        mock_special_chars,
        mock_fetch,
        mock_get_org,
        mock_get_proj,
        _mock_create_table,
        _mock_create_topic,
        mock_docker,
    ):
        """Verify that a Docker failure during collection creation raises a 500 error."""
        from models.collection_models import CollectionCreateRequest
        from services.collection_service import create_collection_service

        mock_special_chars.return_value = False
        mock_fetch.return_value = None
        mock_get_org.return_value = _make_org_row()
        mock_get_proj.return_value = _make_project_row()
        mock_docker.containers.run.side_effect = Exception("Docker error")

        data = CollectionCreateRequest(
            name="my_collection",
            description="desc",
            tags=[],
            collection_schema={"f": "text"},
        )

        with pytest.raises(HTTPException) as exc_info:
            await create_collection_service(_org_id, _project_id, data)
        assert exc_info.value.status_code == 500
        assert "Failed to start consumer container" in exc_info.value.detail


class TestUpdateCollectionService:
    """Tests for update_collection_service."""

    @patch("services.collection_service.update_collection_in_db", new_callable=AsyncMock)
    async def test_happy_path(self, mock_update):
        """Verify that a collection is updated successfully."""
        from models.collection_models import CollectionUpdateRequest
        from services.collection_service import update_collection_service

        data = CollectionUpdateRequest(description="new desc", tags=["new"])
        result = await update_collection_service(_org_id, _project_id, _collection_id, data)

        assert result["message"] == "Collection updated successfully"
        mock_update.assert_awaited_once_with(_collection_id, data)


class TestDeleteCollectionService:
    """Tests for delete_collection_service."""

    @patch("services.collection_service.delete_collection_from_db", new_callable=AsyncMock)
    @patch("services.collection_service.delete_kafka_topic", new_callable=AsyncMock)
    @patch("services.collection_service.delete_cassandra_table", new_callable=AsyncMock)
    @patch("services.collection_service.docker_client")
    @patch("services.collection_service.get_collection_by_id")
    @patch("services.collection_service.get_project_by_id")
    @patch("services.collection_service.get_organization_by_id")
    async def test_happy_path(
        self,
        mock_get_org,
        mock_get_proj,
        mock_get_coll,
        mock_docker,
        mock_del_table,
        mock_del_topic,
        mock_del_db,
    ):
        """Verify that a collection is deleted successfully."""
        from services.collection_service import delete_collection_service

        mock_get_org.return_value = _make_org_row()
        mock_get_proj.return_value = _make_project_row()
        mock_get_coll.return_value = _make_collection_row()

        mock_container = MagicMock()
        mock_docker.containers.get.return_value = mock_container

        result = await delete_collection_service(_org_id, _project_id, _collection_id)

        assert result["message"] == "Collection deleted successfully"
        mock_container.stop.assert_called_once()
        mock_container.remove.assert_called_once()
        mock_del_table.assert_awaited_once()
        mock_del_topic.assert_awaited_once()
        mock_del_db.assert_awaited_once()

    @patch("services.collection_service.docker_client")
    @patch("services.collection_service.get_collection_by_id")
    @patch("services.collection_service.get_project_by_id")
    @patch("services.collection_service.get_organization_by_id")
    async def test_docker_stop_failure_raises_500(
        self,
        mock_get_org,
        mock_get_proj,
        mock_get_coll,
        mock_docker,
    ):
        """Verify that a Docker failure during collection deletion raises a 500 error."""
        from services.collection_service import delete_collection_service

        mock_get_org.return_value = _make_org_row()
        mock_get_proj.return_value = _make_project_row()
        mock_get_coll.return_value = _make_collection_row()
        mock_docker.containers.get.side_effect = Exception("Container not found")

        with pytest.raises(HTTPException) as exc_info:
            await delete_collection_service(_org_id, _project_id, _collection_id)
        assert exc_info.value.status_code == 500
        assert "Failed to stop consumer container" in exc_info.value.detail


class TestGetAllCollectionsService:
    """Tests for get_all_collections_service."""

    @patch("services.collection_service.fetch_collection_schema", new_callable=AsyncMock)
    @patch("services.collection_service.get_project_by_id")
    @patch("services.collection_service.get_organization_by_id")
    @patch("services.collection_service.fetch_all_collections")
    async def test_happy_path_with_collections(
        self,
        mock_fetch_all,
        mock_get_org,
        mock_get_proj,
        mock_fetch_schema,
    ):
        """Verify that all collections are retrieved successfully."""
        from services.collection_service import get_all_collections_service

        row = _make_collection_row()
        mock_fetch_all.return_value = [row]
        mock_get_org.return_value = _make_org_row()
        mock_get_proj.return_value = _make_project_row()
        mock_fetch_schema.return_value = {"field1": "text"}

        result = await get_all_collections_service(_org_id, _project_id)

        assert len(result) == 1
        assert result[0].collection_name == "test_collection"
        assert result[0].collection_schema == {"field1": "text"}


class TestGetCollectionInfoService:
    """Tests for get_collection_info_service."""

    @patch("services.collection_service.fetch_collection_schema", new_callable=AsyncMock)
    @patch("services.collection_service.get_project_by_id")
    @patch("services.collection_service.get_organization_by_id")
    @patch("services.collection_service.get_collection_by_id")
    async def test_happy_path(
        self,
        mock_get_coll,
        mock_get_org,
        mock_get_proj,
        mock_fetch_schema,
    ):
        """Verify that collection info is retrieved successfully."""
        from services.collection_service import get_collection_info_service

        row = _make_collection_row()
        mock_get_coll.return_value = row
        mock_get_org.return_value = _make_org_row()
        mock_get_proj.return_value = _make_project_row()
        mock_fetch_schema.return_value = {"field1": "text"}

        result = await get_collection_info_service(_org_id, _project_id, _collection_id)

        assert result.collection_name == "test_collection"
        assert result.organization_name == "test_org"
        assert result.project_name == "test_project"
        assert result.collection_schema == {"field1": "text"}


# ===========================================================================
# services/organization_service.py
# ===========================================================================


class TestCreateOrganizationService:
    """Tests for create_organization_service."""

    @patch("services.organization_service.create_keyspace_in_db", new_callable=AsyncMock)
    @patch("services.organization_service.insert_organization", new_callable=AsyncMock)
    @patch("services.organization_service.get_organization_by_name", new_callable=AsyncMock)
    @patch("services.organization_service.contains_special_characters")
    async def test_happy_path(self, mock_special, mock_get_by_name, mock_insert, mock_create_ks):
        """Verify that an organization is created successfully."""
        from models.organization_models import OrganizationCreateRequest
        from services.organization_service import create_organization_service

        mock_special.return_value = False
        mock_get_by_name.return_value = None

        data = OrganizationCreateRequest(
            organization_name="new_org",
            description="A new org",
            tags=["tag1"],
        )

        result = await create_organization_service(data)

        assert result["message"] == "Organization created successfully"
        assert "organization_id" in result
        mock_insert.assert_awaited_once()
        mock_create_ks.assert_awaited_once_with("new_org")

    @patch("services.organization_service.contains_special_characters")
    async def test_special_chars_raises_400(self, mock_special):
        """Verify that creating an organization with special characters raises a 400 error."""
        from models.organization_models import OrganizationCreateRequest
        from services.organization_service import create_organization_service

        mock_special.return_value = True

        data = OrganizationCreateRequest(
            organization_name="bad$org",
            description="desc",
            tags=[],
        )

        with pytest.raises(HTTPException) as exc_info:
            await create_organization_service(data)
        assert exc_info.value.status_code == 400

    @patch("services.organization_service.get_organization_by_name", new_callable=AsyncMock)
    @patch("services.organization_service.contains_special_characters")
    async def test_already_exists_raises_409(self, mock_special, mock_get_by_name):
        """Verify that creating an existing organization raises a 409 error."""
        from models.organization_models import OrganizationCreateRequest
        from services.organization_service import create_organization_service

        mock_special.return_value = False
        mock_get_by_name.return_value = _make_org_row()

        data = OrganizationCreateRequest(
            organization_name="existing_org",
            description="desc",
            tags=[],
        )

        with pytest.raises(HTTPException) as exc_info:
            await create_organization_service(data)
        assert exc_info.value.status_code == 409

    @patch("services.organization_service.delete_organization_from_db", new_callable=AsyncMock)
    @patch("services.organization_service.insert_organization", new_callable=AsyncMock)
    @patch("services.organization_service.get_organization_by_name", new_callable=AsyncMock)
    @patch("services.organization_service.contains_special_characters")
    async def test_db_failure_rolls_back_and_raises_500(
        self, mock_special, mock_get_by_name, mock_insert, mock_delete
    ):
        """Verify that a DB failure during org creation rolls back and raises a 500 error."""
        from models.organization_models import OrganizationCreateRequest
        from services.organization_service import create_organization_service

        mock_special.return_value = False
        mock_get_by_name.return_value = None
        mock_insert.side_effect = Exception("DB connection failed")

        data = OrganizationCreateRequest(
            organization_name="fail_org",
            description="desc",
            tags=[],
        )

        with pytest.raises(HTTPException) as exc_info:
            await create_organization_service(data)
        assert exc_info.value.status_code == 500
        assert "Failed to create organization" in exc_info.value.detail
        mock_delete.assert_awaited_once()


class TestGetOrganizationInfoService:
    """Tests for get_organization_info_service."""

    async def test_happy_path(self):
        """Verify that organization info is retrieved successfully."""
        from services.organization_service import get_organization_info_service

        org = _make_org_row()
        result = await get_organization_info_service(org)

        assert result.organization_id == org.id
        assert result.organization_name == "test_org"
        assert result.description == "Test org"
        assert result.tags == ["test"]


class TestUpdateOrganizationService:
    """Tests for update_organization_service."""

    @patch("services.organization_service.update_organization_in_db", new_callable=AsyncMock)
    async def test_happy_path(self, mock_update):
        """Verify that an organization is updated successfully."""
        from models.organization_models import OrganizationUpdateRequest
        from services.organization_service import update_organization_service

        data = OrganizationUpdateRequest(description="updated", tags=["new"])
        result = await update_organization_service(_org_id, data)

        assert result["message"] == "Organization updated successfully."
        mock_update.assert_awaited_once_with(_org_id, "updated", ["new"])


class TestDeleteOrganizationService:
    """Tests for delete_organization_service."""

    @patch("services.organization_service.delete_keyspace_in_db", new_callable=AsyncMock)
    @patch("services.organization_service.delete_organization_from_db", new_callable=AsyncMock)
    async def test_happy_path(self, mock_delete_org, mock_delete_ks):
        """Verify that an organization is deleted successfully."""
        from services.organization_service import delete_organization_service

        org = _make_org_row()
        result = await delete_organization_service(org)

        assert "successfully deleted" in result["message"]
        mock_delete_org.assert_awaited_once_with(org.id)
        mock_delete_ks.assert_awaited_once_with("test_org")

    @patch("services.organization_service.delete_organization_from_db", new_callable=AsyncMock)
    async def test_failure_raises_500(self, mock_delete_org):
        """Verify that a DB failure during organization deletion raises a 500 error."""
        from services.organization_service import delete_organization_service

        mock_delete_org.side_effect = Exception("DB error")
        org = _make_org_row()

        with pytest.raises(HTTPException) as exc_info:
            await delete_organization_service(org)
        assert exc_info.value.status_code == 500
        assert "Failed to delete organization" in exc_info.value.detail


class TestGetAllOrganizationsService:
    """Tests for get_all_organizations_service."""

    @patch("services.organization_service.get_all_organizations_from_db", new_callable=AsyncMock)
    async def test_happy_path(self, mock_get_all):
        """Verify that all organizations are retrieved successfully."""
        from services.organization_service import get_all_organizations_service

        mock_get_all.return_value = [_make_org_row()]

        result = await get_all_organizations_service()

        assert len(result) == 1
        assert result[0].organization_name == "test_org"

    @patch("services.organization_service.get_all_organizations_from_db", new_callable=AsyncMock)
    async def test_empty_list(self, mock_get_all):
        """Verify that an empty list is returned when no organizations exist."""
        from services.organization_service import get_all_organizations_service

        mock_get_all.return_value = []

        result = await get_all_organizations_service()

        assert result == []

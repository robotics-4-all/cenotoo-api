import uuid
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException


class TestCollectionEndpoints:
    """Tests for the collection endpoints."""

    def test_create_collection(self, client, sample_org_id, sample_project_id):
        """Verify creating a collection returns 200."""
        with (
            patch("routers.collections.get_organization_id", return_value=sample_org_id),
            patch(
                "routers.collections.create_collection_service",
                new_callable=AsyncMock,
            ) as mock_svc,
        ):
            coll_id = uuid.uuid4()
            mock_svc.return_value = {
                "message": f"Collection test_coll created successfully with ID {coll_id}",
            }
            response = client.post(
                f"/api/v1/projects/{sample_project_id}/collections",
                json={
                    "name": "test_coll",
                    "description": "A test collection",
                    "tags": ["iot"],
                    "collection_schema": {"temperature": "float", "humidity": "float"},
                },
            )
        assert response.status_code == 200
        assert "message" in response.json()
        assert "test_coll" in response.json()["message"]

    def test_create_collection_already_exists(self, client, sample_org_id, sample_project_id):
        """Verify creating a duplicate collection returns 409."""
        with (
            patch("routers.collections.get_organization_id", return_value=sample_org_id),
            patch(
                "routers.collections.create_collection_service",
                new_callable=AsyncMock,
            ) as mock_svc,
        ):
            mock_svc.side_effect = HTTPException(
                status_code=409, detail="Collection already exists"
            )
            response = client.post(
                f"/api/v1/projects/{sample_project_id}/collections",
                json={
                    "name": "existing_coll",
                    "description": "Duplicate",
                    "tags": [],
                    "collection_schema": {"temp": "float"},
                },
            )
        assert response.status_code == 409

    def test_update_collection(
        self, client, sample_org_id, sample_project_id, sample_collection_id
    ):
        """Verify updating a collection returns 200."""
        with (
            patch("routers.collections.get_organization_id", return_value=sample_org_id),
            patch(
                "routers.collections.update_collection_service",
                new_callable=AsyncMock,
            ) as mock_svc,
        ):
            mock_svc.return_value = {"message": "Collection updated successfully"}
            response = client.put(
                f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}",
                json={"description": "Updated description", "tags": ["updated"]},
            )
        assert response.status_code == 200
        assert response.json()["message"] == "Collection updated successfully"

    def test_delete_collection(
        self, client, sample_org_id, sample_project_id, sample_collection_id
    ):
        """Verify deleting a collection returns 200."""
        with (
            patch("routers.collections.get_organization_id", return_value=sample_org_id),
            patch(
                "routers.collections.delete_collection_service",
                new_callable=AsyncMock,
            ) as mock_svc,
        ):
            mock_svc.return_value = {"message": "Collection deleted successfully"}
            response = client.delete(
                f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}",
            )
        assert response.status_code == 200
        assert response.json()["message"] == "Collection deleted successfully"

    def test_list_collections(self, client, sample_org_id, sample_project_id):
        """Verify listing collections returns 200."""
        coll_id = uuid.uuid4()
        with (
            patch("routers.collections.get_organization_id", return_value=sample_org_id),
            patch(
                "routers.collections.get_all_collections_service",
                new_callable=AsyncMock,
            ) as mock_svc,
        ):
            mock_svc.return_value = [
                {
                    "collection_id": str(coll_id),
                    "collection_name": "coll1",
                    "project_id": str(sample_project_id),
                    "project_name": "test_project",
                    "organization_id": str(sample_org_id),
                    "organization_name": "test_org",
                    "description": "First collection",
                    "tags": ["iot"],
                    "creation_date": "2024-01-01",
                    "collection_schema": {"temp": "float"},
                }
            ]
            response = client.get(
                f"/api/v1/projects/{sample_project_id}/collections",
            )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert len(data["items"]) == 1
        assert data["items"][0]["collection_name"] == "coll1"

    def test_list_collections_empty(self, client, sample_org_id, sample_project_id):
        """Verify listing empty collections returns 200."""
        with (
            patch("routers.collections.get_organization_id", return_value=sample_org_id),
            patch(
                "routers.collections.get_all_collections_service",
                new_callable=AsyncMock,
            ) as mock_svc,
        ):
            mock_svc.return_value = []
            response = client.get(
                f"/api/v1/projects/{sample_project_id}/collections",
            )
        assert response.status_code == 200
        assert response.json()["items"] == []
        assert response.json()["total"] == 0

    def test_get_collection_info(
        self, client, sample_org_id, sample_project_id, sample_collection_id
    ):
        """Verify getting collection info returns 200."""
        with (
            patch("routers.collections.get_organization_id", return_value=sample_org_id),
            patch(
                "routers.collections.get_collection_info_service",
                new_callable=AsyncMock,
            ) as mock_svc,
        ):
            mock_svc.return_value = {
                "collection_id": str(sample_collection_id),
                "collection_name": "test_coll",
                "project_id": str(sample_project_id),
                "project_name": "test_project",
                "organization_id": str(sample_org_id),
                "organization_name": "test_org",
                "description": "A test collection",
                "tags": ["iot"],
                "creation_date": "2024-01-01",
                "collection_schema": {"temperature": "float"},
            }
            response = client.get(
                f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}",
            )
        assert response.status_code == 200
        data = response.json()
        assert data["collection_name"] == "test_coll"
        assert data["collection_id"] == str(sample_collection_id)

    def test_create_collection_missing_schema(self, client, sample_org_id, sample_project_id):
        """Verify creating a collection without schema returns 422."""
        del sample_org_id
        response = client.post(
            f"/api/v1/projects/{sample_project_id}/collections",
            json={
                "name": "test_coll",
                "description": "Missing schema",
                "tags": [],
            },
        )
        assert response.status_code == 422

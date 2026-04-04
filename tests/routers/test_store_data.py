from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestStoreDataEndpoint:
    """Integration tests for the POST /projects/{id}/collections/{id}/store_data endpoint."""

    SCHEMA = {
        "key": "text",
        "timestamp": "text",
        "day": "text",
        "temperature": "float",
    }

    @pytest.fixture(autouse=True)
    def _patch_router_deps(self, sample_org_id):
        """Patch get_organization_id for store_data router."""
        with patch("routers.store_data.get_organization_id", return_value=sample_org_id):
            yield

    def _setup_name_mocks(self, mock_org, mock_proj, mock_coll, mock_schema):
        """Configure standard return values for org/project/collection lookups."""
        mock_org.return_value = MagicMock(organization_name="test_org")
        mock_proj.return_value = MagicMock(project_name="test_project")
        mock_coll.return_value = MagicMock(collection_name="test_collection")
        mock_schema.return_value = self.SCHEMA

    def _build_url(self, project_id, collection_id):
        """Build the store_data endpoint URL."""
        return f"/api/v1/projects/{project_id}/collections/{collection_id}/store_data"

    @patch("routers.store_data.fetch_collection_schema", new_callable=AsyncMock)
    @patch("routers.store_data.get_collection_by_id")
    @patch("routers.store_data.get_project_by_id")
    @patch("routers.store_data.get_organization_by_id")
    def test_store_single_message_success(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        mock_schema,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """POST with a single dict body should return 200 with stored_count=1."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll, mock_schema)

        url = self._build_url(sample_project_id, sample_collection_id)
        response = client.post(url, json={"temperature": 25.0})

        assert response.status_code == 200
        data = response.json()
        assert data["stored_count"] == 1
        assert "successfully" in data["message"]

    @patch("routers.store_data.fetch_collection_schema", new_callable=AsyncMock)
    @patch("routers.store_data.get_collection_by_id")
    @patch("routers.store_data.get_project_by_id")
    @patch("routers.store_data.get_organization_by_id")
    def test_store_batch_success(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        mock_schema,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """POST with a list of dicts should store all valid messages."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll, mock_schema)

        url = self._build_url(sample_project_id, sample_collection_id)
        body = [{"temperature": 20.0}, {"temperature": 30.0}, {"temperature": 40.0}]
        response = client.post(url, json=body)

        assert response.status_code == 200
        data = response.json()
        assert data["stored_count"] == 3

    @patch("routers.store_data.fetch_collection_schema", new_callable=AsyncMock)
    @patch("routers.store_data.get_collection_by_id")
    @patch("routers.store_data.get_project_by_id")
    @patch("routers.store_data.get_organization_by_id")
    def test_store_no_schema_400(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        mock_schema,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """When collection has no schema, should return 400."""
        mock_org.return_value = MagicMock(organization_name="test_org")
        mock_proj.return_value = MagicMock(project_name="test_project")
        mock_coll.return_value = MagicMock(collection_name="test_collection")
        mock_schema.return_value = {}

        url = self._build_url(sample_project_id, sample_collection_id)
        response = client.post(url, json={"temperature": 25.0})

        assert response.status_code == 400
        assert "does not have a defined schema" in response.json()["detail"]

    @patch("routers.store_data.fetch_collection_schema", new_callable=AsyncMock)
    @patch("routers.store_data.get_collection_by_id")
    @patch("routers.store_data.get_project_by_id")
    @patch("routers.store_data.get_organization_by_id")
    def test_store_invalid_type_400(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        mock_schema,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Message with wrong types should return 400 with invalid_messages detail."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll, mock_schema)

        url = self._build_url(sample_project_id, sample_collection_id)
        response = client.post(url, json={"temperature": "hot"})

        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail["valid_count"] == 0
        assert detail["invalid_count"] == 1
        assert len(detail["invalid_messages"]) == 1
        assert "number" in detail["invalid_messages"][0]["error"]

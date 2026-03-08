from collections import namedtuple
from unittest.mock import MagicMock, patch

import pytest


class TestDeleteDataEndpoints:
    """Tests for the delete data endpoints."""

    mock_session: MagicMock

    @pytest.fixture(autouse=True)
    def _patch_router_deps(self, sample_org_id):
        self.mock_session = MagicMock()
        with (
            patch("routers.delete_data.get_organization_id", return_value=sample_org_id),
            patch("routers.delete_data.session", self.mock_session),
        ):
            yield

    def _setup_name_mocks(self, mock_org, mock_proj, mock_coll):
        mock_org.return_value = MagicMock(organization_name="test_org")
        mock_proj.return_value = MagicMock(project_name="test_project")
        mock_coll.return_value = MagicMock(collection_name="test_collection")

    @patch("routers.delete_data.get_collection_by_id")
    @patch("routers.delete_data.get_project_by_id")
    @patch("routers.delete_data.get_organization_by_id")
    def test_delete_by_key(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify deleting data by key returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        DayRow = namedtuple("DayRow", ["day", "key"])
        day_rows = [DayRow(day="2024-01-01", key="sensor1")]
        self.mock_session.execute.side_effect = [day_rows, None]

        response = client.request(
            "DELETE",
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/delete_data",
            json={"key": "sensor1"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Data deleted successfully"
        assert data["criteria"]["key"] == "sensor1"

    @patch("routers.delete_data.get_collection_by_id")
    @patch("routers.delete_data.get_project_by_id")
    @patch("routers.delete_data.get_organization_by_id")
    def test_delete_by_timestamp(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify deleting data by timestamp range returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        DayRow = namedtuple("DayRow", ["day", "key"])
        scan_rows = [DayRow(day="2024-01-01", key="sensor1")]
        self.mock_session.execute.side_effect = [scan_rows, None]

        response = client.request(
            "DELETE",
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/delete_data",
            json={
                "timestamp_from": "2024-01-01T00:00:00Z",
                "timestamp_to": "2024-01-02T00:00:00Z",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Data deleted successfully"

    @patch("routers.delete_data.get_collection_by_id")
    @patch("routers.delete_data.get_project_by_id")
    @patch("routers.delete_data.get_organization_by_id")
    def test_delete_no_criteria(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify deleting data with no criteria returns 400."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        response = client.request(
            "DELETE",
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/delete_data",
            json={},
        )
        assert response.status_code == 400
        assert "At least one deletion criteria" in response.json()["detail"]

    @patch("routers.delete_data.get_collection_by_id")
    @patch("routers.delete_data.get_project_by_id")
    @patch("routers.delete_data.get_organization_by_id")
    def test_delete_no_key_no_timestamp(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify deleting data with null criteria returns 400."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        response = client.request(
            "DELETE",
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/delete_data",
            json={"key": None, "timestamp_from": None, "timestamp_to": None},
        )
        assert response.status_code == 400

    @patch("routers.delete_data.get_collection_by_id")
    @patch("routers.delete_data.get_project_by_id")
    @patch("routers.delete_data.get_organization_by_id")
    def test_delete_by_key_no_data_found(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify deleting data by key when no data exists returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        self.mock_session.execute.return_value = []

        response = client.request(
            "DELETE",
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/delete_data",
            json={"key": "nonexistent_sensor"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "No data found" in data["message"]

    @patch("routers.delete_data.get_collection_by_id")
    @patch("routers.delete_data.get_project_by_id")
    @patch("routers.delete_data.get_organization_by_id")
    def test_delete_by_key_with_timestamp_range(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify deleting data by key and timestamp range returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        DayRow = namedtuple("DayRow", ["day", "key"])
        scan_rows = [DayRow(day="2024-01-01", key="sensor1")]
        self.mock_session.execute.side_effect = [scan_rows, None]

        response = client.request(
            "DELETE",
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/delete_data",
            json={
                "key": "sensor1",
                "timestamp_from": "2024-01-01T00:00:00Z",
                "timestamp_to": "2024-01-02T00:00:00Z",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Data deleted successfully"
        assert data["criteria"]["key"] == "sensor1"

    @patch("routers.delete_data.get_collection_by_id")
    @patch("routers.delete_data.get_project_by_id")
    @patch("routers.delete_data.get_organization_by_id")
    def test_delete_invalid_timestamp_format(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify deleting data with invalid timestamp_from returns 400."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        response = client.request(
            "DELETE",
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/delete_data",
            json={
                "timestamp_from": "not-a-date",
                "timestamp_to": "2024-01-02T00:00:00Z",
            },
        )
        assert response.status_code == 400
        assert "Invalid timestamp_from format" in response.json()["detail"]

    @patch("routers.delete_data.get_collection_by_id")
    @patch("routers.delete_data.get_project_by_id")
    @patch("routers.delete_data.get_organization_by_id")
    def test_delete_invalid_timestamp_to(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify deleting data with invalid timestamp_to returns 400."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        response = client.request(
            "DELETE",
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/delete_data",
            json={
                "timestamp_from": "2024-01-01T00:00:00Z",
                "timestamp_to": "not-a-date",
            },
        )
        assert response.status_code == 400
        assert "Invalid timestamp_to format" in response.json()["detail"]

    @patch("routers.delete_data.get_collection_by_id")
    @patch("routers.delete_data.get_project_by_id")
    @patch("routers.delete_data.get_organization_by_id")
    def test_delete_key_with_ts_from_only(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify deleting data by key and timestamp_from returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        DayRow = namedtuple("DayRow", ["day", "key"])
        scan_rows = [DayRow(day="2024-01-01", key="sensor1")]
        self.mock_session.execute.side_effect = [scan_rows, None]

        response = client.request(
            "DELETE",
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/delete_data",
            json={
                "key": "sensor1",
                "timestamp_from": "2024-01-01T00:00:00Z",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Data deleted successfully"
        assert data["criteria"]["key"] == "sensor1"
        assert data["criteria"]["timestamp_from"] == "2024-01-01T00:00:00Z"
        assert data["criteria"]["timestamp_to"] is None

    @patch("routers.delete_data.get_collection_by_id")
    @patch("routers.delete_data.get_project_by_id")
    @patch("routers.delete_data.get_organization_by_id")
    def test_delete_key_with_ts_to_only(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify deleting data by key and timestamp_to returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        DayRow = namedtuple("DayRow", ["day", "key"])
        scan_rows = [DayRow(day="2024-01-01", key="sensor1")]
        self.mock_session.execute.side_effect = [scan_rows, None]

        response = client.request(
            "DELETE",
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/delete_data",
            json={
                "key": "sensor1",
                "timestamp_to": "2024-01-02T00:00:00Z",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Data deleted successfully"
        assert data["criteria"]["key"] == "sensor1"
        assert data["criteria"]["timestamp_from"] is None
        assert data["criteria"]["timestamp_to"] == "2024-01-02T00:00:00Z"

    @patch("routers.delete_data.get_collection_by_id")
    @patch("routers.delete_data.get_project_by_id")
    @patch("routers.delete_data.get_organization_by_id")
    def test_delete_key_with_timestamp_range_no_data(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify deleting data by key and timestamp range when no data exists returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        self.mock_session.execute.return_value = []

        response = client.request(
            "DELETE",
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/delete_data",
            json={
                "key": "sensor1",
                "timestamp_from": "2024-01-01T00:00:00Z",
                "timestamp_to": "2024-01-02T00:00:00Z",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "No data found for the specified criteria"
        assert data["criteria"]["key"] == "sensor1"

    @patch("routers.delete_data.get_collection_by_id")
    @patch("routers.delete_data.get_project_by_id")
    @patch("routers.delete_data.get_organization_by_id")
    def test_delete_no_key_ts_from_only(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify deleting data by timestamp_from only returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        DayRow = namedtuple("DayRow", ["day", "key"])
        scan_rows = [DayRow(day="2024-01-01", key="sensor1")]
        self.mock_session.execute.side_effect = [scan_rows, None]

        response = client.request(
            "DELETE",
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/delete_data",
            json={
                "timestamp_from": "2024-01-01T00:00:00Z",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Data deleted successfully"
        assert data["criteria"]["key"] is None
        assert data["criteria"]["timestamp_from"] == "2024-01-01T00:00:00Z"
        assert data["criteria"]["timestamp_to"] is None

    @patch("routers.delete_data.get_collection_by_id")
    @patch("routers.delete_data.get_project_by_id")
    @patch("routers.delete_data.get_organization_by_id")
    def test_delete_no_key_ts_to_only(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify deleting data by timestamp_to only returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        DayRow = namedtuple("DayRow", ["day", "key"])
        scan_rows = [DayRow(day="2024-01-01", key="sensor1")]
        self.mock_session.execute.side_effect = [scan_rows, None]

        response = client.request(
            "DELETE",
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/delete_data",
            json={
                "timestamp_to": "2024-01-02T00:00:00Z",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Data deleted successfully"
        assert data["criteria"]["key"] is None
        assert data["criteria"]["timestamp_from"] is None
        assert data["criteria"]["timestamp_to"] == "2024-01-02T00:00:00Z"

    @patch("routers.delete_data.get_collection_by_id")
    @patch("routers.delete_data.get_project_by_id")
    @patch("routers.delete_data.get_organization_by_id")
    def test_delete_no_key_timestamp_range_no_data(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify deleting data by timestamp range when no data exists returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        self.mock_session.execute.return_value = []

        response = client.request(
            "DELETE",
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/delete_data",
            json={
                "timestamp_from": "2024-01-01T00:00:00Z",
                "timestamp_to": "2024-01-02T00:00:00Z",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "No data found for the specified time range"
        assert data["criteria"]["key"] is None

    @patch("routers.delete_data.get_collection_by_id")
    @patch("routers.delete_data.get_project_by_id")
    @patch("routers.delete_data.get_organization_by_id")
    def test_delete_db_error(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify deleting data handles database errors and returns 500."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        self.mock_session.execute.side_effect = RuntimeError("Connection lost")

        response = client.request(
            "DELETE",
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/delete_data",
            json={"key": "sensor1"},
        )
        assert response.status_code == 500
        assert "Failed to delete data" in response.json()["detail"]

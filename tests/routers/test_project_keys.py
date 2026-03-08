"""Tests for routers/project_keys.py — API key management endpoints."""

from datetime import datetime
from unittest.mock import AsyncMock, patch


class TestProjectKeyEndpoints:
    """Tests for the project keys endpoints."""

    def test_create_key(self, client, sample_project_id):
        """Verify creating a project key returns 200."""
        with patch(
            "routers.project_keys.create_project_key_service", new_callable=AsyncMock
        ) as mock_svc:
            mock_svc.return_value = {
                "api_key": "abc123",
                "project_id": str(sample_project_id),
                "key_type": "read",
                "created_at": datetime.utcnow().isoformat(),
            }
            resp = client.post(
                f"/api/v1/projects/{sample_project_id}/keys",
                json={"key_type": "read"},
            )
            assert resp.status_code == 200
            assert resp.json()["api_key"] == "abc123"

    def test_fetch_keys_default_category(self, client, sample_project_id):
        """Verify fetching keys with default category returns 200."""
        with patch(
            "routers.project_keys.fetch_project_keys_by_category_service", new_callable=AsyncMock
        ) as mock_svc:
            mock_svc.return_value = [
                {
                    "api_key": "key1",
                    "project_id": str(sample_project_id),
                    "key_type": "read",
                    "created_at": datetime.utcnow().isoformat(),
                }
            ]
            resp = client.get(f"/api/v1/projects/{sample_project_id}/keys")
            assert resp.status_code == 200
            mock_svc.assert_called_once_with(sample_project_id, "all")

    def test_fetch_keys_specific_category(self, client, sample_project_id):
        """Verify fetching keys with specific category calls service correctly."""
        with patch(
            "routers.project_keys.fetch_project_keys_by_category_service", new_callable=AsyncMock
        ) as mock_svc:
            mock_svc.return_value = []
            client.get(f"/api/v1/projects/{sample_project_id}/keys?key_category=read")
            mock_svc.assert_called_once_with(sample_project_id, "read")

    def test_regenerate_key(self, client, sample_project_id):
        """Verify regenerating a key returns 200."""
        with patch(
            "routers.project_keys.regenerate_key_service", new_callable=AsyncMock
        ) as mock_svc:
            mock_svc.return_value = {
                "api_key": "newkey456",
                "project_id": str(sample_project_id),
                "key_type": "write",
                "created_at": datetime.utcnow().isoformat(),
            }
            resp = client.put(
                f"/api/v1/projects/{sample_project_id}/keys/regenerate",
                json={"key_value": "oldkey123"},
            )
            assert resp.status_code == 200
            assert resp.json()["api_key"] == "newkey456"

    def test_delete_keys_by_category(self, client, sample_project_id):
        """Verify deleting keys by category returns 200."""
        with patch(
            "routers.project_keys.delete_keys_by_category_service", new_callable=AsyncMock
        ) as mock_svc:
            mock_svc.return_value = {"message": "Keys deleted successfully."}
            resp = client.delete(
                f"/api/v1/projects/{sample_project_id}/keys/delete_keys?key_category=read"
            )
            assert resp.status_code == 200

    def test_delete_key_by_value(self, client, sample_project_id):
        """Verify deleting a key by value returns 200."""
        with patch(
            "routers.project_keys.delete_key_by_value_service", new_callable=AsyncMock
        ) as mock_svc:
            mock_svc.return_value = {"message": "Key deleted successfully."}
            resp = client.request(
                "DELETE",
                f"/api/v1/projects/{sample_project_id}/keys/delete_key",
                json={"key_value": "somekey"},
            )
            assert resp.status_code == 200

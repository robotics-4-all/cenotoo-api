import uuid
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException


class TestUserEndpoints:
    """Tests for the user endpoints."""

    def test_create_user(self, client, sample_org_id):
        """Verify creating a user returns 200."""
        with patch("routers.users.create_user_service", new_callable=AsyncMock) as mock_svc:
            user_id = uuid.uuid4()
            mock_svc.return_value = {
                "message": "User created successfully",
                "user_id": str(user_id),
            }
            response = client.post(
                f"/api/v1/organizations/{sample_org_id}/users",
                json={"username": "newuser", "password": "securepass123"},
            )
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "User created successfully"
        assert "user_id" in data

    def test_create_user_duplicate(self, client, sample_org_id):
        """Verify creating a duplicate user returns 409."""
        with patch("routers.users.create_user_service", new_callable=AsyncMock) as mock_svc:
            mock_svc.side_effect = HTTPException(status_code=409, detail="Username already exists")
            response = client.post(
                f"/api/v1/organizations/{sample_org_id}/users",
                json={"username": "existing_user", "password": "pass123"},
            )
        assert response.status_code == 409

    def test_create_user_missing_password(self, client, sample_org_id):
        """Verify creating a user without password returns 422."""
        response = client.post(
            f"/api/v1/organizations/{sample_org_id}/users",
            json={"username": "newuser"},
        )
        assert response.status_code == 422

    def test_list_users(self, client, sample_org_id):
        """Verify listing users returns 200."""
        with patch("routers.users.get_all_users_service", new_callable=AsyncMock) as mock_svc:
            mock_svc.return_value = {
                "users": [
                    {"username": "user1", "role": "user"},
                    {"username": "admin1", "role": "superadmin"},
                ]
            }
            response = client.get(
                f"/api/v1/organizations/{sample_org_id}/users",
            )
        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert len(data["items"]) == 2
        assert data["total"] == 2

    def test_list_users_empty(self, client, sample_org_id):
        """Verify listing users when empty returns 200."""
        with patch("routers.users.get_all_users_service", new_callable=AsyncMock) as mock_svc:
            mock_svc.return_value = {"users": []}
            response = client.get(
                f"/api/v1/organizations/{sample_org_id}/users",
            )
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    def test_update_password(self, client, sample_org_id):
        """Verify updating a user password returns 200."""
        with patch(
            "routers.users.update_user_password_service", new_callable=AsyncMock
        ) as mock_svc:
            mock_svc.return_value = {"message": "User password updated successfully"}
            response = client.put(
                f"/api/v1/organizations/{sample_org_id}/users/update_password",
                json={"username": "testuser", "password": "newpass123"},
            )
        assert response.status_code == 200
        assert response.json()["message"] == "User password updated successfully"

    def test_update_password_user_not_found(self, client, sample_org_id):
        """Verify updating password for non-existent user returns 404."""
        with patch(
            "routers.users.update_user_password_service", new_callable=AsyncMock
        ) as mock_svc:
            mock_svc.side_effect = HTTPException(status_code=404, detail="User not found")
            response = client.put(
                f"/api/v1/organizations/{sample_org_id}/users/update_password",
                json={"username": "nonexistent", "password": "newpass123"},
            )
        assert response.status_code == 404

    def test_delete_user(self, client, sample_org_id):
        """Verify deleting a user returns 200."""
        with patch("routers.users.delete_user_service", new_callable=AsyncMock) as mock_svc:
            mock_svc.return_value = {"message": "User deleted successfully"}
            response = client.request(
                "DELETE",
                f"/api/v1/organizations/{sample_org_id}/users",
                json={"username": "user_to_delete"},
            )
        assert response.status_code == 200
        assert response.json()["message"] == "User deleted successfully"

    def test_delete_user_not_found(self, client, sample_org_id):
        """Verify deleting a non-existent user returns 404."""
        with patch("routers.users.delete_user_service", new_callable=AsyncMock) as mock_svc:
            mock_svc.side_effect = HTTPException(status_code=404, detail="User not found")
            response = client.request(
                "DELETE",
                f"/api/v1/organizations/{sample_org_id}/users",
                json={"username": "nonexistent"},
            )
        assert response.status_code == 404

    def test_delete_user_missing_username(self, client, sample_org_id):
        """Verify deleting a user without username returns 422."""
        response = client.request(
            "DELETE",
            f"/api/v1/organizations/{sample_org_id}/users",
            json={},
        )
        assert response.status_code == 422

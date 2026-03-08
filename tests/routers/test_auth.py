from collections import namedtuple
from unittest.mock import patch

from services.auth_service import hash_password


class TestLoginEndpoint:
    """Tests for the login endpoint."""

    def test_valid_login(self, client):
        """Verify valid login returns access token."""
        hashed = hash_password("testpass")
        UserRow = namedtuple("UserRow", ["id", "username", "password", "role", "organization_id"])
        mock_user = UserRow(
            id="user-id",
            username="testuser",
            password=hashed,
            role="user",
            organization_id="org-id",
        )
        with patch("routers.auth.get_user_by_username", return_value=mock_user):
            response = client.post(
                "/api/v1/token",
                data={"username": "testuser", "password": "testpass"},
            )
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"

    def test_wrong_password(self, client):
        """Verify login with wrong password returns 401."""
        hashed = hash_password("correct")
        UserRow = namedtuple("UserRow", ["id", "username", "password", "role", "organization_id"])
        mock_user = UserRow(
            id="user-id",
            username="testuser",
            password=hashed,
            role="user",
            organization_id="org-id",
        )
        with patch("routers.auth.get_user_by_username", return_value=mock_user):
            response = client.post(
                "/api/v1/token",
                data={"username": "testuser", "password": "wrong"},
            )
        assert response.status_code == 401

    def test_user_not_found(self, client):
        """Verify login with non-existent user returns 401."""
        with patch("routers.auth.get_user_by_username", return_value=None):
            response = client.post(
                "/api/v1/token",
                data={"username": "nobody", "password": "whatever"},
            )
        assert response.status_code == 401

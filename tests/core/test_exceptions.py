"""Tests for core.exceptions module."""

from core.exceptions import (
    AppException,
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    InfrastructureError,
    NotFoundError,
    ValidationError,
)


class TestAppException:
    """Tests for AppException."""

    def test_status_code_and_detail(self):
        """Verify that AppException correctly sets status code and detail."""
        exc = AppException(status_code=400, code="TEST_ERROR", detail="oops")
        assert exc.status_code == 400
        assert exc.detail == {"code": "TEST_ERROR", "message": "oops"}
        assert exc.code == "TEST_ERROR"


class TestNotFoundError:
    """Tests for NotFoundError."""

    def test_without_identifier(self):
        """Verify that NotFoundError sets the correct message without an identifier."""
        exc = NotFoundError("Project")
        assert exc.status_code == 404
        assert "Project not found" in exc.detail["message"]

    def test_with_identifier(self):
        """Verify that NotFoundError includes the identifier in the message."""
        exc = NotFoundError("Project", "abc-123")
        assert "abc-123" in exc.detail["message"]


class TestConflictError:
    """Tests for ConflictError."""

    def test_message(self):
        """Verify that ConflictError sets the correct message."""
        exc = ConflictError("already exists")
        assert exc.status_code == 409
        assert exc.detail["message"] == "already exists"


class TestValidationError:
    """Tests for ValidationError."""

    def test_message(self):
        """Verify that ValidationError sets the correct message and code."""
        exc = ValidationError("bad input")
        assert exc.status_code == 422
        assert exc.detail["code"] == "VALIDATION_ERROR"


class TestAuthenticationError:
    """Tests for AuthenticationError."""

    def test_default_message(self):
        """Verify that AuthenticationError sets the default message."""
        exc = AuthenticationError()
        assert exc.status_code == 401
        assert "credentials" in exc.detail["message"].lower()

    def test_custom_message(self):
        """Verify that AuthenticationError sets a custom message."""
        exc = AuthenticationError("Token expired")
        assert exc.detail["message"] == "Token expired"


class TestAuthorizationError:
    """Tests for AuthorizationError."""

    def test_default_message(self):
        """Verify that AuthorizationError sets the default message."""
        exc = AuthorizationError()
        assert exc.status_code == 403

    def test_custom_message(self):
        """Verify that AuthorizationError sets a custom message."""
        exc = AuthorizationError("Not admin")
        assert exc.detail["message"] == "Not admin"


class TestInfrastructureError:
    """Tests for InfrastructureError."""

    def test_service_prefix(self):
        """Verify that InfrastructureError prefixes the message with the service name."""
        exc = InfrastructureError("Cassandra", "connection refused")
        assert exc.status_code == 500
        assert exc.detail["message"] == "Cassandra error: connection refused"

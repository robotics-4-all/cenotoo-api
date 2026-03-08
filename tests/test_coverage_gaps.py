"""Targeted tests for coverage gaps in core, models, and main modules."""

from unittest.mock import MagicMock, patch

import pytest
from starlette.requests import Request
from starlette.responses import JSONResponse


class TestAppExceptionHandler:
    """Tests for app_exception_handler."""

    @pytest.mark.asyncio
    async def test_returns_json_response(self):
        """Verify that app_exception_handler returns a JSONResponse with correct status code."""
        from core.exceptions import AppException, app_exception_handler

        exc = AppException(status_code=422, code="TEST", detail="bad input")
        mock_request = MagicMock(spec=Request)
        resp = await app_exception_handler(mock_request, exc)
        assert isinstance(resp, JSONResponse)
        assert resp.status_code == 422


class TestProjectKeyValidators:
    """Tests for project key validators."""

    def test_valid_hex_key(self):
        """Test that a valid 64-character hex key is accepted."""
        from models.project_keys_models import BaseKeyModel

        key = "a" * 64
        req = BaseKeyModel(key_value=key)
        assert req.key_value == key

    def test_invalid_hex_key_raises(self):
        """Test that a key shorter than 64 characters raises a validation error."""
        from pydantic import ValidationError as PydanticValidationError

        from models.project_keys_models import BaseKeyModel

        with pytest.raises(PydanticValidationError):
            BaseKeyModel(key_value="tooshort")

    def test_non_hex_chars_raises(self):
        """Test that a key with non-hex characters raises a validation error."""
        from pydantic import ValidationError as PydanticValidationError

        from models.project_keys_models import BaseKeyModel

        with pytest.raises(PydanticValidationError):
            BaseKeyModel(key_value="g" * 64)


class TestMainLifespan:
    """Tests for main lifespan."""

    @pytest.mark.asyncio
    async def test_production_default_jwt_raises(self):
        """Verify that using the default JWT secret in production raises a RuntimeError."""
        from main import lifespan

        mock_app = MagicMock()
        with patch("main.settings") as mock_settings:
            mock_settings.environment = "production"
            mock_settings.jwt_secret_key = "supersecretkey"
            mock_settings.api_key_secret = "safe-secret"
            with pytest.raises(RuntimeError, match="JWT_SECRET_KEY"):
                async with lifespan(mock_app):
                    pass

    @pytest.mark.asyncio
    async def test_production_default_api_key_raises(self):
        """Verify that using the default API key secret in production raises a RuntimeError."""
        from main import lifespan

        mock_app = MagicMock()
        with patch("main.settings") as mock_settings:
            mock_settings.environment = "production"
            mock_settings.jwt_secret_key = "safe-jwt-key"
            mock_settings.api_key_secret = "default-api-key-secret"
            with pytest.raises(RuntimeError, match="API_KEY_SECRET"):
                async with lifespan(mock_app):
                    pass


class TestKafkaConnector:
    """Tests for Kafka connector."""

    def test_get_kafka_admin_client(self):
        """Test that get_kafka_admin_client returns a valid admin client instance."""
        with patch("utilities.kafka_connector.AdminClient") as mock_admin:
            mock_admin.return_value = MagicMock()
            from utilities.kafka_connector import get_kafka_admin_client

            result = get_kafka_admin_client()
            assert result is not None

    def test_get_kafka_producer(self):
        """Test that get_kafka_producer returns a valid producer instance."""
        with patch("utilities.kafka_connector.Producer") as mock_producer:
            mock_producer.return_value = MagicMock()
            from utilities.kafka_connector import get_kafka_producer

            result = get_kafka_producer()
            assert result is not None

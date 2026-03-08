from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from routers.send_data import (
    convert_simple_schema_to_jsonschema,
    validate_message_against_simple_schema,
)


class TestConvertSimpleSchemaToJsonSchema:
    """Tests for convert_simple_schema_to_jsonschema function."""

    def test_text_field(self):
        """Verify text field converts to string type."""
        result = convert_simple_schema_to_jsonschema({"name": "text"})
        assert result["properties"]["name"]["type"] == "string"
        assert "name" in result["required"]

    def test_int_field(self):
        """Verify int field converts to integer type."""
        result = convert_simple_schema_to_jsonschema({"count": "int"})
        assert result["properties"]["count"]["type"] == "integer"

    def test_float_field(self):
        """Verify float field converts to number type."""
        result = convert_simple_schema_to_jsonschema({"temp": "float"})
        assert result["properties"]["temp"]["type"] == "number"

    def test_bool_field(self):
        """Verify bool field converts to boolean type."""
        result = convert_simple_schema_to_jsonschema({"active": "bool"})
        assert result["properties"]["active"]["type"] == "boolean"

    def test_date_field_has_format(self):
        """Verify date field has date format."""
        result = convert_simple_schema_to_jsonschema({"day": "date"})
        assert result["properties"]["day"]["format"] == "date"

    def test_timestamp_field_has_pattern(self):
        """Verify timestamp field has pattern."""
        result = convert_simple_schema_to_jsonschema({"ts": "timestamp"})
        assert "pattern" in result["properties"]["ts"]

    def test_no_additional_properties(self):
        """Verify additional properties are not allowed."""
        result = convert_simple_schema_to_jsonschema({"x": "text"})
        assert result["additionalProperties"] is False


class TestValidateMessageAgainstSimpleSchema:
    """Tests for validate_message_against_simple_schema function."""

    SCHEMA = {
        "key": "text",
        "timestamp": "text",
        "day": "text",
        "temperature": "float",
        "name": "text",
        "active": "bool",
        "count": "int",
    }

    def test_valid_message(self):
        """Verify valid message passes schema validation."""
        msg = {"temperature": 25.0, "name": "sensor1", "active": True, "count": 5}
        is_valid, errors = validate_message_against_simple_schema(msg, self.SCHEMA)
        assert is_valid is True
        assert errors == ""

    def test_missing_required_field(self):
        """Verify missing required field fails validation."""
        msg = {"temperature": 25.0}
        is_valid, errors = validate_message_against_simple_schema(msg, self.SCHEMA)
        assert is_valid is False
        assert "Missing required fields" in errors

    def test_extra_field(self):
        """Verify extra field fails validation."""
        msg = {
            "temperature": 25.0,
            "name": "s1",
            "active": True,
            "count": 1,
            "extra": "bad",
        }
        is_valid, errors = validate_message_against_simple_schema(msg, self.SCHEMA)
        assert is_valid is False
        assert "Extra fields" in errors

    def test_wrong_type_int(self):
        """Verify wrong int type fails validation."""
        msg = {"temperature": 25.0, "name": "s1", "active": True, "count": "not_int"}
        is_valid, errors = validate_message_against_simple_schema(msg, self.SCHEMA)
        assert is_valid is False
        assert "integer" in errors

    def test_wrong_type_float(self):
        """Verify wrong float type fails validation."""
        msg = {"temperature": "hot", "name": "s1", "active": True, "count": 1}
        is_valid, errors = validate_message_against_simple_schema(msg, self.SCHEMA)
        assert is_valid is False
        assert "number" in errors

    def test_wrong_type_bool(self):
        """Verify wrong bool type fails validation."""
        msg = {"temperature": 25.0, "name": "s1", "active": "yes", "count": 1}
        is_valid, errors = validate_message_against_simple_schema(msg, self.SCHEMA)
        assert is_valid is False
        assert "boolean" in errors

    def test_wrong_type_string(self):
        """Verify wrong string type fails validation."""
        msg = {"temperature": 25.0, "name": 123, "active": True, "count": 1}
        is_valid, errors = validate_message_against_simple_schema(msg, self.SCHEMA)
        assert is_valid is False
        assert "string" in errors

    def test_null_allowed_for_non_key_fields(self):
        """Verify null is allowed for non-key fields."""
        msg = {"temperature": None, "name": "s1", "active": True, "count": 1}
        is_valid, _ = validate_message_against_simple_schema(msg, self.SCHEMA)
        assert is_valid is True

    def test_null_key_rejected(self):
        """Verify null key fails validation."""
        msg = {
            "key": None,
            "temperature": 25.0,
            "name": "s1",
            "active": True,
            "count": 1,
        }
        is_valid, errors = validate_message_against_simple_schema(msg, self.SCHEMA)
        assert is_valid is False
        assert "cannot be null" in errors

    def test_null_timestamp_rejected(self):
        """Verify null timestamp fails validation."""
        msg = {
            "timestamp": None,
            "temperature": 25.0,
            "name": "s1",
            "active": True,
            "count": 1,
        }
        is_valid, errors = validate_message_against_simple_schema(msg, self.SCHEMA)
        assert is_valid is False
        assert "cannot be null" in errors

    def test_int_passes_float_check(self):
        """Verify int passes float validation check."""
        msg = {"temperature": 25, "name": "s1", "active": True, "count": 1}
        is_valid, _ = validate_message_against_simple_schema(msg, self.SCHEMA)
        assert is_valid is True


class TestSendDataEndpoint:
    """Integration tests for the POST /projects/{id}/collections/{id}/send_data endpoint."""

    SCHEMA = {
        "key": "text",
        "timestamp": "text",
        "day": "text",
        "temperature": "float",
    }

    @pytest.fixture(autouse=True)
    def _patch_router_deps(self, sample_org_id):
        """Patch get_organization_id and the Kafka producer factory."""
        mock_producer = MagicMock()
        with (
            patch("routers.send_data.get_organization_id", return_value=sample_org_id),
            patch("routers.send_data.get_kafka_producer", return_value=mock_producer),
        ):
            yield

    def _setup_name_mocks(self, mock_org, mock_proj, mock_coll, mock_schema):
        """Configure standard return values for org/project/collection lookups."""
        mock_org.return_value = MagicMock(organization_name="test_org")
        mock_proj.return_value = MagicMock(project_name="test_project")
        mock_coll.return_value = MagicMock(collection_name="test_collection")
        mock_schema.return_value = self.SCHEMA

    def _build_url(self, project_id, collection_id):
        """Build the send_data endpoint URL."""
        return f"/api/v1/projects/{project_id}/collections/{collection_id}/send_data"

    @patch("routers.send_data.fetch_collection_schema", new_callable=AsyncMock)
    @patch("routers.send_data.get_collection_by_id")
    @patch("routers.send_data.get_project_by_id")
    @patch("routers.send_data.get_organization_by_id")
    def test_send_single_message_success(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        mock_schema,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """POST with a single dict body should return 200 with processed_count=1."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll, mock_schema)

        url = self._build_url(sample_project_id, sample_collection_id)
        response = client.post(url, json={"temperature": 25.0})

        assert response.status_code == 200
        data = response.json()
        assert data["processed_count"] == 1
        assert "successfully" in data["message"]

    @patch("routers.send_data.fetch_collection_schema", new_callable=AsyncMock)
    @patch("routers.send_data.get_collection_by_id")
    @patch("routers.send_data.get_project_by_id")
    @patch("routers.send_data.get_organization_by_id")
    def test_send_batch_messages_success(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        mock_schema,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """POST with a list of dicts should process all valid messages."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll, mock_schema)

        url = self._build_url(sample_project_id, sample_collection_id)
        body = [{"temperature": 20.0}, {"temperature": 30.0}, {"temperature": 40.0}]
        response = client.post(url, json=body)

        assert response.status_code == 200
        data = response.json()
        assert data["processed_count"] == 3

    @patch("routers.send_data.fetch_collection_schema", new_callable=AsyncMock)
    @patch("routers.send_data.get_collection_by_id")
    @patch("routers.send_data.get_project_by_id")
    @patch("routers.send_data.get_organization_by_id")
    def test_send_no_schema_returns_400(
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

    @patch("routers.send_data.fetch_collection_schema", new_callable=AsyncMock)
    @patch("routers.send_data.get_collection_by_id")
    @patch("routers.send_data.get_project_by_id")
    @patch("routers.send_data.get_organization_by_id")
    def test_send_invalid_message_schema_returns_400(
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

    @patch("routers.send_data.fetch_collection_schema", new_callable=AsyncMock)
    @patch("routers.send_data.get_collection_by_id")
    @patch("routers.send_data.get_project_by_id")
    @patch("routers.send_data.get_organization_by_id")
    def test_send_invalid_timestamp_format(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        mock_schema,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Message with user-provided invalid timestamp should return 400 via ValueError path."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll, mock_schema)

        url = self._build_url(sample_project_id, sample_collection_id)
        response = client.post(
            url,
            json={"temperature": 25.0, "timestamp": "not-a-timestamp"},
        )

        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail["invalid_count"] == 1
        assert "Invalid timestamp format" in detail["invalid_messages"][0]["error"]

    @patch("routers.send_data.fetch_collection_schema", new_callable=AsyncMock)
    @patch("routers.send_data.get_collection_by_id")
    @patch("routers.send_data.get_project_by_id")
    @patch("routers.send_data.get_organization_by_id")
    def test_send_message_fails_final_validation(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        mock_schema,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Message that passes initial validation but fails after auto-fields are added.

        Uses a schema where 'day' expects int (not text), so the auto-generated
        day string will fail final validation.
        """
        mock_org.return_value = MagicMock(organization_name="test_org")
        mock_proj.return_value = MagicMock(project_name="test_project")
        mock_coll.return_value = MagicMock(collection_name="test_collection")
        mock_schema.return_value = {
            "key": "text",
            "timestamp": "text",
            "day": "int",
            "temperature": "float",
        }

        url = self._build_url(sample_project_id, sample_collection_id)
        response = client.post(url, json={"temperature": 25.0})

        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail["invalid_count"] == 1
        invalid_msg = detail["invalid_messages"][0]
        assert "processed_message" in invalid_msg
        assert "integer" in invalid_msg["error"]

    @patch("routers.send_data.fetch_collection_schema", new_callable=AsyncMock)
    @patch("routers.send_data.get_collection_by_id")
    @patch("routers.send_data.get_project_by_id")
    @patch("routers.send_data.get_organization_by_id")
    def test_send_date_validation_invalid(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        mock_schema,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Message with invalid date format should fail validation (covers lines 103-106)."""
        mock_org.return_value = MagicMock(organization_name="test_org")
        mock_proj.return_value = MagicMock(project_name="test_project")
        mock_coll.return_value = MagicMock(collection_name="test_collection")
        mock_schema.return_value = {
            "key": "text",
            "timestamp": "text",
            "day": "text",
            "event_date": "date",
            "temperature": "float",
        }

        url = self._build_url(sample_project_id, sample_collection_id)
        response = client.post(
            url,
            json={"temperature": 25.0, "event_date": "not-a-date"},
        )

        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail["invalid_count"] == 1
        assert "valid date format" in detail["invalid_messages"][0]["error"]

    @patch("routers.send_data.fetch_collection_schema", new_callable=AsyncMock)
    @patch("routers.send_data.get_collection_by_id")
    @patch("routers.send_data.get_project_by_id")
    @patch("routers.send_data.get_organization_by_id")
    def test_send_timestamp_validation_invalid(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        mock_schema,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Message with invalid timestamp type field should fail validation
        (covers lines 108-111)."""
        mock_org.return_value = MagicMock(organization_name="test_org")
        mock_proj.return_value = MagicMock(project_name="test_project")
        mock_coll.return_value = MagicMock(collection_name="test_collection")
        mock_schema.return_value = {
            "key": "text",
            "timestamp": "text",
            "day": "text",
            "event_ts": "timestamp",
            "temperature": "float",
        }

        url = self._build_url(sample_project_id, sample_collection_id)
        response = client.post(
            url,
            json={"temperature": 25.0, "event_ts": "not-valid-iso"},
        )

        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail["invalid_count"] == 1
        assert "valid timestamp format" in detail["invalid_messages"][0]["error"]

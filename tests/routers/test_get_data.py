import json
from collections import namedtuple
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest


class TestGetDataEndpoints:
    """Tests for the get data endpoints."""

    mock_session: MagicMock

    @pytest.fixture(autouse=True)
    def _patch_router_deps(self, sample_org_id):
        self.mock_session = MagicMock()
        with (
            patch("routers.get_data.get_organization_id", return_value=sample_org_id),
            patch("routers.get_data.session", self.mock_session),
        ):
            yield

    def _setup_name_mocks(self, mock_org, mock_proj, mock_coll):
        mock_org.return_value = MagicMock(organization_name="test_org")
        mock_proj.return_value = MagicMock(project_name="test_project")
        mock_coll.return_value = MagicMock(collection_name="test_collection")

    def _make_schema_rows(self, schema_dict):
        SchemaRow = namedtuple("SchemaRow", ["column_name", "type"])
        return [SchemaRow(column_name=k, type=v) for k, v in schema_dict.items()]

    def _make_data_rows(self, data_list, fields):
        DataRow = namedtuple("DataRow", fields)
        return [DataRow(**row) for row in data_list]

    @patch("routers.get_data.get_collection_by_id")
    @patch("routers.get_data.get_project_by_id")
    @patch("routers.get_data.get_organization_by_id")
    def test_get_data_basic(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting data returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )
        data_fields = ["key", "timestamp", "temperature"]
        data_rows = self._make_data_rows(
            [{"key": "sensor1", "timestamp": "2024-01-01T00:00:00", "temperature": 25.0}],
            data_fields,
        )

        self.mock_session.execute.side_effect = [schema_rows, data_rows]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/get_data",
        )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "total_count" in data
        assert data["total_count"] == 1

    @patch("routers.get_data.get_collection_by_id")
    @patch("routers.get_data.get_project_by_id")
    @patch("routers.get_data.get_organization_by_id")
    def test_get_data_with_limit(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting data with limit returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )
        data_fields = ["key", "timestamp", "temperature"]
        data_rows = self._make_data_rows(
            [
                {"key": "s1", "timestamp": "2024-01-01T00:00:00", "temperature": 20.0},
                {"key": "s2", "timestamp": "2024-01-01T01:00:00", "temperature": 22.0},
                {"key": "s3", "timestamp": "2024-01-01T02:00:00", "temperature": 24.0},
            ],
            data_fields,
        )

        self.mock_session.execute.side_effect = [schema_rows, data_rows]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/get_data",
            params={"limit": 2},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 3

    @patch("routers.get_data.get_collection_by_id")
    @patch("routers.get_data.get_project_by_id")
    @patch("routers.get_data.get_organization_by_id")
    def test_get_data_invalid_filter(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting data with invalid filter returns 422."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )
        self.mock_session.execute.side_effect = [schema_rows]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/get_data",
            params={"filters": "not-valid-json"},
        )
        assert response.status_code == 422
        assert "Invalid filter format" in response.json()["detail"]

    @patch("routers.get_data.get_collection_by_id")
    @patch("routers.get_data.get_project_by_id")
    @patch("routers.get_data.get_organization_by_id")
    def test_get_data_with_attributes(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting data with attributes returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float", "humidity": "float"}
        )
        data_fields = ["temperature"]
        data_rows = self._make_data_rows(
            [{"temperature": 25.0}],
            data_fields,
        )

        self.mock_session.execute.side_effect = [schema_rows, data_rows]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/get_data",
            params={"attributes": ["temperature"]},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 1

    @patch("routers.get_data.get_collection_by_id")
    @patch("routers.get_data.get_project_by_id")
    @patch("routers.get_data.get_organization_by_id")
    def test_get_data_invalid_attribute(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting data with invalid attribute returns 422."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )
        self.mock_session.execute.side_effect = [schema_rows]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/get_data",
            params={"attributes": ["nonexistent_field"]},
        )
        assert response.status_code == 422
        assert "Attributes not in schema" in response.json()["detail"]

    @patch("routers.get_data.get_collection_by_id")
    @patch("routers.get_data.get_project_by_id")
    @patch("routers.get_data.get_organization_by_id")
    def test_get_data_empty_result(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting data with empty result returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )
        self.mock_session.execute.side_effect = [schema_rows, []]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/get_data",
        )
        assert response.status_code == 200
        data = response.json()
        assert data["data"] == []
        assert data["total_count"] == 0

    @patch("routers.get_data.get_collection_by_id")
    @patch("routers.get_data.get_project_by_id")
    @patch("routers.get_data.get_organization_by_id")
    def test_get_data_invalid_order_by(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting data with invalid order_by returns 422."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )
        self.mock_session.execute.side_effect = [schema_rows, []]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/get_data",
            params={"order_by": "not-valid-json"},
        )
        assert response.status_code == 422
        assert "Invalid order_by format" in response.json()["detail"]

    @patch("routers.get_data.get_collection_by_id")
    @patch("routers.get_data.get_project_by_id")
    @patch("routers.get_data.get_organization_by_id")
    def test_get_data_schema_lowercase_fallback(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting data with lowercase schema fallback returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows({"key": "text", "temperature": "float"})
        data_rows = self._make_data_rows(
            [{"key": "sensor1", "temperature": 22.0}],
            ["key", "temperature"],
        )

        self.mock_session.execute.side_effect = [[], schema_rows, data_rows]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/get_data",
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 1
        assert data["data"][0]["key"] == "sensor1"

    @patch("routers.get_data.get_collection_by_id")
    @patch("routers.get_data.get_project_by_id")
    @patch("routers.get_data.get_organization_by_id")
    def test_get_data_no_schema_with_attributes_raises_422(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting data with attributes when no schema exists returns 422."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        TableRow = namedtuple("TableRow", ["table_name"])
        tables_rows = [TableRow(table_name="test_project_test_collection")]

        self.mock_session.execute.side_effect = [[], [], tables_rows]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/get_data",
            params={"attributes": ["temperature"]},
        )
        assert response.status_code == 422
        assert "does not have a defined schema" in response.json()["detail"]

    @patch("routers.get_data.get_collection_by_id")
    @patch("routers.get_data.get_project_by_id")
    @patch("routers.get_data.get_organization_by_id")
    def test_get_data_no_schema_no_attributes_ok(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting data without schema and attributes returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        TableRow = namedtuple("TableRow", ["table_name"])
        tables_rows = [TableRow(table_name="test_project_test_collection")]

        self.mock_session.execute.side_effect = [[], [], tables_rows, []]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/get_data",
        )
        assert response.status_code == 200
        data = response.json()
        assert data["data"] == []
        assert data["total_count"] == 0

    @patch("routers.get_data.generate_filter_condition")
    @patch("routers.get_data.get_collection_by_id")
    @patch("routers.get_data.get_project_by_id")
    @patch("routers.get_data.get_organization_by_id")
    def test_get_data_with_filters_or_and_regular(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        mock_gen_filter,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting data with complex filters returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows({"key": "text", "temperature": "float"})
        data_rows = self._make_data_rows(
            [{"key": "s1", "temperature": 25.0}],
            ["key", "temperature"],
        )

        self.mock_session.execute.side_effect = [schema_rows, data_rows]

        mock_gen_filter.side_effect = [
            "key='s1'",
            "temperature>20",
        ]

        filters_param = json.dumps(
            [
                {
                    "operator": "or",
                    "operands": [
                        {"property_name": "key", "operator": "eq", "property_value": "s1"},
                    ],
                },
                {"property_name": "temperature", "operator": "gt", "property_value": 20},
            ]
        )

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/get_data",
            params={"filters": filters_param},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 1
        assert mock_gen_filter.call_count == 2

    @patch("routers.get_data.get_collection_by_id")
    @patch("routers.get_data.get_project_by_id")
    @patch("routers.get_data.get_organization_by_id")
    def test_get_data_decimal_conversion(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting data with decimal conversion returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows({"key": "text", "temperature": "decimal"})

        DataRow = namedtuple("DataRow", ["key", "temperature"])
        data_rows = [DataRow(key="sensor1", temperature=Decimal("25.5"))]

        self.mock_session.execute.side_effect = [schema_rows, data_rows]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/get_data",
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 1
        assert data["data"][0]["temperature"] == 25.5
        assert isinstance(data["data"][0]["temperature"], float)

    @patch("routers.get_data.get_collection_by_id")
    @patch("routers.get_data.get_project_by_id")
    @patch("routers.get_data.get_organization_by_id")
    def test_get_data_order_by_field_not_in_schema(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting data with order_by field not in schema returns 422."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows({"key": "text", "temperature": "float"})
        data_rows = self._make_data_rows(
            [{"key": "s1", "temperature": 20.0}],
            ["key", "temperature"],
        )

        self.mock_session.execute.side_effect = [schema_rows, data_rows]

        order_by_param = json.dumps({"field": "nonexistent", "order": "asc"})

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/get_data",
            params={"order_by": order_by_param},
        )
        assert response.status_code == 422
        assert "Order field 'nonexistent' not in schema" in response.json()["detail"]

    @patch("routers.get_data.get_collection_by_id")
    @patch("routers.get_data.get_project_by_id")
    @patch("routers.get_data.get_organization_by_id")
    def test_get_data_db_error(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting data handles database errors and returns 500."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows({"key": "text", "temperature": "float"})

        self.mock_session.execute.side_effect = [schema_rows, Exception("DB error")]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/get_data",
        )
        assert response.status_code == 500
        assert "Database query failed" in response.json()["detail"]

    @patch("routers.get_data.get_collection_by_id")
    @patch("routers.get_data.get_project_by_id")
    @patch("routers.get_data.get_organization_by_id")
    def test_get_data_with_offset(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting data with offset returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows({"key": "text", "temperature": "float"})
        data_rows = self._make_data_rows(
            [
                {"key": "s1", "temperature": 20.0},
                {"key": "s2", "temperature": 22.0},
                {"key": "s3", "temperature": 24.0},
            ],
            ["key", "temperature"],
        )

        self.mock_session.execute.side_effect = [schema_rows, data_rows]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/get_data",
            params={"offset": 1},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 3
        assert len(data["data"]) == 2

    @patch("routers.get_data.unflatten_data")
    @patch("routers.get_data.get_collection_by_id")
    @patch("routers.get_data.get_project_by_id")
    @patch("routers.get_data.get_organization_by_id")
    def test_get_data_nested_false(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        mock_unflatten,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting data with nested=false returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows({"key": "text", "temperature": "float"})
        data_rows = self._make_data_rows(
            [{"key": "sensor1", "temperature": 25.0}],
            ["key", "temperature"],
        )

        self.mock_session.execute.side_effect = [schema_rows, data_rows]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/get_data",
            params={"nested": "false"},
        )
        assert response.status_code == 200
        mock_unflatten.assert_not_called()

    @patch("routers.get_data.get_collection_by_id")
    @patch("routers.get_data.get_project_by_id")
    @patch("routers.get_data.get_organization_by_id")
    def test_get_data_valid_order_by(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting data with valid order_by returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows({"key": "text", "temperature": "float"})
        data_rows = self._make_data_rows(
            [
                {"key": "s1", "temperature": 20.0},
                {"key": "s2", "temperature": 30.0},
                {"key": "s3", "temperature": 10.0},
            ],
            ["key", "temperature"],
        )

        self.mock_session.execute.side_effect = [schema_rows, data_rows]

        order_by_param = json.dumps({"field": "temperature", "order": "desc"})

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/get_data",
            params={"order_by": order_by_param},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 3
        temps = [item["temperature"] for item in data["data"]]
        assert temps == [30.0, 20.0, 10.0]

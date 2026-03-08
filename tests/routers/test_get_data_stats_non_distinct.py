import json
from collections import namedtuple
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest


class TestGetDataStatsNonDistinct:
    """Tests for the get data stats non-distinct endpoints."""

    mock_session: MagicMock

    @pytest.fixture(autouse=True)
    def _patch_router_deps(self, sample_org_id):
        self.mock_session = MagicMock()
        with (
            patch(
                "routers.get_data_stats.get_organization_id",
                return_value=sample_org_id,
            ),
            patch("routers.get_data_stats.session", self.mock_session),
        ):
            yield

    def _setup_name_mocks(self, mock_org, mock_proj, mock_coll):
        mock_org.return_value = MagicMock(organization_name="test_org")
        mock_proj.return_value = MagicMock(project_name="test_project")
        mock_coll.return_value = MagicMock(collection_name="test_collection")

    def _make_schema_rows(self, schema_dict):
        SchemaRow = namedtuple("SchemaRow", ["column_name", "type"])
        return [SchemaRow(column_name=k, type=v) for k, v in schema_dict.items()]

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    @patch("routers.get_data_stats.aggregate_data")
    def test_stats_schema_lowercase_fallback(
        self,
        mock_aggregate,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify stats fallback to lowercase schema returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )

        DataRow = namedtuple("DataRow", ["key", "timestamp", "temperature"])
        data_rows = [
            DataRow(key="sensor1", timestamp=datetime(2024, 1, 1), temperature=25.0),
        ]

        self.mock_session.execute.side_effect = [[], schema_rows, data_rows]

        mock_aggregate.return_value = [{"key": "sensor1", "avg_temperature": 25.0}]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "avg",
                "interval": "every_1_days",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    def test_stats_invalid_group_by(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify stats with invalid group_by returns 422."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )
        self.mock_session.execute.side_effect = [schema_rows]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "avg",
                "interval": "every_1_days",
                "group_by": "nonexistent_field",
            },
        )
        assert response.status_code == 422
        assert "does not exist" in response.json()["detail"]

    @patch("routers.get_data_stats.generate_filter_condition")
    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    @patch("routers.get_data_stats.aggregate_data")
    def test_stats_with_filters_non_distinct(
        self,
        mock_aggregate,
        mock_org,
        mock_proj,
        mock_coll,
        mock_gen_filter,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify stats with filters returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)
        mock_gen_filter.return_value = "\"key\" = 'sensor1'"

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )

        DataRow = namedtuple("DataRow", ["key", "timestamp", "temperature"])
        data_rows = [
            DataRow(key="sensor1", timestamp=datetime(2024, 1, 1), temperature=25.0),
        ]

        self.mock_session.execute.side_effect = [schema_rows, data_rows]

        mock_aggregate.return_value = [{"key": "sensor1", "avg_temperature": 25.0}]

        filters_param = json.dumps(
            [
                {"property_name": "key", "operator": "eq", "property_value": "sensor1"},
                {
                    "operator": "or",
                    "operands": [
                        {"property_name": "temperature", "operator": "gt", "property_value": 20},
                    ],
                },
            ]
        )

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "avg",
                "interval": "every_1_days",
                "filters": filters_param,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert mock_gen_filter.call_count >= 2

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    @patch("routers.get_data_stats.aggregate_data")
    def test_stats_with_start_and_end_time_non_distinct(
        self,
        mock_aggregate,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify stats with start and end time returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )

        DataRow = namedtuple("DataRow", ["key", "timestamp", "temperature"])
        data_rows = [
            DataRow(key="sensor1", timestamp=datetime(2024, 1, 1), temperature=25.0),
        ]

        self.mock_session.execute.side_effect = [schema_rows, data_rows]

        mock_aggregate.return_value = [{"key": "sensor1", "avg_temperature": 25.0}]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "avg",
                "interval": "every_1_days",
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    @patch("routers.get_data_stats.generate_filter_condition")
    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    @patch("routers.get_data_stats.aggregate_data")
    def test_stats_with_time_and_filters_non_distinct(
        self,
        mock_aggregate,
        mock_org,
        mock_proj,
        mock_coll,
        mock_gen_filter,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify stats with time and filters returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)
        mock_gen_filter.return_value = "\"key\" = 'sensor1'"

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )

        DataRow = namedtuple("DataRow", ["key", "timestamp", "temperature"])
        data_rows = [
            DataRow(key="sensor1", timestamp=datetime(2024, 1, 1), temperature=25.0),
        ]

        self.mock_session.execute.side_effect = [schema_rows, data_rows]

        mock_aggregate.return_value = [{"key": "sensor1", "avg_temperature": 25.0}]

        filters_param = json.dumps(
            [
                {"property_name": "key", "operator": "eq", "property_value": "sensor1"},
            ]
        )

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "avg",
                "interval": "every_1_days",
                "filters": filters_param,
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    @patch("routers.get_data_stats.aggregate_data")
    def test_stats_decimal_conversion_non_distinct(
        self,
        mock_aggregate,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify stats decimal conversion returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )

        class FakeDecimal:
            """Fake Decimal class for testing."""

            def __init__(self, val):
                self._val = val

            def __str__(self):
                return str(self._val)

            def __float__(self):
                return float(self._val)

        FakeDecimal.__name__ = "Decimal"

        DataRow = namedtuple("DataRow", ["key", "timestamp", "temperature"])
        data_rows = [
            DataRow(key="sensor1", timestamp=datetime(2024, 1, 1), temperature=FakeDecimal(25.5)),
        ]

        self.mock_session.execute.side_effect = [schema_rows, data_rows]

        mock_aggregate.return_value = [{"key": "sensor1", "avg_temperature": 25.5}]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "avg",
                "interval": "every_1_days",
            },
        )
        assert response.status_code == 200

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    def test_stats_unsupported_interval_unit(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify stats with unsupported interval unit returns empty list."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )

        DataRow = namedtuple("DataRow", ["key", "timestamp", "temperature"])
        data_rows = [
            DataRow(key="sensor1", timestamp=datetime(2024, 1, 1), temperature=25.0),
        ]

        self.mock_session.execute.side_effect = [schema_rows, data_rows]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "avg",
                "interval": "every_2_centuries",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data == []

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    def test_stats_query_error(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify stats query error returns empty list."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )

        self.mock_session.execute.side_effect = [schema_rows, Exception("Query failed")]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "avg",
                "interval": "every_1_days",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data == []

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    @patch("routers.get_data_stats.aggregate_data")
    def test_stats_with_order_desc(
        self,
        mock_aggregate,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify stats with descending order returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )

        DataRow = namedtuple("DataRow", ["key", "timestamp", "temperature"])
        data_rows = [
            DataRow(key="sensor1", timestamp=datetime(2024, 1, 1), temperature=25.0),
            DataRow(key="sensor2", timestamp=datetime(2024, 1, 2), temperature=30.0),
        ]

        self.mock_session.execute.side_effect = [schema_rows, data_rows]

        mock_aggregate.return_value = [
            {"key": "sensor1", "avg_temperature": 25.0},
            {"key": "sensor2", "avg_temperature": 30.0},
        ]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "avg",
                "interval": "every_1_days",
                "order": "desc",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["avg_temperature"] == 30.0
        assert data[1]["avg_temperature"] == 25.0

import json
from collections import namedtuple
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

import routers.get_data_stats as _stats_module


class TestGetDataStatsNonDistinct:
    """Tests for the get data stats non-distinct endpoints."""

    mock_session: MagicMock

    @pytest.fixture(autouse=True)
    def _patch_router_deps(self, sample_org_id):
        self.mock_session = MagicMock()
        _stats_module._schema_cache.clear()
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

    @patch("routers.get_data_stats.generate_filter_condition_parameterized")
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
        mock_gen_filter.return_value = ('"key" = %s', ["sensor1"])

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

    @patch("routers.get_data_stats.generate_filter_condition_parameterized")
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
        mock_gen_filter.return_value = ('"key" = %s', ["sensor1"])

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
        """Verify stats with unsupported interval unit returns 422."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "avg",
                "interval": "every_2_centuries",
            },
        )
        assert response.status_code == 422

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
        """Verify stats query error returns 500."""
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
        assert response.status_code == 500

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

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    def test_stats_row_cap_raises_400(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify that hitting the row cap returns 400."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        from config import settings

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )

        DataRow = namedtuple("DataRow", ["key", "timestamp", "temperature"])
        cap = settings.max_stats_rows
        data_rows = [DataRow(key="s", timestamp=datetime(2024, 1, 1), temperature=1.0)] * cap

        self.mock_session.execute.side_effect = [schema_rows, data_rows]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "avg",
                "interval": "every_1_days",
            },
        )
        assert response.status_code == 400
        assert "Result set too large" in response.json()["detail"]

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    @patch("routers.get_data_stats.aggregate_data")
    def test_stats_row_cap_not_triggered(
        self,
        mock_aggregate,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify that fewer rows than the cap returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )

        DataRow = namedtuple("DataRow", ["key", "timestamp", "temperature"])
        data_rows = [DataRow(key="sensor1", timestamp=datetime(2024, 1, 1), temperature=25.0)]

        self.mock_session.execute.side_effect = [schema_rows, data_rows]
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

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    @patch("routers.get_data_stats.aggregate_data")
    def test_schema_cached_on_second_call(
        self,
        mock_aggregate,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify schema is fetched only once across two identical requests."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )

        DataRow = namedtuple("DataRow", ["key", "timestamp", "temperature"])
        data_rows = [DataRow(key="sensor1", timestamp=datetime(2024, 1, 1), temperature=25.0)]

        mock_aggregate.return_value = [{"key": "sensor1", "avg_temperature": 25.0}]

        _stats_module._schema_cache.clear()

        self.mock_session.execute.side_effect = [schema_rows, data_rows, data_rows]

        url = f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics"
        params = {"attribute": "temperature", "stat": "avg", "interval": "every_1_days"}

        client.get(url, params=params)
        client.get(url, params=params)

        schema_calls = [
            c for c in self.mock_session.execute.call_args_list if "system_schema" in str(c)
        ]
        assert len(schema_calls) == 1

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    @patch("routers.get_data_stats.aggregate_data")
    def test_schema_cache_expires(
        self,
        mock_aggregate,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify schema is re-fetched after TTL expires."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )

        DataRow = namedtuple("DataRow", ["key", "timestamp", "temperature"])
        data_rows = [DataRow(key="sensor1", timestamp=datetime(2024, 1, 1), temperature=25.0)]

        mock_aggregate.return_value = [{"key": "sensor1", "avg_temperature": 25.0}]

        _stats_module._schema_cache.clear()

        self.mock_session.execute.side_effect = [schema_rows, data_rows, schema_rows, data_rows]

        url = f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics"
        params = {"attribute": "temperature", "stat": "avg", "interval": "every_1_days"}

        with patch("routers.get_data_stats.time") as mock_time:
            mock_time.monotonic.return_value = 0.0
            client.get(url, params=params)

            mock_time.monotonic.return_value = _stats_module._SCHEMA_CACHE_TTL + 1.0
            client.get(url, params=params)

        schema_calls = [
            c for c in self.mock_session.execute.call_args_list if "system_schema" in str(c)
        ]
        assert len(schema_calls) == 2

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    @patch("routers.get_data_stats.aggregate_data")
    def test_stats_count_query_excludes_attribute_column(
        self,
        mock_aggregate,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify count stat does not select the attribute column in the data query."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )

        DataRow = namedtuple("DataRow", ["key", "timestamp"])
        data_rows = [DataRow(key="sensor1", timestamp=datetime(2024, 1, 1))]

        self.mock_session.execute.side_effect = [schema_rows, data_rows]
        mock_aggregate.return_value = [{"key": "sensor1", "count_temperature": 1}]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "count",
                "interval": "every_1_days",
            },
        )
        assert response.status_code == 200

        data_query_call = self.mock_session.execute.call_args_list[1]
        data_query_str = str(data_query_call)
        assert "temperature" not in data_query_str or "system_schema" in str(
            self.mock_session.execute.call_args_list[0]
        )

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    def test_stats_invalid_interval_format_returns_422(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify invalid interval format returns 422."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "avg",
                "interval": "every_bad_format",
            },
        )
        assert response.status_code == 422
        assert "every_N_" in response.json()["detail"]

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    def test_stats_invalid_interval_unit_returns_422(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify invalid interval unit returns 422."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "avg",
                "interval": "every_2_centuries",
            },
        )
        assert response.status_code == 422

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    @patch("routers.get_data_stats.aggregate_data")
    def test_stats_p90_returns_200(
        self,
        mock_aggregate,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify p90 stat returns 200 with p90_attribute key."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )

        DataRow = namedtuple("DataRow", ["key", "timestamp", "temperature"])
        data_rows = [
            DataRow(key="sensor1", timestamp=datetime(2024, 1, 1), temperature=float(i))
            for i in range(10)
        ]

        self.mock_session.execute.side_effect = [schema_rows, data_rows]
        mock_aggregate.return_value = [
            {"key": "sensor1", "interval_start": datetime(2024, 1, 1), "p90_temperature": 8.1}
        ]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "p90",
                "interval": "every_1_days",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert "p90_temperature" in data[0]

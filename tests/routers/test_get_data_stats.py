import json
from collections import namedtuple
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest


class TestGetDataStatsEndpoints:
    """Tests for the get data stats endpoints."""

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
    def test_stats_avg(
        self,
        mock_aggregate,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting average stats returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )

        DataRow = namedtuple("DataRow", ["key", "timestamp", "temperature"])
        data_rows = [
            DataRow(key="sensor1", timestamp=datetime(2024, 1, 1), temperature=25.0),
            DataRow(key="sensor1", timestamp=datetime(2024, 1, 2), temperature=27.0),
        ]

        self.mock_session.execute.side_effect = [schema_rows, data_rows]

        mock_aggregate.return_value = [
            {
                "key": "sensor1",
                "interval_start": "2024-01-01T00:00:00",
                "avg_temperature": 26.0,
            }
        ]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "avg",
                "interval": "every_2_days",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["avg_temperature"] == 26.0

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    def test_stats_missing_attribute(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting stats with missing attribute returns 422."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={"stat": "avg", "interval": "every_2_days"},
        )
        assert response.status_code == 422

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    def test_stats_distinct(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting distinct stats returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        DataRow = namedtuple("DataRow", ["key", "temperature", "timestamp"])
        data_rows = [
            DataRow(key="sensor1", temperature=25.0, timestamp=datetime(2024, 1, 1, 10, 0)),
            DataRow(key="sensor1", temperature=26.0, timestamp=datetime(2024, 1, 1, 11, 0)),
            DataRow(key="sensor1", temperature=25.0, timestamp=datetime(2024, 1, 1, 12, 0)),
        ]

        self.mock_session.execute.return_value = data_rows

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "distinct",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["stat"] == "distinct"
        assert data["attribute"] == "temperature"
        assert data["total_keys"] == 1
        assert len(data["key_statistics"]) == 1
        assert data["key_statistics"][0]["key"] == "sensor1"

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    def test_stats_distinct_missing_attribute(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting distinct stats with missing attribute returns 422."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={"stat": "distinct"},
        )
        assert response.status_code == 422
        assert "attribute is required" in response.json()["detail"]

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    def test_stats_attribute_not_in_schema(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting stats with attribute not in schema returns 422."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )
        self.mock_session.execute.side_effect = [schema_rows]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "nonexistent",
                "stat": "avg",
                "interval": "every_2_days",
            },
        )
        assert response.status_code == 422
        assert "does not exist" in response.json()["detail"]

    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    @patch("routers.get_data_stats.aggregate_data")
    def test_stats_count(
        self,
        mock_aggregate,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting count stats returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        schema_rows = self._make_schema_rows(
            {"key": "text", "timestamp": "timestamp", "temperature": "float"}
        )

        DataRow = namedtuple("DataRow", ["key", "timestamp", "temperature"])
        data_rows = [
            DataRow(key="sensor1", timestamp=datetime(2024, 1, 1), temperature=25.0),
        ]

        self.mock_session.execute.side_effect = [schema_rows, data_rows]

        mock_aggregate.return_value = [
            {
                "key": "sensor1",
                "interval_start": "2024-01-01T00:00:00",
                "count_temperature": 1,
            }
        ]

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "count",
                "interval": "every_1_days",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert data[0]["count_temperature"] == 1

    # ------------------------------------------------------------------
    # Distinct: filters (OR + regular) — covers lines 75-92, 103
    # ------------------------------------------------------------------
    @patch("routers.get_data_stats.generate_filter_condition")
    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    def test_stats_distinct_with_filters(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        mock_gen_filter,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting distinct stats with filters returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)
        mock_gen_filter.return_value = "\"key\" = 'sensor1'"

        DataRow = namedtuple("DataRow", ["key", "temperature", "timestamp"])
        data_rows = [
            DataRow(key="sensor1", temperature=25.0, timestamp=datetime(2024, 1, 1, 10, 0)),
        ]
        self.mock_session.execute.return_value = data_rows

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
                "stat": "distinct",
                "filters": filters_param,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["stat"] == "distinct"
        assert data["total_keys"] == 1
        assert mock_gen_filter.call_count >= 2

    # ------------------------------------------------------------------
    # Distinct: start_time AND end_time — covers line 96
    # ------------------------------------------------------------------
    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    def test_stats_distinct_with_start_and_end_time(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting distinct stats with start and end time returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        DataRow = namedtuple("DataRow", ["key", "temperature", "timestamp"])
        data_rows = [
            DataRow(key="sensor1", temperature=25.0, timestamp=datetime(2024, 1, 1, 10, 0)),
        ]
        self.mock_session.execute.return_value = data_rows

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "distinct",
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["stat"] == "distinct"

    # ------------------------------------------------------------------
    # Distinct: start_time only — covers line 98
    # ------------------------------------------------------------------
    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    def test_stats_distinct_with_start_time_only(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting distinct stats with start time only returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        DataRow = namedtuple("DataRow", ["key", "temperature", "timestamp"])
        data_rows = [
            DataRow(key="sensor1", temperature=25.0, timestamp=datetime(2024, 1, 1, 10, 0)),
        ]
        self.mock_session.execute.return_value = data_rows

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "distinct",
                "start_time": "2024-01-01T00:00:00Z",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["stat"] == "distinct"

    # ------------------------------------------------------------------
    # Distinct: end_time only — covers line 100
    # ------------------------------------------------------------------
    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    def test_stats_distinct_with_end_time_only(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting distinct stats with end time only returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        DataRow = namedtuple("DataRow", ["key", "temperature", "timestamp"])
        data_rows = [
            DataRow(key="sensor1", temperature=25.0, timestamp=datetime(2024, 1, 1, 10, 0)),
        ]
        self.mock_session.execute.return_value = data_rows

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "distinct",
                "end_time": "2024-01-02T00:00:00Z",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["stat"] == "distinct"

    # ------------------------------------------------------------------
    # Distinct: Decimal value conversion — covers line 134
    # ------------------------------------------------------------------
    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    def test_stats_distinct_decimal_value(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting distinct stats with decimal values returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        # Create a fake Decimal class so "Decimal" appears in __class__.__name__
        class FakeDecimal:
            """Fake Decimal class for testing."""

            def __init__(self, val):
                self._val = val

            def __str__(self):
                return str(self._val)

            def __float__(self):
                return float(self._val)

        FakeDecimal.__name__ = "Decimal"

        DataRow = namedtuple("DataRow", ["key", "temperature", "timestamp"])
        data_rows = [
            DataRow(
                key="sensor1", temperature=FakeDecimal(25.5), timestamp=datetime(2024, 1, 1, 10, 0)
            ),
        ]
        self.mock_session.execute.return_value = data_rows

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "distinct",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["stat"] == "distinct"
        assert data["total_keys"] == 1

    # ------------------------------------------------------------------
    # Distinct: order=desc — covers line 179
    # ------------------------------------------------------------------
    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    def test_stats_distinct_ordering(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting distinct stats with ordering returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        DataRow = namedtuple("DataRow", ["key", "temperature", "timestamp"])
        data_rows = [
            DataRow(key="alpha", temperature=25.0, timestamp=datetime(2024, 1, 1, 10, 0)),
            DataRow(key="beta", temperature=26.0, timestamp=datetime(2024, 1, 1, 11, 0)),
        ]
        self.mock_session.execute.return_value = data_rows

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "distinct",
                "order": "desc",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["key_statistics"][0]["key"] == "beta"
        assert data["key_statistics"][1]["key"] == "alpha"

    # ------------------------------------------------------------------
    # Distinct: min/max timestamp updates — covers lines 159, 179
    # ------------------------------------------------------------------
    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    def test_stats_distinct_min_max_timestamps(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting distinct stats with min/max timestamps returns 200."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)

        DataRow = namedtuple("DataRow", ["key", "temperature", "timestamp"])
        # Same key, multiple rows — second has earlier timestamp, third has later
        data_rows = [
            DataRow(key="sensor1", temperature=25.0, timestamp=datetime(2024, 1, 2, 10, 0)),
            DataRow(key="sensor1", temperature=26.0, timestamp=datetime(2024, 1, 1, 8, 0)),
            DataRow(key="sensor1", temperature=27.0, timestamp=datetime(2024, 1, 3, 15, 0)),
        ]
        self.mock_session.execute.return_value = data_rows

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "distinct",
            },
        )
        assert response.status_code == 200
        data = response.json()
        stats = data["key_statistics"][0]
        assert stats["min_timestamp"] == "2024-01-01T08:00:00"
        assert stats["max_timestamp"] == "2024-01-03T15:00:00"
        assert stats["total_records"] == 3

    # ------------------------------------------------------------------
    # Distinct: error handling — covers lines 189-190
    # ------------------------------------------------------------------
    @patch("routers.get_data_stats.get_collection_by_id")
    @patch("routers.get_data_stats.get_project_by_id")
    @patch("routers.get_data_stats.get_organization_by_id")
    def test_stats_distinct_error(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting distinct stats handles errors and returns 500."""
        self._setup_name_mocks(mock_org, mock_proj, mock_coll)
        self.mock_session.execute.side_effect = Exception("Cassandra connection lost")

        response = client.get(
            f"/api/v1/projects/{sample_project_id}/collections/{sample_collection_id}/statistics",
            params={
                "attribute": "temperature",
                "stat": "distinct",
            },
        )
        assert response.status_code == 500
        assert "Failed to retrieve distinct values" in response.json()["detail"]

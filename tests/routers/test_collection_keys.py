"""Tests for routers/collection_keys.py — key statistics endpoint."""

from collections import namedtuple
from unittest.mock import MagicMock, patch

StatsRow = namedtuple("StatsRow", ["record_count", "first_timestamp", "last_timestamp"])


@patch("routers.collection_keys.session")
@patch("routers.collection_keys.get_collection_by_id")
@patch("routers.collection_keys.get_project_by_id")
@patch("routers.collection_keys.get_organization_by_id")
class TestGetKeyStatistics:
    """Tests for the get key statistics endpoint."""

    def _url(self, project_id, collection_id, key):
        return f"/api/v1/projects/{project_id}/collections/{collection_id}/keys/{key}/stats"

    def test_no_data_returns_500(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        mock_session,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting stats with no data returns 500."""
        mock_org.return_value = MagicMock(organization_name="org")
        mock_proj.return_value = MagicMock(project_name="proj")
        mock_coll.return_value = MagicMock(collection_name="coll")

        stats = StatsRow(record_count=0, first_timestamp=None, last_timestamp=None)
        mock_session.execute.return_value = MagicMock(one=MagicMock(return_value=stats))

        resp = client.get(self._url(sample_project_id, sample_collection_id, "missing"))
        assert resp.status_code == 500

    def test_db_error_returns_500(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        mock_session,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Verify getting stats handles database errors and returns 500."""
        mock_org.return_value = MagicMock(organization_name="org")
        mock_proj.return_value = MagicMock(project_name="proj")
        mock_coll.return_value = MagicMock(collection_name="coll")

        mock_session.execute.side_effect = Exception("DB down")

        resp = client.get(self._url(sample_project_id, sample_collection_id, "key1"))
        assert resp.status_code == 500

    def test_daily_breakdown_with_driver_date(
        self,
        mock_org,
        mock_proj,
        mock_coll,
        mock_session,
        client,
        sample_project_id,
        sample_collection_id,
    ):
        """Daily breakdown must handle cassandra.util.Date (no strftime).

        The driver decodes CQL DATE to cassandra.util.Date, which has no
        strftime — str() already yields ISO YYYY-MM-DD.
        """
        from datetime import datetime

        from cassandra.util import Date

        mock_org.return_value = MagicMock(organization_name="org")
        mock_proj.return_value = MagicMock(project_name="proj")
        mock_coll.return_value = MagicMock(collection_name="coll")

        DayRow = namedtuple("DayRow", ["day"])
        stats = StatsRow(
            record_count=3,
            first_timestamp=datetime(2026, 10, 21, 12, 0, 0),
            last_timestamp=datetime(2026, 10, 21, 12, 5, 0),
        )
        mock_session.execute.side_effect = [
            MagicMock(one=MagicMock(return_value=stats)),
            [DayRow(Date(20720)), DayRow(Date(20720)), DayRow(Date(20721))],
        ]

        resp = client.get(self._url(sample_project_id, sample_collection_id, "key1"))
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_records"] == 3
        assert {d["date"]: d["record_count"] for d in body["daily_breakdown"]} == {
            "2026-09-24": 2,
            "2026-09-25": 1,
        }

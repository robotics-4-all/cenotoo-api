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

import datetime
import uuid
from collections import namedtuple
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from models.collection_models import CollectionCreateRequest, CollectionUpdateRequest
from utilities.collection_utils import (
    check_collection_exists,
    create_kafka_topic,
    delete_collection_from_db,
    delete_kafka_topic,
    fetch_all_collections,
    fetch_collection_by_name,
    fetch_collection_schema,
    get_collection_by_id,
    insert_collection,
    insert_data_into_table,
    update_collection_in_db,
)

CollectionRow = namedtuple(
    "CollectionRow",
    [
        "id",
        "collection_name",
        "description",
        "tags",
        "creation_date",
        "project_id",
        "organization_id",
    ],
)

SchemaRow = namedtuple("SchemaRow", ["column_name", "type"])


class TestGetCollectionById:
    """Tests for get_collection_by_id."""

    def test_returns_row(self):
        """Verify get_collection_by_id returns the expected collection row."""
        coll_id = uuid.uuid4()
        proj_id = uuid.uuid4()
        org_id = uuid.uuid4()
        row = CollectionRow(
            id=coll_id,
            collection_name="coll",
            description="d",
            tags=[],
            creation_date="2024-01-01",
            project_id=proj_id,
            organization_id=org_id,
        )

        with patch("utilities.collection_utils.pg_fetchone", return_value=row):
            result = get_collection_by_id(coll_id, proj_id, org_id)

        assert result == row

    def test_returns_none_when_not_found(self):
        """Verify get_collection_by_id returns None when collection does not exist."""
        coll_id = uuid.uuid4()
        proj_id = uuid.uuid4()
        org_id = uuid.uuid4()

        with patch("utilities.collection_utils.pg_fetchone", return_value=None):
            result = get_collection_by_id(coll_id, proj_id, org_id)

        assert result is None


class TestInsertCollection:
    """Tests for insert_collection."""

    @pytest.mark.asyncio
    async def test_returns_collection_id(self):
        """Verify insert_collection successfully inserts and returns a collection ID."""
        org_id = uuid.uuid4()
        proj_id = uuid.uuid4()
        data = CollectionCreateRequest(
            name="my_coll",
            description="desc",
            tags=["t1"],
            collection_schema={"temp": 25.0},
        )

        with patch("utilities.collection_utils.pg_execute") as mock_exec:
            result = await insert_collection(org_id, proj_id, data)

        assert isinstance(result, uuid.UUID)
        mock_exec.assert_called_once()

    @pytest.mark.asyncio
    async def test_insert_query_contains_collection_name(self):
        """Verify insert_collection uses INSERT INTO collection query."""
        org_id = uuid.uuid4()
        proj_id = uuid.uuid4()
        data = CollectionCreateRequest(
            name="sensors",
            description="sensor data",
            tags=[],
            collection_schema={"temp": "float"},
        )

        with patch("utilities.collection_utils.pg_execute") as mock_exec:
            await insert_collection(org_id, proj_id, data)

        call_args = mock_exec.call_args[0]
        assert "INSERT INTO collection" in call_args[0]


class TestUpdateCollectionInDb:
    """Tests for update_collection_in_db."""

    @pytest.mark.asyncio
    async def test_update_with_description(self):
        """Verify update_collection_in_db updates description correctly."""
        coll_id = uuid.uuid4()
        data = CollectionUpdateRequest(description="new desc")

        with patch("utilities.collection_utils.pg_execute") as mock_exec:
            await update_collection_in_db(coll_id, data)

        args = mock_exec.call_args[0]
        assert "description=%s" in args[0]

    @pytest.mark.asyncio
    async def test_update_with_tags(self):
        """Verify update_collection_in_db updates tags correctly."""
        coll_id = uuid.uuid4()
        data = CollectionUpdateRequest(tags=["new_tag"])

        with patch("utilities.collection_utils.pg_execute") as mock_exec:
            await update_collection_in_db(coll_id, data)

        args = mock_exec.call_args[0]
        assert "tags=%s" in args[0]

    @pytest.mark.asyncio
    async def test_update_query_contains_where_id(self):
        """Verify update_collection_in_db WHERE clause targets collection id."""
        coll_id = uuid.uuid4()
        data = CollectionUpdateRequest(description="updated")

        with patch("utilities.collection_utils.pg_execute") as mock_exec:
            await update_collection_in_db(coll_id, data)

        args = mock_exec.call_args[0]
        assert "WHERE id=%s" in args[0]
        assert coll_id in args[1]


class TestDeleteCollectionFromDb:
    """Tests for delete_collection_from_db."""

    @pytest.mark.asyncio
    async def test_executes_delete(self):
        """Verify delete_collection_from_db executes correct SQL query."""
        coll_id = uuid.uuid4()

        with patch("utilities.collection_utils.pg_execute") as mock_exec:
            await delete_collection_from_db(coll_id)

        args = mock_exec.call_args[0]
        assert "DELETE FROM collection" in args[0]

    @pytest.mark.asyncio
    async def test_delete_passes_collection_id(self):
        """Verify delete_collection_from_db passes the collection ID as parameter."""
        coll_id = uuid.uuid4()

        with patch("utilities.collection_utils.pg_execute") as mock_exec:
            await delete_collection_from_db(coll_id)

        args = mock_exec.call_args[0]
        assert coll_id in args[1]


class TestFetchCollectionByName:
    """Tests for fetch_collection_by_name."""

    @pytest.mark.asyncio
    async def test_returns_row(self):
        """Verify fetch_collection_by_name returns the expected collection row."""
        org_id = uuid.uuid4()
        proj_id = uuid.uuid4()
        row = MagicMock(collection_name="coll")

        with patch("utilities.collection_utils.pg_fetchone", return_value=row):
            result = await fetch_collection_by_name(org_id, proj_id, "coll")

        assert result == row

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self):
        """Verify fetch_collection_by_name returns None when not found."""
        org_id = uuid.uuid4()
        proj_id = uuid.uuid4()

        with patch("utilities.collection_utils.pg_fetchone", return_value=None):
            result = await fetch_collection_by_name(org_id, proj_id, "missing")

        assert result is None


class TestFetchCollectionSchema:
    """Tests for fetch_collection_schema — still uses Cassandra session."""

    @pytest.mark.asyncio
    async def test_exact_case_match(self, mock_cassandra_session):
        """Verify fetch_collection_schema returns schema with exact case match."""
        schema_rows = [
            SchemaRow(column_name="temp", type="decimal"),
            SchemaRow(column_name="day", type="date"),
        ]
        mock_cassandra_session.execute.return_value = schema_rows

        result = await fetch_collection_schema("OrgName", "proj", "coll")

        assert "temp" in result
        mock_cassandra_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_lowercase_fallback(self, mock_cassandra_session):
        """Verify fetch_collection_schema falls back to lowercase keyspace/table names."""
        schema_rows = [
            SchemaRow(column_name="temp", type="decimal"),
        ]
        mock_cassandra_session.execute.side_effect = [[], schema_rows]

        result = await fetch_collection_schema("OrgName", "Proj", "Coll")

        assert "temp" in result
        assert mock_cassandra_session.execute.call_count == 2


class TestFetchAllCollections:
    """Tests for fetch_all_collections."""

    def test_returns_rows(self):
        """Verify fetch_all_collections returns a list of collections."""
        org_id = uuid.uuid4()
        proj_id = uuid.uuid4()
        rows = [MagicMock(collection_name="c1"), MagicMock(collection_name="c2")]

        with patch("utilities.collection_utils.pg_fetchall", return_value=rows):
            result = fetch_all_collections(org_id, proj_id)

        assert result == rows

    def test_returns_empty_list_when_none(self):
        """Verify fetch_all_collections returns empty list when no collections exist."""
        org_id = uuid.uuid4()
        proj_id = uuid.uuid4()

        with patch("utilities.collection_utils.pg_fetchall", return_value=[]):
            result = fetch_all_collections(org_id, proj_id)

        assert result == []


class TestCreateCassandraTable:
    """Tests for create_kafka_topic (Kafka topic creation)."""

    @pytest.mark.asyncio
    async def test_happy_path(self):
        """Verify create_kafka_topic succeeds when Kafka admin client succeeds."""
        mock_future = MagicMock()
        mock_future.result.return_value = None
        mock_admin = MagicMock()
        mock_admin.create_topics.return_value = {"org.proj.coll": mock_future}

        with patch("utilities.collection_utils.get_kafka_admin_client", return_value=mock_admin):
            await create_kafka_topic("org", "proj", "coll")

        mock_admin.create_topics.assert_called_once()

    @pytest.mark.asyncio
    async def test_failure_raises_http_500(self):
        """Verify create_kafka_topic raises HTTP 500 when Kafka admin client fails."""
        mock_future = MagicMock()
        mock_future.result.side_effect = Exception("Kafka error")
        mock_admin = MagicMock()
        mock_admin.create_topics.return_value = {"org.proj.coll": mock_future}

        with (
            patch("utilities.collection_utils.get_kafka_admin_client", return_value=mock_admin),
            pytest.raises(HTTPException) as exc_info,
        ):
            await create_kafka_topic("org", "proj", "coll")

        assert exc_info.value.status_code == 500


class TestDeleteKafkaTopic:
    """Tests for delete_kafka_topic."""

    @pytest.mark.asyncio
    async def test_happy_path(self):
        """Verify delete_kafka_topic succeeds when Kafka admin client succeeds."""
        mock_future = MagicMock()
        mock_future.result.return_value = None
        mock_admin = MagicMock()
        mock_admin.delete_topics.return_value = {"org.proj.coll": mock_future}

        with patch("utilities.collection_utils.get_kafka_admin_client", return_value=mock_admin):
            await delete_kafka_topic("org", "proj", "coll")

        mock_admin.delete_topics.assert_called_once()

    @pytest.mark.asyncio
    async def test_failure_raises_http_500(self):
        """Verify delete_kafka_topic raises HTTP 500 when Kafka admin client fails."""
        mock_future = MagicMock()
        mock_future.result.side_effect = Exception("Kafka error")
        mock_admin = MagicMock()
        mock_admin.delete_topics.return_value = {"org.proj.coll": mock_future}

        with (
            patch("utilities.collection_utils.get_kafka_admin_client", return_value=mock_admin),
            pytest.raises(HTTPException) as exc_info,
        ):
            await delete_kafka_topic("org", "proj", "coll")

        assert exc_info.value.status_code == 500


class TestCheckCollectionExists:
    """Tests for check_collection_exists."""

    def test_found(self):
        """Verify check_collection_exists returns the collection row when found."""
        coll_id = uuid.uuid4()
        proj_id = uuid.uuid4()
        row = MagicMock(id=coll_id)

        with (
            patch("utilities.collection_utils.get_organization_id", return_value=uuid.uuid4()),
            patch("utilities.collection_utils.pg_fetchone", return_value=row),
        ):
            result = check_collection_exists(coll_id, proj_id)

        assert result == row

    def test_not_found_raises_404(self):
        """Verify check_collection_exists raises HTTP 404 when collection is not found."""
        coll_id = uuid.uuid4()
        proj_id = uuid.uuid4()

        with (
            patch("utilities.collection_utils.get_organization_id", return_value=uuid.uuid4()),
            patch("utilities.collection_utils.pg_fetchone", return_value=None),
            pytest.raises(HTTPException) as exc_info,
        ):
            check_collection_exists(coll_id, proj_id)

        assert exc_info.value.status_code == 404


class TestInsertDataIntoTable:
    """Tests for insert_data_into_table — still uses Cassandra session."""

    @pytest.mark.asyncio
    async def test_happy_path_with_timestamp_string(self, mock_cassandra_session):
        """Verify insert_data_into_table succeeds with a valid timestamp string."""
        records = [{"temp": 25.0, "timestamp": "2024-01-01T12:00:00Z"}]

        await insert_data_into_table("org_name", "proj_name", "coll_name", records)

        mock_cassandra_session.execute.assert_called_once()
        query = mock_cassandra_session.execute.call_args[0][0]
        assert "INSERT INTO" in query

    @pytest.mark.asyncio
    async def test_happy_path_without_timestamp(self, mock_cassandra_session):
        """Verify insert_data_into_table succeeds when no timestamp is provided."""
        records = [{"temp": 25.0}]

        await insert_data_into_table("org_name", "proj_name", "coll_name", records)

        mock_cassandra_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_happy_path_with_datetime_timestamp(self, mock_cassandra_session):
        """Verify insert_data_into_table succeeds with a datetime object timestamp."""
        records = [{"temp": 25.0, "timestamp": datetime.datetime(2024, 1, 1, 12, 0, 0)}]

        await insert_data_into_table("org_name", "proj_name", "coll_name", records)

        mock_cassandra_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_happy_path_with_non_datetime_timestamp(self, mock_cassandra_session):
        """Verify insert_data_into_table succeeds with a non-datetime timestamp."""
        records = [{"temp": 25.0, "timestamp": 12345}]

        await insert_data_into_table("org_name", "proj_name", "coll_name", records)

        mock_cassandra_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_timestamp_string_uses_utcnow(self, mock_cassandra_session):
        """Verify insert_data_into_table uses utcnow for invalid timestamp strings."""
        records = [{"temp": 25.0, "timestamp": "not-a-date"}]

        await insert_data_into_table("org_name", "proj_name", "coll_name", records)

        mock_cassandra_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_db_exception_raises_http_500(self, mock_cassandra_session):
        """Verify insert_data_into_table raises HTTP 500 on database error."""
        records = [{"temp": 25.0}]
        mock_cassandra_session.execute.side_effect = Exception("CQL error")

        with pytest.raises(HTTPException) as exc_info:
            await insert_data_into_table("org_name", "proj_name", "coll_name", records)

        assert exc_info.value.status_code == 500

import uuid
from collections import namedtuple
from unittest.mock import MagicMock

import pytest

from utilities.organization_utils import (
    create_keyspace_in_db,
    delete_keyspace_in_db,
    delete_organization_from_db,
    get_all_organizations_from_db,
    get_organization_by_id,
    get_organization_by_name,
    insert_organization,
    update_organization_in_db,
)

OrgRow = namedtuple("OrgRow", ["id", "organization_name", "description", "creation_date", "tags"])


class TestInsertOrganization:
    """Tests for insert_organization."""

    @pytest.mark.asyncio
    async def test_executes_insert(self, mock_cassandra_session):
        """Verify insert_organization executes correct CQL query."""
        org_id = uuid.uuid4()
        data = MagicMock()
        data.organization_name = "TestOrg"
        data.description = "A test org"
        data.tags = ["test"]

        await insert_organization(org_id, data)

        mock_cassandra_session.execute.assert_called_once()
        args = mock_cassandra_session.execute.call_args
        assert "INSERT INTO organization" in args[0][0]
        assert args[0][1] == (org_id, "TestOrg", "A test org", ["test"])


class TestGetOrganizationById:
    """Tests for get_organization_by_id."""

    def test_returns_row(self, mock_cassandra_session):
        """Verify get_organization_by_id returns the expected row."""
        org_id = uuid.uuid4()
        row = OrgRow(
            id=org_id,
            organization_name="TestOrg",
            description="desc",
            creation_date="2024-01-01",
            tags=["test"],
        )
        mock_cassandra_session.execute.return_value = MagicMock(one=MagicMock(return_value=row))

        result = get_organization_by_id(org_id)

        assert result == row
        mock_cassandra_session.execute.assert_called_once()


class TestUpdateOrganizationInDb:
    """Tests for update_organization_in_db."""

    @pytest.mark.asyncio
    async def test_update_with_description(self, mock_cassandra_session):
        """Verify update_organization_in_db updates description correctly."""
        org_id = uuid.uuid4()

        result = await update_organization_in_db(org_id, description="new desc")

        assert result == {"message": "Organization updated successfully"}
        args = mock_cassandra_session.execute.call_args
        assert "description=%s" in args[0][0]
        assert args[0][1] == ("new desc", org_id)

    @pytest.mark.asyncio
    async def test_update_with_tags(self, mock_cassandra_session):
        """Verify update_organization_in_db updates tags correctly."""
        org_id = uuid.uuid4()

        result = await update_organization_in_db(org_id, tags=["tag1", "tag2"])

        assert result == {"message": "Organization updated successfully"}
        args = mock_cassandra_session.execute.call_args
        assert "tags=%s" in args[0][0]
        assert args[0][1] == (["tag1", "tag2"], org_id)

    @pytest.mark.asyncio
    async def test_update_with_both(self, mock_cassandra_session):
        """Verify update_organization_in_db updates both description and tags."""
        org_id = uuid.uuid4()

        result = await update_organization_in_db(org_id, description="desc", tags=["t"])

        assert result == {"message": "Organization updated successfully"}
        args = mock_cassandra_session.execute.call_args
        assert "description=%s" in args[0][0]
        assert "tags=%s" in args[0][0]
        assert args[0][1] == ("desc", ["t"], org_id)

    @pytest.mark.asyncio
    async def test_update_with_neither_raises(self, mock_cassandra_session):
        """Verify update_organization_in_db raises ValueError when no fields provided."""
        del mock_cassandra_session
        org_id = uuid.uuid4()

        with pytest.raises(ValueError, match="No fields to update"):
            await update_organization_in_db(org_id)


class TestDeleteOrganizationFromDb:
    """Tests for delete_organization_from_db."""

    @pytest.mark.asyncio
    async def test_executes_delete(self, mock_cassandra_session):
        """Verify delete_organization_from_db executes correct CQL query."""
        org_id = uuid.uuid4()

        await delete_organization_from_db(org_id)

        mock_cassandra_session.execute.assert_called_once()
        args = mock_cassandra_session.execute.call_args
        assert "DELETE FROM organization" in args[0][0]
        assert args[0][1] == (org_id,)


class TestCreateKeyspaceInDb:
    """Tests for create_keyspace_in_db."""

    @pytest.mark.asyncio
    async def test_executes_create_keyspace(self, mock_cassandra_session):
        """Verify create_keyspace_in_db executes correct CQL query."""
        await create_keyspace_in_db("My Org")

        mock_cassandra_session.execute.assert_called_once()
        query = mock_cassandra_session.execute.call_args[0][0]
        assert "CREATE KEYSPACE IF NOT EXISTS" in query
        assert '"My_Org"' in query

    @pytest.mark.asyncio
    async def test_replaces_spaces_with_underscores(self, mock_cassandra_session):
        """Verify create_keyspace_in_db replaces spaces with underscores in keyspace name."""
        await create_keyspace_in_db("org with spaces")

        query = mock_cassandra_session.execute.call_args[0][0]
        assert '"org_with_spaces"' in query


class TestDeleteKeyspaceInDb:
    """Tests for delete_keyspace_in_db."""

    @pytest.mark.asyncio
    async def test_executes_drop_keyspace(self, mock_cassandra_session):
        """Verify delete_keyspace_in_db executes correct CQL query."""
        await delete_keyspace_in_db("My Org")

        mock_cassandra_session.execute.assert_called_once()
        query = mock_cassandra_session.execute.call_args[0][0]
        assert "DROP KEYSPACE IF EXISTS" in query
        assert '"My_Org"' in query


class TestGetAllOrganizationsFromDb:
    """Tests for get_all_organizations_from_db."""

    @pytest.mark.asyncio
    async def test_returns_list(self, mock_cassandra_session):
        """Verify get_all_organizations_from_db returns a list of organizations."""
        rows = [
            OrgRow(
                id=uuid.uuid4(),
                organization_name="Org1",
                description="d1",
                creation_date="2024-01-01",
                tags=[],
            ),
            OrgRow(
                id=uuid.uuid4(),
                organization_name="Org2",
                description="d2",
                creation_date="2024-01-02",
                tags=[],
            ),
        ]
        mock_cassandra_session.execute.return_value = MagicMock(all=MagicMock(return_value=rows))

        result = await get_all_organizations_from_db()

        assert result == rows
        assert len(result) == 2


class TestGetOrganizationByName:
    """Tests for get_organization_by_name."""

    @pytest.mark.asyncio
    async def test_returns_result(self, mock_cassandra_session):
        """Verify get_organization_by_name returns the expected organization."""
        row = MagicMock(id=uuid.uuid4())
        mock_cassandra_session.execute.return_value = MagicMock(one=MagicMock(return_value=row))

        result = await get_organization_by_name("TestOrg")

        assert result == row
        mock_cassandra_session.execute.assert_called_once()

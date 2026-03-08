import uuid
from collections import namedtuple
from unittest.mock import MagicMock

import pytest

from models.project_models import ProjectCreateRequest, ProjectUpdateRequest
from utilities.project_utils import (
    create_project_in_db,
    delete_project_in_db,
    get_all_organization_projects_from_db,
    get_project_by_id,
    get_project_by_name,
    update_project_in_db,
)

ProjectRow = namedtuple(
    "ProjectRow", ["id", "organization_id", "project_name", "description", "tags", "creation_date"]
)


class TestCreateProjectInDb:
    """Tests for create_project_in_db."""

    @pytest.mark.asyncio
    async def test_returns_project_id(self, mock_cassandra_session):
        """Verify create_project_in_db returns a valid project ID."""
        org_id = uuid.uuid4()
        data = ProjectCreateRequest(
            project_name="my_project",
            description="A project",
            tags=["iot"],
        )

        result = await create_project_in_db(org_id, data)

        assert "project_id" in result
        assert isinstance(result["project_id"], uuid.UUID)
        mock_cassandra_session.execute.assert_called_once()
        args = mock_cassandra_session.execute.call_args
        assert "INSERT INTO project" in args[0][0]


class TestUpdateProjectInDb:
    """Tests for update_project_in_db."""

    @pytest.mark.asyncio
    async def test_update_with_description(self, mock_cassandra_session):
        """Verify update_project_in_db updates description correctly."""
        project_id = uuid.uuid4()
        data = ProjectUpdateRequest(description="new desc", tags=None)

        result = await update_project_in_db(project_id, data)

        assert result is True
        args = mock_cassandra_session.execute.call_args
        assert "description=%s" in args[0][0]

    @pytest.mark.asyncio
    async def test_update_with_tags(self, mock_cassandra_session):
        """Verify update_project_in_db updates tags correctly."""
        project_id = uuid.uuid4()
        data = ProjectUpdateRequest(description=None, tags=["new_tag"])

        result = await update_project_in_db(project_id, data)

        assert result is True
        args = mock_cassandra_session.execute.call_args
        assert "tags=%s" in args[0][0]

    @pytest.mark.asyncio
    async def test_update_with_both(self, mock_cassandra_session):
        """Verify update_project_in_db updates both description and tags."""
        project_id = uuid.uuid4()
        data = ProjectUpdateRequest(description="desc", tags=["t1"])

        result = await update_project_in_db(project_id, data)

        assert result is True
        args = mock_cassandra_session.execute.call_args
        assert "description=%s" in args[0][0]
        assert "tags=%s" in args[0][0]


class TestDeleteProjectInDb:
    """Tests for delete_project_in_db."""

    @pytest.mark.asyncio
    async def test_returns_true(self, mock_cassandra_session):
        """Verify delete_project_in_db executes correct CQL query and returns True."""
        project_id = uuid.uuid4()

        result = await delete_project_in_db(project_id)

        assert result is True
        args = mock_cassandra_session.execute.call_args
        assert "DELETE FROM project" in args[0][0]
        assert args[0][1] == (project_id,)


class TestGetAllOrganizationProjectsFromDb:
    """Tests for get_all_organization_projects_from_db."""

    @pytest.mark.asyncio
    async def test_returns_rows(self, mock_cassandra_session):
        """Verify get_all_organization_projects_from_db returns a list of projects."""
        org_id = uuid.uuid4()
        rows = [
            ProjectRow(
                id=uuid.uuid4(),
                organization_id=org_id,
                project_name="p1",
                description="d1",
                tags=[],
                creation_date="2024-01-01",
            ),
        ]
        mock_cassandra_session.execute.return_value = MagicMock(all=MagicMock(return_value=rows))

        result = await get_all_organization_projects_from_db(org_id)

        assert result == rows


class TestGetProjectById:
    """Tests for get_project_by_id."""

    def test_returns_row(self, mock_cassandra_session):
        """Verify get_project_by_id returns the expected project row."""
        project_id = uuid.uuid4()
        org_id = uuid.uuid4()
        row = ProjectRow(
            id=project_id,
            organization_id=org_id,
            project_name="proj",
            description="d",
            tags=[],
            creation_date="2024-01-01",
        )
        mock_cassandra_session.execute.return_value = MagicMock(one=MagicMock(return_value=row))

        result = get_project_by_id(project_id, org_id)

        assert result == row


class TestGetProjectByName:
    """Tests for get_project_by_name."""

    @pytest.mark.asyncio
    async def test_returns_row_when_found(self, mock_cassandra_session):
        """Verify get_project_by_name returns the project row when found."""
        org_id = uuid.uuid4()
        row = MagicMock(id=uuid.uuid4())
        # session.execute returns an iterable; the code does list(result)
        mock_cassandra_session.execute.return_value = [row]

        result = await get_project_by_name(org_id, "my_project")

        assert result == row

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, mock_cassandra_session):
        """Verify get_project_by_name returns None when project is not found."""
        org_id = uuid.uuid4()
        mock_cassandra_session.execute.return_value = []

        result = await get_project_by_name(org_id, "nonexistent")

        assert result is None

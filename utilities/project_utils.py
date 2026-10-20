"""Project database utility functions.

This module provides utility functions for CRUD operations on projects
in the PostgreSQL database.
"""

import uuid
from typing import Any

from models.project_models import ProjectCreateRequest, ProjectUpdateRequest
from utilities.postgres_connector import pg_execute, pg_fetchall, pg_fetchone


async def create_project_in_db(organization_id: uuid.UUID, project_data: ProjectCreateRequest):
    """Create a new project in the database.

    Args:
        organization_id: UUID of the owning organization.
        project_data: ProjectCreateRequest with project details.

    Returns:
        Dictionary containing the new project_id.
    """

    project_id = uuid.uuid4()
    query = """
    INSERT INTO project (id, organization_id, project_name, description, tags, creation_date)
    VALUES (%s, %s, %s, %s, %s, NOW())
    """
    pg_execute(
        query,
        (
            project_id,
            organization_id,
            project_data.project_name,
            project_data.description,
            project_data.tags,
        ),
    )

    return {"project_id": project_id}


async def update_project_in_db(project_id: uuid.UUID, project_data: ProjectUpdateRequest):
    """Update project details in the database.

    Args:
        project_id: UUID of the project to update.
        project_data: ProjectUpdateRequest with updated fields.

    Returns:
        True if successful.
    """

    update_query = "UPDATE project SET "
    update_params: list[Any] = []

    if project_data.description is not None:
        update_query += "description=%s, "
        update_params.append(project_data.description)

    if project_data.tags is not None:
        update_query += "tags=%s, "
        update_params.append(project_data.tags)

    update_query = update_query.rstrip(", ") + " WHERE id=%s"
    update_params.extend([project_id])

    pg_execute(update_query, tuple(update_params))
    return True


async def delete_project_in_db(project_id: uuid.UUID):
    """Delete a project from the database.

    Args:
        project_id: UUID of the project to delete.

    Returns:
        True if successful.
    """

    pg_execute("DELETE FROM project WHERE id=%s", (project_id,))
    return True


async def get_all_organization_projects_from_db(organization_id: uuid.UUID):
    """Retrieve all projects for an organization.

    Args:
        organization_id: UUID of the organization.

    Returns:
        List of project records.
    """

    return pg_fetchall(
        "SELECT id, project_name, description, tags, creation_date, organization_id "
        "FROM project WHERE organization_id=%s",
        (organization_id,),
    )


def get_project_by_id(project_id: uuid.UUID, organization_id: uuid.UUID):
    """Retrieve a project by its ID and organization ID.

    Args:
        project_id: UUID of the project.
        organization_id: UUID of the owning organization.

    Returns:
        Project record from database.
    """

    return pg_fetchone(
        "SELECT id, project_name, description, tags, creation_date, "
        "organization_id FROM project WHERE id=%s AND organization_id=%s",
        (project_id, organization_id),
    )


async def get_project_by_name(organization_id: uuid.UUID, project_name: str):
    """Retrieve a project by its name within an organization.

    Args:
        organization_id: UUID of the organization.
        project_name: Name of the project to find.

    Returns:
        Project record if found, None otherwise.
    """
    return pg_fetchone(
        "SELECT id FROM project WHERE organization_id=%s AND project_name=%s",
        (organization_id, project_name),
    )

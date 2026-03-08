"""Project management endpoints.

This module provides API endpoints for creating, updating, deleting,
and retrieving projects within organizations.
"""

import uuid

from fastapi import APIRouter, Depends, Query

from dependencies import (
    check_project_exists,
    get_organization_id,
    verify_endpoint_access,
    verify_user_belongs_to_organization,
)
from models.common import PaginatedResponse
from models.project_models import ProjectCreateRequest, ProjectResponse, ProjectUpdateRequest
from services.project_service import (
    create_project_service,
    delete_project_service,
    get_all_projects_service,
    get_project_by_id_service,
    update_project_service,
)

router = APIRouter()
TAG = "Project Management"


@router.post("/projects", tags=[TAG], dependencies=[Depends(verify_user_belongs_to_organization)])
async def create_project(project_data: ProjectCreateRequest):
    """Create a new project in the organization.

    Args:
        project_data: Project creation request with name, description, and tags.

    Returns:
        Created project information.
    """
    organization_id = get_organization_id()
    return await create_project_service(organization_id, project_data)


@router.put(
    "/projects/{project_id}",
    tags=[TAG],
    dependencies=[Depends(verify_user_belongs_to_organization), Depends(check_project_exists)],
)
async def update_project(
    project_id: uuid.UUID,
    project_data: ProjectUpdateRequest,
):
    """Update an existing project.

    Args:
        project_id: UUID of the project to update.
        project_data: Project update request with new description and/or tags.

    Returns:
        Success message.
    """
    await update_project_service(project_id, project_data)
    return {"message": "Project successfully updated"}


@router.delete(
    "/projects/{project_id}",
    tags=[TAG],
    dependencies=[Depends(verify_user_belongs_to_organization), Depends(check_project_exists)],
)
async def delete_project(
    project_id: uuid.UUID,
):
    """Delete a project.

    Args:
        project_id: UUID of the project to delete.

    Returns:
        Success message.
    """
    await delete_project_service(project_id)
    return {"message": "Project successfully deleted"}


@router.get("/projects", tags=[TAG], dependencies=[Depends(verify_user_belongs_to_organization)])
async def get_all_projects(
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
):
    """Retrieve all projects for the current organization with pagination."""
    organization_id = get_organization_id()
    all_projects = await get_all_projects_service(organization_id)
    page = all_projects[offset : offset + limit]
    return PaginatedResponse(items=page, total=len(all_projects), offset=offset, limit=limit)


@router.get(
    "/projects/{project_id}",
    tags=[TAG],
    response_model=ProjectResponse,
    dependencies=[Depends(verify_endpoint_access)],
)
async def get_project_by_id(project=Depends(check_project_exists)):
    """Retrieve a specific project by its ID.

    Args:
        project: The project object from dependency.

    Returns:
        Project information.
    """
    return await get_project_by_id_service(project)

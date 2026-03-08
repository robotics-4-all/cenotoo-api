import uuid

from fastapi import HTTPException

from dependencies import contains_special_characters, get_organization_by_id
from models.project_models import ProjectCreateRequest, ProjectResponse, ProjectUpdateRequest
from utilities.project_utils import (
    create_project_in_db,
    delete_project_in_db,
    get_all_organization_projects_from_db,
    get_project_by_name,
    update_project_in_db,
)


async def create_project_service(organization_id: uuid.UUID, project_data: ProjectCreateRequest):
    """Create a new project after validating its name."""
    # Create the project and return its data in response format
    # For project names, keep strict validation (no special characters except
    # underscore)
    if contains_special_characters(
        project_data.project_name,
        allow_spaces=False,
        allow_underscores=True,
        allow_special_chars=False,
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid name format. Names can only contain latin letters, "
                "numbers and underscores."
            ),
        )

    # Check if project with same name exists
    existing_project = await get_project_by_name(organization_id, project_data.project_name)
    if existing_project:
        raise HTTPException(status_code=409, detail="Project with this name already exists")

    project = await create_project_in_db(organization_id, project_data)
    return {"message": f"Project {project_data.project_name} created succesfully", "id": project}


async def update_project_service(project_id: uuid.UUID, project_data: ProjectUpdateRequest):
    """Update an existing project."""
    await update_project_in_db(project_id, project_data)


async def delete_project_service(project_id: uuid.UUID):
    """Delete a project."""
    await delete_project_in_db(project_id)


async def get_all_projects_service(organization_id: uuid.UUID) -> list[ProjectResponse]:
    """Retrieve all projects for an organization."""
    rows = await get_all_organization_projects_from_db(organization_id)
    return [
        ProjectResponse(
            project_id=row.id,
            organization_id=row.organization_id,
            organization_name=get_organization_by_id(row.organization_id).organization_name,
            project_name=row.project_name,
            description=row.description,
            tags=row.tags if row.tags else [],
            creation_date=str(row.creation_date),
        )
        for row in rows
    ]


async def get_project_by_id_service(row) -> ProjectResponse:
    """Format a project database row into a response model."""
    return ProjectResponse(
        project_id=row.id,
        organization_id=row.organization_id,
        organization_name=get_organization_by_id(row.organization_id).organization_name,
        project_name=row.project_name,
        description=row.description,
        tags=row.tags if row.tags else [],
        creation_date=str(row.creation_date),
    )

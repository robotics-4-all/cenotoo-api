"""Project API key management endpoints.

This module provides API endpoints for creating, retrieving, regenerating,
and deleting project API keys.
"""

import uuid

from fastapi import APIRouter, Depends, Query

from dependencies import check_project_exists, verify_user_belongs_to_organization
from models.project_keys_models import (
    DeleteKeyRequest,
    ProjectKeyCreateRequest,
    ProjectKeyResponse,
    RegenerateKeyRequest,
)
from services.project_keys_service import (
    create_project_key_service,
    delete_key_by_value_service,
    delete_keys_by_category_service,
    fetch_project_keys_by_category_service,
    regenerate_key_service,
)

router = APIRouter(
    dependencies=[Depends(check_project_exists), Depends(verify_user_belongs_to_organization)]
)
TAG = "Project Key Management"

# Create a new project key


@router.post("/projects/{project_id}/keys", tags=[TAG], response_model=ProjectKeyResponse)
async def create_project_key(
    project_id: uuid.UUID,
    key_data: ProjectKeyCreateRequest,
):
    """Create a new API key for a project.

    Args:
        project_id: UUID of the project.
        key_data: Request containing the key type.

    Returns:
        The created API key information.
    """
    return await create_project_key_service(project_id, key_data)


# Fetch project keys by category or all


@router.get("/projects/{project_id}/keys", tags=[TAG], response_model=list[ProjectKeyResponse])
async def fetch_project_keys_by_category(
    project_id: uuid.UUID,
    key_category: str | None = Query(None, enum=["read", "write", "master", "all"]),
):
    """Retrieve project API keys by category or all keys.

    Args:
        project_id: UUID of the project.
        key_category: Category of keys to fetch ('read', 'write', 'master', or 'all').

    Returns:
        List of API key information.
    """
    return await fetch_project_keys_by_category_service(project_id, key_category or "all")


# Regenerate a project key by providing the key value in the body


@router.put("/projects/{project_id}/keys/regenerate", tags=[TAG], response_model=ProjectKeyResponse)
async def regenerate_project_key(
    project_id: uuid.UUID,
    key_data: RegenerateKeyRequest,
):
    """Regenerate an existing project API key.

    Args:
        project_id: UUID of the project.
        key_data: Model containing the key value to regenerate.

    Returns:
        The newly generated API key information.
    """
    return await regenerate_key_service(project_id, key_data)


# Delete keys by category (query parameter)


@router.delete("/projects/{project_id}/keys/delete_keys", tags=[TAG])
async def delete_keys_by_category(
    project_id: uuid.UUID,
    key_category: str = Query(..., enum=["read", "write", "master", "all"]),
):
    """Delete all project API keys of a specific category.

    Args:
        project_id: UUID of the project.
        key_category: Category of keys to delete.

    Returns:
        Success message.
    """
    return await delete_keys_by_category_service(project_id, key_category)


# Delete a key by providing the key value in the body


@router.delete("/projects/{project_id}/keys/delete_key", tags=[TAG])
async def delete_key_by_value(
    project_id: uuid.UUID,
    key_data: DeleteKeyRequest,
):
    """Delete a specific project API key.

    Args:
        project_id: UUID of the project.
        key_data: Model containing the key value to delete.

    Returns:
        Success message.
    """
    return await delete_key_by_value_service(project_id, key_data)

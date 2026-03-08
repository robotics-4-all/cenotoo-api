"""Project API key service layer.

This module provides business logic for managing project API keys,
including creation, regeneration, and deletion operations.
"""

from datetime import datetime
from uuid import UUID

from fastapi import HTTPException

from models.project_keys_models import (
    DeleteKeyRequest,
    ProjectKeyCreateRequest,
    ProjectKeyResponse,
    RegenerateKeyRequest,
)
from utilities.project_keys_utils import (
    delete_key_by_value,
    delete_keys_by_category,
    fetch_project_keys_by_category,
    get_project_key_by_value,
    insert_project_key,
    update_project_key,
)

# Create a new project key


async def create_project_key_service(
    project_id: UUID, key_data: ProjectKeyCreateRequest
) -> ProjectKeyResponse:
    """Create a new project API key.

    Args:
        project_id: UUID of the project.
        key_data: Request containing the key type.

    Returns:
        ProjectKeyResponse with the created key information.
    """
    key_value = insert_project_key(project_id, key_data.key_type)
    return ProjectKeyResponse(
        api_key=key_value,
        project_id=project_id,
        key_type=key_data.key_type,
        created_at=datetime.utcnow().isoformat(),
    )


# Fetch project keys by category or all keys


async def fetch_project_keys_by_category_service(project_id: UUID, key_category: str):
    """Retrieve project API keys by category.

    Args:
        project_id: UUID of the project.
        key_category: Category of keys to fetch ('all', 'read', 'write', 'master').

    Returns:
        List of ProjectKeyResponse objects.

    Raises:
        HTTPException: If no keys found.
    """
    project_keys = fetch_project_keys_by_category(project_id, key_category)

    if not project_keys:
        raise HTTPException(status_code=404, detail="No keys found for this project.")

    return [
        ProjectKeyResponse(
            api_key=key.api_key,
            project_id=key.project_id,
            key_type=key.key_type,
            created_at=key.created_at.isoformat(),
        )
        for key in project_keys
    ]


# Regenerate a project key by providing the key value


async def regenerate_key_service(
    project_id: UUID, key_data: RegenerateKeyRequest
) -> ProjectKeyResponse:
    """Regenerate an existing project API key.

    Args:
        project_id: UUID of the project.
        key_data: Request containing the key value to regenerate.

    Returns:
        ProjectKeyResponse with the new key information.

    Raises:
        HTTPException: If key not found.
    """
    existing_key = get_project_key_by_value(key_data.key_value, project_id)

    if not existing_key:
        raise HTTPException(
            status_code=404, detail="Key not found or doesn't belong to this project."
        )

    new_key_value = update_project_key(existing_key.id, key_data.key_value)
    return ProjectKeyResponse(
        api_key=new_key_value,
        project_id=project_id,
        key_type=existing_key.key_type,
        created_at=datetime.utcnow().isoformat(),
    )


# Delete keys by category


async def delete_keys_by_category_service(project_id: UUID, key_category: str):
    """Delete all project API keys of a specific category.

    Args:
        project_id: UUID of the project.
        key_category: Category of keys to delete.

    Returns:
        Success message dictionary.
    """
    return delete_keys_by_category(project_id, key_category)


# Delete a key by value


async def delete_key_by_value_service(project_id: UUID, key_data: DeleteKeyRequest):
    """Delete a specific project API key.

    Args:
        project_id: UUID of the project.
        key_data: Request containing the key value to delete.

    Raises:
        HTTPException: If key not found.
    """
    existing_key = get_project_key_by_value(key_data.key_value, project_id)

    if not existing_key:
        raise HTTPException(
            status_code=404, detail="Key not found or doesn't belong to this project."
        )

    delete_key_by_value(existing_key.id)

    return {"message": "Key deleted successfully."}

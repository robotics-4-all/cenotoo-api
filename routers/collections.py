"""Collection management endpoints.

This module provides API endpoints for creating, updating, deleting,
and retrieving collections within projects.
"""

import uuid

from fastapi import APIRouter, Depends, Query

from dependencies import (
    check_project_exists,
    get_organization_id,
    verify_endpoint_access,
    verify_master_access,
)
from models.collection_models import (
    CollectionCreateRequest,
    CollectionResponse,
    CollectionUpdateRequest,
)
from models.common import PaginatedResponse
from services.collection_service import (
    create_collection_service,
    delete_collection_service,
    get_all_collections_service,
    get_collection_info_service,
    update_collection_service,
)
from utilities.collection_utils import check_collection_exists

router = APIRouter(dependencies=[Depends(check_project_exists)])
TAG = "Collection Management"

# Create a new collection


@router.post(
    "/projects/{project_id}/collections", tags=[TAG], dependencies=[Depends(verify_master_access)]
)
async def create_collection(
    project_id: uuid.UUID,
    collection_data: CollectionCreateRequest,
):
    """Create a new collection in a project.

    Args:
        project_id: UUID of the project.
        collection_data: Collection creation request with name, description, tags, and schema.

    Returns:
        Created collection information.
    """
    organization_id = get_organization_id()
    return await create_collection_service(organization_id, project_id, collection_data)


# Update an existing collection


@router.put(
    "/projects/{project_id}/collections/{collection_id}",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_master_access)],
)
async def update_collection(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    collection_data: CollectionUpdateRequest,
):
    """Update an existing collection.

    Args:
        project_id: UUID of the project.
        collection_id: UUID of the collection to update.
        collection_data: Collection update request with new description and/or tags.

    Returns:
        Updated collection information.
    """
    organization_id = get_organization_id()
    return await update_collection_service(
        organization_id, project_id, collection_id, collection_data
    )


# Delete a collection


@router.delete(
    "/projects/{project_id}/collections/{collection_id}",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_master_access)],
)
async def delete_collection(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
):
    """Delete a collection.

    Args:
        project_id: UUID of the project.
        collection_id: UUID of the collection to delete.

    Returns:
        Success message.
    """
    organization_id = get_organization_id()
    return await delete_collection_service(organization_id, project_id, collection_id)


# Get all collections of a project


@router.get(
    "/projects/{project_id}/collections",
    tags=[TAG],
    dependencies=[Depends(verify_endpoint_access)],
)
async def get_all_collections(
    project_id: uuid.UUID,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
):
    """Retrieve all collections in a project.

    Args:
        project_id: UUID of the project.

    Returns:
        List of all collections in the project.
    """
    organization_id = get_organization_id()
    all_items = await get_all_collections_service(organization_id, project_id)
    page = all_items[offset : offset + limit]
    return PaginatedResponse(items=page, total=len(all_items), offset=offset, limit=limit)


# Get information for a specific collection


@router.get(
    "/projects/{project_id}/collections/{collection_id}",
    tags=[TAG],
    response_model=CollectionResponse,
    dependencies=[Depends(check_collection_exists), Depends(verify_endpoint_access)],
)
async def get_collection_info(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
):
    """Retrieve information for a specific collection.

    Args:
        project_id: UUID of the project.
        collection_id: UUID of the collection.

    Returns:
        Collection information.
    """
    organization_id = get_organization_id()
    return await get_collection_info_service(organization_id, project_id, collection_id)

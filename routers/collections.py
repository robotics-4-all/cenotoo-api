"""Collection management endpoints.

This module provides API endpoints for creating, updating, deleting,
and retrieving collections within projects.
"""

import datetime
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query

from core.validators import validate_cql_identifier
from dependencies import (
    check_project_exists,
    get_organization_id,
    verify_endpoint_access,
    verify_master_access,
)
from models.collection_models import (
    CollectionCreateRequest,
    CollectionMetricsResponse,
    CollectionResponse,
    CollectionUpdateRequest,
    SchemaEvolutionRequest,
)
from models.common import PaginatedResponse
from services.collection_service import (
    create_collection_service,
    delete_collection_service,
    get_all_collections_service,
    get_collection_info_service,
    update_collection_service,
)
from utilities.cassandra_connector import get_cassandra_session
from utilities.collection_utils import (
    alter_cassandra_table_add_columns,
    alter_cassandra_table_drop_columns,
    check_collection_exists,
    fetch_collection_schema,
    get_collection_by_id,
)
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id

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


@router.patch(
    "/projects/{project_id}/collections/{collection_id}/schema",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_master_access)],
)
async def evolve_collection_schema(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    data: SchemaEvolutionRequest,
):
    if not data.add_fields and not data.remove_fields:
        raise HTTPException(
            status_code=400, detail="At least one of add_fields or remove_fields must be provided."
        )
    organization_id = get_organization_id()
    organization_name = get_organization_by_id(organization_id).organization_name
    collection = get_collection_by_id(collection_id, project_id, organization_id)
    project_name = get_project_by_id(project_id, organization_id).project_name
    collection_name = collection.collection_name

    added: list[str] = []
    removed: list[str] = []

    if data.add_fields:
        added = await alter_cassandra_table_add_columns(
            organization_name, project_name, collection_name, data.add_fields
        )
    if data.remove_fields:
        removed = await alter_cassandra_table_drop_columns(
            organization_name, project_name, collection_name, data.remove_fields
        )

    parts = []
    if added:
        parts.append(f"Added {len(added)} field(s)")
    if removed:
        parts.append(f"Removed {len(removed)} field(s)")
    return {"message": "; ".join(parts) + ".", "added": added, "removed": removed}


@router.get(
    "/projects/{project_id}/collections/{collection_id}/metrics",
    tags=[TAG],
    response_model=CollectionMetricsResponse,
    dependencies=[Depends(check_collection_exists), Depends(verify_endpoint_access)],
)
async def get_collection_metrics(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
):
    organization_id = get_organization_id()
    organization_name = get_organization_by_id(organization_id).organization_name
    project_name = get_project_by_id(project_id, organization_id).project_name
    collection = get_collection_by_id(collection_id, project_id, organization_id)
    collection_name = collection.collection_name

    kafka_topic = f"{organization_name}.{project_name}.{collection_name}"
    schema = await fetch_collection_schema(organization_name, project_name, collection_name)

    validate_cql_identifier(organization_name, "keyspace")
    validate_cql_identifier(project_name, "project")
    validate_cql_identifier(collection_name, "collection")

    keyspace_name = f'"{organization_name}"'
    table_name = f'"{project_name}_{collection_name}"'

    session = get_cassandra_session()

    record_count = None
    try:
        count_query = (
            f"SELECT COUNT(*) FROM {keyspace_name}.{table_name} LIMIT 100000 ALLOW FILTERING"
        )
        row = session.execute(count_query).one()
        if row:
            record_count = row.count
    except Exception:
        pass

    last_ingested_at = None
    try:
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)

        for day in (today, yesterday):
            query = f"SELECT timestamp FROM {keyspace_name}.{table_name} WHERE day=%s LIMIT 1 ALLOW FILTERING"
            row = session.execute(query, (day,)).one()
            if row and row.timestamp:
                last_ingested_at = row.timestamp.isoformat()
                break
    except Exception:
        pass

    return {
        "collection_id": collection_id,
        "collection_name": collection_name,
        "project_id": project_id,
        "kafka_topic": kafka_topic,
        "schema_fields": schema,
        "record_count": record_count,
        "record_count_limit": 100000,
        "last_ingested_at": last_ingested_at,
    }

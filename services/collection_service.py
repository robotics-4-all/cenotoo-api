import uuid

import docker
from fastapi import HTTPException

from config import settings
from dependencies import contains_special_characters
from models.collection_models import (
    CollectionCreateRequest,
    CollectionResponse,
    CollectionUpdateRequest,
)
from utilities.collection_utils import (
    create_cassandra_table,
    create_kafka_topic,
    delete_cassandra_table,
    delete_collection_from_db,
    delete_kafka_topic,
    fetch_all_collections,
    fetch_collection_by_name,
    fetch_collection_schema,
    get_collection_by_id,
    insert_collection,
    update_collection_in_db,
)
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id

docker_client = docker.from_env()


async def create_collection_service(
    organization_id: uuid.UUID, project_id: uuid.UUID, data: CollectionCreateRequest
):
    """Create a new collection and its associated infrastructure."""
    # For collection names, keep strict validation (no special characters
    # except underscore)
    if contains_special_characters(
        data.name, allow_spaces=False, allow_underscores=True, allow_special_chars=False
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid collection name format."
                " Collection names can only contain latin letters, numbers, and underscores."
            ),
        )
    if await fetch_collection_by_name(organization_id, project_id, data.name):
        raise HTTPException(status_code=409, detail="Collection already exists")
    organization_name = get_organization_by_id(organization_id).organization_name
    project_name = get_project_by_id(project_id, organization_id).project_name
    await create_cassandra_table(organization_name, project_name, data)
    await create_kafka_topic(organization_name, project_name, data.name)
    try:
        container_name = f"{organization_name}_{project_name}_{data.name}_consumer"
        docker_client.containers.run(
            "kafka-to-cassandra",  # Updated to use the new image name
            name=container_name,
            environment={
                "KAFKA_BOOTSTRAP_SERVERS": settings.kafka_brokers,
                "CASSANDRA_CONTACT_POINTS": settings.cassandra_contact_points,
                "CASSANDRA_PORT": settings.cassandra_port,
                "COLLECTION_NAME": data.name,  # Just the collection name: test214
                "PROJECT_NAME": project_name,  # Just the project name: test_special
                # Just the organization name: test_nostradamus
                "ORGANIZATION_NAME": organization_name,
            },
            detach=True,
            restart_policy={"Name": "always"},
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to start consumer container: {str(e)}"
        ) from e
    collection_id = await insert_collection(organization_id, project_id, data)
    return {"message": f"Collection {data.name} created successfully with ID {collection_id}"}


async def update_collection_service(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    data: CollectionUpdateRequest,
):
    """Update an existing collection's metadata."""
    del organization_id, project_id
    await update_collection_in_db(collection_id, data)
    return {"message": "Collection updated successfully"}


async def delete_collection_service(
    organization_id: uuid.UUID, project_id: uuid.UUID, collection_id: uuid.UUID
):
    """Delete a collection and its associated infrastructure."""
    # Get names needed for resource cleanup
    organization = get_organization_by_id(organization_id)
    project = get_project_by_id(project_id, organization_id)
    collection = get_collection_by_id(collection_id, project_id, organization_id)

    organization_name = organization.organization_name
    project_name = project.project_name
    collection_name = collection.collection_name

    # Stop and remove the consumer container
    try:
        container_name = f"{organization_name}_{project_name}_{collection_name}_consumer"
        container = docker_client.containers.get(container_name)
        container.stop()
        container.remove()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to stop consumer container: {str(e)}"
        ) from e

    # Delete associated resources
    await delete_cassandra_table(organization_id, project_id, collection_id)
    await delete_kafka_topic(organization_name, project_name, collection_name)
    await delete_collection_from_db(collection_id)

    return {"message": "Collection deleted successfully"}


async def get_all_collections_service(organization_id: uuid.UUID, project_id: uuid.UUID):
    """Retrieve all collections for a given project."""
    collections = fetch_all_collections(organization_id, project_id)
    return [
        CollectionResponse(
            collection_id=row.id,
            collection_name=row.collection_name,
            project_id=row.project_id,
            project_name=get_project_by_id(project_id, organization_id).project_name,
            organization_id=row.organization_id,
            organization_name=get_organization_by_id(organization_id).organization_name,
            description=row.description,
            tags=row.tags if row.tags else [],
            creation_date=str(row.creation_date),
            collection_schema=await fetch_collection_schema(
                get_organization_by_id(organization_id).organization_name,
                get_project_by_id(project_id, organization_id).project_name,
                row.collection_name,
            ),
        )
        for row in collections
    ]


async def get_collection_info_service(
    organization_id: uuid.UUID, project_id: uuid.UUID, collection_id: uuid.UUID
):
    """Retrieve detailed information for a specific collection."""
    row = get_collection_by_id(collection_id, project_id, organization_id)
    return CollectionResponse(
        collection_id=row.id,
        collection_name=row.collection_name,
        project_id=row.project_id,
        project_name=get_project_by_id(project_id, organization_id).project_name,
        organization_id=row.organization_id,
        organization_name=get_organization_by_id(organization_id).organization_name,
        description=row.description,
        tags=row.tags if row.tags else [],
        creation_date=str(row.creation_date),
        collection_schema=await fetch_collection_schema(
            get_organization_by_id(
                organization_id,
            ).organization_name,
            get_project_by_id(project_id, organization_id).project_name,
            row.collection_name,
        ),
    )

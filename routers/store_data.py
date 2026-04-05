import logging
import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Body, Depends, HTTPException

from dependencies import (
    check_project_exists,
    get_organization_id,
    verify_write_access,
)
from routers.send_data import validate_message_against_simple_schema
from utilities.collection_utils import (
    check_collection_exists,
    fetch_collection_schema,
    get_collection_by_id,
    insert_data_into_table,
)
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id

logger = logging.getLogger(__name__)
router = APIRouter(dependencies=[Depends(check_project_exists)])
TAG = "Store Data"


@router.post(
    "/projects/{project_id}/collections/{collection_id}/store_data",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_write_access)],
)
async def store_data_to_collection(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    data: dict | list[dict] = Body(...),
):
    """Validate and write data directly to Cassandra (no Kafka)."""
    organization_id = get_organization_id()
    organization_name = get_organization_by_id(organization_id).organization_name
    project_name = get_project_by_id(project_id, organization_id).project_name
    collection = get_collection_by_id(collection_id, project_id, organization_id)
    collection_name = collection.collection_name

    # Get the collection's schema
    schema = await fetch_collection_schema(organization_name, project_name, collection_name)
    logger.debug("Schema for collection '%s': %s", collection_name, schema)

    if not schema:
        raise HTTPException(
            status_code=400, detail=f"Collection '{collection_name}' does not have a defined schema"
        )

    # Format data as a list of messages
    messages = [data] if isinstance(data, dict) else data

    valid_messages = []
    invalid_messages = []

    for i, original_message in enumerate(messages):
        # Create a copy to avoid modifying the original
        message = original_message.copy()

        # STEP 1: Validate original message structure against schema (before
        # adding auto-fields)
        is_valid, error_msg = validate_message_against_simple_schema(original_message, schema)

        if not is_valid:
            invalid_messages.append(
                {
                    "message_index": i,
                    "original_message": original_message,
                    "error": error_msg,
                }
            )
            continue

        # STEP 2: Add required auto-generated fields if they don't exist
        if "key" not in message:
            message["key"] = "key"

        if "timestamp" not in message:
            message["timestamp"] = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            logger.debug("Added missing 'timestamp' field to message %d", i)
        # Convert the 'timestamp' string to a datetime object and extract 'day'
        try:
            timestamp_dt = datetime.fromisoformat(message["timestamp"].replace("Z", "+00:00"))
            message["day"] = timestamp_dt.strftime("%Y-%m-%d")
        except ValueError:
            invalid_messages.append(
                {
                    "message_index": i,
                    "original_message": original_message,
                    "error": f"Invalid timestamp format: {message.get('timestamp', 'missing')}",
                }
            )
            continue

        # STEP 3: Final validation with all fields
        final_is_valid, final_error = validate_message_against_simple_schema(message, schema)

        if final_is_valid:
            message["id"] = str(uuid.uuid4())
            valid_messages.append(message)
        else:
            invalid_messages.append(
                {
                    "message_index": i,
                    "original_message": original_message,
                    "processed_message": message,
                    "error": final_error,
                }
            )

    logger.debug("Valid messages: %s", valid_messages)
    logger.debug("Invalid messages: %s", invalid_messages)

    # If there are invalid messages, return detailed error
    if invalid_messages:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Some messages failed schema validation",
                "total_messages": len(messages),
                "valid_count": len(valid_messages),
                "invalid_count": len(invalid_messages),
                "invalid_messages": invalid_messages,
                "schema": schema,
            },
        )

    await insert_data_into_table(organization_name, project_name, collection_name, valid_messages)

    return {
        "message": f"Data stored to collection '{collection_name}' successfully.",
        "stored_count": len(valid_messages),
    }

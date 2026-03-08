import json
import logging
import uuid
from datetime import datetime

from fastapi import APIRouter, Body, Depends, HTTPException

from dependencies import (
    check_project_exists,
    get_organization_id,
    verify_write_access,
)
from utilities.collection_utils import (
    check_collection_exists,
    fetch_collection_schema,
    get_collection_by_id,
)
from utilities.kafka_connector import get_kafka_producer
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id

logger = logging.getLogger(__name__)
flush_threshold = 1
router = APIRouter(dependencies=[Depends(check_project_exists)])
TAG = "Send Data"


def convert_simple_schema_to_jsonschema(simple_schema: dict[str, str]) -> dict:
    """
    Convert simplified schema format to proper JSON Schema format
    """
    type_mapping = {
        "text": "string",
        "int": "integer",
        "float": "number",
        "bool": "boolean",
        "date": "string",  # Will add format validation
        "timestamp": "string",  # Will add format validation
    }

    properties = {}
    required_fields = []

    for field_name, field_type in simple_schema.items():
        json_type = type_mapping.get(field_type, "string")

        property_def = {"type": json_type}

        # Add format validation for special types
        if field_type == "date":
            property_def["format"] = "date"
        elif field_type == "timestamp":
            property_def["pattern"] = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z?$"

        properties[field_name] = property_def
        required_fields.append(field_name)

    return {
        "type": "object",
        "properties": properties,
        "required": required_fields,
        "additionalProperties": False,  # Don't allow extra fields
    }


def validate_message_against_simple_schema(
    message: dict, simple_schema: dict[str, str]
) -> tuple[bool, str]:
    """
    Validate message against simplified schema format
    """
    errors = []

    # Check for required fields (excluding auto-generated ones)
    auto_generated_fields = {"key", "timestamp", "day"}
    required_fields = set(simple_schema.keys()) - auto_generated_fields
    message_fields = set(message.keys()) - auto_generated_fields

    # Check for missing required fields
    missing_fields = required_fields - message_fields
    if missing_fields:
        errors.append(f"Missing required fields: {', '.join(missing_fields)}")

    # Check for extra fields
    extra_fields = message_fields - set(simple_schema.keys())
    if extra_fields:
        errors.append(f"Extra fields not allowed: {', '.join(extra_fields)}")

    # Validate field types, allow None (null) values for all fields except
    # 'key' and 'timestamp'
    for field_name, expected_type in simple_schema.items():
        if field_name in message:
            value = message[field_name]
            if value is None:
                if field_name in ("key", "timestamp"):
                    errors.append(f"Field '{field_name}' cannot be null")
                    continue
                continue  # Accept null (None) values for other fields
            if expected_type == "int" and not isinstance(value, int):
                errors.append(f"Field '{field_name}' should be integer, got {type(value).__name__}")
            elif expected_type == "float" and not isinstance(value, (int, float)):
                errors.append(f"Field '{field_name}' should be number, got {type(value).__name__}")
            elif expected_type == "text" and not isinstance(value, str):
                errors.append(f"Field '{field_name}' should be string, got {type(value).__name__}")
            elif expected_type == "bool" and not isinstance(value, bool):
                errors.append(f"Field '{field_name}' should be boolean, got {type(value).__name__}")
            elif expected_type == "date" and isinstance(value, str):
                try:
                    datetime.strptime(value, "%Y-%m-%d")
                except ValueError:
                    errors.append(f"Field '{field_name}' should be valid date format (YYYY-MM-DD)")
            elif expected_type == "timestamp" and isinstance(value, str):
                try:
                    datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError:
                    errors.append(
                        f"Field '{field_name}' should be valid timestamp format (ISO 8601)"
                    )
    return len(errors) == 0, "; ".join(errors)


@router.post(
    "/projects/{project_id}/collections/{collection_id}/send_data",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_write_access)],
)
async def send_data_to_collection(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    data: dict | list[dict] = Body(...),
):
    """Validate and send data to a collection's Kafka topic."""
    kafka_producer = get_kafka_producer()

    organization_id = get_organization_id()
    organization_name = get_organization_by_id(organization_id).organization_name
    project_name = get_project_by_id(project_id, organization_id).project_name
    collection = get_collection_by_id(collection_id, project_id, organization_id)
    collection_name = collection.collection_name
    topic_name = f"{organization_name}.{project_name}.{collection_name}"

    # Get the collection's schema
    schema = await fetch_collection_schema(organization_name, project_name, collection_name)
    print(f"Schema for collection '{collection_name}': {schema}")

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
            message["timestamp"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            print(f"Added missing 'timestamp' field to message {i}")
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

    # Send valid messages to Kafka
    for i, message_data in enumerate(valid_messages):
        # Extract the key from the message_data
        message_key = message_data.pop("key")

        # Send the rest of the message data to Kafka, excluding the key
        kafka_producer.produce(topic_name, key=message_key, value=json.dumps(message_data))
        if (i + 1) % flush_threshold == 0:
            kafka_producer.flush()

    kafka_producer.flush()

    return {
        "message": f"Data sent to collection '{collection_name}' successfully.",
        "processed_count": len(valid_messages),
    }

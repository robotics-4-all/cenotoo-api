import csv
import datetime
import io
import json
import logging
import uuid

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile

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
from utilities.kafka_connector import get_kafka_producer
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id

logger = logging.getLogger(__name__)
router = APIRouter(dependencies=[Depends(check_project_exists)])
TAG = "Data Import"


@router.post(
    "/projects/{project_id}/collections/{collection_id}/import",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_write_access)],
)
async def import_data(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    file: UploadFile = File(...),
):
    organization_id = get_organization_id()
    organization_name = get_organization_by_id(organization_id).organization_name
    project_name = get_project_by_id(project_id, organization_id).project_name
    collection = get_collection_by_id(collection_id, project_id, organization_id)
    collection_name = collection.collection_name
    topic_name = f"{organization_name}.{project_name}.{collection_name}"

    schema = await fetch_collection_schema(organization_name, project_name, collection_name)
    if not schema:
        raise HTTPException(
            status_code=400, detail=f"Collection '{collection_name}' does not have a defined schema"
        )

    content = await file.read()
    if len(content) > 50 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large (max 50MB)")

    records = []
    filename = file.filename.lower() if file.filename else ""

    try:
        text_content = content.decode("utf-8")
        if filename.endswith(".csv") or file.content_type == "text/csv":
            reader = csv.DictReader(io.StringIO(text_content))
            for row in reader:
                # Convert string values to appropriate types based on schema
                typed_row = {}
                for k, v in row.items():
                    if not v and v != 0:
                        continue
                    if k in schema:
                        expected_type = schema[k]
                        try:
                            if expected_type == "int":
                                typed_row[k] = int(v)
                            elif expected_type == "float":
                                typed_row[k] = float(v)
                            elif expected_type == "bool":
                                typed_row[k] = str(v).lower() in ("true", "1", "yes", "y")
                            else:
                                typed_row[k] = v
                        except ValueError:
                            typed_row[k] = v  # Let validation catch it
                    else:
                        typed_row[k] = v
                records.append(typed_row)
        else:
            # Try JSON array
            try:
                parsed = json.loads(text_content)
                records = parsed if isinstance(parsed, list) else [parsed]
            except json.JSONDecodeError:
                # Try NDJSON
                for line in text_content.splitlines():
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {str(e)}") from e

    valid_messages = []
    errors = []

    for i, original_message in enumerate(records):
        message = original_message.copy()
        is_valid, error_msg = validate_message_against_simple_schema(original_message, schema)

        if not is_valid:
            errors.append({"row": i, "error": error_msg})
            continue

        if "key" not in message:
            message["key"] = "key"

        if "timestamp" not in message:
            message["timestamp"] = datetime.datetime.now(datetime.UTC).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            )

        try:
            timestamp_dt = datetime.datetime.fromisoformat(
                message["timestamp"].replace("Z", "+00:00")
            )
            message["day"] = timestamp_dt.strftime("%Y-%m-%d")
        except ValueError:
            errors.append(
                {"row": i, "error": f"Invalid timestamp format: {message.get('timestamp')}"}
            )
            continue

        final_is_valid, final_error = validate_message_against_simple_schema(message, schema)

        if final_is_valid:
            valid_messages.append(message)
        else:
            errors.append({"row": i, "error": final_error})

    if valid_messages:
        kafka_producer = get_kafka_producer()
        for i, message_data in enumerate(valid_messages):
            message_key = message_data["key"]
            kafka_value = {k: v for k, v in message_data.items() if k != "key"}
            kafka_producer.produce(topic_name, key=message_key, value=json.dumps(kafka_value))
            if (i + 1) % 1000 == 0:
                kafka_producer.flush()
        kafka_producer.flush()

        await insert_data_into_table(
            organization_name, project_name, collection_name, valid_messages
        )

    return {
        "imported": len(valid_messages),
        "skipped": len(errors),
        "errors": errors,
        "total": len(records),
    }

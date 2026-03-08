import datetime
import logging
import uuid
from typing import Any

from confluent_kafka.admin import NewTopic
from fastapi import HTTPException, status

from core.validators import validate_cql_identifier
from dependencies import get_organization_id
from models.collection_models import CollectionCreateRequest, CollectionUpdateRequest
from utilities.cassandra_connector import get_cassandra_session
from utilities.kafka_connector import get_kafka_admin_client
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id
from utilities.schema_utils import (
    flatten_object,
    is_list_of_same_schema,
    unflatten_schema,
)

logger = logging.getLogger(__name__)

session = get_cassandra_session()


# Get collection by ID


def get_collection_by_id(
    collection_id: uuid.UUID, project_id: uuid.UUID, organization_id: uuid.UUID
):
    """Return a collection by its ID."""
    query = (
        "SELECT id, collection_name, description, tags, creation_date, project_id, organization_id "
        "FROM collection WHERE id=%s AND project_id=%s AND organization_id=%s "
        "LIMIT 1 ALLOW FILTERING"
    )
    return session.execute(query, (collection_id, project_id, organization_id)).one()


# Insert a new collection into the database


async def insert_collection(
    organization_id: uuid.UUID, project_id: uuid.UUID, data: CollectionCreateRequest
):
    """Insert a new collection into the database."""
    query = """
    INSERT INTO collection (id, collection_name, creation_date, description,
    organization_id, project_id, tags)
    VALUES (%s, %s, toTimestamp(now()), %s, %s, %s, %s)
    """
    collection_id = uuid.uuid4()
    session.execute(
        query, (collection_id, data.name, data.description, organization_id, project_id, data.tags)
    )
    return collection_id


# Update an existing collection


async def update_collection_in_db(collection_id: uuid.UUID, data: CollectionUpdateRequest):
    """Update an existing collection in the database."""
    update_query = "UPDATE collection SET "
    update_params: list[Any] = []

    if data.description:
        update_query += "description=%s, "
        update_params.append(data.description)

    if data.tags:
        update_query += "tags=%s, "
        update_params.append(data.tags)

    update_query = update_query.rstrip(", ") + " WHERE id=%s"
    update_params.append(collection_id)

    session.execute(update_query, tuple(update_params))


# Delete a collection from the database


async def delete_collection_from_db(collection_id: uuid.UUID):
    """Delete a collection from the database."""
    query = "DELETE FROM collection WHERE id=%s"
    session.execute(query, (collection_id,))


# Fetch a collection by its ID


async def fetch_collection_by_name(
    organization_id: uuid.UUID, project_id: uuid.UUID, collection_name: str
):
    """Fetch a collection by its name."""
    query = (
        "SELECT * FROM collection WHERE collection_name=%s AND project_id=%s "
        "AND organization_id=%s ALLOW FILTERING"
    )
    return session.execute(query, (collection_name, project_id, organization_id)).one()


async def fetch_collection_schema(organization_name: str, project_name: str, collection_name: str):
    """Fetch the schema of a collection."""
    # Try with exact case first
    schema_query = """
    SELECT column_name, type
    FROM system_schema.columns
    WHERE keyspace_name=%s AND table_name=%s
    """
    rows = session.execute(schema_query, (organization_name, f"{project_name}_{collection_name}"))
    flat_schema = {row.column_name: row.type for row in rows}

    # If no schema found, try with lowercase as fallback
    if not flat_schema:
        rows = session.execute(
            schema_query,
            (organization_name.lower(), f"{project_name.lower()}_{collection_name.lower()}"),
        )
        flat_schema = {row.column_name: row.type for row in rows}

    return unflatten_schema(flat_schema)


# Fetch all collections for a project


def fetch_all_collections(organization_id: uuid.UUID, project_id: uuid.UUID):
    """Fetch all collections for a project."""
    query = "SELECT * FROM collection WHERE organization_id=%s AND project_id=%s ALLOW FILTERING"
    rows = session.execute(query, (organization_id, project_id))
    return rows.all()


# Create a Cassandra table for the collection


async def create_cassandra_table(
    organization_name: str, project_name: str, data: CollectionCreateRequest
):
    """Create a Cassandra table for the collection."""
    keyspace_name = organization_name
    table_name = f'"{project_name}_{data.name}"'
    # If the schema contains lists, ensure they are consistent
    if isinstance(data.collection_schema, list) and not is_list_of_same_schema(
        data.collection_schema
    ):
        raise HTTPException(
            status_code=400, detail="The provided schema contains lists with inconsistent types."
        )
    flattened_schema = flatten_object(data.collection_schema)
    flattened_schema["timestamp"] = "TIMESTAMP"
    flattened_schema["day"] = "DATE"
    # Check for key column, if not present add it
    has_key = any(key.lower() == "key" for key in flattened_schema)
    if not has_key:
        flattened_schema["key"] = "TEXT"
    columns = ", ".join(
        [f'"{col_name}" {col_type}' for col_name, col_type in flattened_schema.items()]
    )
    primary_key = "((day, key), timestamp)"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {keyspace_name}.{table_name} (
        {columns},
        PRIMARY KEY {primary_key}
    ) WITH CLUSTERING ORDER BY (timestamp DESC)
    """
    try:
        session.execute(create_table_query)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to create the Cassandra table: {str(e)}"
        ) from e


# Delete a Cassandra table


async def delete_cassandra_table(
    organization_id: uuid.UUID, project_id: uuid.UUID, collection_id: uuid.UUID
):
    """Delete a Cassandra table."""
    keyspace_name = get_organization_by_id(organization_id).organization_name
    table_name = (
        f"{get_project_by_id(project_id, organization_id).project_name}_"
        f"{get_collection_by_id(collection_id, project_id, organization_id).collection_name}"
    )
    query = f"DROP TABLE IF EXISTS {keyspace_name}.{table_name}"
    session.execute(query)


# Create a Kafka topic


async def create_kafka_topic(organization_name: str, project_name: str, collection_name: str):
    """Create a Kafka topic for the collection."""
    kafka_topic_name = f"{organization_name}.{project_name}.{collection_name}"
    kafka_admin_client = get_kafka_admin_client()
    print(kafka_admin_client)
    new_topic = NewTopic(kafka_topic_name, num_partitions=1, replication_factor=1)
    fs = kafka_admin_client.create_topics([new_topic])
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} created successfully")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")
            raise HTTPException(
                status_code=500, detail=f"Failed to create Kafka topic {topic}: {str(e)}"
            ) from e


# Delete a Kafka topic


async def delete_kafka_topic(organization_name: str, project_name: str, collection_name: str):
    """Delete a Kafka topic for the collection."""
    kafka_topic_name = f"{organization_name}.{project_name}.{collection_name}"
    kafka_admin_client = get_kafka_admin_client()
    try:
        # Delete the topic and get the futures
        fs = kafka_admin_client.delete_topics([kafka_topic_name])

        # Wait for operation to complete and check results
        for topic, f in fs.items():
            try:
                f.result(timeout=30)  # 30-second timeout
                print(f"Topic {topic} deleted successfully")
            except Exception as e:
                print(f"Failed to delete topic {topic}: {e}")
                raise HTTPException(
                    status_code=500, detail=f"Failed to delete Kafka topic {topic}: {str(e)}"
                ) from e
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to delete Kafka topic {kafka_topic_name}: {str(e)}"
        ) from e


def check_collection_exists(collection_id: uuid.UUID, project_id: uuid.UUID):
    """Check if a collection exists and return it."""
    collection = get_collection_by_id(collection_id, project_id, get_organization_id())
    if not collection:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Collection not found.")
    return collection


async def insert_data_into_table(
    organization_name: str, project_name: str, collection_name: str, records: list[dict]
):
    """Insert data records into a Cassandra table."""
    validate_cql_identifier(organization_name, "keyspace")
    validate_cql_identifier(project_name, "project")
    validate_cql_identifier(collection_name, "collection")
    keyspace_name = organization_name
    table_name = f'"{project_name}_{collection_name}"'

    # Loop through each record and insert it into the table
    for record in records:
        flattened_record = flatten_object(record, return_value=True)

        # Ensure 'timestamp' is in the record and convert to datetime object
        # (schema expects TIMESTAMP)
        if not any(key.lower() == "timestamp" for key in flattened_record):
            flattened_record["timestamp"] = datetime.datetime.utcnow()
        elif isinstance(flattened_record["timestamp"], str):
            try:
                # Parse string to datetime object (schema expects TIMESTAMP)
                flattened_record["timestamp"] = datetime.datetime.fromisoformat(
                    flattened_record["timestamp"].replace("Z", "+00:00")
                )
            except ValueError:
                flattened_record["timestamp"] = datetime.datetime.utcnow()
        elif not isinstance(flattened_record["timestamp"], datetime.datetime):
            # If it's not a datetime object, use current time
            flattened_record["timestamp"] = datetime.datetime.utcnow()

        # Extract day field as date object (schema expects DATE type)
        flattened_record["day"] = flattened_record["timestamp"].date()
        # Extract the column names and values for insertion
        column_names = ", ".join([f'"{key}"' for key in flattened_record])
        placeholders = ", ".join(["%s" for _ in flattened_record.values()])
        values = list(flattened_record.values())
        # Insert the record into the table
        insert_query = f"""
        INSERT INTO {keyspace_name}.{table_name} ({column_names})
        VALUES ({placeholders})
        """
        try:
            session.execute(insert_query, values)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to insert data into Cassandra: {str(e)}",
            ) from e

import logging

from confluent_kafka.admin import NewTopic
from fastapi import HTTPException

from utilities.kafka_connector import get_kafka_admin_client

logger = logging.getLogger(__name__)


# Create a Kafka topic
async def create_kafka_topic(organization_name: str, project_name: str, collection_name: str):
    """Create a new Kafka topic for a collection."""
    kafka_topic_name = f"{organization_name}.{project_name}.{collection_name}"
    kafka_admin_client = get_kafka_admin_client()
    # Removed debug print of kafka_admin_client
    new_topic = NewTopic(kafka_topic_name, num_partitions=1, replication_factor=1)
    fs = kafka_admin_client.create_topics([new_topic])
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logger.info("Topic %s created successfully", topic)
        except Exception as e:
            logger.error("Failed to create topic %s: %s", topic, e)
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create Kafka topic {topic}: {str(e)}",
            ) from e


# Delete a Kafka topic
async def delete_kafka_topic(organization_name: str, project_name: str, collection_name: str):
    """Delete an existing Kafka topic for a collection."""
    kafka_topic_name = f"{organization_name}.{project_name}.{collection_name}"
    kafka_admin_client = get_kafka_admin_client()
    try:
        # Delete the topic and get the futures
        fs = kafka_admin_client.delete_topics([kafka_topic_name])

        # Wait for operation to complete and check results
        for topic, f in fs.items():
            try:
                f.result(timeout=30)  # 30-second timeout
                logger.info("Topic %s deleted successfully", topic)
            except Exception as e:
                logger.error("Failed to delete topic %s: %s", topic, e)
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to delete Kafka topic {topic}: {str(e)}",
                ) from e
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete Kafka topic {kafka_topic_name}: {str(e)}",
        ) from e

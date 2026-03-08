"""Kafka connection management.

This module provides functions to create Kafka admin clients and producers
for message streaming operations.
"""

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

from config import settings


def get_kafka_admin_client():
    """Create and return a Kafka admin client.

    Returns:
        Configured Kafka AdminClient instance.
    """
    return AdminClient({"bootstrap.servers": settings.kafka_brokers})


def get_kafka_producer():
    """Create and return a Kafka producer.

    Returns:
        Configured Kafka Producer instance.
    """
    return Producer(
        {"bootstrap.servers": settings.kafka_brokers, "queue.buffering.max.messages": 200000}
    )

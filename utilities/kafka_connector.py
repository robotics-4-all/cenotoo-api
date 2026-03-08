"""Kafka connection management.

This module provides functions to create Kafka admin clients and producers
for message streaming operations.
"""

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

from config import settings


def _apply_sasl_config(config):
    """Add SASL authentication keys to a Kafka config dict if credentials are set."""
    if settings.kafka_username:
        config["security.protocol"] = settings.kafka_security_protocol
        config["sasl.mechanism"] = settings.kafka_sasl_mechanism
        config["sasl.username"] = settings.kafka_username
        config["sasl.password"] = settings.kafka_password
    return config


def get_kafka_admin_client():
    """Create and return a Kafka admin client.

    Returns:
        Configured Kafka AdminClient instance.
    """
    config = {"bootstrap.servers": settings.kafka_brokers}
    _apply_sasl_config(config)
    return AdminClient(config)


def get_kafka_producer():
    """Create and return a Kafka producer.

    Returns:
        Configured Kafka Producer instance.
    """
    config = {"bootstrap.servers": settings.kafka_brokers, "queue.buffering.max.messages": 200000}
    _apply_sasl_config(config)
    return Producer(config)

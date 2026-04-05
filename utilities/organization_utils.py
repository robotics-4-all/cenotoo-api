"""Organization database utility functions.

This module provides utility functions for CRUD operations on organizations
in the PostgreSQL database. Cassandra keyspace operations (for IoT data)
are kept using the Cassandra session.
"""

import uuid
from typing import Any

from utilities.cassandra_connector import get_cassandra_session
from utilities.postgres_connector import pg_execute, pg_fetchall, pg_fetchone


# Insert a new organization into the database


async def insert_organization(org_id, data):
    """Insert a new organization into the database.

    Args:
        org_id: UUID for the new organization.
        data: OrganizationCreateRequest with organization details.
    """
    pg_execute(
        """
        INSERT INTO organization (id, organization_name, description, creation_date, tags)
        VALUES (%s, %s, %s, NOW(), %s)
        """,
        (org_id, data.organization_name, data.description, data.tags),
    )


# Get an organization by its ID


def get_organization_by_id(org_id):
    """Retrieve an organization by its ID.

    Args:
        org_id: UUID of the organization.

    Returns:
        Organization record from database.
    """
    return pg_fetchone(
        "SELECT id, organization_name, description, creation_date, tags "
        "FROM organization WHERE id = %s",
        (org_id,),
    )


# Update an organization in the database


async def update_organization_in_db(
    org_id: uuid.UUID, description: str = None, tags: list[str] = None
):
    """Update organization details in the database.

    Args:
        org_id: UUID of the organization to update.
        description: New description (optional).
        tags: New tags list (optional).

    Returns:
        Success message dictionary.

    Raises:
        ValueError: If no fields provided for update.
    """
    update_fields = []
    update_values: list[Any] = []

    if description:
        update_fields.append("description=%s")
        update_values.append(description)

    if tags:
        update_fields.append("tags=%s")
        update_values.append(tags)

    if not update_fields:
        raise ValueError("No fields to update")

    update_values.append(org_id)

    query = f"UPDATE organization SET {', '.join(update_fields)} WHERE id=%s"
    pg_execute(query, tuple(update_values))
    return {"message": "Organization updated successfully"}


# Delete an organization from the database


async def delete_organization_from_db(org_id: uuid.UUID):
    """Delete an organization from the database.

    Args:
        org_id: UUID of the organization to delete.
    """
    pg_execute("DELETE FROM organization WHERE id=%s", (org_id,))


# Create a keyspace in the database (Cassandra — for IoT data)


async def create_keyspace_in_db(organization_name: str):
    """Create a Cassandra keyspace for an organization.

    Args:
        organization_name: Name of the organization (spaces will be replaced with underscores).
    """
    session = get_cassandra_session()
    # Transform organization name: replace spaces with underscores
    keyspace_name = organization_name.replace(" ", "_")

    # Quote the keyspace name to preserve case sensitivity and special
    # characters (like underscores)
    quoted_keyspace_name = f'"{keyspace_name}"'

    # Define replication strategy
    replication_strategy = {"class": "SimpleStrategy", "replication_factor": "2"}

    # Create the keyspace query
    query = (
        f"CREATE KEYSPACE IF NOT EXISTS {quoted_keyspace_name} "
        f"WITH replication = {replication_strategy}"
    )
    session.execute(query)


# Delete a keyspace from the database (Cassandra — for IoT data)


async def delete_keyspace_in_db(organization_name: str):
    """Delete a Cassandra keyspace for an organization.

    Args:
        organization_name: Name of the organization (spaces will be replaced with underscores).
    """
    session = get_cassandra_session()
    # Transform organization name: replace spaces with underscores
    keyspace_name = organization_name.replace(" ", "_")

    # Quote the keyspace name to preserve case sensitivity and special
    # characters (like underscores)
    keyspace_name = f'"{keyspace_name}"'
    query = f"DROP KEYSPACE IF EXISTS {keyspace_name}"
    session.execute(query)


# Fetch all organizations from the database


async def get_all_organizations_from_db():
    """Retrieve all organizations from the database.

    Returns:
        List of all organization records.
    """
    return pg_fetchall(
        "SELECT id, organization_name, description, creation_date, tags FROM organization"
    )


# Fetch an organization by its name


async def get_organization_by_name(organization_name: str):
    """Retrieve an organization by its name.

    Args:
        organization_name: Name of the organization to find.

    Returns:
        Organization record with matching name.
    """
    return pg_fetchone(
        "SELECT id FROM organization WHERE organization_name=%s",
        (organization_name,),
    )

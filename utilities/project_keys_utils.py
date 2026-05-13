"""Project API key utility functions.

This module provides utility functions for generating, managing, and validating
project API keys for authentication and authorization.
"""

import hashlib
import logging
import secrets
import uuid

from fastapi import HTTPException, status

from utilities.postgres_connector import pg_execute, pg_fetchall, pg_fetchone


def generate_key() -> str:
    """Generate a secure random API key."""
    return secrets.token_hex(32)


def hash_api_key(key: str) -> str:
    """Return the SHA-256 hash of an API key."""
    return hashlib.sha256(key.encode()).hexdigest()


def insert_project_key(project_id: uuid.UUID, key_type: str) -> str:
    """Create and insert a new project API key.

    Args:
        project_id: UUID of the project.
        key_type: Type of the key (read/write/master).

    Returns:
        The generated API key string.

    Raises:
        HTTPException: If key creation fails.
    """
    try:
        key_value = generate_key()
        key_id = uuid.uuid4()

        pg_execute(
            "INSERT INTO api_keys (id, api_key, created_at, key_type, project_id) VALUES (%s, %s, NOW(), %s, %s)",
            (key_id, hash_api_key(key_value), key_type, project_id),
        )

        return key_value
    except Exception as e:
        logging.error("Error inserting project key: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create project key"
        ) from e


def fetch_project_keys_by_category(project_id: uuid.UUID, key_category: str):
    """Retrieve project API keys by category or all keys.

    Args:
        project_id: UUID of the project.
        key_category: Category of keys to fetch ('all', 'read', 'write', 'master').

    Returns:
        List of matching API key records.

    Raises:
        HTTPException: If query fails or no keys found.
    """
    try:
        if key_category == "all":
            keys = pg_fetchall(
                "SELECT id, api_key, key_type, created_at, project_id "
                "FROM api_keys WHERE project_id=%s",
                (project_id,),
            )
        else:
            keys = pg_fetchall(
                "SELECT id, api_key, key_type, created_at, project_id "
                "FROM api_keys WHERE project_id=%s AND key_type=%s",
                (project_id, key_category),
            )
    except BaseException as e:
        logging.error("Error fetching project keys by category: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to fetch project keys"
        ) from e
    if not keys:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="No keys found for the specified category"
        )
    return keys


def update_project_key(key_id: uuid.UUID, current_key_value: str) -> str:
    """Regenerate a project API key.

    Args:
        key_id: UUID of the key to regenerate.
        current_key_value: Current key value for reference.

    Returns:
        The newly generated API key string.

    Raises:
        HTTPException: If key update fails.
    """
    del current_key_value
    try:
        new_key_value = generate_key()

        pg_execute(
            "UPDATE api_keys SET api_key=%s, created_at=NOW() WHERE id=%s",
            (hash_api_key(new_key_value), key_id),
        )

        return new_key_value
    except Exception as e:
        logging.error("Error updating project key: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update project key"
        ) from e


def delete_keys_by_category(project_id: uuid.UUID, key_category: str):
    """Delete all project keys of a specific category.

    Args:
        project_id: UUID of the project.
        key_category: Category of keys to delete.

    Returns:
        Success message dictionary.
    """
    removing_keys = fetch_project_keys_by_category(project_id, key_category)
    for key in removing_keys:
        delete_key_by_value(key.id)
    return {"message": "Keys deleted successfully."}


def delete_key_by_value(key_id: uuid.UUID):
    """Delete a specific project API key by its ID.

    Args:
        key_id: UUID of the key to delete.

    Raises:
        HTTPException: If deletion fails.
    """
    try:
        pg_execute("DELETE FROM api_keys WHERE id=%s", (key_id,))
    except Exception as e:
        logging.error("Error deleting key by value: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to delete project key"
        ) from e


def get_project_key_by_value(key_value: str, project_id: uuid.UUID):
    """Retrieve a project API key by its value.

    Args:
        key_value: The API key string to find.
        project_id: UUID of the project.

    Returns:
        API key record from database.

    Raises:
        HTTPException: If key not found or query fails.
    """
    try:
        key = pg_fetchone(
            "SELECT id, api_key, key_type, project_id, created_at "
            "FROM api_keys WHERE api_key=%s AND project_id=%s LIMIT 1",
            (hash_api_key(key_value), project_id),
        )
    except Exception as e:
        logging.error("Error retrieving project key by value: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to fetch project key"
        ) from e
    if not key:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Key not found")
    return key

"""User database utility functions.

This module provides utility functions for CRUD operations on users
in the PostgreSQL database.
"""

import uuid

from utilities.postgres_connector import pg_execute, pg_fetchall, pg_fetchone


def get_user_by_username(username: str):
    """Retrieve a user by their username.

    Args:
        username: The username to search for.

    Returns:
        User record if found, None otherwise.
    """
    user = pg_fetchone("SELECT * FROM users WHERE username = %s", (username,))
    if user:
        return user
    return None


async def get_user_by_username_and_org_id(username: str, organization_id: uuid.UUID):
    """Retrieve a user by username and organization ID.

    Args:
        username: The username to search for.
        organization_id: UUID of the organization.

    Returns:
        User record from database.
    """
    return pg_fetchone(
        "SELECT id FROM users WHERE username=%s AND organization_id=%s",
        (username, organization_id),
    )


async def insert_user(
    user_id: uuid.UUID,
    organization_id: uuid.UUID,
    username: str,
    hashed_password: str,
    role: str = "member",
):
    """Insert a new user into the database.

    Args:
        user_id: UUID for the new user.
        organization_id: UUID of the organization.
        username: The user's username.
        hashed_password: The hashed password.
        role: The user's role within the organization.
    """
    pg_execute(
        "INSERT INTO users (id, organization_id, username, password, role) VALUES (%s, %s, %s, %s, %s)",
        (user_id, organization_id, username, hashed_password, role),
    )


async def delete_user_from_db(user_id: uuid.UUID):
    """Delete a user from the database.

    Args:
        user_id: UUID of the user to delete.
    """
    pg_execute("DELETE FROM users WHERE id=%s", (user_id,))


async def update_user_password_in_db(user_id: uuid.UUID, hashed_password: str):
    """Update a user's password in the database.

    Args:
        user_id: UUID of the user.
        hashed_password: The new hashed password.
    """
    pg_execute("UPDATE users SET password=%s WHERE id=%s", (hashed_password, user_id))


async def get_all_users_in_organization(organization_id: uuid.UUID):
    """Retrieve all users in an organization.

    Args:
        organization_id: UUID of the organization.

    Returns:
        List of user dictionaries with username and role.
    """
    users = pg_fetchall(
        "SELECT id, username, role FROM users WHERE organization_id=%s", (organization_id,)
    )
    return [{"username": user.username, "role": user.role or "member"} for user in users]


async def update_user_role_in_db(user_id: uuid.UUID, role: str):
    """Update a user's role in the database.

    Args:
        user_id: UUID of the user.
        role: The new role to assign.
    """
    pg_execute("UPDATE users SET role=%s WHERE id=%s", (role, user_id))

"""User database utility functions.

This module provides utility functions for CRUD operations on users
in the Cassandra database.
"""

import uuid

from cassandra.cluster import Session

from utilities.cassandra_connector import get_cassandra_session

session: Session = get_cassandra_session()


def get_user_by_username(username: str):
    """Retrieve a user by their username.

    Args:
        username: The username to search for.

    Returns:
        User record if found, None otherwise.
    """
    query = "SELECT * FROM user WHERE username = %s LIMIT 1 ALLOW FILTERING"
    result = session.execute(query, (username,))
    user = result.one()
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
    query = "SELECT id FROM user WHERE username=%s AND organization_id=%s LIMIT 1 ALLOW FILTERING"
    result = session.execute(query, (username, organization_id)).one()
    return result


async def insert_user(
    user_id: uuid.UUID, organization_id: uuid.UUID, username: str, hashed_password: str
):
    """Insert a new user into the database.

    Args:
        user_id: UUID for the new user.
        organization_id: UUID of the organization.
        username: The user's username.
        hashed_password: The hashed password.
    """
    query = """
        INSERT INTO user (id, organization_id, username, password)
        VALUES (%s, %s, %s, %s)
    """
    session.execute(query, (user_id, organization_id, username, hashed_password))


async def delete_user_from_db(user_id: uuid.UUID):
    """Delete a user from the database.

    Args:
        user_id: UUID of the user to delete.
    """
    query = "DELETE FROM user WHERE id=%s"
    session.execute(query, (user_id,))


async def update_user_password_in_db(user_id: uuid.UUID, hashed_password: str):
    """Update a user's password in the database.

    Args:
        user_id: UUID of the user.
        hashed_password: The new hashed password.
    """
    query = "UPDATE user SET password=%s WHERE id=%s"
    session.execute(query, (hashed_password, user_id))


async def get_all_users_in_organization(organization_id: uuid.UUID):
    """Retrieve all users in an organization.

    Args:
        organization_id: UUID of the organization.

    Returns:
        List of user dictionaries with usernames.
    """
    query = "SELECT id, username FROM user WHERE organization_id=%s ALLOW FILTERING"
    users = session.execute(query, (organization_id,)).all()
    return [{"username": user.username} for user in users]

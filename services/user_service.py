"""User service layer.

This module provides business logic for user management operations,
including creation, password updates, and deletion.
"""

import uuid
from typing import Any

from fastapi import HTTPException

from core.validators import validate_password_strength
from dependencies import contains_special_characters
from models.user_models import UserRequest
from services.auth_service import hash_password
from utilities.user_utils import (
    delete_user_from_db,
    get_all_users_in_organization,
    get_user_by_username,
    get_user_by_username_and_org_id,
    insert_user,
    update_user_password_in_db,
)


async def create_user_service(organization_id: uuid.UUID, user_data: UserRequest):
    """Create a new user in an organization.

    Args:
        organization_id: UUID of the organization.
        user_data: User creation request with username and password.

    Returns:
        Success message with user ID.

    Raises:
        HTTPException: If username is invalid or already exists.
    """
    # For usernames, keep strict validation (no special characters except
    # underscore)
    if contains_special_characters(
        user_data.username, allow_spaces=False, allow_underscores=True, allow_special_chars=False
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid username format. Usernames can only contain latin letters, "
                "numbers, and underscores."
            ),
        )
    existing_user = get_user_by_username(user_data.username)
    if existing_user:
        raise HTTPException(status_code=409, detail="Username already exists")

    validate_password_strength(user_data.password)
    hashed_password = hash_password(user_data.password)
    user_id = uuid.uuid4()

    await insert_user(user_id, organization_id, user_data.username, hashed_password)

    return {"message": "User created successfully", "user_id": str(user_id)}


async def delete_user_service(organization_id: uuid.UUID, username: str, current_user: Any):
    """Delete a user from an organization.

    Args:
        organization_id: UUID of the organization.
        username: Username of the user to delete.
        current_user: Currently authenticated user.

    Returns:
        Success message.

    Raises:
        HTTPException: If user not found or unauthorized.
    """
    user_to_delete = await get_user_by_username_and_org_id(username, organization_id)
    if not user_to_delete:
        raise HTTPException(status_code=404, detail="User not found")

    # Superadmins can delete any user; users can only delete themselves
    if current_user.role != "superadmin" and current_user.username != username:
        raise HTTPException(status_code=403, detail="Unauthorized to delete this user")

    await delete_user_from_db(user_to_delete.id)
    return {"message": "User deleted successfully"}


async def update_user_password_service(
    organization_id: uuid.UUID, data: UserRequest, current_user: Any
):
    """Update a user's password.

    Args:
        organization_id: UUID of the organization.
        data: User request with username and new password.
        current_user: Currently authenticated user.

    Returns:
        Success message.

    Raises:
        HTTPException: If user not found or unauthorized.
    """
    user_to_update = await get_user_by_username_and_org_id(data.username, organization_id)
    if not user_to_update:
        raise HTTPException(status_code=404, detail="User not found")

    # Superadmins can update any user's password; users can only update their
    # own passwords
    if current_user.role != "superadmin" and current_user.username != data.username:
        raise HTTPException(status_code=403, detail="Unauthorized to update password for this user")

    validate_password_strength(data.password)
    hashed_password = hash_password(data.password)
    await update_user_password_in_db(user_to_update.id, hashed_password)

    return {"message": "User password updated successfully"}


async def get_all_users_service(organization_id: uuid.UUID):
    """Retrieve all users in an organization.

    Args:
        organization_id: UUID of the organization.

    Returns:
        Dictionary containing list of users.
    """
    users = await get_all_users_in_organization(organization_id)
    return {"users": users}

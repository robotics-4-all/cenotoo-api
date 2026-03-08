"""User management endpoints.

This module provides API endpoints for creating, updating, deleting,
and retrieving organization users.
"""

import uuid
from typing import Any

from fastapi import APIRouter, Depends, Query

from dependencies import check_organization_exists, get_current_user_from_jwt, verify_superadmin
from models.common import MessageResponse, PaginatedResponse
from models.user_models import Username, UserRequest
from services.user_service import (
    create_user_service,
    delete_user_service,
    get_all_users_service,
    update_user_password_service,
)

router = APIRouter(
    tags=["Organization User Management"], dependencies=[Depends(check_organization_exists)]
)


@router.post(
    "/organizations/{organization_id}/users",
    summary="Create user",
    description="Create a new user in an organization. Requires superadmin privileges.",
)
async def create_user(
    organization_id: uuid.UUID,
    user_data: UserRequest,
    _current_user: Any = Depends(verify_superadmin),
):
    """Create a new user in an organization.

    Args:
        organization_id: UUID of the organization.
        user_data: User creation request with username and password.
        _current_user: Authenticated superadmin user.

    Returns:
        Success message with user ID.
    """
    return await create_user_service(organization_id, user_data)


@router.get(
    "/organizations/{organization_id}/users",
    summary="List users",
    description=(
        "List all users in an organization with pagination. Requires superadmin privileges."
    ),
)
async def get_all_users(
    organization_id: uuid.UUID,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    _current_user: Any = Depends(verify_superadmin),
):
    """Retrieve all users in an organization."""
    result = await get_all_users_service(organization_id)
    all_users = result.get("users", [])
    page = all_users[offset : offset + limit]
    return PaginatedResponse(items=page, total=len(all_users), offset=offset, limit=limit)


@router.put(
    "/organizations/{organization_id}/users/update_password",
    response_model=MessageResponse,
    summary="Update user password",
    description=(
        "Update a user's password. Superadmins can update any user; "
        "regular users can only update their own."
    ),
)
async def update_user_password(
    organization_id: uuid.UUID,
    data: UserRequest,
    current_user: Any = Depends(get_current_user_from_jwt),
):
    """Update a user's password.

    Args:
        organization_id: UUID of the organization.
        data: User request with username and new password.
        current_user: Currently authenticated user.

    Returns:
        Success message.
    """
    return await update_user_password_service(organization_id, data, current_user)


@router.delete(
    "/organizations/{organization_id}/users",
    response_model=MessageResponse,
    summary="Delete user",
    description=(
        "Delete a user from an organization. Superadmins can delete any user; "
        "regular users can only delete themselves."
    ),
)
async def delete_user(
    organization_id: uuid.UUID,
    data: Username,
    current_user: Any = Depends(get_current_user_from_jwt),
):
    """Delete a user from an organization.

    Args:
        organization_id: UUID of the organization.
        data: Username model containing username to delete.
        current_user: Currently authenticated user.

    Returns:
        Success message.
    """
    return await delete_user_service(organization_id, data.username, current_user)

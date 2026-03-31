# pylint: disable=too-few-public-methods
"""User data models.

This module defines Pydantic models for user-related requests and responses.
"""

from pydantic import BaseModel


class UserRequest(BaseModel):
    """User creation or authentication request model.

    Attributes:
        username: The user's unique username.
        password: The user's password (plain text for input, hashed for storage).
        role: The user's role within the organization. Defaults to 'member'.
    """

    username: str
    password: str
    role: str = "member"


class Username(BaseModel):
    """Model containing only a username.

    Attributes:
        username: The user's unique username.
    """

    username: str


class UserRoleUpdateRequest(BaseModel):
    username: str
    role: str

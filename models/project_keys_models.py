# pylint: disable=too-few-public-methods
"""Project API key data models.

This module defines Pydantic models for project API key-related requests and responses,
including key validation logic.
"""

import re
import uuid
from typing import Literal

from pydantic import BaseModel, Field, field_validator

# Base model to include key validation logic


class BaseKeyModel(BaseModel):
    """Base model for API key operations with validation.

    Attributes:
        key_value: The 64-character hexadecimal API key string.
    """

    key_value: str

    @field_validator("key_value")
    def check_key_format(cls, value):  # pylint: disable=no-self-argument
        """Validate that the key is a 64-character hexadecimal string."""
        if not re.fullmatch(r"^[a-f0-9]{64}$", value):
            raise ValueError("Invalid key format. Expected a 64-character hexadecimal string.")
        return value


class ProjectKeyCreateRequest(BaseModel):
    """Request model for creating a new project API key.

    Attributes:
        key_type: The type of key - 'read', 'write', or 'master'.
    """

    key_type: Literal["read", "write", "master"] = Field(
        ...,
        description="The type of the key for the project. It can be 'read', 'write', or 'master'.",
        json_schema_extra={"examples": ["read"]},
    )


class ProjectKeyResponse(BaseModel):
    """Response model for project API key information.

    Attributes:
        api_key: The generated API key string.
        project_id: The UUID of the associated project.
        key_type: The type of the key.
        created_at: ISO timestamp of key creation.
    """

    api_key: str
    project_id: uuid.UUID
    key_type: str
    created_at: str


class DeleteKeyRequest(BaseModel):
    """Request model for deleting a project API key.

    Attributes:
        key_value: The API key to delete.
    """

    key_value: str


class RegenerateKeyRequest(BaseModel):
    """Request model for regenerating a project key."""

    key_value: str

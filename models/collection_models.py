# pylint: disable=too-few-public-methods
"""Collection data models.

This module defines Pydantic models for collection-related requests and responses.
"""

import uuid
from typing import Any

from pydantic import BaseModel


class CollectionCreateRequest(BaseModel):
    """Request model for creating a new collection.

    Attributes:
        name: The collection name.
        description: Description of the collection.
        tags: List of tags for categorization.
        collection_schema: Schema definition for the collection data.
    """

    name: str
    description: str
    tags: list[str] = []
    collection_schema: dict[str, Any]


class CollectionUpdateRequest(BaseModel):
    """Request model for updating an existing collection.

    Attributes:
        description: Updated description (optional).
        tags: Updated tags list (optional).
    """

    description: str | None = None
    tags: list[str] | None = None


class CollectionResponse(BaseModel):
    """Response model for collection information.

    Contains complete collection details including IDs, names, and schema.
    """

    collection_name: str
    collection_id: uuid.UUID
    project_id: uuid.UUID
    project_name: str
    organization_id: uuid.UUID
    organization_name: str
    description: str
    tags: list[str]
    creation_date: str
    collection_schema: dict[str, Any]

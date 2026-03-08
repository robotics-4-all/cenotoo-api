from typing import Any, Generic, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic response model for paginated lists."""

    items: list[T]
    total: int
    offset: int
    limit: int


class MessageResponse(BaseModel):
    """Generic response model for simple messages."""

    message: str


class TokenResponse(BaseModel):
    """Response model for authentication tokens."""

    access_token: str
    refresh_token: str | None = None
    token_type: str


class DataListResponse(BaseModel):
    """Response model for a list of data records."""

    data: list[dict[str, Any]]
    total_count: int


class SendDataResponse(BaseModel):
    """Response model for data ingestion results."""

    message: str
    processed_count: int


class DeleteDataResponse(BaseModel):
    """Response model for data deletion results."""

    message: str
    criteria: dict[str, str | None]


class KeyStatisticsResponse(BaseModel):
    """Response model for API key statistics."""

    key: str
    collection_name: str
    total_records: int
    first_timestamp: str | None = None
    last_timestamp: str | None = None
    daily_breakdown: list[dict[str, Any]]


class HealthResponse(BaseModel):
    """Response model for health check status."""

    status: str


class ReadyResponse(BaseModel):
    """Response model for readiness check status."""

    status: str
    checks: dict[str, str]

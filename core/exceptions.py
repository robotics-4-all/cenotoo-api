"""
Application exception hierarchy.

All custom exceptions extend AppException which wraps FastAPI's HTTPException
with structured error codes for consistent API responses.
"""

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse


async def app_exception_handler(_request: Request, exc: "AppException"):
    """Handle AppException and return a structured JSON response."""
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.detail,
    )


class AppException(HTTPException):
    """Base exception for all application errors."""

    def __init__(self, status_code: int, code: str, detail: str):
        self.code = code
        super().__init__(status_code=status_code, detail={"code": code, "message": detail})


class NotFoundError(AppException):
    """Resource not found."""

    def __init__(self, resource: str, identifier: str = ""):
        detail = f"{resource} not found" + (f": {identifier}" if identifier else "")
        super().__init__(status_code=404, code="NOT_FOUND", detail=detail)


class ConflictError(AppException):
    """Resource already exists or conflicts with existing state."""

    def __init__(self, detail: str):
        super().__init__(status_code=409, code="CONFLICT", detail=detail)


class ValidationError(AppException):
    """Input validation failure."""

    def __init__(self, detail: str):
        super().__init__(status_code=422, code="VALIDATION_ERROR", detail=detail)


class AuthenticationError(AppException):
    """Authentication failure — invalid or missing credentials."""

    def __init__(self, detail: str = "Could not validate credentials"):
        super().__init__(status_code=401, code="AUTHENTICATION_ERROR", detail=detail)


class AuthorizationError(AppException):
    """Authorization failure — insufficient permissions."""

    def __init__(self, detail: str = "Insufficient permissions"):
        super().__init__(status_code=403, code="AUTHORIZATION_ERROR", detail=detail)


class InfrastructureError(AppException):
    """External service failure (Cassandra, Kafka, Docker)."""

    def __init__(self, service: str, detail: str):
        super().__init__(
            status_code=500,
            code="INFRASTRUCTURE_ERROR",
            detail=f"{service} error: {detail}",
        )

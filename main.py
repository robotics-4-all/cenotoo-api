"""Main FastAPI application entry point."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from api.v1 import router as v1_router
from config import settings
from core.exceptions import AppException, app_exception_handler
from core.middleware import RequestLoggingMiddleware
from core.tracing import setup_tracing
from routers.health import router as health_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI):
    """Manage application startup and shutdown events."""
    del application
    logger.info("Starting Cenotoo API")
    if settings.environment != "development":
        if settings.jwt_secret_key == "supersecretkey":
            raise RuntimeError("JWT_SECRET_KEY must be changed from default in production")
        if settings.api_key_secret == "default-api-key-secret":
            raise RuntimeError("API_KEY_SECRET must be changed from default in production")
    yield
    from utilities.cassandra_connector import shutdown_cassandra

    shutdown_cassandra()
    logger.info("Cenotoo API shutdown complete")


# Rate limiter

limiter = Limiter(key_func=get_remote_address, default_limits=[settings.rate_limit_default])

app = FastAPI(
    title=settings.app_name,
    version="1.0.1",
    description=(
        "The Cenotoo API provides endpoints for managing the Cenotoo data streaming platform."
    ),
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]
app.add_exception_handler(AppException, app_exception_handler)  # type: ignore[arg-type]

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(SlowAPIMiddleware)

app.include_router(health_router)
app.include_router(v1_router, prefix="/api/v1")

setup_tracing(app)

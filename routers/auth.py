"""Authentication endpoints."""

from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address

from config import settings
from services.auth_service import (
    create_access_token,
    create_refresh_token,
    revoke_token,
    verify_password,
    verify_refresh_token,
)
from utilities.user_utils import get_user_by_username

limiter = Limiter(key_func=get_remote_address)
router = APIRouter(tags=["Authentication"])


class RefreshRequest(BaseModel):
    """Request model for refreshing an access token."""

    refresh_token: str


class RevokeRequest(BaseModel):
    """Request model for revoking a token."""

    token: str


@router.post("/token")
@limiter.limit(settings.rate_limit_auth)
async def login_for_access_token(
    request: Request, form_data: OAuth2PasswordRequestForm = Depends()
):
    """Authenticate user and return access and refresh tokens."""
    del request
    user = get_user_by_username(form_data.username)

    if not user or not verify_password(form_data.password, user.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token_expires = timedelta(minutes=settings.jwt_expiration_minutes)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )

    refresh_token_expires = timedelta(days=settings.jwt_refresh_expiration_days)
    refresh_token = create_refresh_token(
        data={"sub": user.username}, expires_delta=refresh_token_expires
    )

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
    }


@router.post("/token/refresh")
@limiter.limit(settings.rate_limit_auth)
async def refresh_access_token(request: Request, body: RefreshRequest):
    """Generate a new access token using a valid refresh token."""
    del request
    username = verify_refresh_token(body.refresh_token)

    access_token_expires = timedelta(minutes=settings.jwt_expiration_minutes)
    new_access_token = create_access_token(
        data={"sub": username}, expires_delta=access_token_expires
    )

    return {
        "access_token": new_access_token,
        "token_type": "bearer",
    }


@router.post("/token/revoke")
async def revoke_user_token(body: RevokeRequest):
    """Revoke an active token."""
    revoke_token(body.token)
    return {"message": "Token revoked successfully"}

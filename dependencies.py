"""FastAPI dependency injection functions.

This module provides dependency functions for authentication, authorization,
and validation of organizations, projects, and collections across the API.
"""

import re
import uuid
from datetime import timedelta
from typing import Any

import numpy as np
import pandas as pd
from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, OAuth2PasswordBearer

from config import settings
from services.auth_service import verify_jwt_token
from utilities.cassandra_connector import get_cassandra_session
from utilities.organization_utils import get_organization_by_id
from utilities.project_keys_utils import hash_api_key
from utilities.project_utils import get_project_by_id
from utilities.user_utils import get_user_by_username

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/token", auto_error=False)
header_scheme = APIKeyHeader(name="X-API-Key", auto_error=False)


def get_organization_id() -> uuid.UUID:
    """Return the configured organization ID."""
    return uuid.UUID(settings.organization_id)


def get_current_user_from_jwt(token: str = Depends(oauth2_scheme)):
    """Extract and verify user from JWT token.

    Args:
        token: JWT token from OAuth2 scheme.

    Returns:
        User object from database.

    Raises:
        HTTPException: If token is invalid or user not found.
    """
    if token is not None:
        username = verify_jwt_token(token)
        user = get_user_by_username(username)
        if user is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        return user
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No credentials provided")


def validate_api_key(api_key: str, project_id: uuid.UUID):
    """Validate API key against the database.

    Args:
        api_key: The API key to validate.
        project_id: The project UUID to validate against.

    Returns:
        Tuple of (key_type, project_id) if valid.

    Raises:
        HTTPException: If API key is invalid.
    """
    if not api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key is required")
    query = (
        "SELECT key_type, project_id FROM api_keys "
        "WHERE api_key=%s AND project_id=%s LIMIT 1 allow filtering"
    )
    session = get_cassandra_session()
    try:
        key_data = session.execute(query, (hash_api_key(api_key), project_id)).one()
        if key_data:
            return key_data.key_type, key_data.project_id
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid API key")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid API key") from e


def check_api_key(
    key_type: str, key_project_id: uuid.UUID, project_id: uuid.UUID, accepted_roles: list[str]
):
    """Check if API key has appropriate permissions for project access.

    Args:
        key_type: The type of the API key (read/write/master).
        key_project_id: The project ID associated with the key.
        project_id: The project ID being accessed.
        accepted_roles: List of acceptable key types.

    Returns:
        True if access is granted.

    Raises:
        HTTPException: If access is denied.
    """
    if project_id != key_project_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="You cannot access this project"
        )
    if key_type not in accepted_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient privileges to access this resource",
        )
    return True


def verify_superadmin(current_user: Any = Depends(get_current_user_from_jwt)):
    """Verify that the current user has superadmin privileges.

    Args:
        current_user: The current authenticated user.

    Returns:
        The current user if they are a superadmin.

    Raises:
        HTTPException: If user lacks superadmin privileges.
    """
    if current_user.role != "superadmin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have superadmin privileges",
        )
    return current_user


def verify_user_belongs_to_organization(
    current_user=Depends(get_current_user_from_jwt),
    organization_id: uuid.UUID | None = None,
):
    """Verify user belongs to the organization or is a superadmin.

    Args:
        current_user: The current authenticated user.
        organization_id: Optional org ID (used by FastAPI DI).

    Returns:
        The current user if authorized.

    Raises:
        HTTPException: If user doesn't have access to the organization.
    """
    del organization_id
    if current_user.role != "superadmin" and current_user.organization_id != get_organization_id():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have access to this organization.",
        )
    return current_user


def verify_endpoint_access(
    project_id: uuid.UUID,
    jwt_token: str | None = Depends(oauth2_scheme),
    api_key: HTTPAuthorizationCredentials | None = Depends(header_scheme),
):
    """Verify access to an endpoint using either JWT or API key.

    Args:
        project_id: The project UUID being accessed.
        jwt_token: Optional JWT token.
        api_key: Optional API key.

    Returns:
        Authentication result.

    Raises:
        HTTPException: If no valid credentials provided.
    """
    roles = ["master", "read", "write"]
    if jwt_token:
        return verify_user_belongs_to_organization(get_current_user_from_jwt(jwt_token))
    if api_key:
        return verify_api_key_access(project_id, roles, api_key)
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No credentials provided")


def verify_master_access(
    project_id: uuid.UUID,
    jwt_token: str | None = Depends(oauth2_scheme),
    api_key: HTTPAuthorizationCredentials | None = Depends(header_scheme),
):
    """Verify master-level access to a project.

    Args:
        project_id: The project UUID being accessed.
        jwt_token: Optional JWT token.
        api_key: Optional API key.

    Returns:
        Authentication result.

    Raises:
        HTTPException: If no valid master credentials provided.
    """
    if jwt_token:
        return verify_user_belongs_to_organization(get_current_user_from_jwt(jwt_token))
    if api_key:
        return verify_api_key_access(project_id, ["master"], api_key)
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No credentials provided")


def verify_write_access(
    project_id: uuid.UUID,
    jwt_token: str | None = Depends(oauth2_scheme),
    api_key: HTTPAuthorizationCredentials | None = Depends(header_scheme),
):
    """Verify write-level access to a project.

    Args:
        project_id: The project UUID being accessed.
        jwt_token: Optional JWT token.
        api_key: Optional API key.

    Returns:
        Authentication result.

    Raises:
        HTTPException: If no valid write credentials provided.
    """
    if jwt_token:
        return verify_user_belongs_to_organization(get_current_user_from_jwt(jwt_token))
    if api_key:
        return verify_api_key_access(project_id, ["write", "master"], api_key)
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No credentials provided")


def verify_api_key_access(
    project_id: uuid.UUID,
    roles: list[str] | None = None,
    api_key: HTTPAuthorizationCredentials | None = Depends(header_scheme),
):
    """Verify API key access with specific role requirements.

    Args:
        project_id: The project UUID being accessed.
        roles: List of acceptable API key roles (defaults to all).
        api_key: The API key from request header.

    Returns:
        True if API key is valid and has required permissions.

    Raises:
        HTTPException: If API key is invalid or lacks permissions.
    """
    if roles is None:
        roles = ["master", "read", "write"]
    key_type, key_project_id = validate_api_key(api_key.credentials if api_key else "", project_id)
    return check_api_key(key_type, key_project_id, project_id, roles)


def contains_special_characters(
    s, allow_spaces=True, allow_underscores=True, allow_special_chars=True
):
    """Check if a string contains special characters based on rules.

    Args:
        s: String to validate.
        allow_spaces: Whether to allow spaces.
        allow_underscores: Whether to allow underscores.
        allow_special_chars: Whether to allow special characters (except $).

    Returns:
        True if invalid characters are found, False otherwise.
    """
    if s.strip() == "":
        return True

    # Check if dollar sign is present (reserved for flattening)
    if "$" in s:
        return True

    # If allowing special characters, only check for dollar sign
    if allow_special_chars:
        return False

    # Legacy validation (letters, numbers, underscores, spaces only)
    pattern = (
        (r"[^a-zA-Z0-9_ ]" if allow_spaces else r"[^a-zA-Z0-9_]")
        if allow_underscores
        else r"[^a-zA-Z0-9]"
    )
    # Search for the pattern in the string
    return bool(re.search(pattern, s))


def check_organization_exists(organization_id: uuid.UUID):
    """Verify that an organization exists in the database.

    Args:
        organization_id: The organization UUID to check.

    Returns:
        The organization object if found.

    Raises:
        HTTPException: If organization not found.
    """
    organization = get_organization_by_id(organization_id)
    if not organization:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Organization not found.")
    return organization


def check_project_exists(
    project_id: uuid.UUID, organization_id: uuid.UUID = Depends(get_organization_id)
):
    """Verify that a project exists in the database.

    Args:
        project_id: The project UUID to check.
        organization_id: The organization UUID that owns the project.

    Returns:
        The project object if found.

    Raises:
        HTTPException: If project not found.
    """
    project = get_project_by_id(project_id, organization_id)
    if not project:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found.")
    return project


def generate_filter_condition(prop_name, operator, prop_value):
    """Generate a filter condition string for database queries.

    Args:
        prop_name: Property name to filter on.
        operator: Comparison operator (eq, ne, lt, gt, in, contains, etc.).
        prop_value: Value or list of values to compare.

    Returns:
        Filter condition string for query.
    """
    if operator == "eq":
        if isinstance(prop_value, (int, float)):
            return f'"{prop_name}" = {prop_value}'
        return f"\"{prop_name}\" = '{prop_value}'"
    if operator == "ne":
        if isinstance(prop_value, (int, float)):
            return f'"{prop_name}" != {prop_value}'
        return f"\"{prop_name}\" != '{prop_value}'"
    if operator in ("lt", "less"):
        if isinstance(prop_value, (int, float)):
            return f'"{prop_name}" < {prop_value}'
        return f"\"{prop_name}\" < '{prop_value}'"
    if operator in ("lte", "le"):
        if isinstance(prop_value, (int, float)):
            return f'"{prop_name}" <= {prop_value}'
        return f"\"{prop_name}\" <= '{prop_value}'"
    if operator in ("gt", "greater"):
        if isinstance(prop_value, (int, float)):
            return f'"{prop_name}" > {prop_value}'
        return f"\"{prop_name}\" > '{prop_value}'"
    if operator in ("gte", "ge"):
        if isinstance(prop_value, (int, float)):
            return f'"{prop_name}" >= {prop_value}'
        return f"\"{prop_name}\" >= '{prop_value}'"
    if operator == "in":
        if all(isinstance(v, (int, float)) for v in prop_value):
            values = ", ".join([str(v) for v in prop_value])
        else:
            values = ", ".join([f"'{v}'" for v in prop_value])
        return f'"{prop_name}" IN ({values})'
    if operator == "contains":
        return f"\"{prop_name}\" CONTAINS '{prop_value}'"
    if operator == "not_contains":
        return f"\"{prop_name}\" NOT CONTAINS '{prop_value}'"
    return ""


def get_interval_start(timestamp, reference_time, interval_unit, interval_value=1):
    """Calculate the start of an interval aligned to a reference time.

    Args:
        timestamp: The timestamp to align.
        reference_time: The reference point for interval alignment.
        interval_unit: Unit of interval (minutes, hours, days, weeks, months).
        interval_value: Number of units per interval.

    Returns:
        The aligned interval start timestamp.

    Raises:
        ValueError: If interval_unit is not supported.
    """
    if interval_unit.lower() in ["minutes", "minute"]:
        # Align to the start of the minute interval from the reference point
        total_minutes_since_reference = int((timestamp - reference_time).total_seconds() // 60)
        interval_start_minutes = (total_minutes_since_reference // interval_value) * interval_value
        interval_start = reference_time + timedelta(minutes=interval_start_minutes)
        return interval_start.replace(second=0, microsecond=0)

    if interval_unit.lower() in ["hours", "hour"]:
        # Align to the start of the hour interval from the reference point
        total_hours_since_reference = int((timestamp - reference_time).total_seconds() // 3600)
        interval_start_hours = (total_hours_since_reference // interval_value) * interval_value
        interval_start = reference_time + timedelta(hours=interval_start_hours)
        return interval_start.replace(minute=0, second=0, microsecond=0)

    if interval_unit.lower() in ["day", "days"]:
        # Align to the start of the day interval, using the reference time
        start_date = reference_time.replace(hour=0, minute=0, second=0, microsecond=0)
        days_offset = (timestamp - start_date).days % interval_value
        interval_start = start_date + timedelta(days=(timestamp - start_date).days - days_offset)
        return interval_start

    if interval_unit.lower() in ["weeks", "week"]:
        # Align to the start of the week interval, starting from the reference
        # week (align to Monday)
        start_of_week = reference_time - timedelta(days=reference_time.weekday())
        weeks_since_reference = (timestamp - start_of_week).days // 7
        interval_start_week = (weeks_since_reference // interval_value) * interval_value
        interval_start = start_of_week + timedelta(weeks=interval_start_week)
        return interval_start.replace(hour=0, minute=0, second=0, microsecond=0)

    if interval_unit.lower() in ["month", "months"]:
        # Calculate the "interval_value" month start from the reference time
        month_offset = (
            (timestamp.year - reference_time.year) * 12 + timestamp.month - reference_time.month
        ) % interval_value
        new_month = timestamp.month - month_offset
        if new_month <= 0:
            new_month += 12
            return timestamp.replace(
                year=timestamp.year - 1,
                month=new_month,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
        return timestamp.replace(month=new_month, day=1, hour=0, minute=0, second=0, microsecond=0)

    raise ValueError(f"Unsupported interval unit: {interval_unit}")


# Modify the aggregate_data function to support intervals consistently
# aligned with reference time


def aggregate_data(data, interval_value, interval_unit, stat, attribute, group_by):
    """Aggregate time-series data into intervals with statistics.

    Args:
        data: List of data points to aggregate.
        interval_value: Number of interval units.
        interval_unit: Unit of time for intervals.
        stat: Statistical function to apply (mean, sum, min, max, count).
        attribute: Attribute name to aggregate.
        group_by: Optional attribute to group by.

    Returns:
        Aggregated data with interval timestamps and statistics.
    """
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Convert DECIMAL values to float for pandas processing
    if attribute in df.columns:
        df[attribute] = pd.to_numeric(df[attribute], errors="coerce")

    # If group_by is not in the DataFrame, raise an error
    if group_by not in df.columns:
        raise KeyError(f"The column '{group_by}' does not exist in the data.")

    # Find the earliest timestamp to use as a reference point for interval
    # calculation
    reference_time = df["timestamp"].min()

    # Calculate the interval start times consistently using the reference time
    df["interval_start"] = df.apply(
        lambda row: get_interval_start(
            row["timestamp"], reference_time, interval_unit, interval_value
        ),
        axis=1,
    )

    # Check if 'interval_start' was successfully added
    if "interval_start" not in df.columns:
        raise KeyError("The 'interval_start' column could not be created.")
    if stat == "avg":
        grouped = df.groupby([group_by, "interval_start"]).agg({attribute: "mean"}).reset_index()
    elif stat == "max":
        grouped = df.groupby([group_by, "interval_start"]).agg({attribute: "max"}).reset_index()
    elif stat == "min":
        grouped = df.groupby([group_by, "interval_start"]).agg({attribute: "min"}).reset_index()
    elif stat == "sum":
        grouped = df.groupby([group_by, "interval_start"]).agg({attribute: "sum"}).reset_index()
    elif stat == "count":
        grouped = df.groupby([group_by, "interval_start"]).agg({attribute: "count"}).reset_index()
    elif stat == "distinct":
        # For distinct operations, we shouldn't reach this point as they're handled separately
        # But if we do, just return the distinct values per group and interval
        grouped = df.groupby([group_by, "interval_start"])[attribute].nunique().reset_index()
        grouped.rename(columns={attribute: f"distinct_{attribute}"}, inplace=True)
        return grouped.to_dict(orient="records")
    else:
        raise ValueError(f"Unsupported statistical operation: {stat}")

    grouped.rename(columns={attribute: f"{stat}_{attribute}"}, inplace=True)

    # Round the values to 3 decimal places
    grouped[f"{stat}_{attribute}"] = grouped[f"{stat}_{attribute}"].round(3)

    # Replace NaN and infinite values with None for JSON serialization
    grouped = grouped.replace([float("inf"), float("-inf"), np.inf, -np.inf, np.nan, pd.NA], None)
    return grouped.to_dict(orient="records")

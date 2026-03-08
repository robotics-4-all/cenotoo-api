"""Tests for dependencies.py — covers uncovered dependency functions.

Targets lines 44-50, 141-143, 164-169, 190-194, 215-219, 240-243,
258-272, 287-290, 306-309, 323-357, 375-418, 439-488.
"""

import uuid
from collections import namedtuple
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

UserRow = namedtuple("UserRow", ["username", "role", "organization_id"])
OrgRow = namedtuple("OrgRow", ["id", "organization_name"])
ProjectRow = namedtuple("ProjectRow", ["id", "organization_id", "project_name"])

_org_id = uuid.uuid4()
_superadmin = UserRow(username="admin", role="superadmin", organization_id=_org_id)
_regular_user = UserRow(username="bob", role="user", organization_id=_org_id)
_other_org_user = UserRow(username="eve", role="user", organization_id=uuid.uuid4())


# ===================================================================
# get_current_user_from_jwt  (lines 44-50)
# ===================================================================


class TestGetCurrentUserFromJwt:
    """Test JWT-based user extraction."""

    @patch("dependencies.get_user_by_username")
    @patch("dependencies.verify_jwt_token")
    def test_valid_token_user_found(self, mock_verify, mock_get_user):
        """Test that a valid JWT token returns the corresponding user."""
        mock_verify.return_value = "testuser"
        mock_get_user.return_value = _superadmin
        from dependencies import get_current_user_from_jwt

        result = get_current_user_from_jwt(token="valid-token")
        assert result.username == "admin"
        mock_verify.assert_called_once_with("valid-token")
        mock_get_user.assert_called_once_with("testuser")

    @patch("dependencies.get_user_by_username")
    @patch("dependencies.verify_jwt_token")
    def test_valid_token_user_not_found(self, mock_verify, mock_get_user):
        """Test that a valid JWT token for a non-existent user raises a 401 error."""
        mock_verify.return_value = "ghost"
        mock_get_user.return_value = None
        from dependencies import get_current_user_from_jwt

        with pytest.raises(HTTPException) as exc_info:
            get_current_user_from_jwt(token="valid-token")
        assert exc_info.value.status_code == 401
        assert "User not found" in exc_info.value.detail

    def test_no_token_raises(self):
        """Test that missing JWT token raises a 401 error."""
        from dependencies import get_current_user_from_jwt

        with pytest.raises(HTTPException) as exc_info:
            get_current_user_from_jwt(token=None)
        assert exc_info.value.status_code == 401
        assert "No credentials provided" in exc_info.value.detail


# ===================================================================
# verify_user_belongs_to_organization  (lines 141-143)
# ===================================================================


class TestVerifyUserBelongsToOrganization:
    """Test organization membership verification."""

    @patch("dependencies.get_organization_id")
    def test_superadmin_passes(self, mock_get_org_id):
        """Test that a superadmin user passes organization verification."""
        mock_get_org_id.return_value = _org_id
        from dependencies import verify_user_belongs_to_organization

        result = verify_user_belongs_to_organization(
            current_user=_superadmin, organization_id=_org_id
        )
        assert result == _superadmin

    @patch("dependencies.get_organization_id")
    def test_same_org_passes(self, mock_get_org_id):
        """Test that a user belonging to the same organization passes verification."""
        mock_get_org_id.return_value = _org_id
        from dependencies import verify_user_belongs_to_organization

        result = verify_user_belongs_to_organization(
            current_user=_regular_user, organization_id=_org_id
        )
        assert result == _regular_user

    @patch("dependencies.get_organization_id")
    def test_different_org_raises(self, mock_get_org_id):
        """Test that a user from a different organization raises a 403 error."""
        mock_get_org_id.return_value = _org_id
        from dependencies import verify_user_belongs_to_organization

        with pytest.raises(HTTPException) as exc_info:
            verify_user_belongs_to_organization(
                current_user=_other_org_user, organization_id=_org_id
            )
        assert exc_info.value.status_code == 403
        assert "access" in exc_info.value.detail.lower()


# ===================================================================
# verify_endpoint_access  (lines 164-169)
# ===================================================================


class TestVerifyEndpointAccess:
    """Test dual-auth endpoint access."""

    @patch("dependencies.verify_user_belongs_to_organization")
    @patch("dependencies.get_current_user_from_jwt")
    def test_with_jwt(self, mock_jwt, mock_org):
        """Test that endpoint access is granted with a valid JWT token."""
        mock_jwt.return_value = _superadmin
        mock_org.return_value = _superadmin
        from dependencies import verify_endpoint_access

        result = verify_endpoint_access(
            project_id=uuid.uuid4(), jwt_token="valid-jwt", api_key=None
        )
        assert result == _superadmin

    @patch("dependencies.verify_api_key_access")
    def test_with_api_key(self, mock_api_access):
        """Test that endpoint access is granted with a valid API key."""
        mock_api_access.return_value = True
        from dependencies import verify_endpoint_access

        api_key_mock = MagicMock()
        api_key_mock.credentials = "test-key"
        result = verify_endpoint_access(
            project_id=uuid.uuid4(), jwt_token=None, api_key=api_key_mock
        )
        assert result is True

    def test_no_credentials_raises(self):
        """Test that missing credentials raises a 401 error for endpoint access."""
        from dependencies import verify_endpoint_access

        with pytest.raises(HTTPException) as exc_info:
            verify_endpoint_access(project_id=uuid.uuid4(), jwt_token=None, api_key=None)
        assert exc_info.value.status_code == 401


# ===================================================================
# verify_master_access  (lines 190-194)
# ===================================================================


class TestVerifyMasterAccess:
    """Test master-level access verification."""

    @patch("dependencies.verify_user_belongs_to_organization")
    @patch("dependencies.get_current_user_from_jwt")
    def test_with_jwt(self, mock_jwt, mock_org):
        """Test that master access is granted with a valid JWT token."""
        mock_jwt.return_value = _superadmin
        mock_org.return_value = _superadmin
        from dependencies import verify_master_access

        result = verify_master_access(project_id=uuid.uuid4(), jwt_token="valid-jwt", api_key=None)
        assert result == _superadmin

    @patch("dependencies.verify_api_key_access")
    def test_with_api_key(self, mock_api_access):
        """Test that master access is granted with a valid API key."""
        mock_api_access.return_value = True
        from dependencies import verify_master_access

        api_key_mock = MagicMock()
        api_key_mock.credentials = "master-key"
        result = verify_master_access(project_id=uuid.uuid4(), jwt_token=None, api_key=api_key_mock)
        assert result is True
        mock_api_access.assert_called_once()
        call_args = mock_api_access.call_args
        assert call_args[0][1] == ["master"]

    def test_no_credentials_raises(self):
        """Test that missing credentials raises a 401 error for master access."""
        from dependencies import verify_master_access

        with pytest.raises(HTTPException) as exc_info:
            verify_master_access(project_id=uuid.uuid4(), jwt_token=None, api_key=None)
        assert exc_info.value.status_code == 401


# ===================================================================
# verify_write_access  (lines 215-219)
# ===================================================================


class TestVerifyWriteAccess:
    """Test write-level access verification."""

    @patch("dependencies.verify_user_belongs_to_organization")
    @patch("dependencies.get_current_user_from_jwt")
    def test_with_jwt(self, mock_jwt, mock_org):
        """Test that write access is granted with a valid JWT token."""
        mock_jwt.return_value = _superadmin
        mock_org.return_value = _superadmin
        from dependencies import verify_write_access

        result = verify_write_access(project_id=uuid.uuid4(), jwt_token="valid-jwt", api_key=None)
        assert result == _superadmin

    @patch("dependencies.verify_api_key_access")
    def test_with_api_key(self, mock_api_access):
        """Test that write access is granted with a valid API key."""
        mock_api_access.return_value = True
        from dependencies import verify_write_access

        api_key_mock = MagicMock()
        api_key_mock.credentials = "write-key"
        result = verify_write_access(project_id=uuid.uuid4(), jwt_token=None, api_key=api_key_mock)
        assert result is True
        call_args = mock_api_access.call_args
        assert call_args[0][1] == ["write", "master"]

    def test_no_credentials_raises(self):
        """Test that missing credentials raises a 401 error for write access."""
        from dependencies import verify_write_access

        with pytest.raises(HTTPException) as exc_info:
            verify_write_access(project_id=uuid.uuid4(), jwt_token=None, api_key=None)
        assert exc_info.value.status_code == 401


# ===================================================================
# verify_api_key_access  (lines 240-243)
# ===================================================================


class TestVerifyApiKeyAccess:
    """Test API key access with role validation."""

    @patch("dependencies.check_api_key")
    @patch("dependencies.validate_api_key")
    def test_valid_key_with_roles(self, mock_validate, mock_check):
        """Test that a valid API key with specific roles is accepted."""
        pid = uuid.uuid4()
        mock_validate.return_value = ("master", pid)
        mock_check.return_value = True
        from dependencies import verify_api_key_access

        api_key_mock = MagicMock()
        api_key_mock.credentials = "test-key"
        result = verify_api_key_access(pid, ["master"], api_key_mock)
        assert result is True
        mock_validate.assert_called_once_with("test-key", pid)
        mock_check.assert_called_once_with("master", pid, pid, ["master"])

    @patch("dependencies.check_api_key")
    @patch("dependencies.validate_api_key")
    def test_roles_default_to_all(self, mock_validate, mock_check):
        """Test that API key roles default to all roles when not specified."""
        pid = uuid.uuid4()
        mock_validate.return_value = ("read", pid)
        mock_check.return_value = True
        from dependencies import verify_api_key_access

        api_key_mock = MagicMock()
        api_key_mock.credentials = "test-key"
        result = verify_api_key_access(pid, None, api_key_mock)
        assert result is True
        mock_check.assert_called_once_with("read", pid, pid, ["master", "read", "write"])

    @patch("dependencies.check_api_key")
    @patch("dependencies.validate_api_key")
    def test_no_api_key_uses_empty_string(self, mock_validate, mock_check):
        """Test that missing API key uses an empty string for validation."""
        pid = uuid.uuid4()
        mock_validate.return_value = ("read", pid)
        mock_check.return_value = True
        from dependencies import verify_api_key_access

        result = verify_api_key_access(pid, ["read"], None)
        assert result is True
        mock_validate.assert_called_once_with("", pid)


# ===================================================================
# contains_special_characters  (lines 258-272)
# ===================================================================


class TestContainsSpecialCharacters:
    """Test special character validation in dependencies module."""

    def test_empty_string_returns_true(self):
        """Test that an empty string is considered to contain special characters."""
        from dependencies import contains_special_characters

        assert contains_special_characters("") is True

    def test_whitespace_only_returns_true(self):
        """Test that a whitespace-only string is considered to contain special characters."""
        from dependencies import contains_special_characters

        assert contains_special_characters("   ") is True

    def test_dollar_sign_returns_true(self):
        """Test that a string with a dollar sign is considered to contain special characters."""
        from dependencies import contains_special_characters

        assert contains_special_characters("price$") is True

    def test_allow_special_chars_returns_false(self):
        """Test that allowing special characters returns False for strings with special chars."""
        from dependencies import contains_special_characters

        assert contains_special_characters("hello!@#") is False

    def test_disallow_special_chars_with_special(self):
        """Test that disallowing special characters returns True for strings with special chars."""
        from dependencies import contains_special_characters

        assert contains_special_characters("hello!", allow_special_chars=False) is True

    def test_disallow_special_chars_clean_string(self):
        """Test that disallowing special characters returns False for clean strings."""
        from dependencies import contains_special_characters

        assert contains_special_characters("hello_world", allow_special_chars=False) is False

    def test_disallow_spaces(self):
        """Test that disallowing spaces returns True for strings with spaces."""
        from dependencies import contains_special_characters

        assert (
            contains_special_characters(
                "hello world", allow_spaces=False, allow_special_chars=False
            )
            is True
        )

    def test_allow_spaces(self):
        """Test that allowing spaces returns False for strings with spaces."""
        from dependencies import contains_special_characters

        assert (
            contains_special_characters("hello world", allow_spaces=True, allow_special_chars=False)
            is False
        )

    def test_disallow_underscores(self):
        """Test that disallowing underscores returns True for strings with underscores."""
        from dependencies import contains_special_characters

        assert (
            contains_special_characters(
                "hello_world",
                allow_underscores=False,
                allow_special_chars=False,
            )
            is True
        )

    def test_alphanumeric_only(self):
        """Test that an alphanumeric string returns False when special chars are disallowed."""
        from dependencies import contains_special_characters

        assert (
            contains_special_characters(
                "hello123",
                allow_underscores=False,
                allow_special_chars=False,
            )
            is False
        )


# ===================================================================
# check_organization_exists  (lines 287-290)
# ===================================================================


class TestCheckOrganizationExists:
    """Test organization existence check."""

    @patch("dependencies.get_organization_by_id")
    def test_found(self, mock_get_org):
        """Test that an existing organization is returned successfully."""
        org = OrgRow(id=_org_id, organization_name="test_org")
        mock_get_org.return_value = org
        from dependencies import check_organization_exists

        result = check_organization_exists(_org_id)
        assert result == org

    @patch("dependencies.get_organization_by_id")
    def test_not_found(self, mock_get_org):
        """Test that a non-existent organization raises a 404 error."""
        mock_get_org.return_value = None
        from dependencies import check_organization_exists

        with pytest.raises(HTTPException) as exc_info:
            check_organization_exists(_org_id)
        assert exc_info.value.status_code == 404
        assert "Organization not found" in exc_info.value.detail


# ===================================================================
# check_project_exists  (lines 306-309)
# ===================================================================


class TestCheckProjectExists:
    """Test project existence check."""

    @patch("dependencies.get_project_by_id")
    def test_found(self, mock_get_proj):
        """Test that an existing project is returned successfully."""
        proj = ProjectRow(id=uuid.uuid4(), organization_id=_org_id, project_name="proj")
        mock_get_proj.return_value = proj
        from dependencies import check_project_exists

        result = check_project_exists(proj.id, _org_id)
        assert result == proj

    @patch("dependencies.get_project_by_id")
    def test_not_found(self, mock_get_proj):
        """Test that a non-existent project raises a 404 error."""
        mock_get_proj.return_value = None
        from dependencies import check_project_exists

        with pytest.raises(HTTPException) as exc_info:
            check_project_exists(uuid.uuid4(), _org_id)
        assert exc_info.value.status_code == 404
        assert "Project not found" in exc_info.value.detail


# ===================================================================
# generate_filter_condition  (lines 323-357)
# Duplicate of core.filters — simple coverage tests
# ===================================================================


class TestGenerateFilterConditionDeps:
    """Cover the duplicate generate_filter_condition in dependencies.py."""

    def test_eq_string(self):
        """Test generating an equality filter condition for a string value."""
        from dependencies import generate_filter_condition

        assert generate_filter_condition("name", "eq", "Alice") == "\"name\" = 'Alice'"

    def test_eq_numeric(self):
        """Test generating an equality filter condition for a numeric value."""
        from dependencies import generate_filter_condition

        assert generate_filter_condition("age", "eq", 42) == '"age" = 42'

    def test_ne_string(self):
        """Test generating a not-equal filter condition for a string value."""
        from dependencies import generate_filter_condition

        assert generate_filter_condition("s", "ne", "x") == "\"s\" != 'x'"

    def test_ne_numeric(self):
        """Test generating a not-equal filter condition for a numeric value."""
        from dependencies import generate_filter_condition

        assert generate_filter_condition("s", "ne", 5) == '"s" != 5'

    def test_lt_numeric(self):
        """Test generating a less-than filter condition for a numeric value."""
        from dependencies import generate_filter_condition

        assert generate_filter_condition("v", "lt", 10) == '"v" < 10'

    def test_lt_string(self):
        """Test generating a less-than filter condition for a string value."""
        from dependencies import generate_filter_condition

        assert generate_filter_condition("v", "less", "a") == "\"v\" < 'a'"

    def test_lte_numeric(self):
        """Test generating a less-than-or-equal filter condition for a numeric value."""
        from dependencies import generate_filter_condition

        assert generate_filter_condition("v", "lte", 10) == '"v" <= 10'

    def test_lte_string(self):
        """Test generating a less-than-or-equal filter condition for a string value."""
        from dependencies import generate_filter_condition

        assert generate_filter_condition("v", "le", "a") == "\"v\" <= 'a'"

    def test_gt_numeric(self):
        """Test generating a greater-than filter condition for a numeric value."""
        from dependencies import generate_filter_condition

        assert generate_filter_condition("v", "gt", 5) == '"v" > 5'

    def test_gt_string(self):
        """Test generating a greater-than filter condition for a string value."""
        from dependencies import generate_filter_condition

        assert generate_filter_condition("v", "greater", "a") == "\"v\" > 'a'"

    def test_gte_numeric(self):
        """Test generating a greater-than-or-equal filter condition for a numeric value."""
        from dependencies import generate_filter_condition

        assert generate_filter_condition("v", "gte", 5) == '"v" >= 5'

    def test_gte_string(self):
        """Test generating a greater-than-or-equal filter condition for a string value."""
        from dependencies import generate_filter_condition

        assert generate_filter_condition("v", "ge", "a") == "\"v\" >= 'a'"

    def test_in_integers(self):
        """Test generating an IN filter condition for a list of integers."""
        from dependencies import generate_filter_condition

        assert generate_filter_condition("id", "in", [1, 2]) == '"id" IN (1, 2)'

    def test_in_strings(self):
        """Test generating an IN filter condition for a list of strings."""
        from dependencies import generate_filter_condition

        assert generate_filter_condition("s", "in", ["a", "b"]) == "\"s\" IN ('a', 'b')"

    def test_contains(self):
        """Test generating a CONTAINS filter condition."""
        from dependencies import generate_filter_condition

        assert generate_filter_condition("t", "contains", "x") == "\"t\" CONTAINS 'x'"

    def test_not_contains(self):
        """Test generating a NOT CONTAINS filter condition."""
        from dependencies import generate_filter_condition

        assert generate_filter_condition("t", "not_contains", "x") == "\"t\" NOT CONTAINS 'x'"

    def test_unknown_operator(self):
        """Test that an unknown operator returns an empty string."""
        from dependencies import generate_filter_condition

        assert generate_filter_condition("x", "LIKE", "y") == ""


# ===================================================================
# get_interval_start  (lines 375-418)
# Duplicate of core.aggregation — simple coverage tests
# ===================================================================


class TestGetIntervalStartDeps:
    """Cover the duplicate get_interval_start in dependencies.py."""

    @pytest.fixture()
    def ref(self):
        """Provide a reference datetime for interval tests."""
        return datetime(2024, 1, 1, 0, 0, 0)

    def test_minutes(self, ref):
        """Test interval start calculation for plural minutes."""
        from dependencies import get_interval_start

        ts = ref + timedelta(minutes=7)
        result = get_interval_start(ts, ref, "minutes", 5)
        assert result == datetime(2024, 1, 1, 0, 5, 0)

    def test_minute_singular(self, ref):
        """Test interval start calculation for singular minute."""
        from dependencies import get_interval_start

        ts = ref + timedelta(minutes=10)
        result = get_interval_start(ts, ref, "minute", 5)
        assert result == datetime(2024, 1, 1, 0, 10, 0)

    def test_hours(self, ref):
        """Test interval start calculation for plural hours."""
        from dependencies import get_interval_start

        ts = ref + timedelta(hours=3, minutes=30)
        result = get_interval_start(ts, ref, "hours", 2)
        assert result == datetime(2024, 1, 1, 2, 0, 0)

    def test_hour_singular(self, ref):
        """Test interval start calculation for singular hour."""
        from dependencies import get_interval_start

        ts = ref + timedelta(hours=4)
        result = get_interval_start(ts, ref, "hour", 2)
        assert result == datetime(2024, 1, 1, 4, 0, 0)

    def test_days(self, ref):
        """Test interval start calculation for plural days."""
        from dependencies import get_interval_start

        ts = ref + timedelta(days=5)
        result = get_interval_start(ts, ref, "days", 3)
        assert result == datetime(2024, 1, 4, 0, 0, 0)

    def test_day_singular(self, ref):
        """Test interval start calculation for singular day."""
        from dependencies import get_interval_start

        ts = ref + timedelta(days=2)
        result = get_interval_start(ts, ref, "day", 1)
        assert result.hour == 0

    def test_weeks(self, ref):
        """Test interval start calculation for plural weeks."""
        from dependencies import get_interval_start

        ts = ref + timedelta(weeks=3)
        result = get_interval_start(ts, ref, "weeks", 2)
        assert result.hour == 0
        assert result.minute == 0

    def test_week_singular(self, ref):
        """Test interval start calculation for singular week."""
        from dependencies import get_interval_start

        ts = ref + timedelta(weeks=1)
        result = get_interval_start(ts, ref, "week", 1)
        assert result.hour == 0

    def test_months(self, ref):
        """Test interval start calculation for plural months."""
        from dependencies import get_interval_start

        ts = datetime(2024, 4, 15)
        result = get_interval_start(ts, ref, "months", 3)
        assert result.day == 1

    def test_month_singular(self, ref):
        """Test interval start calculation for singular month."""
        from dependencies import get_interval_start

        ts = datetime(2024, 2, 15)
        result = get_interval_start(ts, ref, "month", 1)
        assert result.day == 1

    def test_month_wrap_to_previous_year(self, ref):
        """Cover the branch where new_month <= 0 (line 411-414)."""
        del ref
        from dependencies import get_interval_start

        # reference_time in March, timestamp in January with interval_value=3
        # month_offset = (2024-2024)*12 + 1 - 3 = -2, % 3 = 1
        # new_month = 1 - 1 = 0 → triggers the <= 0 branch
        ref_march = datetime(2024, 3, 1)
        ts = datetime(2024, 1, 15)
        result = get_interval_start(ts, ref_march, "month", 3)
        # new_month = 1 - ((-2) % 3) = 1 - 1 = 0 → new_month += 12 = 12, year - 1
        assert result.year == 2023
        assert result.month == 12
        assert result.day == 1

    def test_unsupported_unit_raises(self, ref):
        """Test that an unsupported interval unit raises a ValueError."""
        from dependencies import get_interval_start

        with pytest.raises(ValueError, match="Unsupported interval unit"):
            get_interval_start(ref, ref, "seconds", 1)


# ===================================================================
# aggregate_data  (lines 439-488)
# Duplicate of core.aggregation — simple coverage tests
# ===================================================================


class TestAggregateDataDeps:
    """Cover the duplicate aggregate_data in dependencies.py."""

    @pytest.fixture()
    def sample_data(self):
        """Provide sample time-series data for aggregation tests."""
        base = datetime(2024, 1, 1, 0, 0, 0)
        return [
            {
                "timestamp": (base + timedelta(minutes=i)).isoformat(),
                "sensor": "s1",
                "value": float(i),
            }
            for i in range(10)
        ]

    def test_avg(self, sample_data):
        """Test average aggregation over sample data."""
        from dependencies import aggregate_data

        result = aggregate_data(sample_data, 5, "minutes", "avg", "value", "sensor")
        assert len(result) > 0
        assert any("avg_value" in r for r in result)

    def test_sum(self, sample_data):
        """Test sum aggregation over sample data."""
        from dependencies import aggregate_data

        result = aggregate_data(sample_data, 5, "minutes", "sum", "value", "sensor")
        assert any("sum_value" in r for r in result)

    def test_min(self, sample_data):
        """Test minimum aggregation over sample data."""
        from dependencies import aggregate_data

        result = aggregate_data(sample_data, 5, "minutes", "min", "value", "sensor")
        assert any("min_value" in r for r in result)

    def test_max(self, sample_data):
        """Test maximum aggregation over sample data."""
        from dependencies import aggregate_data

        result = aggregate_data(sample_data, 5, "minutes", "max", "value", "sensor")
        assert any("max_value" in r for r in result)

    def test_count(self, sample_data):
        """Test count aggregation over sample data."""
        from dependencies import aggregate_data

        result = aggregate_data(sample_data, 5, "minutes", "count", "value", "sensor")
        assert any("count_value" in r for r in result)

    def test_distinct(self, sample_data):
        """Test distinct count aggregation over sample data."""
        from dependencies import aggregate_data

        result = aggregate_data(sample_data, 5, "minutes", "distinct", "value", "sensor")
        assert any("distinct_value" in r for r in result)

    def test_unsupported_stat_raises(self, sample_data):
        """Test that an unsupported statistical operation raises a ValueError."""
        from dependencies import aggregate_data

        with pytest.raises(ValueError, match="Unsupported statistical operation"):
            aggregate_data(sample_data, 5, "minutes", "median", "value", "sensor")

    def test_missing_group_by_raises(self, sample_data):
        """Test that a missing group-by column raises a KeyError."""
        from dependencies import aggregate_data

        with pytest.raises(KeyError, match="does not exist"):
            aggregate_data(sample_data, 5, "minutes", "avg", "value", "nonexistent")

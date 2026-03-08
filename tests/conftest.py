"""
Shared test fixtures for the Cenotoo API test suite.

Provides:
- Mocked Cassandra session (no real DB needed)
- Mocked Kafka producer/admin client
- FastAPI TestClient with auth override
- Common test data factories
"""
# pylint: disable=redefined-outer-name

import uuid
from collections import namedtuple
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from config import Settings

# ---------------------------------------------------------------------------
# Settings override: use test defaults so we never hit real infra
# ---------------------------------------------------------------------------

_test_org_id = str(uuid.uuid4())


@pytest.fixture(autouse=True)
def _override_settings(monkeypatch):
    """Force safe settings for every test."""
    monkeypatch.setattr(
        "config.settings",
        Settings(
            environment="development",
            jwt_secret_key="test-secret-key-for-tests",
            jwt_algorithm="HS256",
            jwt_expiration_minutes=30,
            api_key_secret="test-api-key-secret",
            kafka_brokers="localhost:9092",
            cassandra_contact_points="localhost",
            cassandra_port=9042,
            cassandra_keyspace="test_metadata",
            organization_id=_test_org_id,
            cors_origins="*",
        ),
    )


# ---------------------------------------------------------------------------
# Cassandra mock
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_cassandra_session():
    """Return the same MagicMock that module-level session variables hold.

    The root conftest patches get_cassandra_session() at pytest_configure time
    (before imports), so every module-level ``session = get_cassandra_session()``
    already references ``conftest._mock_session``. We must reuse that exact
    object so test-side ``mock_cassandra_session.execute.side_effect`` reaches
    the routers.
    """
    from conftest import _mock_session

    _mock_session.execute.reset_mock()
    _mock_session.execute.return_value = []
    _mock_session.execute.side_effect = None
    return _mock_session


@pytest.fixture(autouse=True)
def _patch_cassandra(mock_cassandra_session):
    """Patch get_cassandra_session globally so no test hits a real cluster."""
    with (
        patch(
            "utilities.cassandra_connector.get_cassandra_session",
            return_value=mock_cassandra_session,
        ),
        patch(
            "dependencies.get_cassandra_session",
            return_value=mock_cassandra_session,
        ),
    ):
        yield mock_cassandra_session


# ---------------------------------------------------------------------------
# Kafka mock
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _patch_kafka():
    """Patch Kafka so no test connects to a real broker."""
    mock_admin = MagicMock()
    mock_producer = MagicMock()

    with (
        patch("utilities.kafka_connector.get_kafka_admin_client", return_value=mock_admin),
        patch("utilities.kafka_connector.get_kafka_producer", return_value=mock_producer),
        patch("utilities.kafka_topics.get_kafka_admin_client", return_value=mock_admin),
        patch("confluent_kafka.Producer", return_value=mock_producer),
    ):
        yield {"admin": mock_admin, "producer": mock_producer}


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

UserRow = namedtuple("UserRow", ["id", "username", "password", "role", "organization_id"])

_test_user = UserRow(
    id=uuid.uuid4(),
    username="testuser",
    password="$2b$12$hashed_password_placeholder",
    role="superadmin",
    organization_id=uuid.UUID(_test_org_id),
)


@pytest.fixture()
def test_user():
    """A fake user row as returned by Cassandra."""
    return _test_user


@pytest.fixture()
def auth_token():
    """Generate a valid JWT token for test_user."""
    from services.auth_service import create_access_token

    return create_access_token(data={"sub": _test_user.username})


@pytest.fixture()
def auth_headers(auth_token):
    """Authorization header dict ready for TestClient requests."""
    return {"Authorization": f"Bearer {auth_token}"}


# ---------------------------------------------------------------------------
# FastAPI TestClient
# ---------------------------------------------------------------------------


@pytest.fixture()
def client(mock_cassandra_session, test_user):
    """Provide a FastAPI TestClient with mocked dependencies."""
    del mock_cassandra_session
    from dependencies import (
        check_organization_exists,
        check_project_exists,
        get_current_user_from_jwt,
        get_organization_id,
        verify_endpoint_access,
        verify_master_access,
        verify_superadmin,
        verify_user_belongs_to_organization,
        verify_write_access,
    )
    from main import app
    from utilities.collection_utils import check_collection_exists

    org_uuid = uuid.UUID(_test_org_id)

    app.dependency_overrides[get_current_user_from_jwt] = lambda: test_user
    app.dependency_overrides[get_organization_id] = lambda: org_uuid
    app.dependency_overrides[verify_user_belongs_to_organization] = lambda: test_user
    app.dependency_overrides[verify_superadmin] = lambda: test_user
    app.dependency_overrides[verify_endpoint_access] = lambda: True
    app.dependency_overrides[verify_master_access] = lambda: True
    app.dependency_overrides[verify_write_access] = lambda: True

    OrgRow = namedtuple(
        "OrgRow", ["id", "organization_name", "description", "tags", "creation_date"]
    )
    mock_org = OrgRow(
        id=org_uuid,
        organization_name="test_org",
        description="Test organization",
        tags=["test"],
        creation_date=datetime.utcnow(),
    )
    app.dependency_overrides[check_organization_exists] = lambda: mock_org

    ProjectRow = namedtuple(
        "ProjectRow",
        [
            "id",
            "organization_id",
            "project_name",
            "description",
            "tags",
            "creation_date",
        ],
    )
    mock_project = ProjectRow(
        id=uuid.uuid4(),
        organization_id=org_uuid,
        project_name="test_project",
        description="Test project",
        tags=["test"],
        creation_date=datetime.utcnow(),
    )
    app.dependency_overrides[check_project_exists] = lambda: mock_project

    CollectionRow = namedtuple(
        "CollectionRow",
        [
            "id",
            "organization_id",
            "project_id",
            "collection_name",
            "description",
            "tags",
            "creation_date",
        ],
    )
    mock_collection = CollectionRow(
        id=uuid.uuid4(),
        organization_id=org_uuid,
        project_id=mock_project.id,
        collection_name="test_collection",
        description="Test collection",
        tags=["test"],
        creation_date=datetime.utcnow(),
    )
    app.dependency_overrides[check_collection_exists] = lambda: mock_collection

    with TestClient(app, raise_server_exceptions=False) as tc:
        yield tc

    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Common test data factories
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_org_id():
    """Return a sample organization UUID for testing."""
    return uuid.UUID(_test_org_id)


@pytest.fixture()
def sample_project_id():
    """Return a sample project UUID for testing."""
    return uuid.uuid4()


@pytest.fixture()
def sample_collection_id():
    """Return a sample collection UUID for testing."""
    return uuid.uuid4()

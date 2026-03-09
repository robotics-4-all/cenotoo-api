# TESTS — KNOWLEDGE BASE

**446 tests** / pytest + pytest-asyncio / `asyncio_mode = "auto"`

## CRITICAL: CASSANDRA PATCHING ORDER

Root `conftest.py` patches `get_cassandra_session()` in `pytest_configure` — **before any module imports**. This is required because 8+ utility modules call `session = get_cassandra_session()` at module level. If this patch runs late, tests hit a real Cassandra (or crash).

```
pytest_configure → patch get_cassandra_session → modules import → tests run
```

**Never move or rename** root `conftest.py`. Never import utility modules before this patch fires.

## FIXTURES (tests/conftest.py)

| Fixture | Autouse | Purpose |
|---------|---------|---------|
| `_override_settings` | YES | Forces test Settings (jwt_secret, api_key_secret, etc.) |
| `mock_cassandra_session` | no | Returns the global `_mock_session` MagicMock, reset per test |
| `_patch_cassandra` | YES | Patches `get_cassandra_session` in `utilities` + `dependencies` |
| `_patch_kafka` | YES | Patches `get_kafka_admin_client`, `get_kafka_producer`, `Producer` |
| `test_user` | no | `namedtuple` UserRow (superadmin, org-bound) |
| `auth_token` | no | Valid JWT for `test_user` |
| `auth_headers` | no | `{"Authorization": "Bearer <token>"}` |
| `client` | no | FastAPI `TestClient` with ALL auth deps overridden |
| `sample_org_id` / `sample_project_id` / `sample_collection_id` | no | Random UUIDs |

## MOCKING PATTERNS

### Cassandra

```python
# Single row return
mock_cassandra_session.execute.return_value = MagicMock(one=MagicMock(return_value=row))

# Multiple rows
mock_cassandra_session.execute.return_value = MagicMock(all=MagicMock(return_value=[r1, r2]))

# Sequential calls (schema fetch → data fetch)
mock_cassandra_session.execute.side_effect = [schema_result, data_result]

# Simulate DB error
mock_cassandra_session.execute.side_effect = Exception("connection refused")
```

**Mock rows**: Always use `namedtuple` — matches cassandra-driver Row interface:
```python
OrgRow = namedtuple("OrgRow", ["id", "organization_name", "description", "tags", "creation_date"])
```

### Kafka

Kafka is globally mocked via `_patch_kafka`. Producer and AdminClient are `MagicMock`. No special setup needed — just verify calls:
```python
mock_producer.produce.assert_called_once_with(topic, key=k, value=v)
```

### Auth Bypass

The `client` fixture overrides ALL auth dependencies with lambdas:
```python
app.dependency_overrides[get_current_user_from_jwt] = lambda: test_user
app.dependency_overrides[verify_master_access] = lambda: True
```

To test auth failures: DON'T use `client` fixture — create a raw `TestClient(app)` without overrides.

### Service Mocking (router tests)

```python
@patch("services.collection_service.create_collection_service", new_callable=AsyncMock)
async def test_create(self, mock_service, client, auth_headers):
    mock_service.return_value = {"message": "created"}
    response = client.post("/api/v1/projects/{pid}/collections", headers=auth_headers, json={...})
```

## CONVENTIONS

- **Class-based** test organization: `class TestFeatureName:`
- **Method naming**: `test_<action>_<scenario>` (e.g., `test_create_collection_duplicate_name`)
- Async tests: `async def test_*` — auto-detected by `asyncio_mode = "auto"`
- `pytest.raises(HTTPException)` for expected errors
- `@patch` with `new_callable=AsyncMock` for async service/utility functions
- `@patch` with default `MagicMock` for sync functions
- No real infrastructure needed — everything is mocked

## STRUCTURE

```
tests/
├── conftest.py           # All shared fixtures
├── __init__.py
├── test_dependencies.py  # Tests for dependencies.py (755 lines — largest test file)
├── test_coverage_gaps.py # Catch-all coverage tests
├── auth/                 # Auth service + dependency tests
├── core/                 # Validators, exceptions, filters, aggregation, tracing
├── routers/              # One test file per router (14 files)
├── services/             # Service layer tests
└── utilities/            # Utility function tests
```

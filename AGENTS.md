# CENOTOO API — PROJECT KNOWLEDGE BASE

**Generated:** 2026-03-08  
**Commit:** `86e0ca8` (main)  
**Stack:** Python 3.11 / FastAPI / Cassandra / Kafka (confluent-kafka)

## OVERVIEW

Single-org-per-deployment REST API for real-time data ingestion (→ Kafka), storage (Cassandra), and retrieval. Auth via JWT + API keys. No consumer containers — shared infrastructure handles Kafka→Cassandra.

## STRUCTURE

```
.
├── main.py              # App entry, lifespan, middleware, rate limiting
├── config.py            # pydantic-settings (all env vars)
├── dependencies.py      # Auth deps + LEGACY filter/aggregation (duplicated in core/)
├── api/v1.py            # Router assembly — all v1 routers mounted here
├── routers/             # HTTP handlers (13 files, uniform pattern)
├── services/            # Thin orchestration: validation → utils calls → response
├── models/              # Flat Pydantic models (no inheritance hierarchy)
├── core/                # Exceptions, validators, filters, aggregation, middleware, tracing
├── utilities/           # Cassandra/Kafka connectors, CQL CRUD, schema helpers
├── tests/               # 446 tests (see tests/AGENTS.md)
├── docker-compose.yml   # Dev stack: API + Cassandra 4.1 + Kafka 4.0 (KRaft)
└── Dockerfile           # Multi-stage, python:3.11-slim, runs as appuser:1000
```

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Add new entity type | `models/` → `utilities/` → `services/` → `routers/` → `api/v1.py` | Follow project/collection pattern |
| Add new route | `routers/` + register in `api/v1.py` | Use `Depends(check_project_exists)` on router |
| Change auth logic | `dependencies.py` + `services/auth_service.py` | JWT via PyJWT, passwords via bcrypt |
| Modify Cassandra queries | `utilities/*_utils.py` | Module-level `session` — see GOTCHAS |
| Modify Kafka config | `utilities/kafka_connector.py` | SASL conditional on `KAFKA_USERNAME` |
| Add config setting | `config.py` → `.env.example` | pydantic-settings auto-loads from env |
| Schema validation | `routers/send_data.py` | Simple type map, not JSON Schema |
| Aggregation/stats | `core/aggregation.py` (canonical) | `dependencies.py` has DUPLICATE — use `core/` |
| Filter generation | `core/filters.py` (canonical) | `dependencies.py` has DUPLICATE — use `core/` |
| Data query (CQL) | `routers/get_data.py`, `routers/get_data_stats.py` | Inline CQL, not via utilities |

## CONVENTIONS

### Naming

| Thing | Convention | Example |
|-------|-----------|---------|
| Kafka topic | `{org}.{project}.{collection}` | `acme.iot.sensors` |
| Cassandra keyspace | org name (1:1) | `acme` |
| Cassandra table | `{project}_{collection}` | `iot_sensors` |
| Primary key | `((day, key), timestamp)` clustering DESC | Partition by day+key |
| Route paths | `/projects/{pid}/collections/{cid}/...` | Nested under project |
| Service functions | `*_service` suffix | `create_collection_service` |
| Utility functions | Verb-first or `get_*` | `fetch_all_collections` |
| Router tags | `TAG = "Section Name"` constant | Per-file constant |

### Patterns

- **Router pattern**: `router = APIRouter(dependencies=[Depends(check_project_exists)])` — project validation at router level
- **Auth**: JWT (`OAuth2PasswordBearer`) OR API key (`X-API-Key` header) — `verify_endpoint_access` checks both
- **API key roles**: `read`, `write`, `master` — checked via `check_api_key()`
- **CQL bind markers**: `%s` everywhere (not `?`) — this is cassandra-driver convention
- **All CQL queries use `ALLOW FILTERING`** — metadata tables lack secondary indexes
- **Pagination**: In-memory (fetch all → slice) via `PaginatedResponse(items, total, offset, limit)`
- **Sorting/ordering**: In-memory Python sort after Cassandra fetch (no CQL ORDER BY)
- **Schema flattening**: Nested objects flattened with `$` separator for Cassandra columns

### Auth Flow

```
Request → OAuth2PasswordBearer (JWT) or APIKeyHeader (X-API-Key)
  ├── JWT path: verify_jwt_token → get_user_by_username → check org membership
  └── API key path: hash_api_key → lookup in api_keys table → check role
```

### Error Handling

```
utilities/ → raise HTTPException(4xx/5xx)
services/  → raise HTTPException(4xx/5xx)
routers/   → catch HTTPException, re-raise; catch Exception → 500
core/      → AppException subclasses (NotFoundError, ConflictError, etc.)
```

`AppException` hierarchy exists in `core/exceptions.py` but most code still uses raw `HTTPException`. The custom exceptions are registered as handlers in `main.py`.

## ANTI-PATTERNS (DO NOT)

- **NEVER** use `docker` module — removed from deps, `flink_utilities.py` still imports it (dead code)
- **NEVER** add `as any` / `type: ignore` beyond the 4 existing ones in `main.py` and `get_data.py`
- **NEVER** use `print()` — 10+ stray prints exist in `collection_utils.py`, `send_data.py`, `get_data.py` — use `logger.info()`
- **NEVER** add filter/aggregation logic to `dependencies.py` — use `core/filters.py` and `core/aggregation.py`
- **NEVER** call `datetime.utcnow()` in new code — it's deprecated in Python 3.12+ (existing code uses it everywhere)
- **NEVER** use f-string interpolation for CQL WHERE values — use `%s` bind markers for injection safety
- **DO NOT** create per-collection Kafka consumers — shared infrastructure handles all topics

## GOTCHAS

### Module-Level Cassandra Sessions

8 files call `session = get_cassandra_session()` at **import time**:
`collection_utils`, `organization_utils`, `project_utils`, `user_utils`, `project_keys_utils`, `get_data`, `get_data_stats`, `delete_data`, `collection_keys`

This means: (1) tests MUST patch before import (root `conftest.py` does this), (2) no connection pooling/lifecycle — each creates a fresh Cluster+Session.

### Dockerfile Missing `api/` Directory

The `COPY` instructions in `Dockerfile` do **not** copy `api/`. The container will fail with ImportError on `from api.v1 import router`. Must add `COPY api/ api/`.

### requirements.txt vs pyproject.toml Drift

`requirements.txt` includes `python-jose` and `passlib[bcrypt]` which are NOT used — code uses `PyJWT` and `bcrypt` directly. `requirements.txt` also lacks `PyJWT`, `bcrypt`, and `slowapi` which ARE used.

### Duplicate Code in dependencies.py

`dependencies.py` (519 lines) contains duplicates of:
- `generate_filter_condition()` — canonical version in `core/filters.py`
- `aggregate_data()` + `get_interval_start()` — canonical version in `core/aggregation.py`
- `contains_special_characters()` — canonical version in `core/validators.py`

Some routers import from `dependencies`, others from `core/`. Use `core/` for new code.

### Flink Utilities (Dead Code)

`utilities/flink_utilities.py` imports `docker` (not in deps) and references hardcoded container names. Not called by any router or service. Generates PyFlink scripts for streaming aggregation — may be useful as reference but will crash if imported.

### Cassandra datacenter1 Hardcoded

`cassandra_connector.py` hardcodes `local_dc="datacenter1"`. Matches k3s StatefulSet config but is not configurable.

### Organization ID

Single org per deployment: `ORGANIZATION_ID` env var → `config.settings.organization_id`. Not a path parameter. All routes operate on this one org.

## COMMANDS

```bash
make install-dev    # Install prod + dev deps
make dev            # uvicorn --reload on :8000
make test           # pytest -v (446 tests, no infra needed)
make lint           # ruff check .
make format         # ruff format . && ruff check --fix .
make build          # docker build -t cenotoo-api .
make up             # docker compose up -d (API + Cassandra + Kafka)
make ci             # lint + test + pylint + mypy + pre-commit
```

## INFRASTRUCTURE CONTEXT

- Deploys to k3s via sister repo `centoo` (robotics-4-all/cenotoo)
- Kafka: Strimzi operator, `cenotoo-kafka-kafka-bootstrap:9092`, SCRAM-SHA-512
- Cassandra: StatefulSet, `cenotoo-cassandra:9042`, PasswordAuthenticator (`cassandra/cassandra`)
- Metadata keyspace init: `centoo/cassandra/create_cassandra_tables.py` (infra repo, not this repo)
- API exposed via NodePort 30080
- Planned: Valkey caching layer (schema, entity, API key, query result caches) — not started

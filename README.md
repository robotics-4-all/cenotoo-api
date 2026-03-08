# Nostradamus IoTO API

FastAPI REST service for IoT data ingestion, storage, and retrieval.

**Stack**: Python 3.11 / FastAPI / Cassandra / Kafka / Docker

**Docs**: https://nostradamus-ioto.issel.ee.auth.gr/docs

## Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose (for infrastructure services)

### Local Development

```bash
git clone https://github.com/robotics-4-all/nostradamus-ioto-api.git
cd nostradamus-ioto-api

python -m venv .venv
source .venv/bin/activate

make install-dev

cp .env.example .env
# Edit .env with your configuration

make dev
```

The API docs are at `http://localhost:8000/docs`.

### Docker Compose (Full Stack)

Starts the API with Cassandra, Kafka, and Zookeeper:

```bash
cp .env.example .env
make up
```

### Docker (API Only)

```bash
make build
docker run -p 8000:8000 --env-file .env nostradamus-ioto-api
```

## Make Targets

| Target | Description |
|--------|-------------|
| `make install` | Install production dependencies |
| `make install-dev` | Install production + dev dependencies |
| `make dev` | Start dev server with hot reload |
| `make test` | Run test suite |
| `make lint` | Run ruff linter |
| `make format` | Auto-format code with ruff |
| `make build` | Build Docker image |
| `make up` | Start all services (docker compose) |
| `make down` | Stop all services |
| `make logs` | Tail API container logs |
| `make clean` | Remove caches and build artifacts |

## Configuration

All configuration is via environment variables. See `.env.example` for the full list.

| Variable | Required | Description |
|----------|----------|-------------|
| `JWT_SECRET_KEY` | Yes (prod) | JWT signing secret |
| `API_KEY_SECRET` | Yes (prod) | API key generation secret |
| `ADMIN_USERNAME` | Yes | Admin login username |
| `ADMIN_PASSWORD` | Yes | Admin login password |
| `KAFKA_BROKERS` | No | Kafka broker addresses (default: `localhost:59498`) |
| `CASSANDRA_CONTACT_POINTS` | No | Cassandra host (default: `localhost`) |
| `CASSANDRA_PORT` | No | Cassandra port (default: `9042`) |
| `NOSTRADAMUS_ORGANIZATION_ID` | No | Organization UUID |
| `RATE_LIMIT_DEFAULT` | No | Global rate limit (default: `120/minute`) |
| `RATE_LIMIT_AUTH` | No | Auth endpoint rate limit (default: `10/minute`) |
| `OTLP_ENDPOINT` | No | OpenTelemetry exporter endpoint (empty = disabled) |
| `OTLP_SERVICE_NAME` | No | Service name for tracing (default: `nostradamus-ioto-api`) |

## Project Structure

```
.
├── main.py              # FastAPI app entry point
├── config.py            # Settings (pydantic-settings)
├── dependencies.py      # Auth dependencies (JWT + API key)
├── api/                 # API version assembly
│   └── v1.py            #   All v1 routers assembled here
├── core/                # Framework layer
│   ├── exceptions.py    #   Custom exception hierarchy
│   ├── validators.py    #   Input validation (CQL identifiers, special chars)
│   ├── filters.py       #   CQL filter generation with injection protection
│   ├── aggregation.py   #   Time-series aggregation (pandas)
│   ├── middleware.py     #   Request logging middleware
│   └── tracing.py       #   OpenTelemetry setup (opt-in via OTLP_ENDPOINT)
├── routers/             # API route handlers
├── services/            # Business logic layer
├── models/              # Pydantic request/response models
├── utilities/           # DB connectors, Kafka, Docker helpers
└── tests/               # pytest test suite (188 tests)
```

## Testing

```bash
make test
```

Tests mock Cassandra, Kafka, and Docker — no infrastructure required.

## Features

- **Rate Limiting**: Configurable via `RATE_LIMIT_DEFAULT` and `RATE_LIMIT_AUTH` env vars (powered by slowapi)
- **OpenTelemetry Tracing**: Opt-in by setting `OTLP_ENDPOINT` (install extra: `pip install -e ".[tracing]"`)
- **Pagination**: All list endpoints return `PaginatedResponse` with `items`, `total`, `offset`, `limit`
- **API Versioning**: All endpoints under `/api/v1` prefix, ready for future versions

## Organization Setup Guide

This section describes the step-by-step process for configuring a new organization and preparing it for data ingestion.

### Prerequisites

Ensure the API and its infrastructure services (Cassandra, Kafka) are running and the following environment variables are set:

| Variable | Purpose |
|----------|---------|
| `ADMIN_USERNAME` | Admin account for initial authentication |
| `ADMIN_PASSWORD` | Admin account password |
| `NOSTRADAMUS_ORGANIZATION_ID` | UUID identifying the organization |
| `JWT_SECRET_KEY` | Secret for signing JWT tokens (must not be default in production) |
| `API_KEY_SECRET` | Secret for API key generation (must not be default in production) |

All examples below use `$BASE_URL` (e.g., `http://localhost:8000`).

### Step 1 — Authenticate

Obtain a JWT access token using the admin credentials:

```bash
curl -X POST "$BASE_URL/api/v1/token" \
  -d "username=$ADMIN_USERNAME&password=$ADMIN_PASSWORD"
```

Response:

```json
{
  "access_token": "<jwt_access_token>",
  "refresh_token": "<jwt_refresh_token>",
  "token_type": "bearer"
}
```

Use the `access_token` in all subsequent requests via the `Authorization` header:

```bash
-H "Authorization: Bearer <jwt_access_token>"
```

### Step 2 — Verify Organization

Confirm the organization is reachable:

```bash
curl -X GET "$BASE_URL/api/v1/organization/nostradamus" \
  -H "Authorization: Bearer <token>"
```

Optionally update the organization description and tags:

```bash
curl -X PUT "$BASE_URL/api/v1/organization/nostradamus" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Production IoT deployment",
    "tags": ["iot", "sensors"]
  }'
```

### Step 3 — Create Users

Create users within the organization (requires superadmin privileges):

```bash
curl -X POST "$BASE_URL/api/v1/organizations/<org_id>/users" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "operator1",
    "password": "SecurePass1!"
  }'
```

Password requirements: minimum 8 characters, at least one uppercase letter, one lowercase letter, one digit, and one special character.

### Step 4 — Create a Project

Projects are logical containers for collections within an organization:

```bash
curl -X POST "$BASE_URL/api/v1/projects" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "project_name": "smart_building",
    "description": "Smart building sensor network",
    "tags": ["building", "sensors"]
  }'
```

Response includes the `id` (UUID) of the created project, used in subsequent steps.

### Step 5 — Generate API Keys

Create API keys for programmatic access. Key types are `read`, `write`, or `master`:

```bash
curl -X POST "$BASE_URL/api/v1/projects/<project_id>/keys" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"key_type": "write"}'
```

Response:

```json
{
  "api_key": "<64-char-hex-key>",
  "project_id": "<project_id>",
  "key_type": "write"
}
```

API keys can be used for authentication via the `X-API-Key` header as an alternative to JWT tokens. Access levels:

| Key Type | Permissions |
|----------|-------------|
| `read` | Read data from collections |
| `write` | Read and write data to collections |
| `master` | Full access including collection management |

### Step 6 — Create a Collection

Collections define the schema for ingested data. Each collection provisions a Cassandra table, a Kafka topic, and a Docker consumer container:

```bash
curl -X POST "$BASE_URL/api/v1/projects/<project_id>/collections" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "temperature_readings",
    "description": "Temperature sensor data",
    "tags": ["temperature"],
    "collection_schema": {
      "sensor_id": "text",
      "temperature": "float",
      "humidity": "float",
      "location": "text"
    }
  }'
```

Supported schema types: `text`, `int`, `float`, `bool`, `date`, `timestamp`.

### Step 7 — Ingest Data

Send data to a collection. The payload is validated against the collection schema:

```bash
curl -X POST \
  "$BASE_URL/api/v1/projects/<project_id>/collections/<collection_id>/send_data" \
  -H "X-API-Key: <write_api_key>" \
  -H "Content-Type: application/json" \
  -d '{
    "sensor_id": "sensor-001",
    "temperature": 23.5,
    "humidity": 45.2,
    "location": "floor-3"
  }'
```

Batch ingestion is supported by sending a JSON array:

```bash
curl -X POST \
  "$BASE_URL/api/v1/projects/<project_id>/collections/<collection_id>/send_data" \
  -H "X-API-Key: <write_api_key>" \
  -H "Content-Type: application/json" \
  -d '[
    {"sensor_id": "sensor-001", "temperature": 23.5, "humidity": 45.2, "location": "floor-3"},
    {"sensor_id": "sensor-002", "temperature": 21.0, "humidity": 50.1, "location": "floor-1"}
  ]'
```

The `key` (unique identifier) and `timestamp` fields are auto-generated if not provided.

### Step 8 — Query Data

Retrieve data with optional filtering, ordering, and pagination:

```bash
curl -X GET \
  "$BASE_URL/api/v1/projects/<project_id>/collections/<collection_id>/get_data?limit=10&offset=0" \
  -H "X-API-Key: <read_api_key>"
```

Retrieve aggregated statistics:

```bash
curl -X GET \
  "$BASE_URL/api/v1/projects/<project_id>/collections/<collection_id>/statistics?attribute=temperature&stat=avg&interval=every_1_days" \
  -H "X-API-Key: <read_api_key>"
```

Available statistics: `avg`, `max`, `min`, `sum`, `count`, `distinct`.

## API

All endpoints are under `/api/v1` except:
- `GET /health` — liveness check
- `GET /ready` — readiness check (Cassandra + Kafka)

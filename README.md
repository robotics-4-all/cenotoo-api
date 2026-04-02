<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white" alt="Python 3.11" />
  <img src="https://img.shields.io/badge/FastAPI-0.100+-009688?logo=fastapi&logoColor=white" alt="FastAPI" />
  <img src="https://img.shields.io/badge/Cassandra-4.x-1287B1?logo=apachecassandra&logoColor=white" alt="Cassandra" />
  <img src="https://img.shields.io/badge/Kafka-KRaft-blue?logo=apachekafka&logoColor=white" alt="Kafka" />
  <img src="https://img.shields.io/badge/tests-456_passing-brightgreen" alt="456 tests passing" />
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-orange?logo=apache&logoColor=white" alt="Apache 2.0" /></a>
</p>

<h1 align="center">Cenotoo API</h1>

<p align="center">
  <strong>The REST interface for the Cenotoo IoT data platform.</strong><br/>
  Ingest · Query · Stream · Manage devices — all over HTTP, with JWT and API key auth.
</p>

<p align="center">
  <a href="https://github.com/robotics-4-all/cenotoo">⬅ Platform Repository</a>
  &nbsp;·&nbsp;
  <a href="#-quick-start">Quick Start</a>
  &nbsp;·&nbsp;
  <a href="#features">Features</a>
  &nbsp;·&nbsp;
  <a href="#organization-setup-guide">Setup Guide</a>
</p>

---

## Features

| Feature | Endpoint | Description |
|---------|----------|-------------|
| **Data Ingestion** | `POST /send_data` | Single records or JSON arrays, validated against collection schema |
| **Historical Query** | `GET /get_data` | Filter by field, time range, and order; fully paginated |
| **Time-series Stats** | `GET /statistics` | `avg`, `max`, `min`, `sum`, `count`, `distinct` over configurable intervals |
| **SSE Streaming** | `GET /stream` | Live Kafka messages delivered as Server-Sent Events — zero polling |
| **Schema Evolution** | `PATCH /schema` | Add or remove Cassandra columns on live tables with zero downtime |
| **Device Registry** | `POST/GET/PUT/DELETE /devices` | Register, list, update, and deactivate devices per project |
| **Device Shadow / Twin** | `GET /shadow` | Separate `desired` and `reported` state; automatic `delta` computation |
| **Dual Auth** | `Authorization` / `X-API-Key` | JWT bearer tokens for users; scoped API keys for devices and services |
| **Rate Limiting** | all endpoints | Configurable per-endpoint limits via `slowapi` |
| **OpenTelemetry** | — | Opt-in distributed tracing via `OTLP_ENDPOINT` env var |
| **Pagination** | all list endpoints | `PaginatedResponse` with `items`, `total`, `offset`, `limit` |
| **Collection Metrics** | `GET /metrics` | Health, record count, and last ingested timestamp |
| **Data Export** | `GET /export` | Download collection data in CSV or Parquet format |
| **Bulk Import** | `POST /import` | Upload CSV or JSON files with partial success handling |
| **Webhooks & Alerts** | `CRUD /rules` | Trigger HTTP webhooks based on data thresholds |

---

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose (for the full stack)

### Local Development

```bash
git clone https://github.com/robotics-4-all/cenotoo-api.git
cd cenotoo-api

python -m venv .venv && source .venv/bin/activate
make install-dev

cp .env.example .env   # edit with your config
make dev               # starts uvicorn with hot reload
```

Swagger UI: `http://localhost:8000/docs`

### Docker Compose (Full Stack)

Starts the API together with Cassandra and Kafka (KRaft, no ZooKeeper):

```bash
cp .env.example .env
make up
```

### Docker (API Only)

```bash
make build
docker run -p 8000:8000 --env-file .env cenotoo-api
```

---

## Make Targets

| Target | Description |
|--------|-------------|
| `make install` | Install production dependencies |
| `make install-dev` | Install production + dev dependencies |
| `make dev` | Start dev server with hot reload |
| `make test` | Run test suite (456 tests, no infra required) |
| `make lint` | Run ruff linter |
| `make format` | Auto-format with ruff |
| `make build` | Build Docker image |
| `make up` | Start all services (docker compose) |
| `make down` | Stop all services |
| `make logs` | Tail API container logs |
| `make clean` | Remove caches and build artifacts |

---

## Configuration

All configuration is via environment variables. See `.env.example` for the full list.

| Variable | Required | Description |
|----------|:--------:|-------------|
| `JWT_SECRET_KEY` | prod | JWT signing secret |
| `API_KEY_SECRET` | prod | API key HMAC secret |
| `ADMIN_USERNAME` | yes | Admin login username |
| `ADMIN_PASSWORD` | yes | Admin login password |
| `KAFKA_BROKERS` | no | Broker addresses (default: `localhost:9092`) |
| `KAFKA_USERNAME` | no | SASL username (empty = no auth) |
| `KAFKA_PASSWORD` | no | SASL password |
| `KAFKA_SASL_MECHANISM` | no | SASL mechanism (default: `SCRAM-SHA-512`) |
| `KAFKA_SECURITY_PROTOCOL` | no | Security protocol (default: `SASL_PLAINTEXT`) |
| `CASSANDRA_CONTACT_POINTS` | no | Cassandra host (default: `localhost`) |
| `CASSANDRA_PORT` | no | Cassandra port (default: `9042`) |
| `CASSANDRA_USERNAME` | no | Cassandra username (empty = no auth) |
| `CASSANDRA_PASSWORD` | no | Cassandra password |
| `ORGANIZATION_ID` | no | Organization UUID |
| `RATE_LIMIT_DEFAULT` | no | Global rate limit (default: `120/minute`) |
| `RATE_LIMIT_AUTH` | no | Auth endpoint rate limit (default: `10/minute`) |
| `OTLP_ENDPOINT` | no | OpenTelemetry exporter (empty = disabled) |
| `OTLP_SERVICE_NAME` | no | Service name for tracing (default: `cenotoo-api`) |

---

## Project Structure

```
.
├── main.py              # FastAPI app entry point
├── config.py            # Settings (pydantic-settings)
├── dependencies.py      # Auth dependencies (JWT + API key)
├── api/
│   └── v1.py            # All v1 routers assembled here
├── core/                # Framework layer
│   ├── exceptions.py    #   Custom exception hierarchy
│   ├── validators.py    #   CQL identifier + special-char validation
│   ├── filters.py       #   CQL filter generation (injection-safe)
│   ├── aggregation.py   #   Time-series aggregation (pandas)
│   ├── middleware.py     #   Request logging middleware
│   └── tracing.py       #   OpenTelemetry setup
├── routers/             # HTTP route handlers (one file per resource)
├── services/            # Business logic layer
├── models/              # Pydantic request/response models
├── utilities/           # Cassandra + Kafka connectors and CRUD helpers
└── tests/               # 456 pytest tests (no infrastructure required)
```

---

## Testing

```bash
make test
```

All 456 tests mock Cassandra and Kafka — no running infrastructure needed.

---

## Organization Setup Guide

This section describes the step-by-step process for configuring a new organization and preparing it for data ingestion.

### Prerequisites

Ensure the API and its infrastructure services (Cassandra, Kafka) are running and the following environment variables are set:

| Variable | Purpose |
|----------|---------|
| `ADMIN_USERNAME` | Admin account for initial authentication |
| `ADMIN_PASSWORD` | Admin account password |
| `ORGANIZATION_ID` | UUID identifying the organization |
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
curl -X GET "$BASE_URL/api/v1/organization" \
  -H "Authorization: Bearer <token>"
```

Optionally update the organization description and tags:

```bash
curl -X PUT "$BASE_URL/api/v1/organization" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Production data streaming deployment",
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

Collections define the schema for ingested data. Each collection provisions a Cassandra table and a Kafka topic:

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

### Step 9 — Stream Live Data (SSE)

Subscribe to a real-time stream of new messages published to a collection:

```bash
curl -N \
  "$BASE_URL/api/v1/projects/<project_id>/collections/<collection_id>/stream" \
  -H "X-API-Key: <read_api_key>"
```

Each message is delivered as a Server-Sent Event:

```
data: {"sensor_id": "sensor-001", "temperature": 23.5, "timestamp": "..."}

data: {"sensor_id": "sensor-002", "temperature": 21.0, "timestamp": "..."}
```

The stream starts from the latest offset (live-only, not historical) and sends `: keepalive` comments every ~15 seconds to keep proxies alive.

### Step 10 — Register and Manage Devices

Register a physical or logical device to a project:

```bash
curl -X POST "$BASE_URL/api/v1/projects/<project_id>/devices" \
  -H "X-API-Key: <master_api_key>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "temperature-sensor-01",
    "description": "Rooftop temperature sensor",
    "tags": ["rooftop", "temperature"]
  }'
```

Update the device desired state (cloud → device):

```bash
curl -X PUT "$BASE_URL/api/v1/projects/<project_id>/devices/<device_id>/shadow/desired" \
  -H "X-API-Key: <write_api_key>" \
  -H "Content-Type: application/json" \
  -d '{"state": {"mode": "active", "threshold": 30.0}}'
```

Get the device shadow including delta between reported and desired state:

```bash
curl "$BASE_URL/api/v1/projects/<project_id>/devices/<device_id>/shadow" \
  -H "X-API-Key: <read_api_key>"
```

Response:

```json
{
  "device_id": "<uuid>",
  "reported": {"mode": "idle", "threshold": 30.0},
  "desired":  {"mode": "active", "threshold": 30.0},
  "delta":    {"mode": "active"},
  "reported_at": "...",
  "desired_at": "..."
}
```

### Step 11 — Evolve a Collection Schema

Add or remove fields from an existing collection without recreating it:

```bash
curl -X PATCH \
  "$BASE_URL/api/v1/projects/<project_id>/collections/<collection_id>/schema" \
  -H "X-API-Key: <master_api_key>" \
  -H "Content-Type: application/json" \
  -d '{
    "add_fields": {"battery_level": "float", "firmware": "text"},
    "remove_fields": ["legacy_field"]
  }'
```

Supported types for `add_fields`: `text`, `int`, `float`, `bool`, `date`, `timestamp`. System fields (`key`, `timestamp`, `day`) cannot be added or removed. Adding a field that already exists returns `409`; removing a non-existent field returns `404`.

## API

All endpoints are under `/api/v1` except:
- `GET /health` — liveness check
- `GET /ready` — readiness check (Cassandra + Kafka)

## License

This project is licensed under the [Apache License 2.0](LICENSE).

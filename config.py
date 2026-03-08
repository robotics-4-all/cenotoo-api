# pylint: disable=too-few-public-methods
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """FastAPI application settings and configuration."""

    app_name: str = "services"
    environment: str = "development"
    admin_username: str = ""
    admin_password: str = ""

    jwt_secret_key: str = "supersecretkey"
    jwt_algorithm: str = "HS256"
    jwt_expiration_minutes: int = 15
    jwt_refresh_expiration_days: int = 7

    kafka_brokers: str = "localhost:59498"

    cassandra_contact_points: str = "localhost"
    cassandra_port: int = 9042
    cassandra_keyspace: str = "metadata"

    api_key_secret: str = "default-api-key-secret"
    organization_id: str = Field(
        default="default-organization-id",
        validation_alias=AliasChoices("NOSTRADAMUS_ORGANIZATION_ID", "organization_id"),
    )

    cors_origins: str = "*"

    rate_limit_default: str = "120/minute"
    rate_limit_auth: str = "10/minute"

    otlp_endpoint: str = ""
    otlp_service_name: str = "nostradamus-ioto-api"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


settings = Settings()

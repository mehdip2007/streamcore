"""
Centralized configuration for StreamCore.

This module reads environment variables and validates them at startup.
If any required config is missing or malformed, the application fails
immediately with a clear error message — instead of crashing later.

This is called the 'fail-fast' pattern. It's a hallmark of production code.
"""
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    """All Kafka-related configuration."""

    model_config = SettingsConfigDict(
        env_prefix="KAFKA_",
        env_file=".env",
        extra="ignore",
    )

    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Comma-separated list of Kafka broker addresses",
    )
    topic_view_events: str = Field(default="streamcore.video.views")
    topic_watch_events: str = Field(default="streamcore.video.watch_progress")
    client_id: str = Field(default="streamcore-producer")


class PostgresSettings(BaseSettings):
    """All Postgres-related configuration."""

    model_config = SettingsConfigDict(
        env_prefix="POSTGRES_",
        env_file=".env",
        extra="ignore",
    )

    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    db: str = Field(default="streamcore")
    user: str = Field(default="streamcore")
    password: str = Field(default="change_me_locally")

    @property
    def dsn(self) -> str:
        """Build a Postgres connection string from individual settings."""
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.db}"
        )


class ProducerSettings(BaseSettings):
    """Behavior tuning for the event producer."""

    model_config = SettingsConfigDict(
        env_prefix="PRODUCER_",
        env_file=".env",
        extra="ignore",
    )

    events_per_second: int = Field(default=10, ge=1, le=10000)
    simulated_users: int = Field(default=100, ge=1)
    simulated_videos: int = Field(default=50, ge=1)


class AppSettings(BaseSettings):
    """Top-level application configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )

    app_env: str = Field(default="local")
    log_level: str = Field(default="INFO")


# ----- Singleton Pattern via lru_cache -----
# We only want ONE instance of each settings class loaded.
# lru_cache ensures that — the first call loads from .env,
# every subsequent call returns the cached instance.

@lru_cache
def get_kafka_settings() -> KafkaSettings:
    return KafkaSettings()


@lru_cache
def get_postgres_settings() -> PostgresSettings:
    return PostgresSettings()


@lru_cache
def get_producer_settings() -> ProducerSettings:
    return ProducerSettings()


@lru_cache
def get_app_settings() -> AppSettings:
    return AppSettings()

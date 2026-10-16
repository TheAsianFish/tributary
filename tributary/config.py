"""Configuration from the environment (prefix ``TRIBUTARY_``); see ``.env.example``."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

LIVE_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
DEFAULT_SAMPLE = Path(__file__).resolve().parent.parent / "data/sample/recentchange.jsonl.gz"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="TRIBUTARY_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # Logging
    log_format: Literal["auto", "json", "console"] = "auto"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    # Kafka
    kafka_bootstrap: str = "localhost:9094"
    topic: str = "wiki.recentchange"
    topic_partitions: int = Field(default=3, ge=1)

    # PostgreSQL warehouse
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_database: str = "tributary"
    pg_user: str = "tributary"
    pg_password: SecretStr = SecretStr("tributary")

    # Producer
    sample_path: Path = DEFAULT_SAMPLE
    produce_rate: float = Field(default=500.0, ge=0.0, description="events/s; 0 = unthrottled")
    live_url: str = LIVE_STREAM_URL
    user_agent: str = "tributary/0.1 (https://github.com/TheAsianFish/tributary)"

    # Spark stream job
    checkpoint_dir: Path = Path("spark-checkpoints/warehouse")
    watermark_delay: str = "10 minutes"
    trigger_interval: str = "5 seconds"
    max_offsets_per_trigger: int = Field(default=20_000, ge=1)
    spark_master: str = "local[3]"
    # Maven coordinates resolved at start-up; empty when the jars are already on the
    # classpath (the stream image bakes them into /opt/spark/jars).
    spark_packages: str = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3"

    @property
    def pg_conninfo(self) -> str:
        """libpq connection string (password included; never log it)."""
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_database} "
            f"user={self.pg_user} password={self.pg_password.get_secret_value()}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

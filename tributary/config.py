"""Configuration from the environment (prefix ``TRIBUTARY_``); see ``.env.example``."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from psycopg.conninfo import make_conninfo
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

LIVE_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SAMPLE = REPO_ROOT / "data/sample/recentchange.jsonl.gz"


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
    spark_packages: str = ",".join(
        (
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3",
            "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
            "org.apache.iceberg:iceberg-aws-bundle:1.11.0",
            "org.postgresql:postgresql:42.7.14",
        )
    )

    # Lakehouse: the second streaming query writes lake.raw.recentchange (Iceberg).
    lake_enabled: bool = True
    lake_checkpoint_dir: Path = Path("spark-checkpoints/lake")
    # Every trigger commits a snapshot and at least one file per hour touched, so the
    # lake triggers less often than the warehouse query: fewer, larger files.
    lake_trigger_interval: str = "1 minute"
    # Per-trigger cap for the lake query. At one trigger a minute the warehouse's cap
    # would limit the lake to 333 events/s, below the default replay rate.
    lake_max_offsets_per_trigger: int = Field(default=200_000, ge=1)
    s3_endpoint: str = "http://localhost:9000"
    s3_region: str = "us-east-1"
    s3_access_key: str = "tributary"
    s3_secret_key: SecretStr = SecretStr("tributary-dev-secret")
    lake_warehouse: str = "s3://lake/warehouse"
    # PostgreSQL schema holding the Iceberg JDBC catalog's two tables.
    iceberg_catalog_schema: str = "iceberg_catalog"
    # Snapshots and unreferenced files older than this are deleted by lake_maintenance.
    # The streaming sink finds its last committed batch by walking snapshot ancestry,
    # so this must stay far above any restart window; Trino's floor is set to match.
    lake_snapshot_retention: str = "1d"
    # Closed hours checked against the warehouse on each lake_maintenance run.
    reconcile_lookback_hours: int = Field(default=24, ge=1)
    # The lake may trail the warehouse by this much event time before it is flagged.
    lake_max_lag_minutes: int = Field(default=15, ge=1)

    # Trino (profile lakehouse)
    trino_host: str = "localhost"
    trino_port: int = 8081
    trino_user: str = "tributary"
    trino_lake_table: str = "iceberg.raw.recentchange"

    # Warehouse: dbt project and the published catalog the agent reads.
    dbt_project_dir: Path = REPO_ROOT / "warehouse"
    catalog_dir: Path = Path("catalog")
    # Read-only role for Trino's postgresql catalog and, from M3, the agent.
    reader_user: str = "tributary_reader"
    reader_password: SecretStr = SecretStr("tributary_reader")

    @property
    def pg_conninfo(self) -> str:
        """libpq connection string, values quoted (password included; never log it)."""
        return make_conninfo(
            host=self.pg_host,
            port=self.pg_port,
            dbname=self.pg_database,
            user=self.pg_user,
            password=self.pg_password.get_secret_value(),
        )

    @property
    def iceberg_jdbc_url(self) -> str:
        """JDBC URL of the Iceberg catalog (credentials are passed separately)."""
        return (
            f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_database}"
            f"?currentSchema={self.iceberg_catalog_schema}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

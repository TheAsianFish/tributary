"""The lake query: the same parsed, deduplicated events, appended to an Iceberg table.

``lake.raw.recentchange`` is an Iceberg format-v2 table partitioned by
``hours(event_ts)``: hidden partitioning, so a query that filters on
``event_ts`` prunes whole hours without knowing a partition column exists. Its
catalog is Iceberg's JDBC catalog, two tables in a PostgreSQL schema that
Spark and Trino both read, and its files live on S3-compatible storage (MinIO
in Compose).

Why a second streaming query instead of writing both sinks from one
``foreachBatch``: Iceberg's streaming sink stores the query id and the batch id
in every snapshot it commits and skips a batch it finds already committed, so
a micro-batch replayed after a crash cannot append the same rows twice. A dual
write inside ``foreachBatch`` would have to ``MERGE`` on ``event_id`` to stay
idempotent and would tie the two sinks' failures together. The price is that
Kafka is read twice.

Unlike ``raw.recentchange`` the Iceberg table has no primary key to absorb a
duplicate that slips past the dedup state, so the per-hour reconciliation
against PostgreSQL (``tributary.lake.reconcile``) is what would catch one.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import StructType

if TYPE_CHECKING:
    from tributary.config import Settings

CATALOG = "lake"
NAMESPACE = f"{CATALOG}.raw"
TABLE = f"{NAMESPACE}.recentchange"
QUERY_NAME = "lake"

TABLE_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
    # Rows of one micro-batch span several hours (late events), so each task writes
    # one open file per hour instead of requiring the batch sorted by partition.
    "write.spark.fanout.enabled": "true",
    # A streaming table commits once per trigger; without this its metadata.json
    # files (one per commit) would accumulate forever. Snapshots are expired by
    # the lake_maintenance DAG.
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
}

EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def catalog_conf(settings: Settings) -> dict[str, str]:
    """Spark settings that register the ``lake`` Iceberg catalog.

    The password and the S3 secret end up in the Spark configuration, which the
    driver UI (port 4040, not published by Compose) can display.
    """
    prefix = f"spark.sql.catalog.{CATALOG}"
    return {
        "spark.sql.extensions": EXTENSIONS,
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "jdbc",
        f"{prefix}.uri": settings.iceberg_jdbc_url,
        f"{prefix}.jdbc.user": settings.pg_user,
        f"{prefix}.jdbc.password": settings.pg_password.get_secret_value(),
        f"{prefix}.warehouse": settings.lake_warehouse,
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{prefix}.s3.endpoint": settings.s3_endpoint,
        f"{prefix}.s3.path-style-access": "true",
        f"{prefix}.s3.access-key-id": settings.s3_access_key,
        f"{prefix}.s3.secret-access-key": settings.s3_secret_key.get_secret_value(),
        f"{prefix}.client.region": settings.s3_region,
    }


def lake_frame(events: DataFrame) -> DataFrame:
    """The warehouse columns plus ``ingested_at``, the micro-batch's processing time."""
    return events.withColumn("ingested_at", F.current_timestamp())


def create_table_sql(schema: StructType) -> str:
    """``CREATE TABLE IF NOT EXISTS`` for the lake table, columns taken from ``schema``.

    Deriving the columns from the parsed stream keeps the two sinks from
    drifting apart: a column added to the parser appears in a fresh lake table
    without a second hand-written DDL.
    """
    columns = ",\n    ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in schema.fields)
    properties = ",\n    ".join(f"'{k}' = '{v}'" for k, v in TABLE_PROPERTIES.items())
    return (
        f"CREATE TABLE IF NOT EXISTS {TABLE} (\n    {columns}\n)\n"
        "USING iceberg\n"
        "PARTITIONED BY (hours(event_ts))\n"
        f"TBLPROPERTIES (\n    {properties}\n)"
    )


def ensure_table(spark: SparkSession, schema: StructType) -> None:
    """Create the namespace and table if missing; an existing table is left as it is."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
    spark.sql(create_table_sql(schema))


def start_lake_query(
    spark: SparkSession, events: DataFrame, settings: Settings, *, available_now: bool = False
) -> StreamingQuery:
    """Append ``events`` (parsed and deduplicated) to the lake table as the ``lake`` query."""
    frame = lake_frame(events)
    ensure_table(spark, frame.schema)
    writer = (
        frame.writeStream.queryName(QUERY_NAME)
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", str(settings.lake_checkpoint_dir))
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=settings.lake_trigger_interval)
    return writer.toTable(TABLE)

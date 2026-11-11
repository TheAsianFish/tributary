"""Scale the sample to N rows and write it to PostgreSQL and Iceberg in one Spark batch job.

The 50,000 sampled events are tiled in time: copy *i* of every event is
shifted by *i* times the sample's span (first to last event time, plus one
second, so copies never overlap) and gets the id ``uuid5(namespace_i,
original id)``. The distributions (wikis, types, titles, bot share, the
lateness of each event) are the real stream's; only the volume is synthetic.
A row count that is not a multiple of the sample keeps the first rows of the
last copy in sample order.

One DataFrame, three writes, so the stores hold the same rows:

- ``bench.<name>`` in PostgreSQL, written in ``event_ts`` order from one
  connection with ``COPY``, then given its primary key and a BRIN index on
  ``event_ts`` (the row store's counterpart to partition pruning: it only
  works because the heap is in time order, as a streamed table is) and
  vacuumed, so the visibility map is set before any query is timed.
- ``lake.bench.<name>``: Iceberg, partitioned by ``hours(event_ts)``, each
  hour's rows sent to one task, so every hour is one file (the compacted layout).
- ``lake.bench.<name>_stream``: the same rows in the layout the streaming
  sink leaves before compaction: one file per one-minute trigger, per task
  and per event hour. Rows are grouped into simulated micro-batches by their
  arrival time and into the stream's three tasks by a hash of their id, and
  each (batch, task) group becomes exactly one Spark task, so the fanout
  writer writes one file per batch, task and hour, as the stream does (the
  stream's tasks follow Kafka's partitioner, so the match is statistical). It is written in one
  commit; the stream would have committed once per minute, so this layout
  has the stream's files but not its thousands of snapshots and manifests.

The ids are computed in the JVM (SHA-1 over the namespace and the name, with
the version and variant bits set, as RFC 4122 specifies), so twenty million
of them never cross into Python; a unit test checks them against ``uuid.uuid5``.
"""

from __future__ import annotations

import json
import math
import time
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import psycopg
import structlog
from psycopg import sql
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from tributary.events import COLUMNS, InvalidEventError, event_time, to_row
from tributary.ingest.sample import read_sample
from tributary.stream.lake import CATALOG, EXTENSIONS, catalog_conf

if TYPE_CHECKING:
    from pathlib import Path

    from tributary.config import Settings

log = structlog.get_logger(__name__)

NAMESPACE = uuid.UUID("6c1f0e1e-3b7a-5d6e-9a43-2f6f1d6b7c10")
"""Root of every copy's id namespace (an arbitrary fixed UUID)."""

SCHEMA = "bench"
"""PostgreSQL schema and Iceberg namespace of the benchmark tables."""

STREAM_SUFFIX = "_stream"
FLAT_SUFFIX = "_flat"

TRIGGER_SECONDS = 60
"""The lake query's trigger interval (``TRIBUTARY_LAKE_TRIGGER_INTERVAL``)."""
STREAM_TASKS = 3
"""Tasks per micro-batch in the stream job: one per Kafka partition (the Compose default)."""

TABLE_COLUMNS = (*COLUMNS, "ingested_at")
"""The columns of ``raw.recentchange`` and of ``lake.raw.recentchange``, in order."""

_TIME_COLUMNS = ("event_ts", "emitted_at", "kafka_ts", "ingested_at")

_BASE_SCHEMA = StructType(
    [
        StructField("seq", LongType(), False),
        StructField("event_id", StringType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("emitted_at", TimestampType()),
        StructField("rc_id", LongType()),
        StructField("wiki", StringType(), False),
        StructField("domain", StringType()),
        StructField("event_type", StringType(), False),
        StructField("namespace", IntegerType()),
        StructField("title", StringType()),
        StructField("user_name", StringType()),
        StructField("is_bot", BooleanType()),
        StructField("is_minor", BooleanType()),
        StructField("is_patrolled", BooleanType()),
        StructField("length_old", IntegerType()),
        StructField("length_new", IntegerType()),
        StructField("rev_old", LongType()),
        StructField("rev_new", LongType()),
        StructField("log_type", StringType()),
        StructField("log_action", StringType()),
    ]
)


def table_name(rows: int) -> str:
    """``events_1m`` for 1,000,000 rows, ``events_20m`` for 20,000,000, else ``events_<rows>``."""
    if rows % 1_000_000 == 0:
        return f"events_{rows // 1_000_000}m"
    return f"events_{rows}"


def copy_namespace(copy: int) -> uuid.UUID:
    """The id namespace of copy ``copy`` (copy 0 included: no original id is reused)."""
    return uuid.uuid5(NAMESPACE, str(copy))


def scaled_id(copy: int, original: str) -> str:
    """The id of ``original``'s copy ``copy``; the Python oracle of :func:`uuid5_column`."""
    return str(uuid.uuid5(copy_namespace(copy), original))


def uuid5_column(namespace_hex: Column, name: Column) -> Column:
    """RFC 4122 version-5 UUID of ``name`` (UTF-8) in the namespace given as 32 hex digits."""
    digest = F.sha1(F.concat(F.unhex(namespace_hex), F.encode(name, "UTF-8")))
    variant_nibble = F.conv(F.substring(digest, 17, 1), 16, 10).cast("int")
    variant = F.lower(F.hex(variant_nibble.bitwiseAND(F.lit(3)).bitwiseOR(F.lit(8))))
    return F.concat(
        F.substring(digest, 1, 8),
        F.lit("-"),
        F.substring(digest, 9, 4),
        F.lit("-5"),
        F.substring(digest, 14, 3),
        F.lit("-"),
        variant,
        F.substring(digest, 18, 3),
        F.lit("-"),
        F.substring(digest, 21, 12),
    )


@dataclass(frozen=True, slots=True)
class Base:
    """The sample as rows, with what tiling needs to know about it."""

    rows: list[tuple[object, ...]]
    first: datetime
    last: datetime

    @property
    def span_seconds(self) -> int:
        """Seconds between consecutive copies: first to last event time, plus one."""
        return int((self.last - self.first).total_seconds()) + 1


def read_base(path: Path) -> Base:
    """The sample's valid events as base rows.

    ``seq`` numbers the valid events densely in file order, so ``copy * len(rows)
    + seq`` is a unique, gap-free offset even if the file holds invalid events.
    """
    rows: list[tuple[object, ...]] = []
    for event in read_sample(path):
        try:
            row = to_row(event, kafka_ts=event_time(event))
        except InvalidEventError:
            continue
        values = row.as_tuple()[: len(COLUMNS) - 3]  # without the Kafka fields
        rows.append((len(rows), *values))
    if not rows:
        raise ValueError(f"{path} holds no valid events")
    times = [ts for r in rows if isinstance(ts := r[2], datetime)]
    return Base(rows=rows, first=min(times), last=max(times))


def copies_needed(rows: int, base_rows: int) -> int:
    if rows <= 0:
        raise ValueError("rows must be positive")
    return math.ceil(rows / base_rows)


def _shift(column: str, micros: Column) -> Column:
    return F.timestamp_micros(F.unix_micros(F.col(column)) + micros)


def scaled_frame(spark: SparkSession, base: Base, rows: int) -> DataFrame:
    """Exactly ``rows`` rows with the columns of ``raw.recentchange``.

    Synthetic Kafka fields: the record time and the arrival time
    (``ingested_at``) are the event's shifted emit time (its event time when it
    has none); the partition is a hash of the id over the stream's three; the
    offset is the row's position in the scaled set (copy * sample size + seq),
    unique and dense.
    """
    n = len(base.rows)
    copies = copies_needed(rows, n)
    base_df = spark.createDataFrame(base.rows, _BASE_SCHEMA)
    namespaces = spark.createDataFrame(
        [(i, copy_namespace(i).hex) for i in range(copies)], ["copy", "ns"]
    )
    shift = F.col("copy") * F.lit(base.span_seconds * 1_000_000)
    offset = F.col("copy") * F.lit(n) + F.col("seq")
    frame = (
        base_df.crossJoin(F.broadcast(namespaces))
        .withColumn("kafka_offset", offset)
        .where(F.col("kafka_offset") < F.lit(rows))
        .withColumn("event_id", uuid5_column(F.col("ns"), F.col("event_id")))
        .withColumn("event_ts", _shift("event_ts", shift))
        .withColumn("emitted_at", _shift("emitted_at", shift))
        .withColumn("kafka_ts", F.coalesce(F.col("emitted_at"), F.col("event_ts")))
        .withColumn("ingested_at", F.col("kafka_ts"))
        .withColumn("kafka_partition", F.pmod(F.hash("event_id"), F.lit(STREAM_TASKS)))
    )
    return frame.select(*TABLE_COLUMNS)


# ------------------------------------------------------------------ the streaming layout


def hash_slots(spark: SparkSession, partitions: int) -> list[int]:
    """For each p in ``range(partitions)`` a long whose Spark hash lands in partition p.

    ``repartition(K, col)`` places a row in ``pmod(murmur3(col), K)``, so a
    column holding ``slots[p]`` for every row of group p puts each group in
    its own task, exactly one group per task.
    """
    found: dict[int, int] = {}
    limit = partitions * 16
    while len(found) < partitions:
        rows = (
            spark.range(0, limit)
            .select("id", F.pmod(F.hash("id"), F.lit(partitions)).alias("p"))
            .groupBy("p")
            .agg(F.min("id").alias("id"))
            .collect()
        )
        found = {int(r["p"]): int(r["id"]) for r in rows}
        limit *= 4
    return [found[p] for p in range(partitions)]


def stream_groups(frame: DataFrame) -> DataFrame:
    """``frame`` with ``_batch`` (arrival minute) and ``_task`` (the stream's task) added."""
    return frame.withColumn(
        "_batch", F.floor(F.unix_seconds(F.col("ingested_at")) / F.lit(TRIGGER_SECONDS))
    ).withColumn("_task", F.pmod(F.hash("event_id"), F.lit(STREAM_TASKS)))


def stream_layout(spark: SparkSession, frame: DataFrame) -> tuple[DataFrame, int]:
    """``frame`` repartitioned so each Spark task holds one simulated (micro-batch, task) group.

    Returns the frame and the number of groups. Needs adaptive execution off
    (it would coalesce the small shuffle partitions).
    """
    grouped = stream_groups(frame)
    keys = grouped.select("_batch", "_task").distinct().orderBy("_batch", "_task").collect()
    slots = hash_slots(spark, len(keys))
    mapping = spark.createDataFrame(
        [(int(k["_batch"]), int(k["_task"]), slots[i]) for i, k in enumerate(keys)],
        ["_batch", "_task", "_slot"],
    )
    laid_out = (
        grouped.join(F.broadcast(mapping), ["_batch", "_task"])
        .repartition(len(keys), F.col("_slot"))
        .select(*TABLE_COLUMNS)
    )
    return laid_out, len(keys)


# ------------------------------------------------------------------ the writes


def build_session(settings: Settings) -> SparkSession:
    """A batch session with the ``lake`` catalog; UTC; adaptive execution off (see above)."""
    builder = (
        SparkSession.builder.appName("tributary-bench-load")
        .master(settings.spark_master)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.extensions", EXTENSIONS)
    )
    for key, value in catalog_conf(settings).items():
        builder = builder.config(key, value)
    if settings.spark_packages:
        builder = builder.config("spark.jars.packages", settings.spark_packages)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


ICEBERG_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
}


def write_iceberg(spark: SparkSession, frame: DataFrame, table: str, *, stream: bool) -> None:
    """Replace ``table`` with ``frame``, partitioned by ``hours(event_ts)``.

    ``stream=False``: hash distribution on the partition, one task (one file)
    per hour. ``stream=True``: no distribution and the fanout writer, so every
    task writes one file per hour it holds, as the stream's tasks do.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    columns = ",\n    ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in frame.schema)
    props = dict(ICEBERG_PROPERTIES)
    if stream:
        props |= {"write.distribution-mode": "none", "write.spark.fanout.enabled": "true"}
    else:
        props |= {"write.distribution-mode": "hash"}
    properties = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
    spark.sql(
        f"CREATE TABLE {table} (\n    {columns}\n) USING iceberg "
        f"PARTITIONED BY (hours(event_ts)) TBLPROPERTIES ({properties})"
    )
    frame.writeTo(table).append()


def _rows_for_copy(rows: Iterator[Any]) -> Iterator[tuple[object, ...]]:
    for row in rows:
        values = list(row)
        for i, name in enumerate(TABLE_COLUMNS):
            value = values[i]
            if name in _TIME_COLUMNS and isinstance(value, datetime) and value.tzinfo is None:
                values[i] = value.astimezone(UTC)
        yield tuple(values)


def postgres_ddl(name: str, reader: str) -> list[sql.Composed]:
    """Statements run after the load: key, BRIN index, read access for Trino's role."""
    table = sql.Identifier(SCHEMA, name)
    return [
        sql.SQL("ALTER TABLE {} ADD PRIMARY KEY (event_id)").format(table),
        sql.SQL("CREATE INDEX {} ON {} USING brin (event_ts)").format(
            sql.Identifier(f"{name}_event_ts_brin"), table
        ),
        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
            sql.Identifier(SCHEMA), sql.Identifier(reader)
        ),
        sql.SQL("GRANT SELECT ON {} TO {}").format(table, sql.Identifier(reader)),
    ]


def write_postgres(settings: Settings, frame: DataFrame, name: str) -> int:
    """Replace ``bench.<name>`` with ``frame`` in ``event_ts`` order; returns rows copied."""
    from tributary.stream.sink import ensure_schema

    ensure_schema(settings.pg_conninfo)  # raw.recentchange is the column template
    table = sql.Identifier(SCHEMA, name)
    ordered = frame.orderBy("event_ts", "event_id")
    copied = 0
    with psycopg.connect(settings.pg_conninfo, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA)))
        conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table))
        conn.execute(
            sql.SQL("CREATE TABLE {} (LIKE raw.recentchange INCLUDING DEFAULTS)").format(table)
        )
        columns = sql.SQL(", ").join(sql.Identifier(c) for c in TABLE_COLUMNS)
        with (
            conn.transaction(),
            conn.cursor() as cur,
            cur.copy(sql.SQL("COPY {} ({}) FROM STDIN").format(table, columns)) as copy,
        ):
            for values in _rows_for_copy(ordered.toLocalIterator(prefetchPartitions=True)):
                copy.write_row(values)
                copied += 1
        for statement in postgres_ddl(name, settings.reader_user):
            conn.execute(statement)
        conn.execute(sql.SQL("VACUUM (ANALYZE) {}").format(table))
    return copied


@dataclass(slots=True)
class LoadReport:
    """What ``tributary bench load`` did, printed as JSON for the benchmark's raw output."""

    rows: int
    name: str
    sample_rows: int
    copies: int
    span_seconds: int
    first_event_ts: str
    last_event_ts: str
    stream_groups: int = 0
    seconds: dict[str, float] = field(default_factory=dict)
    postgres_rows: int = 0

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)


def load(
    settings: Settings,
    rows: int,
    *,
    targets: Sequence[str] = ("postgres", "iceberg", "stream"),
) -> LoadReport:
    """Generate ``rows`` rows and write them to every store named in ``targets``."""
    unknown = set(targets) - {"postgres", "iceberg", "stream"}
    if unknown:
        raise ValueError(f"unknown targets: {sorted(unknown)}")
    base = read_base(settings.sample_path)
    name = table_name(rows)
    copies = copies_needed(rows, len(base.rows))
    first = base.first.timestamp() * 1_000_000
    last_copy_shift = (copies - 1) * base.span_seconds * 1_000_000
    report = LoadReport(
        rows=rows,
        name=name,
        sample_rows=len(base.rows),
        copies=copies,
        span_seconds=base.span_seconds,
        first_event_ts=datetime.fromtimestamp(first / 1e6, UTC).isoformat(),
        last_event_ts=datetime.fromtimestamp(
            (base.last.timestamp() * 1_000_000 + last_copy_shift) / 1e6, UTC
        ).isoformat(),
    )
    spark = build_session(settings)
    try:
        frame = scaled_frame(spark, base, rows)
        if "iceberg" in targets:
            start = time.perf_counter()
            write_iceberg(spark, frame, f"{CATALOG}.{SCHEMA}.{name}", stream=False)
            report.seconds["iceberg"] = round(time.perf_counter() - start, 1)
            log.info("bench.load.iceberg", table=name, seconds=report.seconds["iceberg"])
        if "stream" in targets:
            start = time.perf_counter()
            laid_out, groups = stream_layout(spark, frame)
            write_iceberg(spark, laid_out, f"{CATALOG}.{SCHEMA}.{name}{STREAM_SUFFIX}", stream=True)
            report.stream_groups = groups
            report.seconds["stream"] = round(time.perf_counter() - start, 1)
            log.info("bench.load.stream", table=name, groups=groups, **report.seconds)
        if "postgres" in targets:
            start = time.perf_counter()
            report.postgres_rows = write_postgres(settings, frame, name)
            report.seconds["postgres"] = round(time.perf_counter() - start, 1)
            log.info("bench.load.postgres", table=name, rows=report.postgres_rows)
    finally:
        spark.stop()
    return report

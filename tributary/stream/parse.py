"""Kafka records to warehouse rows: the Spark twin of :func:`tributary.events.to_row`.

The JSON schema is explicit (no inference), so a field of the wrong type becomes
NULL instead of changing the plan. A record is dropped, and counted by reason
in the observed metric ``parse`` of every ``StreamingQueryProgress``, when it is:

``malformed``
    not JSON, without a valid lowercase UUID, without ``timestamp``, ``wiki``
    or ``type``, or with a NUL byte in a text field (PostgreSQL rejects it,
    and the sink would fail the same batch on every replay);
``future``
    well formed, but its ``timestamp`` is more than the skew bound ahead of its
    Kafka record time. Accepted, it would move the watermark past the clock
    and every later real event would be dropped as late (see
    :data:`tributary.events.DEFAULT_FUTURE_SKEW_FRACTION`);
``canary``
    a Wikimedia canary (valid, but not an edit).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from tributary.events import (
    COLUMNS,
    DEFAULT_FUTURE_SKEW_S,
    MAX_EVENT_TS,
    MIN_EVENT_TS,
    UUID_RE,
)

OBSERVATION = "parse"

_OLD_NEW_INT = StructType(
    [StructField("old", IntegerType()), StructField("new", IntegerType())],
)
_OLD_NEW_LONG = StructType(
    [StructField("old", LongType()), StructField("new", LongType())],
)

EVENT_SCHEMA = StructType(
    [
        StructField(
            "meta",
            StructType(
                [
                    StructField("id", StringType()),
                    StructField("dt", StringType()),
                    StructField("domain", StringType()),
                    StructField("stream", StringType()),
                ]
            ),
        ),
        StructField("id", LongType()),
        StructField("type", StringType()),
        StructField("namespace", IntegerType()),
        StructField("title", StringType()),
        StructField("timestamp", LongType()),
        StructField("user", StringType()),
        StructField("bot", BooleanType()),
        StructField("minor", BooleanType()),
        StructField("patrolled", BooleanType()),
        StructField("length", _OLD_NEW_INT),
        StructField("revision", _OLD_NEW_LONG),
        StructField("server_name", StringType()),
        StructField("wiki", StringType()),
        StructField("log_id", LongType()),
        StructField("log_type", StringType()),
        StructField("log_action", StringType()),
    ]
)
"""Schema of a trimmed recentchange event (see :func:`tributary.events.trim_event`)."""

TEXT_FIELDS = ("title", "user", "wiki", "type", "log_type", "log_action", "server_name")
"""Free-text fields that land in ``text`` columns; a NUL byte in any of them is malformed."""


def _has_nul(e: Column) -> Column:
    tests = [e["meta"]["domain"].contains("\x00")] + [e[f].contains("\x00") for f in TEXT_FIELDS]
    clause = tests[0]
    for t in tests[1:]:
        clause = clause | t
    return F.coalesce(clause, F.lit(False))


def _is_wellformed(e: Column) -> Column:
    return F.coalesce(
        e["meta"]["id"].rlike(UUID_RE.pattern)
        & e["timestamp"].between(MIN_EVENT_TS, MAX_EVENT_TS)
        & e["wiki"].isNotNull()
        & e["type"].isNotNull()
        & ~_has_nul(e),
        F.lit(False),
    )


def _within_skew(e: Column, kafka_ts: Column, max_future_skew_s: int) -> Column:
    """``timestamp`` at most the bound ahead of the record time (whole seconds, as stored)."""
    return F.coalesce(
        e["timestamp"] <= F.unix_timestamp(kafka_ts) + F.lit(max_future_skew_s), F.lit(False)
    )


def _representable(ts: Column) -> Column:
    """NULL for timestamps past year 9999, which PySpark cannot hand to Python."""
    return F.when(F.year(ts) <= 9999, ts)


def _as_int(flag: Column) -> Column:
    return F.when(flag, 1).otherwise(0)


def parse_events(kafka_df: DataFrame, max_future_skew_s: int = DEFAULT_FUTURE_SKEW_S) -> DataFrame:
    """Parse Kafka source rows into the columns of ``raw.recentchange``.

    Reads the Kafka source columns ``value``, ``partition``, ``offset`` and
    ``timestamp`` and returns exactly :data:`tributary.events.COLUMNS`, in order.
    ``event_ts`` comes from the MediaWiki ``timestamp``, ``emitted_at`` from
    ``meta.dt`` (NULL when it does not parse). Requires the session time zone UTC.
    """
    e = F.col("e")
    flagged = (
        kafka_df.select(
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp"),
        )
        .withColumn("_wellformed", _is_wellformed(e))
        .withColumn(
            "_valid",
            F.col("_wellformed") & _within_skew(e, F.col("timestamp"), max_future_skew_s),
        )
        .withColumn("_canary", F.coalesce(e["meta"]["domain"] == F.lit("canary"), F.lit(False)))
        .observe(
            OBSERVATION,
            F.count(F.lit(1)).alias("input_rows"),
            F.sum(_as_int(~F.col("_wellformed"))).alias("malformed_rows"),
            F.sum(_as_int(F.col("_wellformed") & ~F.col("_valid"))).alias("future_rows"),
            F.sum(_as_int(F.col("_valid") & F.col("_canary"))).alias("canary_rows"),
        )
        .where(F.col("_valid") & ~F.col("_canary"))
    )
    columns = {
        "event_id": e["meta"]["id"],
        "event_ts": F.timestamp_seconds(e["timestamp"]),
        "emitted_at": _representable(F.try_to_timestamp(e["meta"]["dt"])),
        "rc_id": e["id"],
        "wiki": e["wiki"],
        "domain": e["meta"]["domain"],
        "event_type": e["type"],
        "namespace": e["namespace"],
        "title": e["title"],
        "user_name": e["user"],
        "is_bot": e["bot"],
        "is_minor": e["minor"],
        "is_patrolled": e["patrolled"],
        "length_old": e["length"]["old"],
        "length_new": e["length"]["new"],
        "rev_old": e["revision"]["old"],
        "rev_new": e["revision"]["new"],
        "log_type": e["log_type"],
        "log_action": e["log_action"],
        "kafka_partition": F.col("partition"),
        "kafka_offset": F.col("offset"),
        "kafka_ts": F.col("timestamp"),
    }
    return flagged.select(*(columns[name].alias(name) for name in COLUMNS))


def deduplicate(df: DataFrame, watermark_delay: str) -> DataFrame:
    """Drop repeated ``event_id`` values within the event-time watermark.

    State for an id is kept until the watermark passes ``event_ts + delay``.
    Rows whose ``event_ts`` is already behind the watermark are dropped by the
    operator, so the delay must cover the lateness measured on the stream.
    """
    return df.withWatermark("event_ts", watermark_delay).dropDuplicatesWithinWatermark(["event_id"])

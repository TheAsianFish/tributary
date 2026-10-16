"""The Spark Structured Streaming job: Kafka to ``raw.recentchange``."""

from __future__ import annotations

import json
import signal
import threading
from types import FrameType
from typing import TYPE_CHECKING, override

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming.listener import StreamingQueryListener
from pyspark.sql.streaming.query import StreamingQuery

from tributary.stream.parse import deduplicate, parse_events
from tributary.stream.sink import PostgresSink, ensure_schema

if TYPE_CHECKING:
    from pyspark.sql.streaming.listener import (
        QueryProgressEvent,
        QueryStartedEvent,
        QueryTerminatedEvent,
    )

    from tributary.config import Settings

QUERY_NAME = "warehouse"
ROCKSDB_PROVIDER = "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
_DURATIONS = ("addBatch", "latestOffset", "queryPlanning", "walCommit", "triggerExecution")

log = structlog.get_logger("tributary.stream.job")


def build_session(settings: Settings) -> SparkSession:
    """Create the Spark session for the stream job.

    State lives in RocksDB rather than the default JVM-heap store. The dedup
    operator keeps one key per event id until the watermark (newest event time
    minus the delay) passes its event time plus the delay, so state spans about
    twice the delay of event time, whatever the replay speed: 29,245 keys at the
    end of the 50,000-event sample (~23 events/s) with a 10-minute delay.
    The heap store would cope with that, but it snapshots the whole map and
    competes with the job for heap as the delay or the rate grows; RocksDB keeps
    state off-heap and changelog checkpointing writes only each batch's changes.
    The provider is fixed for the lifetime of a checkpoint, so it is chosen up front.
    """
    builder = (
        SparkSession.builder.appName("tributary-stream")
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(settings.topic_partitions))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.streaming.stateStore.providerClass", ROCKSDB_PROVIDER)
        .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
    )
    if settings.spark_packages:
        builder = builder.config("spark.jars.packages", settings.spark_packages)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def kafka_source(spark: SparkSession, settings: Settings) -> DataFrame:
    """Subscribe to the topic from the earliest offset, bounded per trigger.

    ``failOnDataLoss`` stays true: if offsets the checkpoint expects have been
    deleted by retention or the topic was recreated, silently skipping them
    would turn "no loss" into an unverifiable claim. Failing the query makes
    the loss visible; recovery is a deliberate reset of the checkpoint.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", str(settings.max_offsets_per_trigger))
        .option("failOnDataLoss", "true")
        .load()
    )


def start_warehouse_query(
    spark: SparkSession, settings: Settings, *, available_now: bool = False
) -> StreamingQuery:
    """Start Kafka, parse, dedup, PostgreSQL as the ``warehouse`` query."""
    events = deduplicate(parse_events(kafka_source(spark, settings)), settings.watermark_delay)
    writer = (
        events.writeStream.queryName(QUERY_NAME)
        .foreachBatch(PostgresSink(settings.pg_conninfo))
        .option("checkpointLocation", str(settings.checkpoint_dir))
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=settings.trigger_interval)
    return writer.start()


class ProgressLogger(StreamingQueryListener):
    """Log one structured line per query lifecycle event and micro-batch."""

    def __init__(self) -> None:
        # Listener callbacks run on a py4j thread, which does not inherit contextvars.
        self._log = log.bind(**structlog.contextvars.get_contextvars())

    @override
    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        self._log.info("stream.started", query=event.name, id=str(event.id), run=str(event.runId))

    @override
    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        p = event.progress
        self._log.info(
            "stream.progress",
            query=p.name,
            batch_id=p.batchId,
            input_rows=p.numInputRows,
            input_rows_per_s=round(p.inputRowsPerSecond, 1),
            processed_rows_per_s=round(p.processedRowsPerSecond, 1),
            duration_ms={k: p.durationMs[k] for k in _DURATIONS if k in p.durationMs},
            watermark=p.eventTime.get("watermark"),
            state=[
                {
                    "operator": s.operatorName,
                    "rows_total": s.numRowsTotal,
                    "rows_dropped_by_watermark": s.numRowsDroppedByWatermark,
                    "rows_removed": s.numRowsRemoved,
                }
                for s in p.stateOperators
            ],
            observed={name: row.asDict() for name, row in p.observedMetrics.items()},
            sources=[
                {
                    "start": _offsets(s.startOffset),
                    "end": _offsets(s.endOffset),
                    "latest": _offsets(s.latestOffset),
                }
                for s in p.sources
            ],
        )

    @override
    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        if event.exception:
            self._log.error("stream.terminated", id=str(event.id), exception=event.exception)
        else:
            self._log.info("stream.terminated", id=str(event.id))


def _offsets(raw: str | None) -> object:
    """Kafka offsets arrive as a JSON string; log them as an object."""
    if raw is None:
        return None
    try:
        parsed: object = json.loads(raw)
    except ValueError:
        return raw
    return parsed


def run(settings: Settings, *, available_now: bool = False) -> None:
    """Run the warehouse query until it ends, fails, or SIGTERM/SIGINT stops it.

    Stopping interrupts the micro-batch in flight; it is redone on restart
    from the checkpoint, and the idempotent sink absorbs the replay.
    """
    from tributary.ingest.producer import ensure_topic

    ensure_topic(settings.kafka_bootstrap, settings.topic, settings.topic_partitions)
    ensure_schema(settings.pg_conninfo)
    spark = build_session(settings)
    stop = threading.Event()

    def request_stop(signum: int, _frame: FrameType | None) -> None:
        log.info("stream.stop_requested", signal=signal.Signals(signum).name)
        stop.set()

    previous = {s: signal.signal(s, request_stop) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        spark.streams.addListener(ProgressLogger())
        query = start_warehouse_query(spark, settings, available_now=available_now)
        while query.isActive and not stop.is_set():
            query.awaitTermination(1)
        if query.isActive:
            query.stop()
        query.awaitTermination()
    finally:
        for s, handler in previous.items():
            signal.signal(s, handler)
        spark.stop()

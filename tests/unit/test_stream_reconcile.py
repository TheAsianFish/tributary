"""The reconciliation pass in Spark local mode, and what it changes in the stream_loss check.

The multi-batch harness of ``test_stream_spark.py`` (one JSON file per micro-batch
through the job's own parser and dedup operator) stands in for the topic: each
record carries the partition and offset it would have in Kafka, so the batches'
offsets and a pass's window are what the job and a pass would record live.
"""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.streaming.listener import StreamingQueryProgress
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from bench.skew import Record, sample_records
from tests.unit.test_stream_spark import (
    KAFKA_SCHEMA,
    KAFKA_TS,
    MALFORMED,
    VALID,
    by_batch,
    kafka_batch,
)
from tributary.stream.parse import REJECT_REASON, classify_events
from tributary.stream.reconcile import MemoryStore, judge_records
from tributary.warehouse.stream_health import (
    DroppedBatch,
    PassWindow,
    Thresholds,
    WindowCounts,
    covers,
    evaluate,
)

pytestmark = pytest.mark.spark

TOPIC = "wiki.recentchange"
PARTITIONS = 3
DELAY = "10 minutes"
NOW = datetime(2026, 10, 17, 1, tzinfo=UTC)
FILE_SCHEMA = StructType(
    [
        StructField("value", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
    ]
)


class Topic:
    """Records appended in order, one file per micro-batch, offsets kept per partition."""

    def __init__(self, src: Path) -> None:
        self.src = src
        src.mkdir()
        self.next: dict[int, int] = dict.fromkeys(range(PARTITIONS), 0)
        self.batches: list[tuple[dict[str, int], dict[str, int]]] = []
        self.rows: list[tuple[str, int, int, datetime]] = []

    def append(self, records: Sequence[Record]) -> None:
        b = len(self.batches)
        start = {str(p): o for p, o in self.next.items()}
        path = self.src / f"batch-{b:03d}.json"
        with path.open("w") as f:
            for i, r in enumerate(records):
                p = i % PARTITIONS
                ts = datetime.fromtimestamp(r.kafka_ms / 1000, UTC)
                row = (r.payload, p, self.next[p], ts)
                self.rows.append(row)
                record = {"value": r.payload, "partition": p, "offset": self.next[p]}
                f.write(json.dumps({**record, "timestamp": ts.isoformat()}) + "\n")
                self.next[p] += 1
        os.utime(path, (1_000_000 + b, 1_000_000 + b))
        self.batches.append((start, {str(p): o for p, o in self.next.items()}))

    def as_batch(self, spark: SparkSession) -> DataFrame:
        """Everything appended, as a Kafka batch read would return it."""
        rows = [(v.encode(), p, o, ts) for v, p, o, ts in self.rows]
        return spark.createDataFrame(rows, KAFKA_SCHEMA)

    def window(self) -> PassWindow:
        """A pass over every offset appended (``earliest`` to the end)."""
        start = {TOPIC: dict.fromkeys(map(str, range(PARTITIONS)), 0)}
        return PassWindow(1, "warehouse", start, {TOPIC: {str(p): o for p, o in self.next.items()}})


def run_stream(
    spark: SparkSession, topic: Topic, checkpoint: Path, sink: IdCollector
) -> dict[int, StreamingQueryProgress]:
    from tributary.config import Settings
    from tributary.stream.parse import deduplicate, parse_events

    source = (
        spark.readStream.schema(FILE_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .json(str(topic.src))
        .withColumn("value", F.col("value").cast("binary"))
    )
    bound = Settings(_env_file=None).future_skew_s
    query = (
        deduplicate(parse_events(source, bound), DELAY)
        .writeStream.foreachBatch(sink)
        .option("checkpointLocation", str(checkpoint))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return by_batch(list(query.recentProgress))


def dropped(progress: dict[int, StreamingQueryProgress], topic: Topic) -> list[DroppedBatch]:
    """The batches that dropped rows, with the offsets the job would have recorded."""
    out = []
    for b, p in sorted(progress.items()):
        n = sum(int(s.numRowsDroppedByWatermark) for s in p.stateOperators)
        if n:
            start, end = topic.batches[b]
            out.append(DroppedBatch("warehouse", b, n, {TOPIC: start}, {TOPIC: end}))
    return out


def check(
    batches: Sequence[DroppedBatch], passes: Sequence[PassWindow], still_missing: int = 0
) -> str:
    """The warehouse query's stream_loss status over these batches (no rejects)."""
    total = sum(b.dropped for b in batches)
    covered = sum(b.dropped for b in batches if any(covers(p, b) for p in passes))
    counts = WindowCounts(
        "warehouse",
        batches=len(batches),
        input_rows=100_000,
        dropped_by_watermark=total,
        malformed_rows=0,
        future_rows=0,
        canary_rows=0,
        covered_dropped=covered,
        still_missing=still_missing,
        passes=len(passes),
    )
    return evaluate(counts, Thresholds(), window=timedelta(hours=24), now=NOW).status


def landed_ids(sink: IdCollector) -> list[str]:
    return [i for _, ids in sink.batches for i in ids]


class IdCollector:
    """The sink: every event id each micro-batch delivered."""

    def __init__(self) -> None:
        self.batches: list[tuple[int, list[str]]] = []

    def __call__(self, df: DataFrame, batch_id: int) -> None:
        ids = sorted(str(r["event_id"]) for r in df.select("event_id").collect())
        self.batches.append((batch_id, ids))


def test_the_samples_first_5000_events_sent_twice_lose_nothing_and_a_pass_clears_the_check(
    spark: SparkSession, tmp_path: Path
) -> None:
    """M11's check counted 5,000 drops here with no event missing, and stayed red."""
    sample = sample_records()
    head, rest = sample[:5_000], sample[5_000::10]
    topic = Topic(tmp_path / "src")
    topic.append(head)
    for i in range(3):  # the rest of the sample's time span, so the watermark passes the head
        topic.append(rest[1_500 * i : 1_500 * (i + 1)])
    topic.append(head)  # the first 5,000 again: a redelivery

    sink = IdCollector()
    progress = run_stream(spark, topic, tmp_path / "checkpoint", sink)

    sent = {r.event_id for r in head} | {r.event_id for r in rest}
    out = landed_ids(sink)
    assert len(out) == len(set(out)) == len(sent)  # every event once: 0 lost, 0 twice
    assert set(out) == sent
    batches = dropped(progress, topic)
    assert [(b.batch_id, b.dropped) for b in batches] == [(4, 5_000)]
    assert check(batches, passes=[]) == "fail"  # today's check: red with nothing lost

    verdict = judge_records(topic.as_batch(spark), 300, MemoryStore(out), repair=True)
    assert verdict.records == len(topic.rows) == 14_500  # 9,500 events, 5,000 of them twice
    assert (verdict.landed, len(verdict.missing), verdict.recovered) == (14_500, 0, 0)
    assert (verdict.malformed, verdict.future, verdict.canary) == (0, 0, 0)
    assert check(batches, passes=[topic.window()]) == "pass"


def test_a_late_event_is_found_missing_written_back_and_only_then_green(
    spark: SparkSession, tmp_path: Path
) -> None:
    sample = sample_records()
    head, tail = sample[:2_000], sample[-2_000:]
    late = head[0]
    # The same event, now stamped well after the watermark has passed its time.
    late_copy = Record(
        "0f0f0f0f-0000-4000-8000-000000000001",
        late.event_ts,
        tail[-1].kafka_ms,
        late.payload.replace(late.event_id, "0f0f0f0f-0000-4000-8000-000000000001"),
    )
    topic = Topic(tmp_path / "src")
    topic.append(head)
    topic.append(tail[:1_000])
    topic.append(tail[1_000:])
    topic.append([late_copy])

    sink = IdCollector()
    progress = run_stream(spark, topic, tmp_path / "checkpoint", sink)
    out = landed_ids(sink)
    batches = dropped(progress, topic)
    assert [(b.batch_id, b.dropped) for b in batches] == [(3, 1)]
    assert late_copy.event_id not in out

    report = judge_records(topic.as_batch(spark), 300, MemoryStore(out), repair=False)
    assert [m.event_id for m in report.missing] == [late_copy.event_id]
    assert report.missing[0].event_ts == datetime.fromtimestamp(late.event_ts, UTC)
    _, partition, offset, _ = topic.rows[-1]
    assert (report.missing[0].kafka_partition, report.missing[0].kafka_offset) == (
        partition,
        offset,
    )
    assert report.recovered == 0
    # A pass that covered the batch but left the event missing keeps the check red.
    assert check(batches, [topic.window()], still_missing=1) == "fail"

    repaired = judge_records(topic.as_batch(spark), 300, MemoryStore(out), repair=True)
    assert repaired.recovered == 1
    assert check(batches, [topic.window()], still_missing=0) == "pass"


def test_classify_events_gives_every_record_the_reason_the_job_drops_it(
    spark: SparkSession,
) -> None:
    valid = [json.dumps(e).encode() for e in VALID]
    future = json.dumps({**VALID[0], "timestamp": int(KAFKA_TS.timestamp()) + 301}).encode()
    rows = classify_events(kafka_batch(spark, [*valid, *MALFORMED, future]), 300).collect()

    reasons = [r[REJECT_REASON] for r in rows]
    # MALFORMED holds one canary and one record a day ahead (rejected as future).
    assert reasons.count(None) == len(VALID)
    assert reasons.count("canary") == 1
    assert reasons.count("future") == 2
    assert reasons.count("malformed") == len(MALFORMED) - 2
    for r in rows:
        assert r["kafka_offset"] is not None
        if r[REJECT_REASON] is not None:
            assert r["event_id"] is None
            assert r["event_ts"] is None


def test_classify_and_parse_keep_the_same_rows(spark: SparkSession) -> None:
    from tributary.stream.parse import parse_events

    payloads = [json.dumps(e).encode() for e in VALID] + MALFORMED
    kept = sorted(tuple(r) for r in parse_events(kafka_batch(spark, payloads), 300).collect())
    classified = classify_events(kafka_batch(spark, payloads), 300)
    same = classified.where(F.col(REJECT_REASON).isNull()).drop(REJECT_REASON)
    assert sorted(tuple(r) for r in same.collect()) == kept

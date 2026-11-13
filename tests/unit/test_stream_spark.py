"""The Spark parser and dedup operator, in local mode against batch and file-stream inputs."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest
from structlog.testing import capture_logs

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.streaming.listener import StreamingQueryProgress
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from tributary.events import COLUMNS, DEFAULT_FUTURE_SKEW_S, to_row
from tributary.stream.job import ProgressLogger
from tributary.stream.parse import OBSERVATION, deduplicate, parse_events
from tributary.stream.sink import adapt_row

if TYPE_CHECKING:
    from pyspark.sql.streaming.listener import QueryProgressEvent

pytestmark = pytest.mark.spark

# The record is appended just after the edit (23:34:15), as it would be live.
KAFKA_TS = datetime(2026, 10, 16, 23, 35, tzinfo=UTC)
KAFKA_SCHEMA = StructType(
    [
        StructField("value", BinaryType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
    ]
)


def uuid(n: int) -> str:
    return f"00000000-0000-4000-8000-{n:012d}"


# A trimmed event; values are heterogeneous JSON.
def event(n: int, ts: int = 1792193655, **overrides: Any) -> dict[str, Any]:
    e: dict[str, Any] = {
        "meta": {
            "id": uuid(n),
            "dt": "2026-10-16T23:34:16.644Z",
            "domain": "ku.wiktionary.org",
            "stream": "mediawiki.recentchange",
        },
        "id": 13462683 + n,
        "type": "edit",
        "namespace": 0,
        "title": "top bûn",
        "timestamp": ts,
        "user": "Balyozbot",
        "bot": True,
        "minor": True,
        "patrolled": False,
        "length": {"old": 347, "new": 333},
        "revision": {"old": 6654840, "new": 6802263},
        "server_name": "ku.wiktionary.org",
        "wiki": "kuwiktionary",
    }
    e.update(overrides)
    return e


def kafka_batch(spark: SparkSession, payloads: list[bytes]) -> DataFrame:
    rows = [(value, i % 3, 100 + i, KAFKA_TS) for i, value in enumerate(payloads)]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


VALID = [
    event(1),
    event(2, type="new", length={"new": 12}, revision={"new": 1}),
    {
        "meta": {"id": uuid(3), "dt": "2026-10-16T23:34:17Z", "domain": "commons.wikimedia.org"},
        "id": 99,
        "type": "log",
        "namespace": 6,
        "title": "File:X.jpg",
        "timestamp": 1792193600,
        "user": "Someone",
        "bot": False,
        "log_id": 5,
        "log_type": "upload",
        "log_action": "upload",
        "wiki": "commonswiki",
    },
    event(4, bot="yes", namespace="zero"),
    event(5, meta={"id": uuid(5), "dt": "not a date"}),
    event(13, meta={"id": uuid(13), "dt": "+10000-01-01T00:00:00Z"}),
]
MALFORMED = [
    b"{not json",
    b"",
    json.dumps(event(6, meta={"id": "AAAAAAAA-0000-4000-8000-000000000006"})).encode(),
    json.dumps({k: v for k, v in event(7).items() if k != "meta"}).encode(),
    json.dumps({k: v for k, v in event(8).items() if k != "timestamp"}).encode(),
    json.dumps(event(9, timestamp="1792193655")).encode(),
    json.dumps({k: v for k, v in event(10).items() if k != "wiki"}).encode(),
    json.dumps({k: v for k, v in event(11).items() if k != "type"}).encode(),
    json.dumps(event(12, meta={"id": uuid(12), "domain": "canary"})).encode(),
    # Past year 9999: valid JSON, but PySpark could not convert the row for the sink.
    json.dumps(event(14, ts=300_000_000_000)).encode(),
    json.dumps(event(15, ts=-1)).encode(),
    # More than a day ahead of the Kafka record time: would ratchet the watermark.
    json.dumps(event(18, ts=int(KAFKA_TS.timestamp()) + 2 * 86_400)).encode(),
    # A NUL byte in a text column: PostgreSQL rejects it in COPY, so it must not reach the sink.
    json.dumps(event(16, title="a\u0000b")).encode(),
    json.dumps(event(17, user="\u0000")).encode(),
]


def test_parse_events_matches_the_python_parser(spark: SparkSession) -> None:
    payloads = [json.dumps(e).encode() for e in VALID] + MALFORMED
    parsed = parse_events(kafka_batch(spark, payloads))

    assert tuple(parsed.columns) == COLUMNS
    got = {r["event_id"]: adapt_row(r) for r in parsed.collect()}
    expected = {
        e["meta"]["id"]: to_row(e, partition=n % 3, offset=100 + n, kafka_ts=KAFKA_TS).as_tuple()
        for n, e in enumerate(VALID)
    }
    assert got == expected


def test_parse_events_column_types_match_the_landing_table(spark: SparkSession) -> None:
    types = dict(parse_events(kafka_batch(spark, [])).dtypes)
    assert types == {
        "event_id": "string",
        "event_ts": "timestamp",
        "emitted_at": "timestamp",
        "rc_id": "bigint",
        "wiki": "string",
        "domain": "string",
        "event_type": "string",
        "namespace": "int",
        "title": "string",
        "user_name": "string",
        "is_bot": "boolean",
        "is_minor": "boolean",
        "is_patrolled": "boolean",
        "length_old": "int",
        "length_new": "int",
        "rev_old": "bigint",
        "rev_new": "bigint",
        "log_type": "string",
        "log_action": "string",
        "kafka_partition": "int",
        "kafka_offset": "bigint",
        "kafka_ts": "timestamp",
    }


# Micro-batches fed one file per trigger; event times in unix seconds, delay 60 s.
# The watermark after a batch is max(event_ts) - 60. Spark evicts state with the
# current batch's watermark but filters late rows with the previous batch's.
DEDUP_BATCHES: list[list[tuple[int, int]]] = [
    [(1, 1000), (1, 1000), (2, 1000)],  # 0: duplicate in the batch -> 1, 2
    [(1, 1000), (3, 2000)],  # 1: 1 repeats within the watermark -> 3; watermark 1940
    [(4, 1500), (2, 1000)],  # 2: 4 is behind the watermark but the late filter still
    #                             uses 940, so 4 is kept; 2 is still in state; then
    #                             1, 2 and 4 are evicted (expiry ts + 60 < 1940)
    [(5, 1950), (4, 1960)],  # 3: on time -> 5; key 4 expired in batch 2, so this
    #                             later copy of 4 is a new key and passes (plain
    #                             dropDuplicates would still remember it)
    [(1, 1000), (6, 100)],  # 4: late filter now at 1940: the duplicate of 1 (state
    #                             gone) and the new event 6 are both dropped as late
    [(7, 1990)],  # 5: on time -> 7; the watermark does not move, so no extra batch follows
]
EXPECTED_OUTPUT = {0: [1, 2], 1: [3], 2: [4], 3: [4, 5], 4: [], 5: [7]}


def write_stream_files(src: Path) -> None:
    src.mkdir()
    offset = 0
    for b, batch in enumerate(DEDUP_BATCHES):
        path = src / f"batch-{b}.json"
        with path.open("w") as f:
            for n, ts in batch:
                record = {
                    "value": json.dumps(event(n, ts)),
                    "partition": 0,
                    "offset": offset,
                    "timestamp": "2026-10-16T23:00:00Z",
                }
                f.write(json.dumps(record) + "\n")
                offset += 1
        os.utime(path, (1_000_000 + b, 1_000_000 + b))


class Collector:
    def __init__(self) -> None:
        self.batches: list[tuple[int, list[int]]] = []

    def __call__(self, df: DataFrame, batch_id: int) -> None:
        ids = sorted(int(r["event_id"][-12:]) for r in df.select("event_id").collect())
        self.batches.append((batch_id, ids))


def run_dedup(
    spark: SparkSession,
    src: Path,
    checkpoint: Path,
    sink: Collector,
    *,
    delay: str = "60 seconds",
    max_future_skew_s: int = DEFAULT_FUTURE_SKEW_S,
) -> list[Any]:
    file_schema = StructType(
        [
            StructField("value", StringType()),
            StructField("partition", IntegerType()),
            StructField("offset", LongType()),
            StructField("timestamp", TimestampType()),
        ]
    )
    source = (
        spark.readStream.schema(file_schema)
        .option("maxFilesPerTrigger", 1)
        .json(str(src))
        .withColumn("value", F.col("value").cast("binary"))
    )
    query = (
        deduplicate(parse_events(source, max_future_skew_s), delay)
        .writeStream.foreachBatch(sink)
        .option("checkpointLocation", str(checkpoint))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return list(query.recentProgress)


def by_batch(progress: list[StreamingQueryProgress]) -> dict[int, StreamingQueryProgress]:
    return {p.batchId: p for p in progress if p.numInputRows > 0}


def test_deduplicate_semantics_across_micro_batches(spark: SparkSession, tmp_path: Path) -> None:
    src, checkpoint = tmp_path / "src", tmp_path / "checkpoint"
    write_stream_files(src)
    sink = Collector()

    progress = by_batch(run_dedup(spark, src, checkpoint, sink))

    output = {b: ids for b, ids in sink.batches if b in progress}
    assert output == EXPECTED_OUTPUT
    dropped_late = {
        b: sum(s.numRowsDroppedByWatermark for s in p.stateOperators) for b, p in progress.items()
    }
    assert dropped_late == {0: 0, 1: 0, 2: 0, 3: 0, 4: 2, 5: 0}
    # Eviction uses the current batch's watermark: 1, 2 and 4 expire in batch 2, nothing else.
    evicted = {b: sum(s.numRowsRemoved for s in p.stateOperators) for b, p in progress.items()}
    assert evicted == {0: 0, 1: 0, 2: 3, 3: 0, 4: 0, 5: 0}
    in_state = {b: sum(s.numRowsTotal for s in p.stateOperators) for b, p in progress.items()}
    assert in_state == {0: 2, 1: 3, 2: 1, 3: 3, 4: 3, 5: 4}
    assert progress[2].eventTime["watermark"] == "1970-01-01T00:32:20.000Z"
    observed = progress[0].observedMetrics[OBSERVATION].asDict()
    assert observed == {"input_rows": 3, "malformed_rows": 0, "future_rows": 0, "canary_rows": 0}

    # A crash after the sink wrote batch 5 but before Spark committed it: on restart
    # the same batch id is replayed with the same rows, which only the sink's
    # primary key keeps out of the table a second time.
    last = max(b for b, _ in sink.batches)
    assert last == max(progress)
    for name in (str(last), f".{last}.crc"):
        (checkpoint / "commits" / name).unlink(missing_ok=True)
    replay = Collector()
    run_dedup(spark, src, checkpoint, replay)
    assert replay.batches[0] == (last, EXPECTED_OUTPUT[last])


# The poison scenario on the job's own settings (10-minute delay): six micro-batches
# 30 s of event time apart, four real events each (one of them 100 s late), every
# record stamped in Kafka 2 s after its event time, as the live producer would. In
# batch 1 one more record, otherwise valid, claims an event time 23 h past its own
# record time (a broken clock upstream, or a test that replays into the future).
T0 = 1_792_194_000  # 2026-10-16T23:40:00Z
POISON_ID, POISON_AHEAD_S, POISON_BATCH = 999, 82_800, 1
SKEW_BATCHES = 6
EVENTS_PER_BATCH = 4


def skew_records() -> list[list[tuple[int, int, int]]]:
    """Per batch, (id, event time, Kafka time) in stream order."""
    batches = []
    for b in range(SKEW_BATCHES):
        base = T0 + 30 * b
        times = [base, base + 1, base - 100, base + 2]
        records = [(10 * b + i, ts, ts + 2) for i, ts in enumerate(times)]
        if b == POISON_BATCH:
            kafka = base + 3
            records.insert(2, (POISON_ID, kafka + POISON_AHEAD_S, kafka))
        batches.append(records)
    return batches


def write_skew_files(src: Path) -> None:
    src.mkdir()
    offset = 0
    for b, batch in enumerate(skew_records()):
        path = src / f"batch-{b}.json"
        with path.open("w") as f:
            for n, ts, kafka in batch:
                record = {
                    "value": json.dumps(event(n, ts)),
                    "partition": 0,
                    "offset": offset,
                    "timestamp": datetime.fromtimestamp(kafka, UTC).isoformat(),
                }
                f.write(json.dumps(record) + "\n")
                offset += 1
        os.utime(path, (1_000_000 + b, 1_000_000 + b))


def run_skew(
    spark: SparkSession, tmp_path: Path, max_future_skew_s: int
) -> tuple[dict[int, list[int]], dict[int, StreamingQueryProgress]]:
    src = tmp_path / "src"
    write_skew_files(src)
    sink = Collector()
    progress = by_batch(
        run_dedup(
            spark,
            src,
            tmp_path / "checkpoint",
            sink,
            delay="10 minutes",
            max_future_skew_s=max_future_skew_s,
        )
    )
    return {b: ids for b, ids in sink.batches if b in progress}, progress


def real_ids(batch: int) -> list[int]:
    return [10 * batch + i for i in range(EVENTS_PER_BATCH)]


def test_an_event_23_hours_ahead_is_rejected_and_costs_no_later_event(
    spark: SparkSession, tmp_path: Path
) -> None:
    from tributary.config import Settings

    bound = Settings(_env_file=None).future_skew_s
    assert bound == 300
    output, progress = run_skew(spark, tmp_path, bound)

    assert output == {b: real_ids(b) for b in range(SKEW_BATCHES)}
    dropped = sum(s.numRowsDroppedByWatermark for p in progress.values() for s in p.stateOperators)
    assert dropped == 0
    observed = progress[POISON_BATCH].observedMetrics[OBSERVATION].asDict()
    assert observed == {"input_rows": 5, "malformed_rows": 0, "future_rows": 1, "canary_rows": 0}
    # The watermark trails the real events: newest event time through batch 4, minus 10 min.
    assert progress[5].eventTime["watermark"] == "2026-10-16T23:32:02.000Z"


def test_with_the_old_one_day_bound_the_same_event_drops_every_later_batch(
    spark: SparkSession, tmp_path: Path
) -> None:
    """What the job did before the bound was tied to the watermark delay.

    The poison is accepted and moves the watermark to its event time minus 10 min,
    22 h 50 min past the clock. The late filter reads the watermark of two batches
    back (see DEDUP_BATCHES), so the poison's own batch and the next one survive and
    every real event from batch 3 on, 3 batches x 4 events, is dropped as late.
    """
    output, progress = run_skew(spark, tmp_path, 86_400)

    assert output == {
        0: real_ids(0),
        1: sorted([*real_ids(1), POISON_ID]),
        2: real_ids(2),
        3: [],
        4: [],
        5: [],
    }
    dropped = {
        b: sum(s.numRowsDroppedByWatermark for s in p.stateOperators) for b, p in progress.items()
    }
    assert dropped == {0: 0, 1: 0, 2: 0, 3: 4, 4: 4, 5: 4}
    observed = progress[POISON_BATCH].observedMetrics[OBSERVATION].asDict()
    assert observed["future_rows"] == 0
    poison_watermark = datetime.fromtimestamp(T0 + 30 + 3 + POISON_AHEAD_S - 600, UTC)
    assert progress[3].eventTime["watermark"] == poison_watermark.strftime("%Y-%m-%dT%H:%M:%S.000Z")


class ProgressEvent:
    def __init__(self, progress: StreamingQueryProgress) -> None:
        self.progress = progress


def test_progress_logger_reports_state_and_observed_metrics(
    spark: SparkSession, tmp_path: Path
) -> None:
    src = tmp_path / "src"
    write_stream_files(src)
    progress = by_batch(run_dedup(spark, src, tmp_path / "checkpoint", Collector()))

    with capture_logs() as logs:
        ProgressLogger().onQueryProgress(cast("QueryProgressEvent", ProgressEvent(progress[4])))

    [entry] = logs
    assert entry["event"] == "stream.progress"
    assert entry["batch_id"] == 4
    assert entry["input_rows"] == 2
    assert set(entry["duration_ms"]) >= {"addBatch", "latestOffset", "triggerExecution"}
    assert entry["watermark"] == "1970-01-01T00:32:20.000Z"
    [state] = entry["state"]
    assert state["rows_dropped_by_watermark"] == 2
    assert entry["observed"] == {
        OBSERVATION: {"input_rows": 2, "malformed_rows": 0, "future_rows": 0, "canary_rows": 0}
    }
    assert entry["sources"][0]["end"] is not None


@pytest.mark.parametrize("sink", ["python", "noop"])
def test_a_discarding_sink_warns_at_start(sink: str) -> None:
    from tributary.config import Settings
    from tributary.stream.job import warn_if_discarding

    settings = Settings(_env_file=None, stream_sink=sink)
    with capture_logs() as logs:
        assert warn_if_discarding(settings)
    assert [(e["event"], e["log_level"], e["sink"]) for e in logs] == [
        ("stream.sink_discards_rows", "warning", sink)
    ]


def test_the_postgres_sink_starts_without_a_warning() -> None:
    from tributary.config import Settings
    from tributary.stream.job import warn_if_discarding

    with capture_logs() as logs:
        assert not warn_if_discarding(Settings(_env_file=None))
    assert logs == []


def test_progress_reports_write_each_batchs_loss_counters(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tributary.stream import job
    from tributary.stream.sink import BatchHealth

    _, progress = run_skew(spark, tmp_path, 86_400)
    written: list[tuple[str, BatchHealth]] = []
    monkeypatch.setattr(job, "record_health", lambda c, h: written.append((c, h)))
    listener = job.ProgressLogger("dbname=x")
    for b in sorted(progress):
        listener.onQueryProgress(cast("QueryProgressEvent", ProgressEvent(progress[b])))

    assert [h.batch_id for _, h in written] == sorted(progress)
    assert {c for c, _ in written} == {"dbname=x"}
    by_id = {h.batch_id: h for _, h in written}
    assert [by_id[b].dropped_by_watermark for b in sorted(by_id)] == [0, 0, 0, 4, 4, 4]
    assert by_id[POISON_BATCH].input_rows == 5
    assert by_id[POISON_BATCH].future_rows == 0  # accepted under the old bound
    assert by_id[0].watermark is None  # no watermark before the first batch
    assert by_id[3].watermark == datetime.fromtimestamp(T0 + 33 + POISON_AHEAD_S - 600, UTC)


def test_a_rejected_poison_is_counted_as_future(spark: SparkSession, tmp_path: Path) -> None:
    from tributary.stream.job import batch_health

    _, progress = run_skew(spark, tmp_path, 300)
    health = batch_health(progress[POISON_BATCH])
    assert (health.input_rows, health.future_rows, health.malformed_rows) == (5, 1, 0)
    assert health.rejected_rows == 1
    assert health.dropped_by_watermark == 0


def test_a_failed_counter_write_is_logged_and_never_stops_the_query(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tributary.stream import job

    _, progress = run_skew(spark, tmp_path, 300)

    def refuse(_conninfo: str, _health: object) -> None:
        raise OSError("connection refused")

    monkeypatch.setattr(job, "record_health", refuse)
    with capture_logs() as logs:
        job.ProgressLogger("dbname=x").onQueryProgress(
            cast("QueryProgressEvent", ProgressEvent(progress[0]))
        )
    assert [e["event"] for e in logs] == ["stream.progress", "stream.health_not_recorded"]


def test_without_a_database_the_logger_only_logs(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tributary.stream import job

    _, progress = run_skew(spark, tmp_path, 300)
    monkeypatch.setattr(job, "record_health", lambda *_: pytest.fail("no database configured"))
    job.ProgressLogger().onQueryProgress(cast("QueryProgressEvent", ProgressEvent(progress[0])))


def test_a_discarding_sink_records_its_counters_but_never_its_offsets(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A pass bounded by a benchmark's offsets would write the whole run into the table."""
    from tributary.stream import job
    from tributary.stream.sink import BatchHealth

    _, progress = run_skew(spark, tmp_path, 300)
    written: list[BatchHealth] = []
    monkeypatch.setattr(job, "record_health", lambda _c, h: written.append(h))
    offsets = {"t": {"0": 5}}
    monkeypatch.setattr(job, "kafka_offsets", lambda _raw: offsets)
    event = cast("QueryProgressEvent", ProgressEvent(progress[0]))
    job.ProgressLogger("dbname=x").onQueryProgress(event)
    job.ProgressLogger("dbname=x", warehouse_writes=False).onQueryProgress(event)
    # The harness query has no name, so name it the warehouse query for the second call.
    monkeypatch.setattr(job, "QUERY_NAME", written[0].query)
    job.ProgressLogger("dbname=x", warehouse_writes=False).onQueryProgress(event)
    assert [(h.end_offsets, h.input_rows) for h in written] == [
        (offsets, 4),
        (offsets, 4),
        (None, 4),
    ]
    assert written[0].query_id == str(progress[0].id)

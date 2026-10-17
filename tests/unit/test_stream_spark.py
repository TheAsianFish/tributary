"""The Spark parser and dedup operator, in local mode against batch and file-stream inputs."""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Iterator
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

from tributary.events import COLUMNS, to_row
from tributary.stream.job import ROCKSDB_PROVIDER, ProgressLogger
from tributary.stream.parse import OBSERVATION, deduplicate, parse_events
from tributary.stream.sink import adapt_row

if TYPE_CHECKING:
    from pyspark.sql.streaming.listener import QueryProgressEvent

pytestmark = pytest.mark.spark

KAFKA_TS = datetime(2026, 10, 16, 23, 0, tzinfo=UTC)
KAFKA_SCHEMA = StructType(
    [
        StructField("value", BinaryType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
    ]
)


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tributary-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.streaming.stateStore.providerClass", ROCKSDB_PROVIDER)
        .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


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


def run_dedup(spark: SparkSession, src: Path, checkpoint: Path, sink: Collector) -> list[Any]:
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
        deduplicate(parse_events(source), "60 seconds")
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
    assert observed == {"input_rows": 3, "malformed_rows": 0, "canary_rows": 0}

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
        OBSERVATION: {"input_rows": 2, "malformed_rows": 0, "canary_rows": 0}
    }
    assert entry["sources"][0]["end"] is not None

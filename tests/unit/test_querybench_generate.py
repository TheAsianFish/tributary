"""The scaled data generator: ids, tiling in time, and the streaming file layout."""

from __future__ import annotations

import os
import sys
import uuid
from collections import Counter
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tributary.events import COLUMNS
from tributary.ingest.sample import write_sample
from tributary.querybench import generate
from tributary.querybench.generate import (
    TABLE_COLUMNS,
    Base,
    copies_needed,
    copy_namespace,
    postgres_ddl,
    read_base,
    scaled_id,
    table_name,
)

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402,N812

T0 = datetime(2026, 10, 16, 23, 45, 26, tzinfo=UTC)


def _event(n: int, ts: datetime, *, dt: datetime | None = None, wiki: str = "enwiki") -> dict:  # type: ignore[type-arg]
    meta: dict[str, object] = {"id": str(uuid.UUID(int=n, version=4)), "domain": "en.wikipedia.org"}
    if dt is not None:
        meta["dt"] = dt.isoformat().replace("+00:00", "Z")
    return {
        "meta": meta,
        "id": n,
        "type": "edit",
        "namespace": 0,
        "title": f"Page {n % 7}",
        "timestamp": int(ts.timestamp()),
        "user": f"user{n % 5}",
        "bot": n % 3 == 0,
        "minor": False,
        "length": {"old": 10, "new": 10 + n},
        "revision": {"old": n, "new": n + 1},
        "server_name": "en.wikipedia.org",
        "wiki": wiki,
    }


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tributary-querybench-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sample(tmp_path: Path) -> Path:
    """Ten events over 100 s; event 3 is the latest (emitted 2 s after it happened)."""
    events = [
        _event(n, T0 + timedelta(seconds=10 * n), dt=T0 + timedelta(seconds=10 * n + 1))
        for n in range(10)
    ]
    events[9]["timestamp"] = int((T0 + timedelta(seconds=40)).timestamp())  # late event
    events.insert(2, {"meta": {"id": "not-a-uuid"}, "type": "edit", "wiki": "x", "timestamp": 1})
    path = tmp_path / "sample.jsonl.gz"
    write_sample(path, events)
    return path


def test_table_names_read_as_their_scale() -> None:
    assert table_name(1_000_000) == "events_1m"
    assert table_name(20_000_000) == "events_20m"
    assert table_name(12_345) == "events_12345"


def test_copies_cover_the_requested_rows() -> None:
    assert copies_needed(50_000, 50_000) == 1
    assert copies_needed(1_000_000, 50_000) == 20
    assert copies_needed(1_000_001, 50_000) == 21
    with pytest.raises(ValueError, match="positive"):
        copies_needed(0, 50_000)


def test_every_copy_has_its_own_namespace_and_ids_are_version_5() -> None:
    original = "00000000-0000-4000-8000-000000000001"
    ids = {scaled_id(i, original) for i in range(50)}
    assert len(ids) == 50
    assert original not in ids
    assert {uuid.UUID(i).version for i in ids} == {5}
    assert copy_namespace(0) != copy_namespace(1)


def test_the_base_skips_invalid_events_and_spans_first_to_last(sample: Path) -> None:
    base = read_base(sample)
    assert len(base.rows) == 10
    assert base.first == T0
    assert base.last == T0 + timedelta(seconds=80)
    assert base.span_seconds == 81
    assert [r[0] for r in base.rows] == list(range(10))  # dense, past the invalid third line


def test_the_jvm_uuid5_matches_pythons(spark: SparkSession) -> None:
    names = [str(uuid.UUID(int=n * 7919 + 1, version=4)) for n in range(200)] + ["ü-é", ""]
    rows = [(i % 5, copy_namespace(i % 5).hex, name) for i, name in enumerate(names)]
    df = spark.createDataFrame(rows, ["copy", "ns", "name"])
    got = df.select("copy", "name", generate.uuid5_column(F.col("ns"), F.col("name")).alias("id"))
    for row in got.collect():
        assert row["id"] == scaled_id(row["copy"], row["name"]), row


def test_scaled_rows_tile_time_without_overlap(spark: SparkSession, sample: Path) -> None:
    base = read_base(sample)
    frame = generate.scaled_frame(spark, base, 25)
    assert frame.columns == list(TABLE_COLUMNS)
    rows = frame.collect()
    assert len(rows) == 25
    assert len({r["event_id"] for r in rows}) == 25
    by_copy: dict[int, list[datetime]] = {}
    for r in rows:
        copy = r["kafka_offset"] // 10
        by_copy.setdefault(copy, []).append(r["event_ts"].replace(tzinfo=UTC))
    assert sorted(by_copy) == [0, 1, 2]
    assert len(by_copy[2]) == 5  # the last copy keeps the first five rows in sample order
    for copy in (1, 2):
        assert min(by_copy[copy]) > max(by_copy[copy - 1])
        assert min(by_copy[copy]) - T0 == timedelta(seconds=81 * copy)
    first_copy = {r["kafka_offset"]: r for r in rows if r["kafka_offset"] < 10}
    for seq, row in first_copy.items():
        source = base.rows[seq]
        assert row["event_id"] == scaled_id(0, str(source[1]))
        assert row["wiki"] == source[5]
        assert row["title"] == source[9]
    # Arrival is the emit time, shifted with the copy.
    late = next(r for r in rows if r["kafka_offset"] == 19)
    assert late["ingested_at"] == late["kafka_ts"] == late["emitted_at"]
    assert late["event_ts"].replace(tzinfo=UTC) == T0 + timedelta(seconds=81 + 40)


def test_the_stream_layout_puts_one_micro_batch_task_in_each_spark_task(
    spark: SparkSession, sample: Path
) -> None:
    base = read_base(sample)
    frame = generate.scaled_frame(spark, base, 200)
    laid_out, groups = generate.stream_layout(spark, frame)
    assert laid_out.columns == list(TABLE_COLUMNS)
    tagged = generate.stream_groups(laid_out).withColumn("_pid", F.spark_partition_id())
    per_partition: dict[int, set[tuple[int, int]]] = {}
    total = 0
    for r in tagged.select("_pid", "_batch", "_task").collect():
        per_partition.setdefault(r["_pid"], set()).add((r["_batch"], r["_task"]))
        total += 1
    assert total == 200
    assert laid_out.rdd.getNumPartitions() == groups
    assert len(per_partition) == groups  # no empty task, no task with two groups
    assert all(len(keys) == 1 for keys in per_partition.values())
    assert sum(1 for _ in Counter(k for ks in per_partition.values() for k in ks)) == groups


def test_hash_slots_cover_every_partition(spark: SparkSession) -> None:
    slots = generate.hash_slots(spark, 37)
    got = (
        spark.createDataFrame([(s,) for s in slots], "slot long")
        .select(F.pmod(F.hash("slot"), F.lit(37)).alias("p"))
        .collect()
    )
    assert [r["p"] for r in got] == list(range(37))


def test_the_postgres_table_gets_a_key_a_brin_index_and_the_readers_grant() -> None:
    text = " ".join(s.as_string() for s in postgres_ddl("events_1m", "reader"))
    assert 'ALTER TABLE "bench"."events_1m" ADD PRIMARY KEY (event_id)' in text
    assert "USING brin (event_ts)" in text
    assert 'GRANT SELECT ON "bench"."events_1m" TO "reader"' in text


def test_table_columns_are_the_landing_tables() -> None:
    assert TABLE_COLUMNS[: len(COLUMNS)] == COLUMNS
    assert TABLE_COLUMNS[-1] == "ingested_at"
    assert isinstance(Base(rows=[(0,)], first=T0, last=T0).span_seconds, int)

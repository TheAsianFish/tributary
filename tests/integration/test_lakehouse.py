"""The lake end to end: 5,000 events → Kafka → both streaming queries → PostgreSQL and Iceberg.

Needs the Compose stack with the ``lakehouse`` profile (``make up-lakehouse``):
the stream job running both queries, MinIO and Trino. Reached through the host
ports in ``.env.example``.

The events are the first 5,000 of the sample with fresh ids, re-timed two
seconds apart in file order starting just after anything either store has
landed. That spreads them over almost three event-time hours, so the table has
several ``hours(event_ts)`` partitions of known size, and keeps them in order,
so the watermark drops none. (The sample's own disorder is covered by
``test_pipeline.py``.)
"""

from __future__ import annotations

import contextlib
import copy
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
import trino.dbapi as trino_dbapi

from tributary.config import Settings
from tributary.events import Event, event_id
from tributary.ingest.producer import ensure_topic, make_producer, produce_events
from tributary.ingest.sample import read_sample
from tributary.lake.hours import floor_hour
from tributary.lake.pruning import check_hour

pytestmark = pytest.mark.integration


N_EVENTS = 5_000
SPACING_S = 2
TIMEOUT_S = 300.0
TABLE = "iceberg.raw.recentchange"

PgConn = psycopg.Connection[tuple[object, ...]]


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings()


@pytest.fixture(scope="module")
def conn(settings: Settings) -> Iterator[PgConn]:
    with psycopg.connect(settings.pg_conninfo, autocommit=True) as c:
        yield c


@pytest.fixture(scope="module")
def trino(settings: Settings) -> Iterator[trino_dbapi.Connection]:
    c = trino_dbapi.Connection(
        host=settings.trino_host, port=settings.trino_port, user="tests", timezone="UTC"
    )
    with c:
        try:
            c.cursor().execute("SELECT 1").fetchall()
        except Exception as exc:  # any failure here means the lakehouse profile is not up
            pytest.skip(
                f"Trino not reachable at {settings.trino_host}:{settings.trino_port}: {exc}"
            )
        yield c


def trino_rows(trino: trino_dbapi.Connection, sql: str, *params: object) -> list[list[object]]:
    cur = trino.cursor()
    cur.execute(sql, list(params) if params else None)
    rows: list[list[object]] = cur.fetchall()
    return rows


def scalar(rows: list[list[object]] | list[tuple[object, ...]]) -> object:
    assert len(rows) == 1, rows
    return rows[0][0]


def newest(conn: PgConn, trino: trino_dbapi.Connection) -> datetime | None:
    """The newest event time in either store (None on an empty stack)."""
    candidates: list[object] = []
    with contextlib.suppress(psycopg.errors.UndefinedTable):
        candidates.append(
            scalar(conn.execute("SELECT max(event_ts) FROM raw.recentchange").fetchall())
        )
    # The stream job creates the tables when it starts.
    with contextlib.suppress(trino_dbapi.DatabaseError):
        candidates.append(scalar(trino_rows(trino, f"SELECT max(event_ts) FROM {TABLE}")))
    times = [c for c in candidates if isinstance(c, datetime)]
    return max(times) if times else None


@dataclass(frozen=True)
class Replay:
    ids: list[str]
    start: datetime
    end: datetime  # exclusive


@pytest.fixture(scope="module")
def replay(settings: Settings, conn: PgConn, trino: trino_dbapi.Connection) -> Replay:
    """Produce the re-timed events and wait until both stores hold all of them."""
    landed = newest(conn, trino)
    start = (landed + timedelta(seconds=1)) if landed else datetime.now(UTC) - timedelta(hours=3)
    start = start.replace(microsecond=0)
    events: list[Event] = []
    for i, original in enumerate(read_sample(settings.sample_path, limit=N_EVENTS)):
        event = copy.deepcopy(original)
        event["meta"]["id"] = str(uuid.uuid4())
        event["timestamp"] = int(start.timestamp()) + i * SPACING_S
        events.append(event)
    end = start + timedelta(seconds=N_EVENTS * SPACING_S)

    ensure_topic(settings.kafka_bootstrap, settings.topic, settings.topic_partitions)
    stats = produce_events(events, make_producer(settings.kafka_bootstrap), settings.topic)
    assert stats.failed == 0
    assert stats.delivered == N_EVENTS

    ids = [event_id(e) for e in events]
    deadline = time.monotonic() + TIMEOUT_S
    pg = lake = -1
    while time.monotonic() < deadline:
        pg = pg_distinct(conn, start, end)
        lake = lake_counts(trino, start, end)[1]
        if pg >= N_EVENTS and lake >= N_EVENTS:
            break
        time.sleep(5)
    assert (pg, lake) == (N_EVENTS, N_EVENTS), "not every event landed in both stores"
    return Replay(ids, start, end)


def pg_distinct(conn: PgConn, start: datetime, end: datetime) -> int:
    try:
        rows = conn.execute(
            "SELECT count(DISTINCT event_id) FROM raw.recentchange"
            " WHERE event_ts >= %s AND event_ts < %s",
            (start, end),
        ).fetchall()
    except psycopg.errors.UndefinedTable:
        return 0
    return int(str(scalar(rows)))


def lake_counts(trino: trino_dbapi.Connection, start: datetime, end: datetime) -> tuple[int, int]:
    """(rows, distinct ids) in the lake for ``[start, end)``."""
    try:
        rows = trino_rows(
            trino,
            f"SELECT count(*), count(DISTINCT event_id) FROM {TABLE}"
            " WHERE event_ts >= ? AND event_ts < ?",
            start,
            end,
        )
    except trino_dbapi.DatabaseError:
        return (0, 0)
    n, distinct = rows[0]
    return int(str(n)), int(str(distinct))


def test_postgres_and_trino_over_iceberg_count_the_same_distinct_ids(
    conn: PgConn, trino: trino_dbapi.Connection, replay: Replay
) -> None:
    pg = pg_distinct(conn, replay.start, replay.end)
    lake_rows, lake_distinct = lake_counts(trino, replay.start, replay.end)
    assert pg == lake_distinct == N_EVENTS
    # Iceberg has no primary key: a duplicate that slipped past the dedup state
    # would show up as more rows than ids.
    assert lake_rows == lake_distinct
    # And the ids are the ones produced, not merely the same number of them.
    present = conn.execute(
        "SELECT count(*) FROM raw.recentchange WHERE event_id = ANY(%s::uuid[])", (replay.ids,)
    ).fetchall()
    assert scalar(present) == N_EVENTS


def test_the_warehouse_records_each_fully_committed_batch(conn: PgConn, replay: Replay) -> None:
    """ops.stream_progress catches up with the table once the replay's batches have committed.

    The reconciliation closes hours on this mark rather than on max(event_ts),
    which can run ahead while a batch is committed partition by partition.
    """
    newest = replay.start + timedelta(seconds=(N_EVENTS - 1) * SPACING_S)
    deadline = time.monotonic() + 30
    mark: object = None
    while time.monotonic() < deadline:
        mark = scalar(
            conn.execute(
                "SELECT max(max_event_ts) FROM ops.stream_progress WHERE query = 'warehouse'"
            ).fetchall()
        )
        if isinstance(mark, datetime) and mark >= newest:
            break
        time.sleep(1)
    assert mark == newest


def test_a_one_hour_filter_reads_only_that_hours_data_files(
    settings: Settings, replay: Replay
) -> None:
    # The second hour of the replay is fully covered: 1,800 events at 2 s spacing.
    hour = floor_hour(replay.start) + timedelta(hours=1)
    assert hour + timedelta(hours=1) <= replay.end

    report = check_hour(settings, hour)
    assert report.hour_files >= 1
    assert report.hour_rows == 3600 // SPACING_S
    assert report.table_files > report.hour_files
    # Files this small are one split each, so the splits are the files opened:
    # exactly the hour's files, and exactly their rows, out of the whole table.
    assert report.scan_splits == report.hour_files
    assert report.scan_rows == report.hour_rows
    assert report.scan_rows < report.table_rows
    assert report.pruned

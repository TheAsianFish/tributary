"""End to end: 5,000 sample events → Kafka → Spark stream job → raw.recentchange.

Needs the Compose services (``docker compose up -d --wait``) with the stream job
running, and reaches them through the host ports in ``.env.example``.

The events are the first 5,000 of the sample with fresh ids minted for this run
and event times shifted past anything the stack has landed, so the counts are
about this run even on a stack that has already replayed the whole sample
(whose newer events would otherwise put these behind the watermark).
"""

from __future__ import annotations

import copy
import time
import uuid
from collections.abc import Iterator
from datetime import datetime

import psycopg
import pytest

from tributary.config import Settings
from tributary.events import Event, event_id, event_time
from tributary.ingest.producer import ensure_topic, make_producer, produce_events
from tributary.ingest.sample import read_sample

pytestmark = pytest.mark.integration

N_EVENTS = 5_000
TIMEOUT_S = 240.0


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings()


@pytest.fixture(scope="module")
def conn(settings: Settings) -> Iterator[psycopg.Connection[tuple[object, ...]]]:
    with psycopg.connect(settings.pg_conninfo, autocommit=True) as c:
        yield c


def newest_landed(conn: psycopg.Connection[tuple[object, ...]]) -> datetime | None:
    try:
        row = conn.execute("SELECT max(event_ts) FROM raw.recentchange").fetchone()
    except psycopg.errors.UndefinedTable:
        return None  # the stream job creates the table when it starts
    return row[0] if row is not None and isinstance(row[0], datetime) else None


@pytest.fixture(scope="module")
def events(settings: Settings, conn: psycopg.Connection[tuple[object, ...]]) -> list[Event]:
    """The first 5,000 sample events, re-keyed with fresh ids and moved past the watermark.

    Every event time is shifted by the same amount, so the sample's own disorder
    (at most 143 s of lateness) is kept, and the oldest one lands one second
    after the newest row already in the table. The shift stays within the
    parser's one-day bound on event time ahead of the Kafka record time.
    """
    sample = list(read_sample(settings.sample_path, limit=N_EVENTS))
    oldest = min(event_time(e) for e in sample)
    landed = newest_landed(conn)
    shift = max(0, int((landed - oldest).total_seconds()) + 1) if landed is not None else 0
    fresh = []
    for event in sample:
        event = copy.deepcopy(event)
        event["meta"]["id"] = str(uuid.uuid4())
        event["timestamp"] = int(event["timestamp"]) + shift
        fresh.append(event)
    return fresh


def count_present(conn: psycopg.Connection[tuple[object, ...]], ids: list[str]) -> int:
    row = conn.execute(
        "SELECT count(*) FROM raw.recentchange WHERE event_id = ANY(%s::uuid[])", (ids,)
    ).fetchone()
    assert row is not None
    return int(str(row[0]))


def landed_offsets(
    conn: psycopg.Connection[tuple[object, ...]], ids: list[str]
) -> dict[str, tuple[int, int]]:
    """Kafka (partition, offset) of the copy that landed, per event id."""
    rows = conn.execute(
        "SELECT event_id::text, kafka_partition, kafka_offset FROM raw.recentchange"
        " WHERE event_id = ANY(%s::uuid[])",
        (ids,),
    ).fetchall()
    return {str(r[0]): (int(str(r[1])), int(str(r[2]))) for r in rows}


def wait_for(conn: psycopg.Connection[tuple[object, ...]], ids: list[str], expected: int) -> int:
    deadline = time.monotonic() + TIMEOUT_S
    seen = -1
    while time.monotonic() < deadline:
        try:
            seen = count_present(conn, ids)
        except psycopg.errors.UndefinedTable:
            seen = 0  # the stream job creates the table when it starts
        if seen >= expected:
            return seen
        time.sleep(1.0)
    return seen


def produce(settings: Settings, events: list[Event]) -> None:
    ensure_topic(settings.kafka_bootstrap, settings.topic, settings.topic_partitions)
    stats = produce_events(events, make_producer(settings.kafka_bootstrap), settings.topic)
    assert stats.failed == 0
    assert stats.delivered == len(events) - stats.skipped_invalid


def sentinel_after(conn: psycopg.Connection[tuple[object, ...]], events: list[Event]) -> Event:
    """A fresh event one second newer than anything landed or replayed.

    The watermark trails the newest event time the job has seen, so the
    sentinel must not be older than what is already in the table (a stack that
    has ingested the whole sample) or the job would drop it as late.
    """
    landed = newest_landed(conn)
    newest = max(event_time(e) for e in events)
    if landed is not None:
        newest = max(newest, landed)
    event = copy.deepcopy(events[-1])
    event["meta"]["id"] = str(uuid.uuid4())
    event["timestamp"] = int(newest.timestamp()) + 1
    return event


def test_replay_lands_every_event_once_and_a_resend_adds_nothing(
    settings: Settings, conn: psycopg.Connection[tuple[object, ...]], events: list[Event]
) -> None:
    assert len(events) == N_EVENTS
    ids = sorted({event_id(e) for e in events})
    assert len(ids) == N_EVENTS

    # No loss: every event of the replay is in the table.
    produce(settings, events)
    assert wait_for(conn, ids, len(ids)) == len(ids)
    first_copies = landed_offsets(conn, ids)
    assert len(first_copies) == N_EVENTS

    # Re-send the whole batch (a producer retry storm), then, once every re-sent
    # event is acknowledged, a sentinel behind it. A micro-batch that contains the
    # sentinel was planned after the re-sends were in the log, so when it lands
    # they have been processed too. The primary key makes a duplicate row
    # impossible, so the count alone cannot fail; what can is the copy that was
    # kept: a re-sent event has a new Kafka offset, and the row must still carry
    # the first one (ON CONFLICT DO NOTHING, not an upsert). The sentinel landing
    # within the timeout shows the re-send did not stall the query; a crash that
    # Compose restarted is caught by the restart-count check in `make
    # test-integration`.
    produce(settings, events)
    sentinel = sentinel_after(conn, events)
    produce(settings, [sentinel])
    assert wait_for(conn, [event_id(sentinel)], 1) == 1
    assert landed_offsets(conn, ids) == first_copies

"""End to end: 5,000 sample events → Kafka → Spark stream job → raw.recentchange.

Needs the Compose services (``docker compose up -d --wait``) with the stream job
running, and reaches them through the host ports in ``.env.example``.
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


@pytest.fixture(scope="module")
def events(settings: Settings) -> list[Event]:
    return list(read_sample(settings.sample_path, limit=N_EVENTS))


def count_present(conn: psycopg.Connection[tuple[object, ...]], ids: list[str]) -> int:
    row = conn.execute(
        "SELECT count(*) FROM raw.recentchange WHERE event_id = ANY(%s::uuid[])", (ids,)
    ).fetchone()
    assert row is not None
    return int(str(row[0]))


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
    row = conn.execute("SELECT max(event_ts) FROM raw.recentchange").fetchone()
    landed = row[0] if row is not None and isinstance(row[0], datetime) else None
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

    # No loss: every distinct id of the replay is in the table.
    produce(settings, events)
    assert wait_for(conn, ids, len(ids)) == len(ids)

    # Re-send the whole batch (a producer retry storm), then, once every re-sent
    # event is acknowledged, a sentinel behind it. A micro-batch that contains the
    # sentinel was planned after the re-sends were in the log, so when it lands
    # they have been processed too. The primary key makes a duplicate row
    # impossible; what this checks is that the re-send was absorbed (by Spark's
    # dedup state or the sink's ON CONFLICT) without failing the query.
    produce(settings, events)
    sentinel = sentinel_after(conn, events)
    produce(settings, [sentinel])
    assert wait_for(conn, [event_id(sentinel)], 1) == 1
    assert count_present(conn, ids) == len(ids)

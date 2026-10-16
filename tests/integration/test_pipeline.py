"""End to end: sample events → Kafka → Spark stream job → raw.recentchange.

Needs the Compose services (``docker compose up -d --wait``) with the stream job
running, and reaches them through the host ports in ``.env.example``.
"""

from __future__ import annotations

import copy
import time
import uuid
from collections.abc import Iterator

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


def sentinel_after(events: list[Event]) -> Event:
    """A fresh event, newer than every replayed one so the watermark keeps it."""
    event = copy.deepcopy(events[-1])
    event["meta"]["id"] = str(uuid.uuid4())
    event["timestamp"] = int(max(event_time(e) for e in events).timestamp()) + 1
    return event


def test_replay_lands_every_event_exactly_once(
    settings: Settings, conn: psycopg.Connection[tuple[object, ...]], events: list[Event]
) -> None:
    assert len(events) == N_EVENTS
    ids = sorted({event_id(e) for e in events})

    produce(settings, events)
    assert wait_for(conn, ids, len(ids)) == len(ids)

    # Re-send the whole batch (a producer retry storm), then, once every re-sent
    # event is acknowledged, a sentinel behind it. A micro-batch that contains the
    # sentinel was planned after the re-sends were in the log, so when it lands
    # they have been processed too, and the count must not have moved.
    produce(settings, events)
    sentinel = sentinel_after(events)
    produce(settings, [sentinel])
    assert wait_for(conn, [event_id(sentinel)], 1) == 1
    assert count_present(conn, ids) == len(ids)
    row = conn.execute(
        "SELECT count(*), count(DISTINCT event_id) FROM raw.recentchange "
        "WHERE event_id = ANY(%s::uuid[])",
        ([*ids, event_id(sentinel)],),
    ).fetchone()
    assert row == (len(ids) + 1, len(ids) + 1)

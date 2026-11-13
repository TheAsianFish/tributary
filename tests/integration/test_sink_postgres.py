"""The sink against a real PostgreSQL (connection from the TRIBUTARY_PG_* variables)."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime

import psycopg
import pytest

from tributary.config import Settings
from tributary.events import to_row
from tributary.stream.sink import Connection, ensure_schema, write_partition, write_rows

pytestmark = pytest.mark.integration

# The record is appended just after the edit (23:34:15), as it would be live.
KAFKA_TS = datetime(2026, 10, 16, 23, 35, tzinfo=UTC)


@pytest.fixture(scope="module")
def conninfo() -> str:
    info = Settings().pg_conninfo
    try:
        psycopg.connect(info, connect_timeout=3).close()
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    return info


@pytest.fixture
def ids(conninfo: str) -> Iterator[list[str]]:
    """Fresh random event ids; their rows are deleted afterwards."""
    fresh = [str(uuid.uuid4()) for _ in range(4)]
    yield fresh
    with psycopg.connect(conninfo) as conn:
        conn.execute("DELETE FROM raw.recentchange WHERE event_id = ANY(%s::uuid[])", (fresh,))


@pytest.fixture
def conn(conninfo: str) -> Iterator[Connection]:
    ensure_schema(conninfo)
    with psycopg.connect(conninfo) as c:
        yield c


def row(event_id: str, offset: int) -> tuple[object, ...]:
    event = {
        "meta": {"id": event_id, "dt": "2026-10-16T23:34:16.644Z", "domain": "en.wikipedia.org"},
        "id": 1000 + offset,
        "type": "edit",
        "namespace": 0,
        "title": "Ünïcode title",
        "timestamp": 1792193655,
        "user": "Someone",
        "bot": False,
        "length": {"old": 10, "new": 12},
        "revision": {"old": 5, "new": 6},
        "wiki": "enwiki",
    }
    return to_row(event, partition=offset % 3, offset=offset, kafka_ts=KAFKA_TS).as_tuple()


def stored(conn: Connection, ids: list[str]) -> list[tuple[object, ...]]:
    cur = conn.execute(
        "SELECT event_id::text, event_ts, emitted_at, title, kafka_offset FROM raw.recentchange"
        " WHERE event_id = ANY(%s::uuid[]) ORDER BY kafka_offset",
        (ids,),
    )
    rows = cur.fetchall()
    conn.commit()
    return rows


def test_ensure_schema_is_idempotent(conninfo: str) -> None:
    ensure_schema(conninfo)
    ensure_schema(conninfo)
    with psycopg.connect(conninfo) as c:
        assert c.execute("SELECT to_regclass('raw.recentchange')::text").fetchone() == (
            "raw.recentchange",
        )


def test_duplicates_in_a_batch_and_a_replayed_batch_land_once(
    conn: Connection, ids: list[str]
) -> None:
    a, b, c, d = ids
    first = write_rows(conn, [row(a, 1), row(b, 2), row(a, 3)])
    assert (first.rows, first.inserted) == (3, 2)

    replay = write_rows(conn, [row(a, 1), row(b, 2), row(a, 3)])
    assert (replay.rows, replay.inserted) == (3, 0)

    overlap = write_rows(conn, [row(b, 2), row(c, 4)])
    assert (overlap.rows, overlap.inserted) == (2, 1)

    landed = stored(conn, ids)
    assert [r[0] for r in landed] == [a, b, c]
    assert landed[0][1:4] == (
        datetime(2026, 10, 16, 23, 34, 15, tzinfo=UTC),
        datetime(2026, 10, 16, 23, 34, 16, 644000, tzinfo=UTC),
        "Ünïcode title",
    )
    assert d not in [r[0] for r in landed]


def test_write_partition_from_naive_spark_timestamps(conninfo: str, ids: list[str]) -> None:
    a = ids[0]
    naive = list(row(a, 7))
    naive[1] = datetime(2026, 10, 16, 23, 34, 15, tzinfo=UTC).astimezone().replace(tzinfo=None)
    [result] = list(write_partition(conninfo)([naive]))
    assert (result.rows, result.inserted) == (1, 1)
    with psycopg.connect(conninfo) as c:
        assert stored(c, [a])[0][1] == datetime(2026, 10, 16, 23, 34, 15, tzinfo=UTC)

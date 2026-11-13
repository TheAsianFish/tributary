"""The reconciliation's PostgreSQL half: lookups, the write back, the recorded pass and
what the stream_loss check reads from it.

Rows are written in the 2001 window used by ``test_stream_health_pg.py`` (nothing
else lives there) under ids minted per test, and deleted after.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from tributary.events import COLUMNS
from tributary.stream.reconcile import (
    MissingEvent,
    PassResult,
    PostgresStore,
    committed_offsets,
    record_pass,
)
from tributary.stream.sink import BatchHealth, ensure_schema, record_health
from tributary.warehouse.stream_health import Thresholds, evaluate, window_counts

pytestmark = pytest.mark.warehouse

DAY = datetime(2001, 1, 1, tzinfo=UTC)
TOPIC = "test.reconcile"


def row(event_id: str) -> tuple[object, ...]:
    values: dict[str, object] = dict.fromkeys(COLUMNS)
    values.update(
        event_id=event_id,
        event_ts=DAY,
        wiki="enwiki",
        event_type="edit",
        kafka_partition=0,
        kafka_offset=1,
        kafka_ts=DAY,
    )
    return tuple(values[c] for c in COLUMNS)


@pytest.fixture
def ids(conninfo: str) -> Iterator[list[str]]:
    minted = [str(uuid.uuid4()) for _ in range(4)]
    yield minted
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute("DELETE FROM raw.recentchange WHERE event_id = ANY(%s::uuid[])", (minted,))


@pytest.fixture
def clean_ops(conninfo: str) -> Iterator[None]:
    """Nothing of this test's left in the 2001 window afterwards."""
    ensure_schema(conninfo, "ops.sql")
    yield
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute("DELETE FROM ops.stream_progress WHERE committed_at < '2002-01-01'")
        conn.execute("DELETE FROM ops.reconcile_runs WHERE topic = %s", (TOPIC,))


def test_the_store_finds_absent_ids_and_writes_them_once(conninfo: str, ids: list[str]) -> None:
    store = PostgresStore(conninfo)
    assert store.write([row(ids[0])]) == 1
    assert store.absent(ids) == set(ids[1:])
    # The sink's write: a second copy, in the table or in the same call, adds nothing.
    assert store.write([row(ids[0]), row(ids[1]), row(ids[1])]) == 1
    assert store.absent(ids) == set(ids[2:])
    assert store.absent([]) == set()


def health(batch_id: int, start: int, end: int, dropped: int) -> BatchHealth:
    return BatchHealth(
        query="warehouse",
        batch_id=batch_id,
        input_rows=end - start,
        dropped_by_watermark=dropped,
        malformed_rows=0,
        future_rows=0,
        canary_rows=0,
        watermark=DAY,
        start_offsets={TOPIC: {"0": start}},
        end_offsets={TOPIC: {"0": end}},
    )


def into_2001(conninfo: str) -> None:
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute(
            "UPDATE ops.stream_progress SET committed_at = %s + batch_id * interval '1 minute'"
            " WHERE query = 'warehouse' AND end_offsets ? %s",
            (DAY, TOPIC),
        )
        conn.execute(
            "UPDATE ops.reconcile_runs SET finished_at = %s + interval '30 minutes'"
            " WHERE topic = %s",
            (DAY, TOPIC),
        )


def a_pass(start: int, end: int, missing: list[str], left: set[str]) -> PassResult:
    return PassResult(
        topic=TOPIC,
        start_offsets={TOPIC: {"0": start}},
        end_offsets={TOPIC: {"0": end}},
        records=end - start,
        landed=end - start - len(missing),
        malformed=0,
        future=0,
        canary=0,
        missing_records=len(missing),
        missing=tuple(MissingEvent(m, DAY, 0, start + i) for i, m in enumerate(missing)),
        recovered=len(missing) - len(left),
        left_missing=frozenset(left),
        dry_run=False,
        seconds=1.5,
        started_at=DAY,
    )


def warehouse_check(conninfo: str) -> tuple[str, dict[str, object]]:
    window = timedelta(hours=1)
    [mine] = [c for c in window_counts(conninfo, DAY, DAY + window) if c.query == "warehouse"]
    check = evaluate(mine, Thresholds(), window=window, now=DAY + window)
    return check.status, check.details


@pytest.mark.usefixtures("clean_ops")
def test_a_pass_judges_up_to_the_largest_committed_end_offset(conninfo: str) -> None:
    record_health(conninfo, health(1, 0, 100, 0))
    record_health(conninfo, health(2, 100, 250, 0))
    assert committed_offsets(conninfo, TOPIC) == {"0": 250}
    assert committed_offsets(conninfo, "another.topic") == {}


@pytest.mark.usefixtures("clean_ops")
def test_a_pass_turns_covered_drops_green_until_an_event_it_found_is_still_missing(
    conninfo: str, ids: list[str]
) -> None:
    record_health(conninfo, health(1, 0, 100, 0))
    record_health(conninfo, health(2, 100, 200, 40))  # a redelivery: copies of landed events
    into_2001(conninfo)
    status, details = warehouse_check(conninfo)
    assert status == "fail"  # no pass yet: the counter alone, as before
    assert details["uncovered_dropped"] == 40

    record_pass(conninfo, "test", a_pass(0, 200, [], set()))
    into_2001(conninfo)
    status, details = warehouse_check(conninfo)
    assert status == "pass"
    assert (details["covered_dropped"], details["reconcile_passes"]) == (40, 1)

    # A later batch drops a genuinely late event; a dry run finds it and leaves it.
    record_health(conninfo, health(3, 200, 300, 1))
    record_pass(conninfo, "test", a_pass(0, 300, [ids[0]], {ids[0]}))
    into_2001(conninfo)
    status, details = warehouse_check(conninfo)
    assert status == "fail"
    assert (details["uncovered_dropped"], details["still_missing"]) == (0, 1)

    # Once the event is in the table (a repairing pass wrote it), the same record is green.
    PostgresStore(conninfo).write([row(ids[0])])
    status, details = warehouse_check(conninfo)
    assert status == "pass"
    assert details["still_missing"] == 0


@pytest.mark.usefixtures("clean_ops")
def test_a_recorded_pass_keeps_every_missing_event_and_whether_it_was_left(
    conninfo: str, ids: list[str]
) -> None:
    pass_id = record_pass(conninfo, "test-run", a_pass(0, 10, ids[:3], {ids[2]}))
    with psycopg.connect(conninfo) as conn:
        run = conn.execute(
            "SELECT run_id, records, landed, missing, recovered, left_missing, seconds"
            " FROM ops.reconcile_runs WHERE id = %s",
            (pass_id,),
        ).fetchone()
        missing = conn.execute(
            "SELECT event_id::text, kafka_offset, left_missing FROM ops.reconcile_missing"
            " WHERE pass_id = %s ORDER BY kafka_offset",
            (pass_id,),
        ).fetchall()
    assert run == ("test-run", 10, 7, 3, 2, 1, 1.5)
    assert missing == [(ids[0], 0, False), (ids[1], 1, False), (ids[2], 2, True)]

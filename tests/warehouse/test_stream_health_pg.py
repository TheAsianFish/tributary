"""The stream's loss counters and the stream_loss check against PostgreSQL.

Rows are written under a query name unique to the test, with ``committed_at``
moved into 2001 so the window read here holds nothing else, and deleted after.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from importlib.resources import files

import psycopg
import pytest

from tributary.stream.sink import RECORD_PROGRESS, BatchHealth, ensure_schema, record_health
from tributary.warehouse.stream_health import Thresholds, evaluate, window_counts

pytestmark = pytest.mark.warehouse

DAY = datetime(2001, 1, 1, tzinfo=UTC)


@pytest.fixture
def query(conninfo: str) -> Iterator[str]:
    ensure_schema(conninfo, "ops.sql")
    name = f"test-{uuid.uuid4().hex[:8]}"
    yield name
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute("DELETE FROM ops.stream_progress WHERE query = %s", (name,))


def health(query: str, batch_id: int, **counts: int) -> BatchHealth:
    values = {
        "input_rows": 100,
        "dropped_by_watermark": 0,
        "malformed_rows": 0,
        "future_rows": 0,
        "canary_rows": 0,
    }
    values.update(counts)
    return BatchHealth(query=query, batch_id=batch_id, watermark=DAY, **values)


def move_into_2001(conninfo: str, query: str) -> None:
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute(
            "UPDATE ops.stream_progress SET committed_at = %s + batch_id * interval '1 minute'"
            " WHERE query = %s",
            (DAY, query),
        )


def test_the_counters_join_the_sinks_row_for_the_same_batch(conninfo: str, query: str) -> None:
    newest = DAY + timedelta(minutes=5)
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute(RECORD_PROGRESS, (query, 7, newest, 98, 98))
    record_health(conninfo, health(query, 7, dropped_by_watermark=2))
    # A batch whose every row was dropped has no sink row: the counters stand alone.
    record_health(conninfo, health(query, 8, dropped_by_watermark=100))
    # A replayed progress report replaces, never adds.
    record_health(conninfo, health(query, 8, dropped_by_watermark=100))
    with psycopg.connect(conninfo) as conn:
        rows = conn.execute(
            "SELECT batch_id, max_event_ts, rows, inserted, input_rows, dropped_by_watermark,"
            " watermark FROM ops.stream_progress WHERE query = %s ORDER BY batch_id",
            (query,),
        ).fetchall()
    assert rows == [
        (7, newest, 98, 98, 100, 2, DAY),
        (8, None, 0, 0, 100, 100, DAY),
    ]


def test_the_window_sums_each_querys_batches(conninfo: str, query: str) -> None:
    record_health(conninfo, health(query, 1))
    record_health(conninfo, health(query, 2, future_rows=1))
    record_health(conninfo, health(query, 3, dropped_by_watermark=40, malformed_rows=2))
    move_into_2001(conninfo, query)

    [mine] = [c for c in window_counts(conninfo, DAY, DAY + timedelta(hours=1)) if c.query == query]
    assert (mine.batches, mine.input_rows, mine.dropped_by_watermark) == (3, 300, 40)
    assert (mine.malformed_rows, mine.future_rows, mine.rejected_rows) == (2, 1, 3)
    # Batch 3 committed at 00:03: a window ending at 00:02 leaves it out.
    [early] = [
        c for c in window_counts(conninfo, DAY, DAY + timedelta(minutes=2)) if c.query == query
    ]
    assert (early.batches, early.dropped_by_watermark) == (2, 0)

    check = evaluate(mine, Thresholds(), window=timedelta(hours=1), now=DAY)
    assert check.status == "fail"
    assert check.failures == 43


def test_the_schema_script_stays_idempotent(conninfo: str) -> None:
    ensure_schema(conninfo, "ops.sql")
    ensure_schema(conninfo, "ops.sql")
    with psycopg.connect(conninfo) as conn:
        nullable = conn.execute(
            "SELECT is_nullable FROM information_schema.columns"
            " WHERE table_schema = 'ops' AND table_name = 'stream_progress'"
            " AND column_name = 'max_event_ts'"
        ).fetchone()
    assert nullable == ("YES",)


def test_rerunning_the_schema_script_does_not_wait_behind_a_reader(conninfo: str) -> None:
    """Every recorded check re-runs ops.sql. On a migrated table it must take no lock
    that waits behind an open reader: a waiting ALTER queues every later stream write
    behind it, so the sink would stall for as long as the reader's transaction lasts."""
    ensure_schema(conninfo, "ops.sql")
    with psycopg.connect(conninfo) as reader:
        reader.execute("SELECT count(*) FROM ops.stream_progress")  # holds ACCESS SHARE
        with psycopg.connect(conninfo) as conn:
            conn.execute("SET lock_timeout = '1s'")
            conn.execute(files("tributary").joinpath("sql", "ops.sql").read_bytes())
        reader.rollback()

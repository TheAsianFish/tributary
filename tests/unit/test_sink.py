from __future__ import annotations

import contextlib
from collections.abc import Callable, Iterable, Iterator, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Self, cast

import psycopg
import pytest
from psycopg import sql
from structlog.testing import capture_logs

from tributary.events import COLUMNS, to_row
from tributary.stream import sink
from tributary.stream.sink import (
    PostgresSink,
    SinkResult,
    adapt_row,
    discard_partition,
    write_partition,
    write_rows,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# The record is appended just after the edit (23:34:15), as it would be live.
KAFKA_TS = datetime(2026, 10, 16, 23, 35, tzinfo=UTC)


def event(n: int) -> dict[str, object]:
    return {
        "meta": {
            "id": f"00000000-0000-4000-8000-{n:012d}",
            "dt": "2026-10-16T23:34:16.644Z",
            "domain": "en.wikipedia.org",
            "stream": "mediawiki.recentchange",
        },
        "id": n,
        "type": "edit",
        "timestamp": 1792193655 + n,
        "wiki": "enwiki",
    }


def row(n: int) -> tuple[object, ...]:
    return to_row(event(n), partition=0, offset=n, kafka_ts=KAFKA_TS).as_tuple()


class FakeCopy:
    def __init__(self, rows: list[Sequence[object]]) -> None:
        self.rows = rows

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def write_row(self, values: Sequence[object]) -> None:
        self.rows.append(values)


class FakeCursor:
    """Records statements. INSERT behaves like PostgreSQL on the primary key: with
    ``ON CONFLICT ... DO NOTHING`` it reports one row per distinct new event id,
    without it a repeated id raises ``UniqueViolation``."""

    def __init__(self, conn: FakeConnection) -> None:
        self.conn = conn
        self.rowcount = -1
        self.staged: list[Sequence[object]] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, query: sql.Composable, params: Sequence[object] | None = None) -> None:
        text = query.as_string()
        self.conn.statements.append(text)
        if self.conn.fail_next is not None:
            error, self.conn.fail_next = self.conn.fail_next, None
            raise error
        if text.startswith("INSERT INTO ops.stream_progress"):
            assert params is not None
            self.conn.progress.append(tuple(params))
        elif text.startswith("INSERT"):
            staged = [str(r[0]) for r in self.staged]
            new = set(staged) - self.conn.table
            if "ON CONFLICT (event_id) DO NOTHING" not in text and len(new) < len(staged):
                raise psycopg.errors.UniqueViolation(
                    "duplicate key value violates unique constraint"
                )
            self.conn.table |= new
            self.rowcount = len(new)

    def copy(self, query: sql.Composable) -> FakeCopy:
        self.conn.statements.append(query.as_string())
        return FakeCopy(self.staged)


class FakeInfo:
    transaction_status = psycopg.pq.TransactionStatus.IDLE


class FakeConnection:
    def __init__(self) -> None:
        self.info = FakeInfo()
        self.statements: list[str] = []
        self.table: set[str] = set()
        self.progress: list[tuple[object, ...]] = []
        self.cursors: list[FakeCursor] = []
        self.commits = 0
        self.closed = False
        self.broken = False
        self.fail_next: Exception | None = None

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        before = set(self.table)
        try:
            yield
        except Exception:
            self.table = before
            raise
        self.commits += 1

    def cursor(self) -> FakeCursor:
        cur = FakeCursor(self)
        self.cursors.append(cur)
        return cur

    def close(self) -> None:
        self.closed = True


def as_conn(fake: FakeConnection) -> sink.Connection:
    return cast(sink.Connection, fake)


@pytest.fixture
def connections(monkeypatch: pytest.MonkeyPatch) -> list[FakeConnection]:
    """Replace psycopg.connect; returns every connection opened, in order."""
    opened: list[FakeConnection] = []

    def connect(conninfo: str) -> FakeConnection:
        conn = FakeConnection()
        opened.append(conn)
        return conn

    monkeypatch.setattr(psycopg, "connect", connect)
    monkeypatch.setattr(sink, "_connections", {})
    return opened


def test_write_rows_stages_copies_and_inserts_in_one_transaction() -> None:
    conn = FakeConnection()
    result = write_rows(as_conn(conn), [row(1), row(2)])

    assert (result.rows, result.inserted) == (2, 2)
    assert result.seconds >= 0
    assert conn.commits == 1
    create, copy, insert = conn.statements
    assert create == (
        'CREATE TEMP TABLE "recentchange_stage" (LIKE "raw"."recentchange" INCLUDING DEFAULTS) '
        "ON COMMIT DROP"
    )
    cols = ", ".join(f'"{c}"' for c in COLUMNS)
    assert copy == f'COPY "recentchange_stage" ({cols}) FROM STDIN'
    assert insert == (
        f'INSERT INTO "raw"."recentchange" ({cols}) SELECT {cols} FROM "recentchange_stage" '
        "ON CONFLICT (event_id) DO NOTHING"
    )
    assert conn.cursors[0].staged == [row(1), row(2)]


def test_write_rows_counts_only_new_ids() -> None:
    conn = FakeConnection()
    write_rows(as_conn(conn), [row(1)])
    result = write_rows(as_conn(conn), [row(1), row(2), row(2)])
    assert (result.rows, result.inserted) == (3, 1)


def test_write_rows_accepts_a_generator() -> None:
    conn = FakeConnection()
    result = write_rows(as_conn(conn), (row(n) for n in range(5)))
    assert (result.rows, result.inserted) == (5, 5)


def test_write_rows_empty_input_touches_nothing() -> None:
    conn = FakeConnection()
    assert write_rows(as_conn(conn), []) == SinkResult(0, 0, 0.0)
    assert conn.statements == []
    assert conn.commits == 0


def test_write_rows_rolls_back_on_failure() -> None:
    conn = FakeConnection()
    conn.fail_next = psycopg.DataError("bad row")
    with pytest.raises(psycopg.DataError):
        write_rows(as_conn(conn), [row(1)])
    assert conn.commits == 0
    assert conn.table == set()


def test_write_rows_refuses_a_connection_inside_a_transaction() -> None:
    conn = FakeConnection()
    conn.info.transaction_status = psycopg.pq.TransactionStatus.INTRANS
    with pytest.raises(ValueError, match="no open transaction"):
        write_rows(as_conn(conn), [row(1)])
    assert conn.statements == []


def test_adapt_row_makes_naive_timestamps_aware() -> None:
    naive = datetime(2026, 10, 16, 23, 34, 15)
    spark_like = list(row(1))
    spark_like[COLUMNS.index("event_ts")] = naive
    spark_like[COLUMNS.index("emitted_at")] = None
    adapted = adapt_row(spark_like)
    assert adapted[COLUMNS.index("event_ts")] == naive.astimezone(UTC)
    assert adapted[COLUMNS.index("emitted_at")] is None
    assert adapted[COLUMNS.index("kafka_ts")] == KAFKA_TS


def collect(
    fn: Callable[[Iterable[Sequence[object]]], Iterator[SinkResult]],
    rows: Iterable[Sequence[object]],
) -> list[SinkResult]:
    return list(fn(rows))


def test_write_partition_yields_one_result_and_reuses_the_connection(
    connections: list[FakeConnection],
) -> None:
    fn = write_partition("dbname=x")
    [first] = collect(fn, [row(1), row(2)])
    [second] = collect(fn, [row(2), row(3)])
    assert (first.rows, first.inserted) == (2, 2)
    assert (second.rows, second.inserted) == (2, 1)
    assert len(connections) == 1


def test_write_partition_empty_partition_opens_no_connection(
    connections: list[FakeConnection],
) -> None:
    assert collect(write_partition("dbname=x"), []) == [SinkResult(0, 0, 0.0)]
    assert connections == []


def test_write_partition_reports_its_newest_event_time(
    connections: list[FakeConnection],
) -> None:
    [result] = collect(write_partition("dbname=x"), [row(5), row(9), row(2)])
    assert result.max_event_ts == datetime.fromtimestamp(1792193655 + 9, tz=UTC)


def test_write_partition_replaces_a_broken_connection(
    connections: list[FakeConnection],
) -> None:
    fn = write_partition("dbname=x")
    collect(fn, [row(1)])
    connections[0].broken = True
    collect(fn, [row(2)])
    assert len(connections) == 2


def test_write_partition_retries_once_after_a_dropped_connection(
    connections: list[FakeConnection],
) -> None:
    fn = write_partition("dbname=x")
    collect(fn, [row(1)])
    connections[0].fail_next = psycopg.OperationalError("server closed the connection")
    [result] = collect(fn, [row(2)])
    assert (result.rows, result.inserted) == (1, 1)
    assert len(connections) == 2
    assert connections[0].closed


def test_write_partition_does_not_retry_other_errors(
    connections: list[FakeConnection],
) -> None:
    fn = write_partition("dbname=x")
    collect(fn, [row(1)])
    connections[0].fail_next = psycopg.DataError("bad row")
    with pytest.raises(psycopg.DataError):
        collect(fn, [row(2)])
    assert len(connections) == 1


class FakeRDD:
    def __init__(self, partitions: list[list[tuple[object, ...]]]) -> None:
        self.partitions = partitions

    def mapPartitions(  # noqa: N802 (mirrors the pyspark API)
        self, fn: Callable[[Iterable[Sequence[object]]], Iterator[SinkResult]]
    ) -> FakeRDD:
        self.fn = fn
        return self

    def collect(self) -> list[SinkResult]:
        return [r for part in self.partitions for r in self.fn(iter(part))]


class FakeBatch:
    def __init__(self, partitions: list[list[tuple[object, ...]]]) -> None:
        self.rdd = FakeRDD(partitions)
        self.selected: tuple[str, ...] = ()

    def select(self, *cols: str) -> FakeBatch:
        self.selected = cols
        return self


def run_sink(batch: FakeBatch, batch_id: int) -> None:
    PostgresSink("dbname=x")(cast("DataFrame", batch), batch_id)


def test_postgres_sink_writes_each_partition_and_logs_the_batch(
    connections: list[FakeConnection],
) -> None:
    batch = FakeBatch([[row(1), row(2)], [], [row(3), row(3)]])
    with capture_logs() as logs:
        run_sink(batch, 7)
    assert batch.selected == COLUMNS
    assert connections[0].table == {f"00000000-0000-4000-8000-{n:012d}" for n in (1, 2, 3)}
    [entry] = [e for e in logs if e["event"] == "sink.batch"]
    assert entry["batch_id"] == 7
    assert (entry["rows"], entry["inserted"], entry["duplicates_skipped"]) == (4, 3, 1)
    assert entry["partitions"] == 3
    assert entry["sink_ms"] >= 0
    # Recorded once every partition has committed: the batch's newest event time.
    newest = datetime.fromtimestamp(1792193655 + 3, tz=UTC)
    assert connections[0].progress == [("warehouse", 7, newest, 4, 3)]


def test_postgres_sink_empty_batch_writes_nothing(connections: list[FakeConnection]) -> None:
    with capture_logs() as logs:
        run_sink(FakeBatch([[], []]), 8)
    assert connections == []
    assert [e["event"] for e in logs] == ["sink.batch_empty"]


def test_discarding_sink_counts_rows_but_opens_no_connection(
    connections: list[FakeConnection],
) -> None:
    batch = FakeBatch([[row(1), row(2)], [row(3)]])
    with capture_logs() as logs:
        PostgresSink("dbname=x", discard=True)(cast("DataFrame", batch), 4)
    assert connections == []
    [entry] = [e for e in logs if e["event"] == "sink.batch"]
    assert (entry["rows"], entry["inserted"], entry["partitions"]) == (3, 0, 2)


def test_discard_partition_reports_rows_and_newest_event_time() -> None:
    [result] = collect(discard_partition(), [row(5), row(9)])
    assert (result.rows, result.inserted) == (2, 0)
    assert result.max_event_ts == datetime.fromtimestamp(1792193655 + 9, tz=UTC)

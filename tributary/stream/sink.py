"""Idempotent PostgreSQL sink for ``raw.recentchange``.

Each partition of a micro-batch is copied into a transaction-local staging
table and moved with ``INSERT ... ON CONFLICT (event_id) DO NOTHING`` in one
transaction. A replayed micro-batch, a batch whose partitions committed only in
part, or repeated ids inside one batch therefore never produce duplicate rows:
the primary key, not Spark's state, is what makes delivery effectively once.

The module imports no pyspark at runtime so it is usable and testable without a JVM.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from datetime import UTC, datetime
from importlib.resources import files
from typing import TYPE_CHECKING, NamedTuple

import psycopg
import structlog
from psycopg import sql
from psycopg.rows import TupleRow

from tributary.events import COLUMNS

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

Connection = psycopg.Connection[TupleRow]

TABLE = sql.Identifier("raw", "recentchange")
STAGE = sql.Identifier("recentchange_stage")
_COLUMN_LIST = sql.SQL(", ").join(sql.Identifier(c) for c in COLUMNS)

CREATE_STAGE = sql.SQL(
    "CREATE TEMP TABLE {stage} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP"
).format(stage=STAGE, table=TABLE)
COPY_STAGE = sql.SQL("COPY {stage} ({cols}) FROM STDIN").format(stage=STAGE, cols=_COLUMN_LIST)
INSERT_FROM_STAGE = sql.SQL(
    "INSERT INTO {table} ({cols}) SELECT {cols} FROM {stage} ON CONFLICT (event_id) DO NOTHING"
).format(table=TABLE, cols=_COLUMN_LIST, stage=STAGE)

_TIMESTAMP_INDEXES = tuple(
    i for i, name in enumerate(COLUMNS) if name in ("event_ts", "emitted_at", "kafka_ts")
)

log = structlog.get_logger("tributary.stream.sink")


class SinkResult(NamedTuple):
    """Outcome of one write: rows offered, rows inserted, seconds spent."""

    rows: int
    inserted: int
    seconds: float


def ensure_schema(conninfo: str) -> None:
    """Create the ``raw`` schema and landing table if missing (idempotent)."""
    ddl = files("tributary").joinpath("sql/raw.sql").read_bytes()
    with psycopg.connect(conninfo) as conn:
        conn.execute(ddl)


def write_rows(conn: Connection, rows: Iterable[Sequence[object]]) -> SinkResult:
    """Insert rows (in :data:`~tributary.events.COLUMNS` order) in one transaction.

    Rows whose ``event_id`` already exists, in the table or earlier in ``rows``,
    are skipped; ``inserted`` counts only new rows. An empty input touches nothing.
    """
    start = time.perf_counter()
    it = iter(rows)
    first = next(it, None)
    if first is None:
        return SinkResult(0, 0, 0.0)
    count = 0
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(CREATE_STAGE)
        with cur.copy(COPY_STAGE) as copy:
            copy.write_row(first)
            count = 1
            for row in it:
                copy.write_row(row)
                count += 1
        cur.execute(INSERT_FROM_STAGE)
        inserted = cur.rowcount
    return SinkResult(count, inserted, time.perf_counter() - start)


def adapt_row(row: Sequence[object]) -> tuple[object, ...]:
    """Make the timestamps of a Spark row timezone-aware (UTC).

    PySpark hands ``TimestampType`` values to Python as naive datetimes in the
    worker's local time zone; ``astimezone`` interprets them the same way.
    """
    values = list(row)
    for i in _TIMESTAMP_INDEXES:
        value = values[i]
        if isinstance(value, datetime) and value.tzinfo is None:
            values[i] = value.astimezone(UTC)
    return tuple(values)


_connections: dict[str, Connection] = {}


def _connection(conninfo: str) -> Connection:
    """A connection cached per process (Spark reuses its Python workers)."""
    conn = _connections.get(conninfo)
    if conn is None or conn.closed or conn.broken:
        conn = psycopg.connect(conninfo)
        _connections[conninfo] = conn
    return conn


def _discard(conninfo: str) -> None:
    conn = _connections.pop(conninfo, None)
    if conn is not None:
        conn.close()


def write_partition(
    conninfo: str,
) -> Callable[[Iterable[Sequence[object]]], Iterator[SinkResult]]:
    """Build the ``mapPartitions`` function that writes one partition.

    The partition is materialised (it is bounded by ``maxOffsetsPerTrigger``) so
    that a write failing on a dropped connection can be retried once on a fresh
    one; the retry is safe because the write is idempotent. Empty partitions do
    not open a connection. Yields exactly one :class:`SinkResult`.
    """

    def write(rows: Iterable[Sequence[object]]) -> Iterator[SinkResult]:
        batch = [adapt_row(row) for row in rows]
        if not batch:
            yield SinkResult(0, 0, 0.0)
            return
        try:
            result = write_rows(_connection(conninfo), batch)
        except psycopg.OperationalError:
            _discard(conninfo)
            result = write_rows(_connection(conninfo), batch)
        yield result

    return write


class PostgresSink:
    """``foreachBatch`` function: write a micro-batch from one connection per partition.

    The batch is written by a single action, so the plan (Kafka read, parse,
    dedup) runs once and the dedup state for the batch is committed even when
    the batch is empty. ``sink_ms`` times that action, which includes the
    upstream stage; ``db_ms_max`` is the slowest partition's time in PostgreSQL.
    """

    def __init__(self, conninfo: str) -> None:
        self._conninfo = conninfo
        # foreachBatch runs on a py4j callback thread, which does not inherit contextvars.
        self._log = log.bind(**structlog.contextvars.get_contextvars())

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        start = time.perf_counter()
        results: list[SinkResult] = (
            batch_df.select(*COLUMNS).rdd.mapPartitions(write_partition(self._conninfo)).collect()
        )
        sink_ms = (time.perf_counter() - start) * 1000
        rows = sum(r.rows for r in results)
        if rows == 0:
            self._log.debug("sink.batch_empty", batch_id=batch_id, partitions=len(results))
            return
        inserted = sum(r.inserted for r in results)
        self._log.info(
            "sink.batch",
            batch_id=batch_id,
            rows=rows,
            inserted=inserted,
            duplicates_skipped=rows - inserted,
            partitions=len(results),
            sink_ms=round(sink_ms, 1),
            db_ms_max=round(max(r.seconds for r in results) * 1000, 1),
        )

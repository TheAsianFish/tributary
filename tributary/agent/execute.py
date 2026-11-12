"""Run guarded SQL on the warehouse, with limits the database enforces itself.

The guard decides what may run; this module makes sure that even a query the
guard wrongly lets through cannot do much. Every query

- connects as the agent's role, which can read the marts (and the ``ops``
  tables provenance needs) and nothing else, and whose sessions default to
  read-only transactions,
- is sent as a prepared statement (PostgreSQL's extended protocol), which
  refuses a string holding more than one statement, so a stacked
  ``SELECT 1; COMMIT; ...`` never runs past its first statement,
- runs inside a ``READ ONLY`` transaction with ``statement_timeout``,
  ``lock_timeout`` and an empty ``search_path`` (so an unqualified function
  name can only resolve to ``pg_catalog``, never to a look-alike in another
  schema),
- is priced with ``EXPLAIN`` first and refused above a planner-cost ceiling,
  so a cross join of the fact table is stopped before it starts (the ceiling
  sees rows, not work: ``md5(repeat('x', 10^8))`` costs 0.01 to the planner,
  and only the statement timeout bounds it),
- returns at most ``row_cap`` rows; the server stops producing rows there
  (the cursor stops fetching there with the byte cap off), and the result
  says it was cut,
- returns at most ``max_result_bytes`` of values, measured by the server
  before it sends a row: the query is wrapped so that PostgreSQL adds up the
  text size of every value it is about to send and fails the statement past
  the cap (a cast error carrying the size, reported as ``too_large``). A
  single row over the cap is refused before it is even buffered for the sum,
  so one ``repeat('x', 10^8)`` costs the server no more than running it bare
  and the client nothing. The rows sent before the total crossed the cap
  (under the cap together) are dropped with the error.

One connection per query keeps sessions from sharing state (a ``set_config``
that slipped through would die with its connection). The transaction is
always rolled back: nothing a read can do needs committing.

What the database cannot enforce: any role may raise its own
``statement_timeout`` (``set_config`` inside the query), and every role can
read ``pg_catalog``. Those two are held by the guard alone, which denies
``set_config``, ``SET`` and system-catalog reads; ``make eval-guard-db``
lists every such case.
"""

from __future__ import annotations

import contextlib
import json
import math
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from datetime import time as dtime
from decimal import Decimal
from typing import Any, Literal, Protocol
from uuid import UUID

import psycopg
import structlog
from psycopg import errors, sql

log = structlog.get_logger(__name__)

ErrorKind = Literal[
    "timeout", "too_expensive", "too_large", "read_only", "permission", "database", "connection"
]

# The text the size check casts to bigint when a result crosses the cap; the cast's
# error message quotes it, size included.
OVER_CAP_MARKER = "result_bytes_over_cap:"
_OVER_CAP = re.compile(re.escape(OVER_CAP_MARKER) + r"(\d+)")


class ExecutionError(Exception):
    """The database refused or failed the query; ``kind`` says how."""

    def __init__(self, kind: ErrorKind, message: str, sqlstate: str | None = None) -> None:
        super().__init__(message)
        self.kind: ErrorKind = kind
        self.message = message
        self.sqlstate = sqlstate

    @property
    def repairable(self) -> bool:
        """Whether a different query could succeed where this one failed.

        A syntax or column error, a timeout, a refused cost or size or a write
        in a read-only transaction is the query's fault. An unreachable warehouse
        or a missing privilege is the deployment's: the planner stops rather
        than spend model calls on repairs, and the API reports it as its own
        failure, not the caller's.
        """
        return self.kind not in ("connection", "permission")

    def feedback(self) -> str:
        """The error as the planner shows it to the model for a repair."""
        if self.kind == "connection":
            return f"The warehouse is unreachable: {self.message}"
        if self.kind == "permission":
            return f"The agent's role lacks a privilege: {self.message}"
        if self.kind == "timeout":
            return f"The query was cancelled by the statement timeout: {self.message}"
        if self.kind == "too_expensive":
            return f"The query was refused before running: {self.message}"
        if self.kind == "too_large":
            return f"The query's result was refused: {self.message}"
        state = f" (SQLSTATE {self.sqlstate})" if self.sqlstate else ""
        return f"PostgreSQL error{state}: {self.message}"


@dataclass(frozen=True, slots=True)
class QueryResult:
    columns: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...]
    truncated: bool
    elapsed_ms: float
    cost: float | None = None


class Executor(Protocol):
    def run(self, query: str) -> QueryResult: ...


def jsonable(value: object) -> object:
    """A database value as JSON: exact integers stay integers, everything else is plain.

    JSON has no Infinity or NaN, and PostgreSQL's numeric and float types have
    both, so those become the strings PostgreSQL prints for them (``Infinity``,
    ``-Infinity``, ``NaN``). A ``bytea`` is printed as PostgreSQL does, hex
    with a ``\\x`` prefix. A non-integral numeric becomes a float: ``avg()`` of
    an integer column is a 16-digit numeric, and a client expects a number,
    not a string, so the digits a double cannot hold are given up.
    """
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return value
    if isinstance(value, Decimal):
        if value.is_nan():
            return "NaN"
        if value.is_infinite():
            return "Infinity" if value > 0 else "-Infinity"
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, bytes | bytearray | memoryview):
        return "\\x" + bytes(value).hex()
    if isinstance(value, datetime | date | dtime):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, list | tuple):
        return [jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    return str(value)


def _classify(exc: psycopg.Error, max_result_bytes: int | None = None) -> ExecutionError:
    diag = getattr(exc, "diag", None)
    message = (getattr(diag, "message_primary", None) or str(exc)).strip()
    sqlstate = getattr(exc, "sqlstate", None)
    kind: ErrorKind
    over = _OVER_CAP.search(message) if isinstance(exc, errors.InvalidTextRepresentation) else None
    if over and max_result_bytes is not None:
        size = int(over.group(1))
        return ExecutionError(
            "too_large",
            f"the result reached {size:,} bytes, over the cap of {max_result_bytes:,}; "
            "select fewer or shorter columns, or aggregate",
            sqlstate,
        )
    if isinstance(exc, errors.QueryCanceled):
        kind = "timeout"
    elif isinstance(exc, errors.ReadOnlySqlTransaction):
        kind = "read_only"
    elif isinstance(exc, errors.InsufficientPrivilege):
        kind = "permission"
    elif isinstance(exc, psycopg.OperationalError) and sqlstate is None:
        kind = "connection"
    else:
        kind = "database"
    return ExecutionError(kind, message, sqlstate)


def plan_cost(explain_json: Any) -> float:
    """Total cost of the top plan node from ``EXPLAIN (FORMAT JSON)``.

    Any: the plan document, a list holding one ``{"Plan": {...}}`` object.
    """
    document = json.loads(explain_json) if isinstance(explain_json, str) else explain_json
    try:
        return float(document[0]["Plan"]["Total Cost"])
    except (KeyError, IndexError, TypeError, ValueError):
        raise ExecutionError("database", "EXPLAIN did not return a plan") from None


def capped_columns(width: int) -> list[sql.Identifier]:
    """The positional names a size-capped query's columns get: ``_c1`` ... ``_c{width}``."""
    return [sql.Identifier(f"_c{i}") for i in range(1, width + 1)]


# bpchar: its cast to text strips the padding that the server still sends.
BPCHAR_OID = 1042


def row_bytes(types: Sequence[int]) -> sql.Composed:
    """A row's size in bytes as the cap counts it, over ``r._c1`` ... (a bigint expression).

    ``types`` are the columns' type oids, as the bare query's description gives
    them (a domain reports its base type). The sum of its values' ``::text``
    lengths, a NULL counting 0: what the text protocol sends, less the 4-byte
    length word per value (and but for a boolean, whose cast spells ``true``
    where the protocol sends ``t``). A ``char(n)`` value is measured as it is,
    because its cast to ``text`` drops the trailing spaces the server sends:
    ``''::char(10485760)`` is 0 bytes as text and 10 MiB on the wire.
    """
    return sql.SQL(" + ").join(
        sql.SQL(
            "coalesce(octet_length(r.{}), 0)::bigint"
            if oid == BPCHAR_OID
            else "coalesce(octet_length(r.{}::text), 0)::bigint"
        ).format(c)
        for c, oid in zip(capped_columns(len(types)), types, strict=True)
    )


def size_capped(query: str, types: Sequence[int], *, cap: int, limit: int) -> sql.Composed:
    """``query`` wrapped so that the server refuses a result over ``cap`` bytes.

    The query's columns (``types``, their type oids) are renamed positionally (``_c1`` ...), so a
    duplicate or unnamed column needs no reference by name; the caller takes
    the names from the bare query's description. A row's size is
    :func:`row_bytes`. Each row is checked on its own as it is
    produced, before a window buffers it, and the running total as rows are
    emitted, in order: the first row past the cap fails the statement with a
    cast error quoting ``OVER_CAP_MARKER`` and the size. The last column,
    ``_over_cap``, is NULL on every row that is sent; the caller drops it.
    At most ``limit`` rows are produced.

    Two fences keep the plan honest. The ``LIMIT`` right around ``query``
    stops it being pulled up into the wrapper, where the planner would
    evaluate each of its output expressions once per mention in the size
    expression (constant-folding ``repeat('x', 10^8)`` three times). The
    ``OFFSET 0`` keeps the per-row check below the window, so a row over the
    cap fails before the window copies it into its buffer.
    """
    if not types:
        raise ValueError("a size-capped query needs at least one column")
    columns = capped_columns(len(types))
    size = row_bytes(types)
    over = sql.SQL("({} || {})::bigint").format(sql.Literal(OVER_CAP_MARKER), size)
    total = sql.SQL("sum(s._bytes) OVER w")
    return sql.SQL(
        "SELECT {outer}, CASE WHEN {total} > {cap} THEN ({marker} || {total})::bigint END"
        " AS _over_cap FROM ("
        "SELECT r.*, CASE WHEN {size} > {cap} THEN {over} ELSE {size} END AS _bytes"
        " FROM (SELECT * FROM (\n{query}\n) AS q LIMIT {limit}) AS r({columns})"
        " OFFSET 0"
        ") AS s WINDOW w AS (ROWS UNBOUNDED PRECEDING)"
    ).format(
        outer=sql.SQL(", ").join(sql.SQL("s.{}").format(c) for c in columns),
        total=total,
        cap=sql.Literal(cap),
        marker=sql.Literal(OVER_CAP_MARKER),
        size=size,
        over=over,
        query=sql.SQL(query),
        columns=sql.SQL(", ").join(columns),
        limit=sql.Literal(limit),
    )


class PostgresExecutor:
    """:class:`Executor` over a role-restricted PostgreSQL connection."""

    def __init__(
        self,
        conninfo: str,
        *,
        statement_timeout_ms: int = 10_000,
        row_cap: int = 1000,
        cost_ceiling: float | None = 1_000_000.0,
        connect_timeout: int = 5,
        search_path: str = "",
        parallel: bool = True,
        max_result_bytes: int | None = 8_388_608,
    ) -> None:
        self._conninfo = conninfo
        self.statement_timeout_ms = statement_timeout_ms
        self.row_cap = row_cap
        self.cost_ceiling = cost_ceiling
        self._connect_timeout = connect_timeout
        # Empty for the agent (the guard qualifies every relation); one schema for
        # benchmark gold queries, which BIRD writes unqualified.
        self.search_path = search_path
        # Off for reproducible results: a parallel aggregate adds partial sums in whatever
        # order the workers finish, so a float4 SUM can differ between runs.
        self.parallel = parallel
        # None runs the query bare (benchmarks): no size check, no server-side row limit.
        self.max_result_bytes = max_result_bytes

    def _settings(self) -> Sequence[sql.SQL | sql.Composed]:
        timeout = sql.Literal(f"{self.statement_timeout_ms}ms")
        serial = (
            () if self.parallel else (sql.SQL("SET LOCAL max_parallel_workers_per_gather = 0"),)
        )
        return (
            sql.SQL("SET TRANSACTION READ ONLY"),
            sql.SQL("SET LOCAL statement_timeout = {}").format(timeout),
            sql.SQL("SET LOCAL lock_timeout = '1s'"),
            sql.SQL("SET LOCAL idle_in_transaction_session_timeout = {}").format(timeout),
            sql.SQL("SET LOCAL search_path = {}").format(
                sql.Identifier(self.search_path) if self.search_path else sql.Literal("")
            ),
            *serial,
        )

    def run(self, query: str) -> QueryResult:
        started = time.perf_counter()
        try:
            conn = psycopg.connect(self._conninfo, connect_timeout=self._connect_timeout)
        except psycopg.Error as exc:
            raise ExecutionError("connection", f"cannot connect to the warehouse: {exc}") from None
        try:
            with conn.cursor() as cur:
                for statement in self._settings():
                    cur.execute(statement)
                cost = None
                if self.cost_ceiling is not None:
                    cur.execute(sql.SQL("EXPLAIN (FORMAT JSON) ") + sql.SQL(query), prepare=True)
                    row = cur.fetchone()
                    cost = plan_cost(row[0]) if row else None
                    if cost is not None and cost > self.cost_ceiling:
                        raise ExecutionError(
                            "too_expensive",
                            f"estimated cost {cost:,.0f} is above the ceiling "
                            f"{self.cost_ceiling:,.0f}; filter or aggregate earlier",
                        )
                columns, fetched = self._fetch(cur, query)
        except psycopg.Error as exc:
            raise _classify(exc, self.max_result_bytes) from None
        finally:
            # The query may have killed its own connection (pg_terminate_backend is open to
            # a role for its own sessions); a failed rollback must not mask the error.
            with contextlib.suppress(psycopg.Error):
                conn.rollback()
            conn.close()
        elapsed = (time.perf_counter() - started) * 1000
        truncated = len(fetched) > self.row_cap
        rows = tuple(tuple(r) for r in fetched[: self.row_cap])
        log.info(
            "sql.executed",
            rows=len(rows),
            truncated=truncated,
            cost=cost,
            elapsed_ms=round(elapsed),
        )
        return QueryResult(columns, rows, truncated, elapsed, cost)

    def _fetch(self, cur: psycopg.Cursor[Any], query: str) -> tuple[tuple[str, ...], list[Any]]:
        """The query's column names and up to ``row_cap + 1`` rows, size-capped if set.

        Any: rows are tuples of whatever types the columns decode to.
        """
        if self.max_result_bytes is not None:
            # The bare query's description, without running it (LIMIT 0 pulls no row):
            # its names, duplicates and ``?column?`` included, and its width.
            cur.execute(
                sql.SQL("SELECT * FROM (\n{}\n) AS q LIMIT 0").format(sql.SQL(query)),
                prepare=True,
            )
            if cur.description is None:
                raise ExecutionError("database", "the statement returned no result set")
            columns = tuple(d.name for d in cur.description)
            if columns:  # a zero-column SELECT has no value to measure
                types = [d.type_code for d in cur.description]
                capped = size_capped(
                    query, types, cap=self.max_result_bytes, limit=self.row_cap + 1
                )
                cur.execute(capped, prepare=True)
                return columns, [row[:-1] for row in cur.fetchmany(self.row_cap + 1)]
        cur.execute(sql.SQL(query), prepare=True)
        if cur.description is None:
            raise ExecutionError("database", "the statement returned no result set")
        return tuple(d.name for d in cur.description), cur.fetchmany(self.row_cap + 1)

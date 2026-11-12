"""The executor's pure parts, the agent role's statements and the terminal rendering."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID

import psycopg
import pytest
from psycopg import errors, sql

from tributary.agent.execute import (
    OVER_CAP_MARKER,
    ExecutionError,
    PostgresExecutor,
    _classify,
    jsonable,
    plan_cost,
    size_capped,
)
from tributary.agent.render import MAX_ROWS, MAX_WIDTH, render_answer, table
from tributary.agent.roles import agent_role_statements


@pytest.mark.parametrize(
    ("value", "plain"),
    [
        (None, None),
        (True, True),
        (7, 7),
        (1.5, 1.5),
        ("x", "x"),
        (Decimal("10"), 10),
        (Decimal("10.00"), 10),
        (Decimal("0.125"), 0.125),
        (datetime(2026, 10, 17, 9, 30, tzinfo=UTC), "2026-10-17T09:30:00+00:00"),
        (date(2026, 10, 17), "2026-10-17"),
        (time(9, 30), "09:30:00"),
        (timedelta(hours=1), 3600.0),
        (UUID(int=1), "00000000-0000-0000-0000-000000000001"),
        ([Decimal("1"), (2, None)], [1, [2, None]]),
        ({1: Decimal("2.5")}, {"1": 2.5}),
        # bytea as PostgreSQL prints it; infinities and NaN as PostgreSQL spells them
        (b"ab", "\\x6162"),
        (memoryview(b"\x00\xff"), "\\x00ff"),
        (float("inf"), "Infinity"),
        (float("-inf"), "-Infinity"),
        (float("nan"), "NaN"),
        (Decimal("Infinity"), "Infinity"),
        (Decimal("-Infinity"), "-Infinity"),
        (Decimal("NaN"), "NaN"),
        # a non-integral numeric is a float, digits a double cannot hold included
        (Decimal("502.5192849696922899"), 502.5192849696923),
        (Decimal("1.10"), 1.1),
    ],
)
def test_jsonable(value: object, plain: object) -> None:
    converted = jsonable(value)
    assert converted == plain
    assert type(converted) is type(plain)
    json.dumps(converted)


def test_plan_cost() -> None:
    document = [{"Plan": {"Node Type": "Seq Scan", "Total Cost": 1234.5}}]
    assert plan_cost(document) == 1234.5
    assert plan_cost(json.dumps(document)) == 1234.5


@pytest.mark.parametrize(
    ("exc", "kind", "sqlstate"),
    [
        (errors.QueryCanceled("canceling statement due to statement timeout"), "timeout", "57014"),
        (errors.ReadOnlySqlTransaction("read-only transaction"), "read_only", "25006"),
        (errors.InsufficientPrivilege("permission denied for table x"), "permission", "42501"),
        (errors.UndefinedColumn('column "nope" does not exist'), "database", "42703"),
        (psycopg.OperationalError("connection refused"), "connection", None),
        (errors.AdminShutdown("terminating connection"), "database", "57P01"),
    ],
)
def test_classify(exc: psycopg.Error, kind: str, sqlstate: str | None) -> None:
    error = _classify(exc)
    assert error.kind == kind
    assert error.sqlstate == sqlstate
    assert error.message == str(exc)


@pytest.mark.parametrize(
    ("error", "feedback"),
    [
        (
            ExecutionError("timeout", "canceled", "57014"),
            "The query was cancelled by the statement timeout: canceled",
        ),
        (
            ExecutionError("too_expensive", "estimated cost 9 is above the ceiling 1"),
            "The query was refused before running: estimated cost 9 is above the ceiling 1",
        ),
        (
            ExecutionError("database", 'column "x" does not exist', "42703"),
            'PostgreSQL error (SQLSTATE 42703): column "x" does not exist',
        ),
        (
            ExecutionError("connection", "cannot connect"),
            "The warehouse is unreachable: cannot connect",
        ),
        (ExecutionError("permission", "denied"), "The agent's role lacks a privilege: denied"),
        (
            ExecutionError("too_large", "the result reached 9 bytes, over the cap of 8"),
            "The query's result was refused: the result reached 9 bytes, over the cap of 8",
        ),
    ],
)
def test_feedback(error: ExecutionError, feedback: str) -> None:
    assert error.feedback() == feedback
    assert str(error) == error.message


def test_agent_role_statements() -> None:
    statements = [s.as_string() for s in agent_role_statements("agent", "pa'ss", "tributary")]
    assert statements == [
        "ALTER ROLE \"agent\" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD 'pa''ss'",
        'ALTER ROLE "agent" CONNECTION LIMIT 20',
        'ALTER ROLE "agent" SET default_transaction_read_only = on',
        "ALTER ROLE \"agent\" SET statement_timeout = '30s'",
        'GRANT CONNECT ON DATABASE "tributary" TO "agent"',
        'REVOKE TEMPORARY ON DATABASE "tributary" FROM PUBLIC, "agent"',
        'CREATE SCHEMA IF NOT EXISTS "marts"',
        'CREATE SCHEMA IF NOT EXISTS "ops"',
        'GRANT USAGE ON SCHEMA "marts", "ops" TO "agent"',
        'GRANT SELECT ON ALL TABLES IN SCHEMA "marts", "ops" TO "agent"',
        'ALTER DEFAULT PRIVILEGES IN SCHEMA "marts", "ops" GRANT SELECT ON TABLES TO "agent"',
    ]


def test_agent_role_statements_quote_identifiers() -> None:
    statements = [s.as_string() for s in agent_role_statements('a"b', "p", "d", ("marts",))]
    assert statements[1] == 'ALTER ROLE "a""b" CONNECTION LIMIT 20'
    assert not any('"ops"' in s for s in statements)


def test_table() -> None:
    text = table(["wiki", "edits"], [["dewiki", 3], ["enwiki", None]])
    assert text.splitlines() == [
        "wiki    edits",
        "------  -----",
        "dewiki  3",
        "enwiki  NULL",
    ]


def test_table_cuts_long_cells_and_rows() -> None:
    rows = [["x" * 100] for _ in range(MAX_ROWS + 5)]
    lines = table(["c"], rows).splitlines()
    assert len(lines) == 2 + MAX_ROWS + 1
    assert lines[2] == "x" * (MAX_WIDTH - 1) + "…"
    assert lines[-1] == "... 5 more rows"


def test_render_answer() -> None:
    document: dict[str, object] = {
        "answer": "edits: 42.",
        "sql": "SELECT 1 LIMIT 1000",
        "columns": ["edits"],
        "rows": [[42]],
        "provenance": [
            {
                "relation": "marts.fct_edits",
                "last_refreshed_at": None,
                "quality": "fail",
                "checks": 2,
                "stale": True,
            }
        ],
        "warnings": ["marts.fct_edits has no recorded rebuild"],
        "model": "m",
        "attempts": 1,
        "usage": {"input_tokens": 10, "output_tokens": 2, "cost_usd": 0.001},
        "latency_ms": 12.5,
    }
    assert render_answer(document).splitlines() == [
        "edits: 42.",
        "",
        "SQL:",
        "  SELECT 1 LIMIT 1000",
        "",
        "edits",
        "-----",
        "42",
        "",
        "Provenance:",
        "  marts.fct_edits: rebuilt never, quality fail (2 checks), STALE",
        "",
        "Warnings:",
        "  - marts.fct_edits has no recorded rebuild",
        "",
        "m, 1 attempt(s), 10 in / 2 out tokens, $0.001, 12.5 ms",
    ]


def test_render_answer_minimal() -> None:
    assert render_answer({"answer": "blocked", "sql": None, "rows": []}) == "blocked"


@pytest.mark.parametrize("document", [[], [{"QUERY PLAN": "x"}], "[1]", [{"Plan": {}}]])
def test_plan_cost_without_a_plan_is_an_execution_error(document: object) -> None:
    with pytest.raises(ExecutionError) as caught:
        plan_cost(document)
    assert caught.value.kind == "database"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("Infinity"), "Infinity"),
        (Decimal("-Infinity"), "-Infinity"),
        (Decimal("NaN"), "NaN"),
        (float("inf"), "Infinity"),
        (float("-inf"), "-Infinity"),
        (float("nan"), "NaN"),
    ],
)
def test_non_finite_numbers_are_json_strings(value: object, expected: str) -> None:
    assert jsonable(value) == expected
    json.dumps(jsonable(value), allow_nan=False)


def _rendered(statement: sql.Composable) -> str:
    # Identifiers and string literals render without a connection.
    return statement.as_string(None)


def test_the_agent_runs_with_an_empty_search_path() -> None:
    executor = PostgresExecutor("dbname=x")
    rendered = [_rendered(s) for s in executor._settings()]
    assert "SET LOCAL search_path = ''" in rendered


def test_a_benchmark_executor_resolves_unqualified_names_in_one_schema() -> None:
    executor = PostgresExecutor("dbname=x", search_path="card_games")
    rendered = [_rendered(s) for s in executor._settings()]
    assert 'SET LOCAL search_path = "card_games"' in rendered
    assert "SET TRANSACTION READ ONLY" in rendered


def test_serial_plans_are_opt_in() -> None:
    serial = "SET LOCAL max_parallel_workers_per_gather = 0"
    assert serial not in [_rendered(s) for s in PostgresExecutor("dbname=x")._settings()]
    rendered = [_rendered(s) for s in PostgresExecutor("dbname=x", parallel=False)._settings()]
    assert rendered[0] == "SET TRANSACTION READ ONLY"
    assert serial in rendered


def test_size_capped_wraps_the_query_behind_fences() -> None:
    rendered = size_capped("SELECT 1 AS a, 2 AS a -- note", 2, cap=100, limit=6).as_string(None)
    assert rendered == (
        'SELECT s."_c1", s."_c2", CASE WHEN sum(s._bytes) OVER w > 100'
        " THEN ('result_bytes_over_cap:' || sum(s._bytes) OVER w)::bigint END AS _over_cap"
        " FROM (SELECT r.*, CASE WHEN"
        ' coalesce(octet_length(r."_c1"::text), 0)::bigint'
        ' + coalesce(octet_length(r."_c2"::text), 0)::bigint > 100'
        " THEN ('result_bytes_over_cap:' || "
        'coalesce(octet_length(r."_c1"::text), 0)::bigint'
        ' + coalesce(octet_length(r."_c2"::text), 0)::bigint)::bigint'
        ' ELSE coalesce(octet_length(r."_c1"::text), 0)::bigint'
        ' + coalesce(octet_length(r."_c2"::text), 0)::bigint END AS _bytes'
        # the query on lines of its own: a trailing line comment cannot swallow the ")"
        " FROM (SELECT * FROM (\nSELECT 1 AS a, 2 AS a -- note\n) AS q LIMIT 6)"
        ' AS r("_c1", "_c2") OFFSET 0) AS s WINDOW w AS (ROWS UNBOUNDED PRECEDING)'
    )


def test_size_capped_needs_a_column() -> None:
    with pytest.raises(ValueError, match="at least one column"):
        size_capped("SELECT FROM marts.dim_wiki", 0, cap=1, limit=1)


def test_a_result_over_the_cap_is_too_large() -> None:
    exc = errors.InvalidTextRepresentation(
        f'invalid input syntax for type bigint: "{OVER_CAP_MARKER}200000000"'
    )
    error = _classify(exc, 8_388_608)
    assert error.kind == "too_large"
    assert error.repairable
    assert error.message == (
        "the result reached 200,000,000 bytes, over the cap of 8,388,608; "
        "select fewer or shorter columns, or aggregate"
    )
    assert error.feedback().startswith("The query's result was refused: the result reached")


def test_other_invalid_input_stays_a_database_error() -> None:
    exc = errors.InvalidTextRepresentation('invalid input syntax for type integer: "abc"')
    assert _classify(exc, 8_388_608).kind == "database"
    # without a cap, nothing is classified as over it
    marker = errors.InvalidTextRepresentation(f'... "{OVER_CAP_MARKER}9"')
    assert _classify(marker).kind == "database"


class _Description:
    def __init__(self, name: str) -> None:
        self.name = name


class _Cursor:
    """Just enough of a psycopg cursor for ``_fetch``: records statements, replays rows."""

    def __init__(self, names: list[str], rows: list[tuple[object, ...]]) -> None:
        self.names = names
        self.rows = rows
        self.executed: list[str] = []
        self.description: list[_Description] | None = None

    def execute(self, statement: sql.Composable, *, prepare: bool = False) -> None:
        text = statement.as_string(None)
        self.executed.append(text)
        capped = "_over_cap" in text
        self.description = [_Description(n) for n in self.names] + (
            [_Description("_over_cap")] if capped else []
        )
        self._rows = [(*r, None) for r in self.rows] if capped else list(self.rows)

    def fetchmany(self, size: int) -> list[tuple[object, ...]]:
        return self._rows[:size]


def test_fetch_takes_names_from_the_bare_query_and_drops_the_size_column() -> None:
    executor = PostgresExecutor("dbname=x", row_cap=2, max_result_bytes=1000)
    cur = _Cursor(["a", "a"], [(1, "x"), (2, "y"), (3, "z")])
    columns, rows = executor._fetch(cur, "SELECT 1 AS a, 'x' AS a")  # type: ignore[arg-type]
    assert columns == ("a", "a")
    assert rows == [(1, "x"), (2, "y"), (3, "z")]
    describe, capped = cur.executed
    assert describe == "SELECT * FROM (\nSELECT 1 AS a, 'x' AS a\n) AS q LIMIT 0"
    assert "> 1000 THEN" in capped
    assert "LIMIT 3)" in capped  # row_cap + 1, so the result can say it was cut


def test_no_byte_cap_runs_the_query_bare() -> None:
    executor = PostgresExecutor("dbname=x", max_result_bytes=None)
    cur = _Cursor(["a"], [(1,)])
    columns, rows = executor._fetch(cur, "SELECT 1 AS a")  # type: ignore[arg-type]
    assert cur.executed == ["SELECT 1 AS a"]
    assert (columns, rows) == (("a",), [(1,)])


def test_a_zero_column_result_runs_bare_after_the_description() -> None:
    executor = PostgresExecutor("dbname=x")
    cur = _Cursor([], [(), ()])
    columns, rows = executor._fetch(cur, "SELECT FROM marts.dim_wiki")  # type: ignore[arg-type]
    assert columns == ()
    assert rows == [(), ()]
    assert cur.executed[-1] == "SELECT FROM marts.dim_wiki"


def test_the_byte_cap_defaults_to_8_mib() -> None:
    assert PostgresExecutor("dbname=x").max_result_bytes == 8 * 1024 * 1024

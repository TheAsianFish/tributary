"""The executor's pure parts, the agent role's statements and the terminal rendering."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID

import psycopg
import pytest
from psycopg import errors

from tributary.agent.execute import ExecutionError, _classify, jsonable, plan_cost
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
        (b"ab", "b'ab'"),
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
        (float("inf"), "inf"),
        (float("nan"), "nan"),
    ],
)
def test_non_finite_numbers_are_json_strings(value: object, expected: str) -> None:
    assert jsonable(value) == expected
    json.dumps(jsonable(value), allow_nan=False)

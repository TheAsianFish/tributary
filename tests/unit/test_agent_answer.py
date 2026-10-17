"""Answers composed from rows, with provenance warnings, and their JSON document."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from tests.unit.agent_doubles import NOW, provenance, result
from tributary.agent.answer import MAX_TEXT_ROWS, compose, format_value, result_text
from tributary.agent.execute import QueryResult
from tributary.agent.guard import GuardResult
from tributary.agent.planner import Attempt, PlanOutcome, SQLDraft, Status
from tributary.agent.provenance import TableProvenance
from tributary.agent.provider import Usage

STALE_AFTER = timedelta(minutes=120)
RAN = "SELECT COUNT(*) AS edits FROM marts.fct_edits LIMIT 1000"


def outcome(
    status: Status = "answered",
    *,
    query: QueryResult | None = None,
    attempts: tuple[Attempt, ...] | None = None,
    error: str | None = None,
) -> PlanOutcome:
    d = SQLDraft(
        assumptions=("all wikis",),
        sql="SELECT count(*) AS edits FROM marts.fct_edits",
        explanation="The number of edits.",
        confidence="high",
    )
    guard = GuardResult(True, RAN, (), ("marts.fct_edits",), 1000)
    if attempts is None:
        attempts = (Attempt(d, guard, None),)
    return PlanOutcome(
        question="How many edits?",
        status=status,
        attempts=attempts,
        result=query if query is not None else (result() if status == "answered" else None),
        usage=Usage(200, 40, 0, 0, 0.02),
        latency_ms=1234.56,
        model="scripted-model-v1",
        error=error,
        calls=len(attempts),
    )


def answer_text(o: PlanOutcome, prov: tuple[TableProvenance, ...] = ()) -> str:
    return compose(o, prov, stale_after=STALE_AFTER, now=NOW).text


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (None, "NULL"),
        (True, "true"),
        (False, "false"),
        (1234567, "1,234,567"),
        (Decimal("1234"), "1,234"),
        (Decimal("2.5"), "2.5"),
        (0.123456789, "0.123457"),
        (1e16, "1e+16"),
        ("dewiki", "dewiki"),
        (datetime(2026, 10, 17, 9, 0, tzinfo=UTC), "2026-10-17T09:00:00+00:00"),
        (date(2026, 10, 17), "2026-10-17"),
        (timedelta(minutes=2), "120"),
    ],
)
def test_format_value(value: object, text: str) -> None:
    assert format_value(value) == text


def test_scalar_text() -> None:
    assert result_text(("edits",), [(4321,)], False) == "edits: 4,321."
    o = outcome(query=result(rows=[(4321,)]))
    assert answer_text(o) == "The number of edits. edits: 4,321."


def test_few_rows_inline() -> None:
    text = result_text(("wiki", "edits"), [("dewiki", 3), ("enwiki", 10)], False)
    assert text == "2 rows: wiki dewiki, edits 3; wiki enwiki, edits 10."


def test_many_rows_are_truncated_at_ten() -> None:
    rows = [(f"w{n}", n) for n in range(25)]
    text = result_text(("wiki", "edits"), rows, False)
    assert text.startswith(f"25 rows; first {MAX_TEXT_ROWS} shown: wiki w0, edits 0;")
    assert "wiki w9, edits 9." in text
    assert "w10" not in text


def test_exactly_ten_rows_shows_all() -> None:
    text = result_text(("n",), [(n,) for n in range(10)], False)
    assert text.startswith("10 rows: n 0;")
    assert "shown" not in text


def test_truncated_count_has_a_plus() -> None:
    assert result_text(("n",), [(1,), (2,)], True).startswith("2+ rows:")


def test_empty_result() -> None:
    assert result_text(("edits",), [], False) == "No rows matched."
    o = outcome(query=result(rows=[]))
    assert answer_text(o) == "The number of edits. No rows matched."


@pytest.mark.parametrize(
    ("prov", "warning"),
    [
        (
            provenance(stale=True, refreshed=NOW - timedelta(hours=5)),
            "marts.fct_edits was last rebuilt 5 h ago (limit 2 h)",
        ),
        (provenance(refreshed=None, stale=True), "marts.fct_edits has no recorded rebuild"),
        (
            provenance(quality="fail", failing=("freshness_raw",)),
            "marts.fct_edits: failing checks: freshness_raw",
        ),
        (
            provenance(quality="warn", warning=("row_count",)),
            "marts.fct_edits: checks warning: row_count",
        ),
        (provenance(quality="unknown"), "marts.fct_edits: no quality results recorded"),
    ],
)
def test_provenance_warnings_reach_the_text(prov: TableProvenance, warning: str) -> None:
    a = compose(outcome(), (prov,), stale_after=STALE_AFTER, now=NOW)
    assert a.warnings == (warning,)
    assert a.text == f"The number of edits. edits: 42. Caution: {warning}."


def test_clean_provenance_has_no_caution() -> None:
    a = compose(outcome(), (provenance(),), stale_after=STALE_AFTER, now=NOW)
    assert a.warnings == ()
    assert "Caution" not in a.text


def test_truncated_result_warns() -> None:
    rows = [(n,) for n in range(3)]
    a = compose(outcome(query=result(rows=rows, truncated=True)), (), stale_after=STALE_AFTER)
    assert a.truncated
    assert a.warnings == ("result cut at 3 rows",)
    assert a.text.endswith("Caution: result cut at 3 rows.")


def test_unanswerable_text() -> None:
    d = SQLDraft(answerable=False, assumptions=("no revenue table", "no prices"))
    o = outcome("unanswerable", attempts=(Attempt(d, None, None),))
    a = compose(o, (), stale_after=STALE_AFTER)
    assert a.text == "The warehouse cannot answer this: no revenue table; no prices"
    assert a.sql is None
    assert a.tables == ()
    assert a.rows == ()


def test_unanswerable_without_reason() -> None:
    o = outcome("unanswerable", attempts=(Attempt(SQLDraft(answerable=False), None, None),))
    assert answer_text(o) == "The warehouse cannot answer this: no reason given"


@pytest.mark.parametrize("n", [1, 3])
def test_failed_text(n: int) -> None:
    attempts = tuple(Attempt(None, None, f"error {i}") for i in range(n))
    o = outcome("failed", attempts=attempts, error=f"error {n - 1}")
    a = compose(o, (), stale_after=STALE_AFTER)
    plural = "s" if n != 1 else ""
    assert a.text == (
        f"No query that is allowed to run answered this after {n} attempt{plural}. "
        f"Last error: error {n - 1}"
    )
    assert a.errors == tuple(f"error {i}" for i in range(n))
    assert a.attempts == n
    assert a.sql is None


def test_answered_fields() -> None:
    a = compose(outcome(), (provenance(),), stale_after=STALE_AFTER, now=NOW, request_id="r1")
    assert a.sql == RAN
    assert a.tables == ("marts.fct_edits",)
    assert a.assumptions == ("all wikis",)
    assert a.confidence == "high"
    assert a.model == "scripted-model-v1"
    assert (a.input_tokens, a.output_tokens, a.cost_usd) == (200, 40, 0.02)
    assert a.request_id == "r1"


def test_to_dict_is_json() -> None:
    rows = [
        (Decimal("12"), Decimal("0.25"), datetime(2026, 10, 17, 9, tzinfo=UTC), None),
    ]
    query = result(("n", "share", "hour", "x"), rows)
    a = compose(
        outcome(query=query),
        (provenance(),),
        stale_after=STALE_AFTER,
        now=NOW,
        request_id="r1",
    )

    document = json.loads(json.dumps(a.to_dict()))

    assert document["rows"] == [[12, 0.25, "2026-10-17T09:00:00+00:00", None]]
    assert document["row_count"] == 1
    assert document["request_id"] == "r1"
    assert document["status"] == "answered"
    assert document["answer"] == a.text
    assert document["usage"] == {"input_tokens": 200, "output_tokens": 40, "cost_usd": 0.02}
    assert document["latency_ms"] == 1234.6
    assert document["provenance"][0]["last_refreshed_at"] == "2026-10-17T11:50:00+00:00"
    assert document["provenance"][0]["relation"] == "marts.fct_edits"
    assert isinstance(document["rows"][0][0], int)

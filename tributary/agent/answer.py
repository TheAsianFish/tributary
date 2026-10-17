"""Rows → an answer that says what it read and whether to trust it.

The answer text is composed from the rows, not written by the model: the
model describes the query before it has seen any result (the draft's
``explanation``), and every number in the text is a value from the result
set. A model asked to summarise rows can misquote them; this cannot.

Every answer carries its provenance: the SQL that ran (as the guard
rewrote it), the tables it read, and for each table when it was last rebuilt
and whether its checks, and its sources' checks, passed. Stale or failing
tables become warnings in the answer itself, so they cannot be missed by a
client that only shows the text.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from tributary.agent.execute import jsonable
from tributary.agent.planner import PlanOutcome
from tributary.agent.provenance import TableProvenance

MAX_TEXT_ROWS = 10


@dataclass(frozen=True, slots=True)
class Answer:
    question: str
    status: str
    text: str
    sql: str | None
    columns: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...]
    truncated: bool
    tables: tuple[str, ...]
    provenance: tuple[TableProvenance, ...]
    warnings: tuple[str, ...]
    assumptions: tuple[str, ...] = ()
    explanation: str = ""
    confidence: str | None = None
    attempts: int = 0
    model: str | None = None
    cost_usd: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    latency_ms: float = 0.0
    request_id: str | None = None
    errors: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """JSON-ready (values converted with :func:`jsonable`).

        Any: a JSON document of mixed value types.
        """
        return {
            "request_id": self.request_id,
            "question": self.question,
            "status": self.status,
            "answer": self.text,
            "sql": self.sql,
            "columns": list(self.columns),
            "rows": [[jsonable(v) for v in row] for row in self.rows],
            "row_count": len(self.rows),
            "truncated": self.truncated,
            "tables": list(self.tables),
            "provenance": [p.to_dict() for p in self.provenance],
            "warnings": list(self.warnings),
            "assumptions": list(self.assumptions),
            "explanation": self.explanation,
            "confidence": self.confidence,
            "attempts": self.attempts,
            "errors": list(self.errors),
            "model": self.model,
            "usage": {
                "input_tokens": self.input_tokens,
                "output_tokens": self.output_tokens,
                "cost_usd": round(self.cost_usd, 6),
            },
            "latency_ms": round(self.latency_ms, 1),
        }


def format_value(value: object) -> str:
    plain = jsonable(value)
    if plain is None:
        return "NULL"
    if isinstance(plain, bool):
        return "true" if plain else "false"
    if isinstance(plain, int):
        return f"{plain:,}"
    if isinstance(plain, float):
        return f"{plain:,.6g}" if abs(plain) < 1e15 else str(plain)
    return str(plain)


def result_text(columns: Sequence[str], rows: Sequence[Sequence[object]], truncated: bool) -> str:
    """The rows in words: a scalar as one line, a few rows inline, more rows counted."""
    if not rows:
        return "No rows matched."
    if len(rows) == 1 and len(columns) == 1:
        return f"{columns[0]}: {format_value(rows[0][0])}."
    shown = [
        ", ".join(f"{c} {format_value(v)}" for c, v in zip(columns, row, strict=True))
        for row in rows[:MAX_TEXT_ROWS]
    ]
    count = f"{len(rows):,}{'+' if truncated else ''} rows"
    more = f"; first {MAX_TEXT_ROWS} shown" if len(rows) > MAX_TEXT_ROWS else ""
    return f"{count}{more}: " + "; ".join(shown) + "."


def provenance_warnings(
    provenance: Sequence[TableProvenance], *, stale_after: timedelta, now: datetime | None = None
) -> list[str]:
    when = now or datetime.now(UTC)
    return [w for p in provenance for w in p.warnings(stale_after, when)]


def compose(
    outcome: PlanOutcome,
    provenance: Sequence[TableProvenance],
    *,
    stale_after: timedelta,
    now: datetime | None = None,
    request_id: str | None = None,
) -> Answer:
    """The answer for a plan outcome (pure: the provenance is looked up by the caller)."""
    draft = outcome.draft
    guard = outcome.guard
    result = outcome.result
    warnings = provenance_warnings(provenance, stale_after=stale_after, now=now)
    if result is not None and result.truncated:
        warnings.append(f"result cut at {len(result.rows):,} rows")
    errors = tuple(a.error for a in outcome.attempts if a.error)
    if outcome.status == "answered" and result is not None:
        lead = draft.explanation.strip() if draft and draft.explanation else ""
        text = " ".join(
            p for p in (lead, result_text(result.columns, result.rows, result.truncated)) if p
        )
        if warnings:
            text += " Caution: " + "; ".join(warnings) + "."
    elif outcome.status == "unanswerable":
        why = "; ".join(draft.assumptions) if draft and draft.assumptions else "no reason given"
        text = f"The warehouse cannot answer this: {why}"
    else:
        attempts = len(outcome.attempts)
        text = (
            f"No query that is allowed to run answered this after {attempts} "
            f"attempt{'s' if attempts != 1 else ''}. Last error: {outcome.error or 'unknown'}"
        )
    return Answer(
        question=outcome.question,
        status=outcome.status,
        text=text,
        sql=outcome.sql,
        columns=result.columns if result else (),
        rows=result.rows if result else (),
        truncated=bool(result and result.truncated),
        tables=guard.tables if guard and outcome.status == "answered" else (),
        provenance=tuple(provenance),
        warnings=tuple(warnings),
        assumptions=draft.assumptions if draft else (),
        explanation=draft.explanation if draft else "",
        confidence=draft.confidence if draft else None,
        attempts=len(outcome.attempts),
        model=outcome.model,
        cost_usd=outcome.usage.cost_usd,
        input_tokens=outcome.usage.input_tokens,
        output_tokens=outcome.usage.output_tokens,
        latency_ms=outcome.latency_ms,
        request_id=request_id,
        errors=errors,
    )

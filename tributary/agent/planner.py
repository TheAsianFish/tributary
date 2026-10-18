"""Question → SQL: one structured draft, then up to N repairs fed by the guard and the database.

The model sees a stable system prefix (the rules and the catalog rendered as
DDL with descriptions and indexed values, cached by the provider) and the
question. It replies with an :class:`SQLDraft`, a JSON object whose schema the
provider enforces. The draft goes through the guard and then the executor; a
block or a database error is sent back as the next user turn, with the
previous draft as the assistant turn, so the repair sees exactly what failed.
The loop stops at the first query that runs, when the model says the
question cannot be answered from these tables, or after ``max_repairs``
repairs.

A repair is only offered for errors the model can act on (a guard block, a
PostgreSQL error, a timeout or a cost refusal). An unreachable warehouse or a
privilege the agent's role lacks (``ExecutionError.repairable`` is false)
stops the loop and propagates: no draft fixes a deployment. Provider failures
are not repaired here either: they propagate, because a retry belongs to the
provider's own policy and a refusal should reach the caller as a refusal.
"""

from __future__ import annotations

import json
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Literal

import structlog
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from tributary.agent.catalog import Catalog
from tributary.agent.execute import ExecutionError, Executor, QueryResult
from tributary.agent.guard import Guard, GuardResult
from tributary.agent.provider import (
    InvalidOutput,
    Message,
    ModelRequest,
    Provider,
    Usage,
)

log = structlog.get_logger(__name__)

SCHEMA_NAME = "sql_draft"
# Field order is generation order for models that write the object left to right:
# the interpretation comes before the query, the summary after it.
SQL_DRAFT_SCHEMA: dict[str, object] = {
    "type": "object",
    "properties": {
        "assumptions": {
            "type": "array",
            "items": {"type": "string"},
            "description": "How the question was interpreted, one short sentence each.",
        },
        "answerable": {
            "type": "boolean",
            "description": "False when the tables cannot answer the question; sql is then empty.",
        },
        "sql": {"type": "string", "description": "One PostgreSQL SELECT statement."},
        "tables": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Schema-qualified tables the query reads.",
        },
        "explanation": {
            "type": "string",
            "description": "One sentence saying what the result rows are.",
        },
        "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
    },
    "required": ["assumptions", "answerable", "sql", "tables", "explanation", "confidence"],
    "additionalProperties": False,
}

RULES = """\
You answer questions about a PostgreSQL 17 warehouse of Wikimedia edits by writing \
one SQL query. Rules:
- Write a single SELECT statement (WITH clauses are fine). Never write to the \
database, change settings or call system functions; such queries are rejected.
- Read only the tables listed below and qualify each with its schema, e.g. \
marts.fct_edits.
- Return exactly the columns the question asks for, in the order it names them, \
and nothing else. A question asking "how many" wants one number.
- All timestamps are UTC (timestamptz). Compare them with explicit literals such \
as '2026-10-16 22:00:00+00'.
- When filtering a text column, use one of its listed values exactly as written \
(e.g. German Wikipedia is wiki = 'dewiki').
- Add ORDER BY and LIMIT only when the question asks for a ranking or a number of \
rows. Results are capped at {row_cap} rows.
- Round only when the question asks for it.
- If these tables cannot answer the question, set answerable to false, leave sql \
empty and say why in assumptions.

Tables:

{schema}
"""


class SQLDraft(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    assumptions: tuple[str, ...] = ()
    answerable: bool = True
    sql: str = ""
    tables: tuple[str, ...] = ()
    explanation: str = ""
    confidence: Literal["low", "medium", "high"] = Field(default="medium")


Status = Literal["answered", "unanswerable", "blocked", "failed"]


@dataclass(frozen=True, slots=True)
class Attempt:
    """One draft and what happened to it."""

    draft: SQLDraft | None
    guard: GuardResult | None
    error: str | None


@dataclass(frozen=True, slots=True)
class PlanOutcome:
    question: str
    status: Status
    attempts: tuple[Attempt, ...]
    result: QueryResult | None
    usage: Usage
    latency_ms: float
    model: str
    error: str | None = None
    calls: int = 0
    extra: dict[str, object] = field(default_factory=dict)

    @property
    def draft(self) -> SQLDraft | None:
        last = self.attempts[-1] if self.attempts else None
        return last.draft if last else None

    @property
    def guard(self) -> GuardResult | None:
        last = self.attempts[-1] if self.attempts else None
        return last.guard if last else None

    @property
    def sql(self) -> str | None:
        """The SQL that ran (as the guard rewrote it), when one did."""
        guard = self.guard
        return guard.sql if self.status == "answered" and guard else None


def system_prompt(catalog: Catalog, *, row_cap: int, relations: Sequence[str] | None = None) -> str:
    return RULES.format(row_cap=row_cap, schema=catalog.render(relations))


def repair_message(error: str) -> str:
    return (
        f"That query did not work.\n{error}\n"
        "Write a corrected query for the same question, as the same JSON object."
    )


class Planner:
    def __init__(
        self,
        provider: Provider,
        guard: Guard,
        executor: Executor,
        catalog: Catalog,
        *,
        max_repairs: int = 2,
        row_cap: int = 1000,
        max_tokens: int = 4096,
    ) -> None:
        self.provider = provider
        self.guard = guard
        self.executor = executor
        self.catalog = catalog
        self.max_repairs = max_repairs
        self.system = system_prompt(catalog, row_cap=row_cap)
        self.max_tokens = max_tokens

    def request(self, messages: Sequence[Message]) -> ModelRequest:
        return ModelRequest(
            system=self.system,
            messages=tuple(messages),
            schema=SQL_DRAFT_SCHEMA,
            schema_name=SCHEMA_NAME,
            max_tokens=self.max_tokens,
        )

    def run(self, question: str) -> PlanOutcome:
        started = time.perf_counter()
        messages = [Message("user", f"Question: {question.strip()}")]
        attempts: list[Attempt] = []
        usage = Usage()
        model = self.provider.model
        result: QueryResult | None = None
        status: Status = "failed"
        for n in range(1 + self.max_repairs):
            response = self.provider.complete(self.request(messages))
            usage = usage + response.usage
            model = response.model
            try:
                draft = SQLDraft.model_validate(response.data)
            except ValidationError as exc:
                error = f"The reply did not match the JSON schema: {exc.errors()[0]['msg']}"
                attempts.append(Attempt(None, None, error))
                status = "failed"
                messages += [
                    Message("assistant", response.text),
                    Message("user", repair_message(error)),
                ]
                continue
            if not draft.answerable:
                attempts.append(Attempt(draft, None, None))
                status = "unanswerable"
                break
            guard = self.guard.check(draft.sql)
            if not guard.allowed:
                error = "The query was blocked by the safety policy: " + "; ".join(guard.reasons)
                attempts.append(Attempt(draft, guard, error))
                status = "blocked"
            else:
                try:
                    result = self.executor.run(guard.sql)
                except ExecutionError as exc:
                    if not exc.repairable:
                        # The warehouse is down or the role is misconfigured: no draft
                        # fixes that, and each repair would be a model call for nothing.
                        log.warning(
                            "planner.aborted",
                            kind=exc.kind,
                            attempts=len(attempts) + 1,
                            cost_usd=round(usage.cost_usd, 6),
                        )
                        raise
                    error = exc.feedback()
                    attempts.append(Attempt(draft, guard, error))
                    status = "failed"
                else:
                    attempts.append(Attempt(draft, guard, None))
                    status = "answered"
                    break
            log.info("planner.repair", attempt=n + 1, error=error[:200])
            messages += [
                Message("assistant", response.text),
                Message("user", repair_message(error)),
            ]
        latency = (time.perf_counter() - started) * 1000
        last_error = attempts[-1].error if attempts and status != "answered" else None
        log.info(
            "planner.done",
            status=status,
            attempts=len(attempts),
            cost_usd=round(usage.cost_usd, 6),
            latency_ms=round(latency),
        )
        return PlanOutcome(
            question=question,
            status=status,
            attempts=tuple(attempts),
            result=result,
            usage=usage,
            latency_ms=latency,
            model=model,
            error=last_error,
            calls=len(attempts),
        )


def draft_json(draft: SQLDraft) -> str:
    return json.dumps(draft.model_dump(mode="json"), ensure_ascii=False)


__all__ = [
    "SQL_DRAFT_SCHEMA",
    "Attempt",
    "InvalidOutput",
    "PlanOutcome",
    "Planner",
    "SQLDraft",
    "system_prompt",
]

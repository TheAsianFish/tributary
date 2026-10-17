"""Test doubles for the agent: the committed catalog, a scripted model, a fake database."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from tributary.agent.catalog import Catalog, Table, load_dbt
from tributary.agent.execute import ExecutionError, QueryResult
from tributary.agent.provenance import TableProvenance
from tributary.agent.provider import ModelRequest, ModelResponse, Usage
from tributary.agent.service import Agent, Limits, StaticCatalog
from tributary.config import REPO_ROOT

CATALOG_DIR = REPO_ROOT / "tests" / "fixtures" / "catalog"
NOW = datetime(2026, 10, 17, 12, 0, tzinfo=UTC)
CALL_USAGE = Usage(100, 20, 3000, 0, 0.01)


def catalog() -> Catalog:
    return load_dbt(CATALOG_DIR)


def draft(
    sql: str = "SELECT count(*) AS edits FROM marts.fct_edits",
    *,
    answerable: bool = True,
    assumptions: Sequence[str] = ("all wikis",),
    explanation: str = "The number of edits.",
    confidence: str = "high",
    tables: Sequence[str] = ("marts.fct_edits",),
) -> dict[str, Any]:
    """A reply object as the model would write it (Any: a JSON object)."""
    return {
        "assumptions": list(assumptions),
        "answerable": answerable,
        "sql": sql,
        "tables": list(tables),
        "explanation": explanation,
        "confidence": confidence,
    }


class ScriptedProvider:
    """Returns the queued replies in order and records every request it saw."""

    name = "scripted"
    model = "scripted-model"

    def __init__(
        self,
        replies: Iterable[Mapping[str, Any] | Exception],
        usage: Usage = CALL_USAGE,
    ) -> None:
        self.replies = list(replies)
        self.usage = usage
        self.requests: list[ModelRequest] = []

    def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        if not self.replies:
            raise AssertionError("the scripted provider ran out of replies")
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return ModelResponse(
            data=dict(reply),
            text=json.dumps(reply, sort_keys=True),
            provider=self.name,
            model="scripted-model-v1",
            usage=self.usage,
            latency_ms=5.0,
            stop_reason="end_turn",
        )


def result(
    columns: Sequence[str] = ("edits",),
    rows: Sequence[Sequence[object]] = ((42,),),
    *,
    truncated: bool = False,
) -> QueryResult:
    return QueryResult(tuple(columns), tuple(tuple(r) for r in rows), truncated, 3.0)


class FakeExecutor:
    """Canned results per SQL (or one default), errors for chosen SQL; records what ran."""

    def __init__(
        self,
        results: Mapping[str, QueryResult] | None = None,
        *,
        errors: Mapping[str, ExecutionError] | None = None,
        default: QueryResult | None = None,
    ) -> None:
        self.results = dict(results or {})
        self.errors = dict(errors or {})
        self.default = default if default is not None else result()
        self.ran: list[str] = []

    def run(self, query: str) -> QueryResult:
        self.ran.append(query)
        if query in self.errors:
            raise self.errors[query]
        return self.results.get(query, self.default)


def provenance(
    relation: str = "marts.fct_edits",
    *,
    refreshed: datetime | None = NOW - timedelta(minutes=10),
    stale: bool = False,
    quality: str = "pass",
    failing: tuple[str, ...] = (),
    warning: tuple[str, ...] = (),
) -> TableProvenance:
    return TableProvenance(
        relation=relation,
        last_refreshed_at=refreshed,
        stale=stale,
        quality=quality,  # type: ignore[arg-type]
        checks=3,
        failing=failing,
        warning=warning,
        covers=(relation,),
    )


class FakeProvenance:
    """Chosen provenance per relation (a clean one otherwise); records the lookups."""

    def __init__(self, chosen: Mapping[str, TableProvenance] | None = None) -> None:
        self.chosen = dict(chosen or {})
        self.lookups: list[tuple[str, ...]] = []

    def lookup(self, tables: Sequence[Table]) -> list[TableProvenance]:
        self.lookups.append(tuple(t.relation for t in tables))
        return [self.chosen.get(t.relation, provenance(t.relation)) for t in tables]


def make_agent(
    provider: ScriptedProvider | Callable[[], ScriptedProvider] | None = None,
    executor: FakeExecutor | None = None,
    sources: FakeProvenance | None = None,
    *,
    limits: Limits | None = None,
    the_catalog: Catalog | None = None,
) -> Agent:
    return Agent(
        StaticCatalog(the_catalog or catalog()),
        provider if provider is not None else ScriptedProvider([]),
        executor or FakeExecutor(),
        sources or FakeProvenance(),
        limits or Limits(),
        clock=lambda: NOW,
    )

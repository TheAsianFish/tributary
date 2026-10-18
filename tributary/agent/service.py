"""The agent as one object: ``ask`` a question, run guarded ``sql``, read the catalog.

The API and the CLI both go through :class:`Agent`, so a question asked over
HTTP and one asked in a terminal take the same path. The catalog is re-read
when a new version is published (the ``current`` link is checked on each
call; publishing swaps it atomically), and the guard's allowlist is rebuilt
from it, so a new mart becomes queryable without a restart and a dropped one
stops being queryable.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import structlog

from tributary.agent.answer import Answer, compose, provenance_warnings, result_text
from tributary.agent.catalog import Catalog, load_dbt
from tributary.agent.execute import ExecutionError, Executor, PostgresExecutor
from tributary.agent.guard import Guard
from tributary.agent.planner import Planner, PlanOutcome
from tributary.agent.provenance import PostgresProvenance, ProvenanceSource, TableProvenance
from tributary.agent.provider import Provider, Usage
from tributary.config import Settings
from tributary.warehouse.catalog import current_version

log = structlog.get_logger(__name__)


class CatalogSource:
    """The published catalog, reloaded when ``current`` points at a new version."""

    def __init__(self, catalog_dir: Path, schemas: tuple[str, ...]) -> None:
        self._dir = catalog_dir
        self._schemas = schemas
        self._lock = threading.Lock()
        self._catalog: Catalog | None = None

    def get(self) -> Catalog:
        version = current_version(self._dir)
        with self._lock:
            if self._catalog is None or self._catalog.version != version:
                self._catalog = load_dbt(self._dir, self._schemas)
                log.info("catalog.loaded", version=self._catalog.version)
            return self._catalog


class StaticCatalog:
    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog

    def get(self) -> Catalog:
        return self._catalog


@dataclass(frozen=True, slots=True)
class Limits:
    max_repairs: int = 2
    row_cap: int = 1000
    stale_after: timedelta = timedelta(minutes=120)


class Agent:
    def __init__(
        self,
        catalogs: CatalogSource | StaticCatalog,
        provider: Provider | Callable[[], Provider],
        executor: Executor,
        provenance: ProvenanceSource,
        limits: Limits | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.catalogs = catalogs
        self._provider = provider
        self._provider_lock = threading.Lock()
        self.executor = executor
        self.provenance = provenance
        self.limits = limits or Limits()
        self._clock = clock
        self._planners: dict[str | None, Planner] = {}

    @property
    def provider(self) -> Provider:
        # A factory defers building the provider (and importing its SDK) until a question
        # needs it: /sql and /catalog never do.
        if callable(self._provider) and not hasattr(self._provider, "complete"):
            with self._provider_lock:  # concurrent first requests build one provider
                if callable(self._provider) and not hasattr(self._provider, "complete"):
                    self._provider = self._provider()
        return self._provider  # type: ignore[return-value]

    def catalog(self) -> Catalog:
        return self.catalogs.get()

    def guard(self, catalog: Catalog | None = None) -> Guard:
        return Guard((catalog or self.catalog()).relations(), max_rows=self.limits.row_cap)

    def planner(self) -> Planner:
        catalog = self.catalog()
        planner = self._planners.get(catalog.version)
        if planner is None or planner.catalog is not catalog:
            planner = Planner(
                self.provider,
                self.guard(catalog),
                self.executor,
                catalog,
                max_repairs=self.limits.max_repairs,
                row_cap=self.limits.row_cap,
            )
            self._planners = {catalog.version: planner}
        return planner

    def lookup(self, relations: tuple[str, ...]) -> list[TableProvenance]:
        catalog = self.catalog()
        tables = [catalog.table(r) for r in relations if r in catalog.relations()]
        return self.provenance.lookup(tables)

    def ask(self, question: str, *, request_id: str | None = None) -> Answer:
        outcome = self.planner().run(question)
        relations = outcome.guard.tables if outcome.guard and outcome.status == "answered" else ()
        return compose(
            outcome,
            self.lookup(relations),
            stale_after=self.limits.stale_after,
            now=self._clock(),
            request_id=request_id,
        )

    def sql(self, query: str, *, request_id: str | None = None) -> Answer:
        """Run caller-written SQL through the same guard and limits (no model)."""
        guard = self.guard().check(query)
        if not guard.allowed:
            return _sql_answer(query, "blocked", guard_error(guard.reasons), request_id)
        try:
            result = self.executor.run(guard.sql)
        except ExecutionError as exc:
            if not exc.repairable:
                raise  # the deployment's fault, not the query's: reported as such
            return _sql_answer(query, "failed", exc.feedback(), request_id, sql=guard.sql)
        provenance = self.lookup(guard.tables)
        warnings = provenance_warnings(
            provenance, stale_after=self.limits.stale_after, now=self._clock()
        )
        if result.truncated:
            warnings.append(f"result cut at {len(result.rows):,} rows")
        return Answer(
            question=query,
            status="answered",
            text=result_text(result.columns, result.rows, result.truncated),
            sql=guard.sql,
            columns=result.columns,
            rows=result.rows,
            truncated=result.truncated,
            tables=guard.tables,
            provenance=tuple(provenance),
            warnings=tuple(warnings),
            latency_ms=result.elapsed_ms,
            request_id=request_id,
        )

    def freshness(self) -> list[TableProvenance]:
        catalog = self.catalog()
        return self.provenance.lookup(list(catalog.tables))


def guard_error(reasons: tuple[str, ...]) -> str:
    return "blocked by the safety policy: " + "; ".join(reasons)


def _sql_answer(
    query: str, status: str, error: str, request_id: str | None, *, sql: str | None = None
) -> Answer:
    return Answer(
        question=query,
        status=status,
        text=error,
        sql=sql,
        columns=(),
        rows=(),
        truncated=False,
        tables=(),
        provenance=(),
        warnings=(),
        request_id=request_id,
        errors=(error,),
    )


def build_agent(settings: Settings, provider: Provider | None = None) -> Agent:
    """The agent wired from settings: published catalog, agent role, ops provenance."""
    from tributary.agent.providers import make_provider

    executor = PostgresExecutor(
        settings.agent_conninfo,
        statement_timeout_ms=settings.agent_statement_timeout_ms,
        row_cap=settings.agent_row_cap,
        cost_ceiling=settings.agent_cost_ceiling,
    )
    stale_after = timedelta(minutes=settings.agent_stale_after_minutes)
    return Agent(
        CatalogSource(settings.catalog_dir, settings.agent_schemas),
        provider if provider is not None else (lambda: make_provider(settings)),
        executor,
        PostgresProvenance(settings.agent_conninfo, stale_after=stale_after),
        Limits(
            max_repairs=settings.agent_max_repairs,
            row_cap=settings.agent_row_cap,
            stale_after=stale_after,
        ),
    )


__all__ = [
    "Agent",
    "CatalogSource",
    "Limits",
    "PlanOutcome",
    "StaticCatalog",
    "Usage",
    "build_agent",
]

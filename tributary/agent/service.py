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
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Literal

import structlog

from tributary.agent.agreement import same_rows
from tributary.agent.answer import Answer, compose, provenance_warnings, result_text
from tributary.agent.catalog import Catalog, load_dbt
from tributary.agent.context import value_hints, worked_examples
from tributary.agent.examples import ExamplePool
from tributary.agent.execute import ExecutionError, Executor, PostgresExecutor
from tributary.agent.guard import Guard
from tributary.agent.linker import DEFAULT_K, Link, link_schema
from tributary.agent.planner import Planner, PlanOutcome
from tributary.agent.provenance import PostgresProvenance, ProvenanceSource, TableProvenance
from tributary.agent.provider import Provider, ProviderError, RecordingMissing, Usage
from tributary.agent.values import ValueIndex
from tributary.agent.vote import VotingPlanner
from tributary.config import Settings
from tributary.warehouse.catalog import current_version

log = structlog.get_logger(__name__)

# The second pass's prompt variant: value hints on and this many worked examples.
SECOND_PASS_EXAMPLES = 3


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
    linker: str = "full"
    link_k: int = DEFAULT_K
    # M7's planner features (tributary/agent/context.py, vote.py).
    values: bool = False  # hint the indexed values the question names
    examples: int = 0  # show up to this many worked examples from the dbt docs
    votes: int = 1  # self-consistency draws per question (1: no vote)
    # Ask again with another prompt variant (value hints, SECOND_PASS_EXAMPLES worked
    # examples) and report whether its rows agree; the answer stays the first pass's.
    second_pass: bool = False


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
        _check_second_pass(self.limits)
        self._clock = clock
        self._planners: dict[str | None, Planner | VotingPlanner] = {}
        self._second_planners: dict[str | None, Planner] = {}

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
        return Guard.for_catalog(catalog or self.catalog(), max_rows=self.limits.row_cap)

    def _single_planner(self, catalog: Catalog, *, values: bool, examples: int) -> Planner:
        """One planner on ``catalog`` with these prompt features and the agent's limits."""
        context = []
        if examples:
            pool = ExamplePool.from_catalog(catalog)
            context.append(worked_examples(pool, examples))
        if values:
            context.append(value_hints(ValueIndex.from_catalog(catalog)))
        return Planner(
            self.provider,
            self.guard(catalog),
            self.executor,
            catalog,
            max_repairs=self.limits.max_repairs,
            row_cap=self.limits.row_cap,
            linker=linker_for(self.limits.linker, self.limits.link_k),
            context=context,
        )

    def planner(self) -> Planner | VotingPlanner:
        catalog = self.catalog()
        planner = self._planners.get(catalog.version)
        if planner is None or planner.catalog is not catalog:
            single = self._single_planner(
                catalog, values=self.limits.values, examples=self.limits.examples
            )
            planner = VotingPlanner(single, self.limits.votes) if self.limits.votes > 1 else single
            self._planners = {catalog.version: planner}
        return planner

    def second_planner(self) -> Planner:
        """The second pass's planner: value hints and worked examples, one draw."""
        catalog = self.catalog()
        planner = self._second_planners.get(catalog.version)
        if planner is None or planner.catalog is not catalog:
            planner = self._single_planner(catalog, values=True, examples=SECOND_PASS_EXAMPLES)
            self._second_planners = {catalog.version: planner}
        return planner

    def lookup(self, relations: tuple[str, ...]) -> list[TableProvenance]:
        catalog = self.catalog()
        tables = [catalog.table(r) for r in relations if r in catalog.relations()]
        return self.provenance.lookup(tables)

    def ask(self, question: str, *, request_id: str | None = None) -> Answer:
        """Answer ``question``; with ``Limits.second_pass``, also report agreement.

        The second pass asks the same question with another prompt variant (value
        hints and :data:`SECOND_PASS_EXAMPLES` worked examples, one draw at the
        provider's default temperature) and compares its rows with the first pass's
        by BIRD's set rule (:func:`~tributary.agent.agreement.same_rows`). The answer
        is always the first pass's (rows, SQL, status, text); only ``agreement``,
        the usage and the latency change. ``agreed`` needs both passes answered with
        the same rows; anything else (other rows, a block, a failure, an unanswerable
        or a provider error in either pass) is ``differed``. When the first pass did
        not answer, the second pass is not run: the agreement is ``differed``
        whatever it would return, so the call would buy nothing.
        """
        outcome = self.planner().run(question)
        relations = outcome.guard.tables if outcome.guard and outcome.status == "answered" else ()
        answer = compose(
            outcome,
            self.lookup(relations),
            stale_after=self.limits.stale_after,
            now=self._clock(),
            request_id=request_id,
        )
        if not self.limits.second_pass:
            return answer
        return self._second_pass(question, outcome, answer)

    def _second_pass(self, question: str, first: PlanOutcome, answer: Answer) -> Answer:
        if first.status != "answered" or first.result is None:
            log.info(
                "agent.second_pass",
                agreement="differed",
                first_status=first.status,
                second_status=None,
                second_model_ms=0.0,
            )
            return replace(answer, agreement="differed")
        try:
            second = self.second_planner().run(question)
        except RecordingMissing:
            raise  # a missing recording is a test setup fault, not a disagreement
        except ProviderError as exc:  # the first answer stands; the check could not run
            log.warning(
                "agent.second_pass",
                agreement="differed",
                first_status=first.status,
                second_status="provider_error",
                second_model_ms=0.0,
                error=str(exc),
            )
            return replace(answer, agreement="differed")
        agreed = (
            second.status == "answered"
            and second.result is not None
            and same_rows(first.result.rows, second.result.rows)
        )
        agreement: Literal["agreed", "differed"] = "agreed" if agreed else "differed"
        log.info(
            "agent.second_pass",
            agreement=agreement,
            first_status=first.status,
            second_status=second.status,
            second_model_ms=round(second.model_ms, 1),
            second_cost_usd=second.usage.cost_usd,
        )
        return replace(
            answer,
            agreement=agreement,
            cost_usd=answer.cost_usd + second.usage.cost_usd,
            input_tokens=answer.input_tokens + second.usage.input_tokens,
            output_tokens=answer.output_tokens + second.usage.output_tokens,
            latency_ms=answer.latency_ms + second.latency_ms,
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


def linker_for(
    strategy: str, k: int, values: ValueIndex | None = None
) -> Callable[[str, Catalog], Link] | None:
    """The planner's linker for a strategy; ``None`` for ``full`` (one cached prefix).

    ``values`` is the value index a literal-matching strategy (``core``) uses;
    without it, the catalog's own indexed values. An unavailable strategy is
    refused here, when the agent is built, rather than on the first question.
    """
    if strategy == "full":
        return None
    link_schema("", Catalog(tables=(), source="dbt"), strategy=strategy, k=k)
    return partial(_link, strategy=strategy, k=k, values=values)


def _link(
    question: str, catalog: Catalog, *, strategy: str, k: int, values: ValueIndex | None
) -> Link:
    return link_schema(question, catalog, strategy=strategy, k=k, values=values)


def _check_second_pass(limits: Limits) -> None:
    """Refuse a second pass that would repeat the first prompt, or one combined with a vote."""
    if not limits.second_pass:
        return
    if limits.votes > 1:
        raise ValueError("second_pass cannot be combined with votes > 1: turn one of them off")
    if limits.values and limits.examples == SECOND_PASS_EXAMPLES:
        raise ValueError(
            "second_pass would repeat the first pass's prompt (value hints and "
            f"{SECOND_PASS_EXAMPLES} worked examples): change values or examples, "
            "or turn second_pass off"
        )


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

    linker_for(settings.agent_linker, settings.agent_link_k)  # refuse an unavailable one now
    executor = PostgresExecutor(
        settings.agent_conninfo,
        statement_timeout_ms=settings.agent_statement_timeout_ms,
        row_cap=settings.agent_row_cap,
        cost_ceiling=settings.agent_cost_ceiling,
        max_result_bytes=settings.agent_max_result_bytes,
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
            linker=settings.agent_linker,
            link_k=settings.agent_link_k,
            values=settings.agent_values,
            examples=settings.agent_examples,
            votes=settings.agent_votes,
            second_pass=settings.agent_second_pass,
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

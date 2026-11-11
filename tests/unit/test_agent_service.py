"""The agent object: ask, guarded sql, freshness, catalog reloads and the planner cache."""

from __future__ import annotations

import os
import shutil
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

from tests.unit.agent_doubles import (
    CATALOG_DIR,
    NOW,
    FakeExecutor,
    FakeProvenance,
    ScriptedProvider,
    catalog,
    draft,
    make_agent,
    provenance,
    result,
)
from tributary.agent.catalog import Catalog
from tributary.agent.execute import ExecutionError
from tributary.agent.service import CatalogSource, Limits, StaticCatalog

GOOD = "SELECT count(*) AS edits FROM marts.fct_edits"
GOOD_RAN = "SELECT COUNT(*) AS edits FROM marts.fct_edits LIMIT 1000"


def test_ask_answers_with_provenance() -> None:
    provider = ScriptedProvider([draft(GOOD)])
    sources = FakeProvenance(
        {"marts.fct_edits": provenance(stale=True, refreshed=NOW - timedelta(hours=3))}
    )
    agent = make_agent(provider, FakeExecutor(), sources)

    answer = agent.ask("How many edits?", request_id="r1")

    assert answer.status == "answered"
    assert answer.sql == GOOD_RAN
    assert answer.tables == ("marts.fct_edits",)
    assert sources.lookups == [("marts.fct_edits",)]
    assert answer.request_id == "r1"
    assert answer.warnings == ("marts.fct_edits was last rebuilt 3 h ago (limit 2 h)",)
    assert "Caution:" in answer.text


def test_ask_unanswerable_looks_nothing_up() -> None:
    provider = ScriptedProvider([draft("", answerable=False, assumptions=["no prices"])])
    sources = FakeProvenance()
    executor = FakeExecutor()
    agent = make_agent(provider, executor, sources)

    answer = agent.ask("What was revenue?")

    assert answer.status == "unanswerable"
    assert executor.ran == []
    assert sources.lookups == [()]
    assert answer.provenance == ()


def test_ask_respects_limits() -> None:
    provider = ScriptedProvider([draft("SELECT * FROM raw.recentchange")] * 2)
    agent = make_agent(provider, limits=Limits(max_repairs=1, row_cap=5))

    answer = agent.ask("Everything")

    assert answer.status == "blocked"
    assert answer.attempts == 2
    assert "capped at 5 rows" in provider.requests[0].system


def test_sql_answered() -> None:
    executor = FakeExecutor({GOOD_RAN: result(rows=[(7,)])})
    sources = FakeProvenance({"marts.fct_edits": provenance(quality="unknown")})
    agent = make_agent(executor=executor, sources=sources)

    answer = agent.sql(GOOD, request_id="r2")

    assert answer.status == "answered"
    assert executor.ran == [GOOD_RAN]
    assert answer.sql == GOOD_RAN
    assert answer.text == "edits: 7."
    assert answer.tables == ("marts.fct_edits",)
    assert answer.warnings == ("marts.fct_edits: no quality results recorded",)
    assert answer.model is None
    assert answer.request_id == "r2"


def test_sql_truncated_warns() -> None:
    executor = FakeExecutor(default=result(rows=[(1,), (2,)], truncated=True))
    answer = make_agent(executor=executor).sql(GOOD)
    assert answer.truncated
    assert answer.warnings == ("result cut at 2 rows",)


def test_sql_blocked_runs_nothing() -> None:
    executor = FakeExecutor()
    answer = make_agent(executor=executor).sql("DELETE FROM marts.fct_edits")
    assert answer.status == "blocked"
    assert executor.ran == []
    assert answer.sql is None
    assert answer.text.startswith("blocked by the safety policy: write_or_ddl")
    assert answer.errors == (answer.text,)


def test_sql_failed_keeps_the_rewritten_sql() -> None:
    error = ExecutionError("timeout", "canceling statement", "57014")
    executor = FakeExecutor(errors={GOOD_RAN: error})
    answer = make_agent(executor=executor).sql(GOOD)
    assert answer.status == "failed"
    assert answer.sql == GOOD_RAN
    assert answer.text == error.feedback()


def test_freshness_covers_every_table() -> None:
    sources = FakeProvenance()
    tables = make_agent(sources=sources).freshness()
    assert [t.relation for t in tables] == list(catalog().relations())
    assert sources.lookups == [catalog().relations()]


def test_provider_factory_is_deferred() -> None:
    built: list[ScriptedProvider] = []

    def factory() -> ScriptedProvider:
        provider = ScriptedProvider([draft(GOOD), draft(GOOD)])
        built.append(provider)
        return provider

    agent = make_agent(factory)
    agent.sql(GOOD)
    agent.catalog()
    agent.freshness()
    assert built == []

    agent.ask("How many edits?")
    agent.ask("How many edits?")
    assert len(built) == 1
    assert len(built[0].requests) == 2


class SwitchingCatalog(StaticCatalog):
    def __init__(self, catalog: Catalog) -> None:
        super().__init__(catalog)
        self.current = catalog

    def get(self) -> Catalog:
        return self.current


def test_planner_is_cached_per_catalog_version() -> None:
    first = catalog()
    source = SwitchingCatalog(first)
    agent = make_agent()
    agent.catalogs = source

    planner = agent.planner()
    assert agent.planner() is planner

    narrower = replace(first, tables=first.tables[:1], version="v2")
    source.current = narrower
    rebuilt = agent.planner()
    assert rebuilt is not planner
    assert rebuilt.catalog is narrower
    assert rebuilt.guard.check("SELECT 1 FROM marts.fct_edits").allowed is False
    assert list(agent._planners) == ["v2"]


def test_planner_rebuilt_for_a_new_catalog_object_with_the_same_version() -> None:
    source = SwitchingCatalog(catalog())
    agent = make_agent()
    agent.catalogs = source
    planner = agent.planner()
    source.current = catalog()
    assert agent.planner() is not planner


def publish(root: Path, name: str) -> None:
    version = next(p for p in CATALOG_DIR.iterdir() if p.is_dir())
    shutil.copytree(version, root / name)
    link = root / "current"
    tmp = root / "current.tmp"
    tmp.symlink_to(name)
    os.replace(tmp, link)


def test_catalog_source_reloads_on_publish(tmp_path: Path) -> None:
    source = CatalogSource(tmp_path, ("marts",))
    with pytest.raises(FileNotFoundError):
        source.get()

    publish(tmp_path, "v1")
    first = source.get()
    assert first.version == "v1"
    assert source.get() is first

    publish(tmp_path, "v2")
    second = source.get()
    assert second.version == "v2"
    assert second is not first
    assert second.relations() == first.relations()


def test_ask_raises_without_catalog(tmp_path: Path) -> None:
    agent = make_agent()
    agent.catalogs = CatalogSource(tmp_path, ("marts",))
    with pytest.raises(FileNotFoundError):
        agent.ask("q")


def test_concurrent_first_requests_build_one_provider() -> None:
    import threading
    import time

    built: list[int] = []

    def factory() -> ScriptedProvider:
        time.sleep(0.05)  # widen the race window
        built.append(1)
        return ScriptedProvider([])

    agent = make_agent(factory)
    threads = [threading.Thread(target=lambda: agent.provider) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(built) == 1


def test_sql_deployment_failure_propagates() -> None:
    """A warehouse outage is the server's fault, never reported as a failed query."""
    down = ExecutionError("connection", "cannot connect to the warehouse: connection refused")
    executor = FakeExecutor(errors={GOOD_RAN: down})
    with pytest.raises(ExecutionError) as caught:
        make_agent(executor=executor).sql(GOOD)
    assert caught.value is down


def test_limits_add_value_hints_examples_and_a_vote() -> None:
    from tributary.agent.examples import Example

    cat = replace(
        catalog(),
        examples=(Example("How many edits did dewiki get?", "SELECT 1", "marts.fct_edits"),),
    )
    provider = ScriptedProvider([draft(GOOD), draft(GOOD)])
    agent = make_agent(
        provider,
        the_catalog=cat,
        limits=Limits(values=True, examples=2, votes=3),
    )

    answer = agent.ask("How many edits on dewiki?")

    assert answer.status == "answered"
    # Two draws returned the same rows, so the third was never asked for.
    assert len(provider.requests) == 2
    user = provider.requests[0].messages[0].content
    assert "How many edits did dewiki get?" in user  # the documented example
    assert ".wiki = 'dewiki'" in user  # the value hint, with the column it is in
    assert user.endswith("Question: How many edits on dewiki?")
    assert provider.requests[1].sample == 1


def test_default_limits_send_the_bare_question() -> None:
    provider = ScriptedProvider([draft(GOOD)])
    make_agent(provider).ask("How many edits on dewiki?")
    assert provider.requests[0].messages[0].content == "Question: How many edits on dewiki?"

"""The planner with a schema linker: the prompt shows only the linked tables."""

from __future__ import annotations

import pytest

from tests.unit.agent_doubles import FakeExecutor, ScriptedProvider, draft, result
from tributary.agent.catalog import Catalog, Column, Table
from tributary.agent.guard import Guard
from tributary.agent.linker import Link, link_schema
from tributary.agent.planner import Planner, system_prompt
from tributary.agent.provider import Usage

SQL = "SELECT count(*) AS n FROM s.schools"


def tbl(name: str, marker: str) -> Table:
    return Table("s", name, "", (Column(marker, "text", ""),), (), (), ())


def two_tables() -> Catalog:
    return Catalog(
        (tbl("schools", "school_marker_col"), tbl("orders", "order_marker_col")),
        "information_schema",
    )


def planner(
    catalog: Catalog,
    provider: ScriptedProvider,
    **kwargs: object,
) -> Planner:
    guard = Guard(catalog.relations(), max_rows=1000)
    return Planner(provider, guard, FakeExecutor(default=result()), catalog, **kwargs)  # type: ignore[arg-type]


def only_schools(question: str, catalog: Catalog) -> Link:
    return Link("test", ("s.schools",), {"s.schools": 2.0})


def test_a_linker_limits_the_system_prompt_to_the_linked_tables() -> None:
    catalog = two_tables()
    provider = ScriptedProvider([draft(SQL, tables=("s.schools",))])

    planner(catalog, provider, linker=only_schools).run("How many schools?")

    system = provider.requests[0].system
    assert "school_marker_col" in system
    assert "CREATE TABLE s.schools" in system
    assert "order_marker_col" not in system
    assert "CREATE TABLE s.orders" not in system


def test_the_link_is_on_the_outcome() -> None:
    provider = ScriptedProvider([draft(SQL, tables=("s.schools",))])

    outcome = planner(two_tables(), provider, linker=only_schools).run("How many schools?")

    assert outcome.link == Link("test", ("s.schools",), {"s.schools": 2.0})
    assert outcome.status == "answered"


def test_the_linker_is_called_with_the_question_and_catalog() -> None:
    catalog = two_tables()
    seen: list[tuple[str, Catalog]] = []

    def spy(question: str, cat: Catalog) -> Link:
        seen.append((question, cat))
        return Link("spy", cat.relations())

    provider = ScriptedProvider([draft(SQL)])
    planner(catalog, provider, linker=spy).run("How many schools?")

    assert seen == [("How many schools?", catalog)]


def test_a_real_bm25_linker_follows_the_question() -> None:
    catalog = two_tables()

    def bm25(question: str, cat: Catalog) -> Link:
        return link_schema(question, cat, strategy="bm25", k=1)

    provider = ScriptedProvider([draft(SQL), draft(SQL)])
    subject = planner(catalog, provider, linker=bm25)
    subject.run("school_marker_col please")
    subject.run("order_marker_col please")

    assert "school_marker_col" in provider.requests[0].system
    assert "order_marker_col" not in provider.requests[0].system
    assert "order_marker_col" in provider.requests[1].system
    assert "school_marker_col" not in provider.requests[1].system


def test_repairs_reuse_the_same_linked_system_prompt() -> None:
    provider = ScriptedProvider([draft("SELECT * FROM raw.recentchange"), draft(SQL)])

    outcome = planner(two_tables(), provider, linker=only_schools).run("How many schools?")

    assert outcome.status == "answered"
    assert provider.requests[0].system == provider.requests[1].system
    assert "order_marker_col" not in provider.requests[1].system


def test_without_a_linker_the_prompt_is_the_unchanged_full_prompt() -> None:
    catalog = two_tables()
    provider = ScriptedProvider([draft(SQL)])

    outcome = planner(catalog, provider, row_cap=250).run("How many schools?")

    assert provider.requests[0].system == system_prompt(catalog, row_cap=250)
    assert "school_marker_col" in provider.requests[0].system
    assert "order_marker_col" in provider.requests[0].system
    assert outcome.link is None


def test_rules_replace_the_rules_text() -> None:
    rules = "CUSTOM RULES cap={row_cap}\n{schema}"
    provider = ScriptedProvider([draft(SQL)])

    planner(two_tables(), provider, rules=rules, row_cap=77).run("q")

    system = provider.requests[0].system
    assert system.startswith("CUSTOM RULES cap=77\n")
    assert "You answer questions about" not in system


def test_rules_apply_to_a_linked_prompt_too() -> None:
    rules = "CUSTOM {row_cap}\n{schema}"
    provider = ScriptedProvider([draft(SQL)])

    planner(two_tables(), provider, rules=rules, linker=only_schools).run("q")

    system = provider.requests[0].system
    assert system.startswith("CUSTOM 1000\n")
    assert "order_marker_col" not in system


def test_each_attempt_carries_the_response_usage_and_latency() -> None:
    usage = Usage(10, 5, 0, 0, 0.5)
    provider = ScriptedProvider([draft("SELECT * FROM raw.recentchange"), draft(SQL)], usage)

    outcome = planner(two_tables(), provider).run("q")

    assert len(outcome.attempts) == 2
    for attempt in outcome.attempts:
        assert attempt.usage == usage
        assert attempt.model_ms == 5.0
    assert outcome.usage == usage + usage


def test_the_agent_refuses_an_unavailable_linker_when_built() -> None:
    from tributary.agent.linker import LinkerUnavailable
    from tributary.agent.service import linker_for

    assert linker_for("full", 4) is None
    assert linker_for("bm25", 2) is not None
    with pytest.raises(LinkerUnavailable):
        linker_for("core", 4)

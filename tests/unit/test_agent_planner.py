"""The planner: one structured draft, then repairs fed by the guard and the database."""

from __future__ import annotations

import json

import pytest

from tests.unit.agent_doubles import (
    CALL_USAGE,
    FakeExecutor,
    ScriptedProvider,
    catalog,
    draft,
    result,
)
from tributary.agent.catalog import Catalog
from tributary.agent.execute import ExecutionError
from tributary.agent.guard import Guard
from tributary.agent.planner import (
    SQL_DRAFT_SCHEMA,
    Planner,
    SQLDraft,
    draft_json,
    repair_message,
    system_prompt,
)
from tributary.agent.provider import ProviderRefusal

GOOD = "SELECT count(*) AS edits FROM marts.fct_edits"
GOOD_RAN = "SELECT COUNT(*) AS edits FROM marts.fct_edits LIMIT 1000"
BLOCKED = "SELECT * FROM raw.recentchange"
BROKEN = "SELECT count(*) AS edits FROM marts.fct_edits WHERE nope = 1"
BROKEN_RAN = "SELECT COUNT(*) AS edits FROM marts.fct_edits WHERE nope = 1 LIMIT 1000"
NO_COLUMN = ExecutionError("database", 'column "nope" does not exist', "42703")


@pytest.fixture(scope="module")
def the_catalog() -> Catalog:
    return catalog()


def provider_text(reply: dict[str, object]) -> str:
    """What the scripted provider sent back as the reply text."""
    return json.dumps(reply, sort_keys=True)


def planner(
    the_catalog: Catalog,
    provider: ScriptedProvider,
    executor: FakeExecutor,
    *,
    max_repairs: int = 2,
) -> Planner:
    guard = Guard(the_catalog.relations(), max_rows=1000)
    return Planner(provider, guard, executor, the_catalog, max_repairs=max_repairs)


def test_first_draft_runs(the_catalog: Catalog) -> None:
    provider = ScriptedProvider([draft(GOOD)])
    executor = FakeExecutor({GOOD_RAN: result(rows=[(42,)])})

    outcome = planner(the_catalog, provider, executor).run("How many edits?")

    assert outcome.status == "answered"
    assert len(provider.requests) == 1
    assert outcome.calls == 1
    assert len(outcome.attempts) == 1
    assert outcome.result == result(rows=[(42,)])
    assert outcome.error is None
    assert outcome.model == "scripted-model-v1"
    assert outcome.sql == GOOD_RAN


def test_the_executor_gets_the_guard_rewritten_sql(the_catalog: Catalog) -> None:
    provider = ScriptedProvider([draft("SELECT count(*) AS edits FROM fct_edits")])
    executor = FakeExecutor()

    outcome = planner(the_catalog, provider, executor).run("How many edits?")

    assert executor.ran == [GOOD_RAN]
    assert outcome.sql == GOOD_RAN
    assert outcome.draft is not None
    assert outcome.draft.sql == "SELECT count(*) AS edits FROM fct_edits"


def test_blocked_draft_is_repaired(the_catalog: Catalog) -> None:
    provider = ScriptedProvider([draft(BLOCKED), draft(GOOD)])
    executor = FakeExecutor()

    outcome = planner(the_catalog, provider, executor).run("How many edits?")

    assert outcome.status == "answered"
    assert len(provider.requests) == 2
    assert executor.ran == [GOOD_RAN]
    first = outcome.attempts[0]
    assert first.guard is not None
    assert not first.guard.allowed
    assert first.error is not None
    repair = provider.requests[1].messages
    assert [m.role for m in repair] == ["user", "assistant", "user"]
    assert repair[1].content == provider_text(draft(BLOCKED))
    assert "blocked by the safety policy" in repair[2].content
    assert "raw.recentchange is not on the allowlist" in repair[2].content


def test_database_error_is_repaired(the_catalog: Catalog) -> None:
    provider = ScriptedProvider([draft(BROKEN), draft(GOOD)])
    executor = FakeExecutor(errors={BROKEN_RAN: NO_COLUMN})

    outcome = planner(the_catalog, provider, executor).run("How many edits?")

    assert outcome.status == "answered"
    assert executor.ran == [BROKEN_RAN, GOOD_RAN]
    repair = provider.requests[1].messages
    assert repair[1].content == provider_text(draft(BROKEN))
    assert repair[2].content == repair_message(NO_COLUMN.feedback())
    assert 'PostgreSQL error (SQLSTATE 42703): column "nope" does not exist' in repair[2].content


def test_three_database_failures_fail(the_catalog: Catalog) -> None:
    provider = ScriptedProvider([draft(BROKEN)] * 3)
    executor = FakeExecutor(errors={BROKEN_RAN: NO_COLUMN})

    outcome = planner(the_catalog, provider, executor, max_repairs=2).run("How many edits?")

    assert outcome.status == "failed"
    assert len(outcome.attempts) == 3
    assert len(provider.requests) == 3
    assert outcome.result is None
    assert outcome.sql is None
    assert outcome.error == NO_COLUMN.feedback()


def test_last_failure_by_the_guard_is_blocked(the_catalog: Catalog) -> None:
    provider = ScriptedProvider(
        [draft(BROKEN), draft(BROKEN), draft("DELETE FROM marts.fct_edits")]
    )
    executor = FakeExecutor(errors={BROKEN_RAN: NO_COLUMN})

    outcome = planner(the_catalog, provider, executor, max_repairs=2).run("How many edits?")

    assert outcome.status == "blocked"
    assert len(outcome.attempts) == 3
    assert outcome.error is not None
    assert outcome.error.startswith("The query was blocked by the safety policy: write_or_ddl")
    assert outcome.sql is None
    assert executor.ran == [BROKEN_RAN, BROKEN_RAN]


def test_unanswerable_runs_nothing(the_catalog: Catalog) -> None:
    reply = draft("", answerable=False, assumptions=["there is no revenue table"])
    provider = ScriptedProvider([reply])
    executor = FakeExecutor()

    outcome = planner(the_catalog, provider, executor).run("What was revenue?")

    assert outcome.status == "unanswerable"
    assert executor.ran == []
    assert len(provider.requests) == 1
    assert outcome.guard is None
    assert outcome.sql is None
    assert outcome.draft is not None
    assert outcome.draft.assumptions == ("there is no revenue table",)


@pytest.mark.parametrize(
    "reply",
    [
        {**draft(GOOD), "confidence": "certain"},
        {**draft(GOOD), "extra": 1},
        {"sql": GOOD, "answerable": "perhaps"},
    ],
)
def test_schema_invalid_reply_is_repaired(the_catalog: Catalog, reply: dict[str, object]) -> None:
    provider = ScriptedProvider([reply, draft(GOOD)])
    executor = FakeExecutor()

    outcome = planner(the_catalog, provider, executor).run("How many edits?")

    assert outcome.status == "answered"
    assert outcome.attempts[0].draft is None
    assert outcome.attempts[0].error is not None
    assert "did not match the JSON schema" in outcome.attempts[0].error
    repair = provider.requests[1].messages
    assert repair[1].content == provider_text(reply)
    assert "did not match the JSON schema" in repair[2].content
    assert executor.ran == [GOOD_RAN]


def test_usage_is_summed_across_calls(the_catalog: Catalog) -> None:
    provider = ScriptedProvider([draft(BLOCKED), draft(BROKEN), draft(GOOD)])
    executor = FakeExecutor(errors={BROKEN_RAN: NO_COLUMN})

    outcome = planner(the_catalog, provider, executor).run("How many edits?")

    assert outcome.status == "answered"
    assert outcome.usage == CALL_USAGE + CALL_USAGE + CALL_USAGE
    assert outcome.usage.input_tokens == 300
    assert outcome.usage.cost_usd == pytest.approx(0.03)


def test_system_prompt_is_stable_and_holds_the_catalog(the_catalog: Catalog) -> None:
    provider = ScriptedProvider([draft(BLOCKED), draft(GOOD), draft(GOOD)])
    p = planner(the_catalog, provider, FakeExecutor())

    p.run("How many edits?")
    p.run("How many edits on dewiki?")

    systems = {r.system for r in provider.requests}
    assert len(systems) == 1
    system = systems.pop()
    assert the_catalog.render() in system
    assert system == system_prompt(the_catalog, row_cap=1000)
    assert "capped at 1000 rows" in system
    assert "How many edits" not in system
    assert provider.requests[0].messages[0].content == "Question: How many edits?"
    assert provider.requests[2].messages[0].content == "Question: How many edits on dewiki?"
    assert all(r.schema == SQL_DRAFT_SCHEMA for r in provider.requests)
    assert all(r.schema_name == "sql_draft" for r in provider.requests)


def test_the_question_is_stripped(the_catalog: Catalog) -> None:
    provider = ScriptedProvider([draft(GOOD)])
    outcome = planner(the_catalog, provider, FakeExecutor()).run("  How many edits?\n")
    assert provider.requests[0].messages[0].content == "Question: How many edits?"
    assert outcome.question == "  How many edits?\n"


def test_zero_repairs_stops_after_one_call(the_catalog: Catalog) -> None:
    provider = ScriptedProvider([draft(BLOCKED)])
    outcome = planner(the_catalog, provider, FakeExecutor(), max_repairs=0).run("q")
    assert outcome.status == "blocked"
    assert len(provider.requests) == 1


def test_provider_failures_propagate(the_catalog: Catalog) -> None:
    provider = ScriptedProvider([ProviderRefusal("no")])
    with pytest.raises(ProviderRefusal):
        planner(the_catalog, provider, FakeExecutor()).run("q")


def test_schema_matches_the_model() -> None:
    properties = SQL_DRAFT_SCHEMA["properties"]
    assert isinstance(properties, dict)
    assert list(properties) == list(SQLDraft.model_fields)
    assert SQL_DRAFT_SCHEMA["required"] == list(SQLDraft.model_fields)
    assert SQL_DRAFT_SCHEMA["additionalProperties"] is False


def test_draft_json_round_trips() -> None:
    d = SQLDraft.model_validate(draft(GOOD))
    assert SQLDraft.model_validate_json(draft_json(d)) == d


def test_a_schema_invalid_last_reply_fails(the_catalog: Catalog) -> None:
    """The last attempt decides the status: a block followed by an unusable reply is a failure."""
    provider = ScriptedProvider([draft("DELETE FROM marts.fct_edits"), {"garbage": 1}])

    outcome = planner(the_catalog, provider, FakeExecutor(), max_repairs=1).run("How many?")

    assert outcome.status == "failed"
    assert outcome.error is not None
    assert outcome.error.startswith("The reply did not match the JSON schema")


@pytest.mark.parametrize("kind", ["connection", "permission"])
def test_a_deployment_failure_is_not_repaired(the_catalog: Catalog, kind: str) -> None:
    """An unreachable warehouse or a missing privilege stops the loop: no draft fixes it."""
    provider = ScriptedProvider([draft(GOOD)] * 3)
    down = ExecutionError(kind, "cannot connect to the warehouse: connection refused")  # type: ignore[arg-type]
    executor = FakeExecutor(errors={GOOD_RAN: down})

    with pytest.raises(ExecutionError) as caught:
        planner(the_catalog, provider, executor, max_repairs=2).run("How many edits?")

    assert caught.value is down
    assert not down.repairable
    assert len(provider.requests) == 1, "no model call was spent on a repair"
    assert executor.ran == [GOOD_RAN]


def test_query_errors_are_repairable() -> None:
    for kind in ("database", "timeout", "too_expensive", "read_only"):
        assert ExecutionError(kind, "x").repairable


def test_an_unusable_reply_fails_the_question_without_a_repair() -> None:
    from tributary.agent.provider import InvalidOutput

    cat = catalog()
    provider = ScriptedProvider([InvalidOutput("aborted: token repeat limit reached")])
    executor = FakeExecutor()
    outcome = Planner(provider, Guard(cat.relations()), executor, cat).run("How many edits?")
    assert outcome.status == "failed"
    assert len(provider.requests) == 1
    assert executor.ran == []
    assert outcome.error is not None
    assert "repeat limit" in outcome.error

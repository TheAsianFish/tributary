"""The opt-in second pass: the answer stays the first pass's, its agreement is reported."""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import replace

import pytest
import structlog
from fastapi.testclient import TestClient
from structlog.testing import LogCapture
from typer.testing import CliRunner

from tests.unit.agent_doubles import (
    FakeExecutor,
    ScriptedProvider,
    catalog,
    draft,
    make_agent,
    result,
)
from tributary.agent.examples import Example
from tributary.agent.provider import ProviderRefusal, RecordingMissing
from tributary.agent.service import Agent, Limits, build_agent
from tributary.api.app import create_app
from tributary.cli import app
from tributary.config import Settings, get_settings

FIRST = "SELECT count(*) AS edits FROM marts.fct_edits"
FIRST_RAN = "SELECT COUNT(*) AS edits FROM marts.fct_edits LIMIT 1000"
SECOND = "SELECT count(rev_id) AS edits FROM marts.fct_edits"
QUESTION = "How many edits on dewiki?"
EXAMPLE = Example("How many edits did dewiki get?", "SELECT 1", "marts.fct_edits")
ON = Limits(second_pass=True)


@pytest.fixture(autouse=True)
def _fresh_settings() -> Iterator[None]:
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def logs() -> LogCapture:
    capture = LogCapture()
    structlog.configure(processors=[structlog.contextvars.merge_contextvars, capture])
    return capture


def agent_with(
    provider: ScriptedProvider,
    executor: FakeExecutor | None = None,
    limits: Limits = ON,
) -> Agent:
    the_catalog = replace(catalog(), examples=(EXAMPLE,))
    return make_agent(provider, executor, limits=limits, the_catalog=the_catalog)


def two_results(first: list[tuple[object, ...]], second: list[tuple[object, ...]]) -> FakeExecutor:
    """The first pass's SQL returns ``first``; anything else (the second's) ``second``."""
    return FakeExecutor({FIRST_RAN: result(rows=first)}, default=result(rows=second))


def test_off_runs_one_planner_and_reports_no_agreement() -> None:
    provider = ScriptedProvider([draft(FIRST)])
    answer = agent_with(provider, limits=Limits()).ask(QUESTION)
    assert answer.status == "answered"
    assert answer.agreement is None
    assert len(provider.requests) == 1
    assert answer.to_dict()["agreement"] is None


def test_same_rows_in_another_order_agree_and_the_answer_is_the_first(logs: LogCapture) -> None:
    provider = ScriptedProvider([draft(FIRST), draft(SECOND, explanation="Second.")])
    executor = two_results([(1,), (2,)], [(2,), (1,), (2,)])

    answer = agent_with(provider, executor).ask(QUESTION)

    assert len(provider.requests) == 2
    assert executor.ran[0] == FIRST_RAN
    assert executor.ran[1] != FIRST_RAN  # the second pass wrote other SQL
    assert answer.agreement == "agreed"
    assert answer.status == "answered"
    assert answer.sql == FIRST_RAN
    assert answer.rows == ((1,), (2,))
    assert answer.explanation == "The number of edits."
    # Both passes are paid for: two calls of CALL_USAGE.
    assert answer.input_tokens == 200
    assert answer.output_tokens == 40
    assert answer.cost_usd == pytest.approx(0.02)
    second = [e for e in logs.entries if e["event"] == "agent.second_pass"]
    assert len(second) == 1
    assert second[0]["agreement"] == "agreed"
    assert second[0]["first_status"] == "answered"
    assert second[0]["second_status"] == "answered"
    assert second[0]["second_model_ms"] == 5.0


def test_other_rows_differ_and_the_answer_is_still_the_first() -> None:
    provider = ScriptedProvider([draft(FIRST), draft(SECOND)])
    executor = two_results([(42,)], [(41,)])
    answer = agent_with(provider, executor).ask(QUESTION)
    assert answer.agreement == "differed"
    assert answer.sql == FIRST_RAN
    assert answer.rows == ((42,),)
    assert answer.text == "The number of edits. edits: 42."


@pytest.mark.parametrize(
    "second",
    [
        [draft("SELECT * FROM raw.recentchange")] * 3,  # blocked after every repair
        [draft("", answerable=False, assumptions=["no such data"])],
        [ProviderRefusal("declined")],
    ],
    ids=["blocked", "unanswerable", "refusal"],
)
def test_a_second_pass_that_does_not_answer_differs(second: list[object]) -> None:
    provider = ScriptedProvider([draft(FIRST), *second])  # type: ignore[list-item]
    answer = agent_with(provider).ask(QUESTION)
    assert answer.agreement == "differed"
    assert answer.status == "answered"
    assert answer.sql == FIRST_RAN
    assert provider.replies == []  # the second pass ran


def test_a_missing_recording_in_the_second_pass_propagates() -> None:
    provider = ScriptedProvider([draft(FIRST), RecordingMissing("no recording")])
    with pytest.raises(RecordingMissing):
        agent_with(provider).ask(QUESTION)


def test_first_pass_not_answered_skips_the_second(logs: LogCapture) -> None:
    provider = ScriptedProvider([draft("", answerable=False, assumptions=["no prices"])])
    answer = agent_with(provider).ask("What was revenue?")
    assert answer.status == "unanswerable"
    assert answer.agreement == "differed"
    assert len(provider.requests) == 1
    second = [e for e in logs.entries if e["event"] == "agent.second_pass"]
    assert [(e["first_status"], e["second_status"]) for e in second] == [("unanswerable", None)]


def test_the_second_pass_adds_value_hints_and_examples() -> None:
    provider = ScriptedProvider([draft(FIRST), draft(FIRST)])
    answer = agent_with(provider).ask(QUESTION)
    assert answer.agreement == "agreed"
    first, second = (r.messages[0].content for r in provider.requests)
    assert first == f"Question: {QUESTION}"
    assert "How many edits did dewiki get?" in second  # the worked example
    assert ".wiki = 'dewiki'" in second  # the value hint
    assert second.endswith(f"Question: {QUESTION}")
    assert provider.requests[1].temperature is None
    assert provider.requests[1].sample == 0


def test_the_second_planner_is_cached() -> None:
    agent = agent_with(ScriptedProvider([]))
    assert agent.second_planner() is agent.second_planner()
    assert agent.second_planner() is not agent.planner()


@pytest.mark.parametrize(
    "limits",
    [
        Limits(second_pass=True, values=True, examples=3),
        Limits(second_pass=True, votes=3),
    ],
    ids=["same-prompt", "vote"],
)
def test_refuses_a_second_pass_it_cannot_run(limits: Limits) -> None:
    with pytest.raises(ValueError, match="second_pass"):
        make_agent(limits=limits)


def test_allows_a_first_pass_with_some_features() -> None:
    make_agent(limits=Limits(second_pass=True, values=True, examples=2))
    make_agent(limits=Limits(second_pass=True, examples=3))


def test_build_agent_reads_the_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRIBUTARY_AGENT_SECOND_PASS", "true")
    agent = build_agent(Settings(_env_file=None), provider=ScriptedProvider([]))
    assert agent.limits.second_pass


def test_api_json_carries_the_agreement() -> None:
    provider = ScriptedProvider([draft(FIRST), draft(SECOND)])
    client = TestClient(create_app(agent_with(provider, two_results([(1,)], [(1,)])), ["k"]))
    body = client.post("/query", json={"question": QUESTION}, headers={"X-API-Key": "k"}).json()
    assert body["agreement"] == "agreed"
    assert body["sql"] == FIRST_RAN
    assert body["usage"]["input_tokens"] == 200


def test_cli_flag_turns_the_second_pass_on(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[bool] = []

    def build(settings: Settings) -> Agent:
        seen.append(settings.agent_second_pass)
        limits = Limits(second_pass=settings.agent_second_pass)
        provider = ScriptedProvider([draft(FIRST), draft(SECOND)])
        return agent_with(provider, two_results([(1,)], [(2,)]), limits)

    monkeypatch.setattr("tributary.agent.service.build_agent", build)

    out = CliRunner().invoke(app, ["ask", QUESTION, "--second-pass"])
    assert out.exit_code == 0, out.output
    assert "agreement: differed" in out.stdout
    assert "treat this answer with more caution" in out.stdout

    out = CliRunner().invoke(app, ["ask", QUESTION, "--json"])
    assert out.exit_code == 0, out.output
    assert json.loads(out.stdout)["agreement"] is None
    assert seen == [True, False]


def test_cli_prints_agreed(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = ScriptedProvider([draft(FIRST), draft(SECOND)])
    agent = agent_with(provider, two_results([(1,)], [(1,)]))
    monkeypatch.setattr("tributary.agent.service.build_agent", lambda settings: agent)
    out = CliRunner().invoke(app, ["ask", QUESTION, "--second-pass"])
    assert out.exit_code == 0, out.output
    assert (
        "agreement: agreed (a second prompt variant returned the same rows)"
        in out.stdout.splitlines()
    )


def test_cli_refuses_a_second_pass_that_repeats_the_first(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRIBUTARY_AGENT_VALUES", "true")
    monkeypatch.setenv("TRIBUTARY_AGENT_EXAMPLES", "3")
    out = CliRunner().invoke(app, ["ask", QUESTION, "--second-pass"])
    assert out.exit_code == 2
    assert "second_pass would repeat" in out.stderr


def test_cli_second_pass_is_local_only() -> None:
    out = CliRunner().invoke(app, ["ask", QUESTION, "--second-pass", "--api", "http://api"])
    assert out.exit_code == 2
    assert "local only" in out.stderr

"""M13's `make eval-agree`: agreement, the tables, the replay check and the committed output."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from eval import agree
from eval.agree import (
    PLAIN,
    SECOND,
    Pass,
    ReplayMismatch,
    agreed,
    check_against_report,
    render_into,
    table_of,
)
from eval.agree_stats import Table
from eval.bird.recorded import ReplayExecutor, load_executions
from eval.warehouse_eval import DATABASE, EXECUTIONS, Graded, Verdict, load_questions
from tributary.agent.provider import ModelRequest, ModelResponse, RecordingMissing
from tributary.agent.providers.fake import FakeProvider
from tributary.agent.service import Agent, Limits, StaticCatalog


def graded(
    rows: list[tuple[object, ...]] | None,
    *,
    status: str = "answered",
    correct: bool = True,
    qid: str = "q",
    sql: str | None = "SELECT 1",
) -> Graded:
    verdict = Verdict(
        id=qid,
        difficulty="simple",
        status=status,
        correct=correct,
        attempts=1,
        sql=sql,
        error=None,
        input_tokens=0,
        output_tokens=0,
        cost_usd=0.0,
        unpriced_calls=0,
        latency_ms=0.0,
        predicted_rows=None if rows is None else len(rows),
        gold_rows=1,
    )
    return Graded(verdict, None if rows is None else tuple(rows), ((1,),))


# --- agreement ----------------------------------------------------------------------------------


def test_same_rows_in_another_order_with_duplicates_agree() -> None:
    assert agreed(graded([(1, "a"), (2, "b")]), graded([(2, "b"), (1, "a"), (1, "a")]))


def test_values_are_compared_with_their_types_as_bird_does() -> None:
    assert not agreed(graded([(Decimal("0.1"),)]), graded([(0.1,)]))


def test_an_unanswered_pass_never_agrees() -> None:
    assert not agreed(graded(None, status="failed"), graded(None, status="failed"))
    assert not agreed(graded([(1,)]), graded(None, status="unanswerable"))
    assert not agreed(graded([(1,)], status="blocked"), graded([(1,)]))


def test_table_of_counts_each_cell() -> None:
    pairs = [(True, True), (True, True), (True, False), (False, True), (False, False)]
    assert table_of(pairs) == Table(2, 1, 1, 1)


# --- the replay check ---------------------------------------------------------------------------


def test_a_replay_that_differs_from_the_recorded_run_is_refused(tmp_path: Path) -> None:
    report = tmp_path / "report.json"
    recorded = {"id": "q", "status": "answered", "correct": True, "attempts": 1, "sql": "SELECT 1"}
    report.write_text(json.dumps({"verdicts": [recorded]}))
    run = Pass("plain", tmp_path / "r.jsonl", report, Limits())
    check_against_report(run, [graded([(1,)])])
    with pytest.raises(ReplayMismatch, match="correct replayed as False"):
        check_against_report(run, [graded([(1,)], correct=False)])
    with pytest.raises(ReplayMismatch, match="sql replayed"):
        check_against_report(run, [graded([(1,)], sql="SELECT 2")])


# --- rendering ----------------------------------------------------------------------------------


def test_render_replaces_only_between_its_markers() -> None:
    text = "a\n<!-- agree:x -->\nold\n<!-- /agree:x -->\nb\n"
    assert render_into(text, "x", "new") == "a\n<!-- agree:x -->\nnew\n<!-- /agree:x -->\nb\n"
    assert render_into(text, "y", "new") == text


# --- the committed runs -------------------------------------------------------------------------


def test_both_passes_replay_offline_exactly_as_recorded() -> None:
    # The fake provider and the recorded executions reproduce both recorded runs verdict for
    # verdict (replay_pass raises otherwise), with no model and no database.
    questions = load_questions()
    catalog = agree.load_catalog()
    executions = load_executions(EXECUTIONS)
    plain = agree.replay_pass(PLAIN, questions, catalog, executions)
    second = agree.replay_pass(SECOND, questions, catalog, executions)
    assert sum(g.verdict.correct for g in plain) == 25
    assert len(second) == 40


def test_the_second_pass_sees_examples_and_values_and_the_plain_pass_does_not() -> None:
    assert PLAIN.limits == Limits()
    assert SECOND.limits.values
    assert SECOND.limits.examples == 3
    assert SECOND.limits.votes == 1
    assert SECOND.limits.max_repairs == PLAIN.limits.max_repairs


def test_bird_split_matches_the_numbers_plan_md_states() -> None:
    bird = agree.bird(load_executions(agree.RECORDINGS / "bird-executions.jsonl.gz"))
    pooled = bird["pooled"]["graded_on"]
    full, vfa = pooled["full"], pooled["full+v+f+a"]
    assert (full["agreed_right"], full["agreed_wrong"]) == (32, 9)
    assert (full["differed_right"], full["differed_right"] + full["differed_wrong"]) == (6, 59)
    assert (vfa["agreed_right"], vfa["differed_right"]) == (32, 18)
    assert len(bird["pooled"]["agreed_wrong"]) == 9


def test_committed_output_is_what_the_command_produces() -> None:
    assert agree.main(["--check"], emit=lambda _: None) == 0


class BothRecordings:
    """The fake provider over both passes' recordings (each request lives in one of them)."""

    name = "fake"
    model = "replay"

    def __init__(self, *paths: Path) -> None:
        self.providers = [FakeProvider(p) for p in paths]

    def complete(self, request: ModelRequest) -> ModelResponse:
        for provider in self.providers[:-1]:
            try:
                return provider.complete(request)
            except RecordingMissing:
                continue
        return self.providers[-1].complete(request)


def test_the_products_second_pass_reports_the_agreement_the_evaluation_measured() -> None:
    # `TRIBUTARY_AGENT_SECOND_PASS` replayed on the recordings: the answer is the plain pass's
    # and its `agreement` is the evaluation's verdict, question by question.
    questions = load_questions()
    catalog = agree.load_catalog()
    executions = load_executions(EXECUTIONS)
    measured = {
        q["id"]: q["agreed"] for q in json.loads(agree.OUT.read_text())["warehouse"]["questions"]
    }
    product = Agent(
        StaticCatalog(catalog),
        BothRecordings(PLAIN.recordings, SECOND.recordings),
        ReplayExecutor(executions, DATABASE),
        agree.NoProvenance(),
        Limits(second_pass=True),
    )
    plain = {g.verdict.id: g for g in agree.replay_pass(PLAIN, questions, catalog, executions)}
    for q in questions:
        answer = product.ask(q.question)
        assert answer.agreement == ("agreed" if measured[q.id] else "differed"), q.id
        assert answer.sql == plain[q.id].verdict.sql

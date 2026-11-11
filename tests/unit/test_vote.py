"""Self-consistency voting: answers grouped by the rows they return, draws sampled, early stop."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from decimal import Decimal
from typing import Any

import pytest

from tests.unit.agent_doubles import FakeExecutor, ScriptedProvider, catalog, draft, result
from tributary.agent.execute import ExecutionError, QueryResult
from tributary.agent.guard import Guard
from tributary.agent.planner import Planner
from tributary.agent.provider import Message, ModelRequest
from tributary.agent.vote import VotingPlanner, result_key, settled, tally

A = "SELECT count(*) AS edits FROM marts.fct_edits"
B = "SELECT count(*) AS edits FROM marts.fct_edits WHERE is_bot"
C = "SELECT count(*) AS edits FROM marts.fct_edits WHERE NOT is_bot"


def test_the_key_ignores_row_order_duplicates_and_number_types() -> None:
    one = result(("a", "b"), ((1, "x"), (2, "y")))
    assert result_key(one) == result_key(result(("a", "b"), ((2, "y"), (1, "x"), (1, "x"))))
    assert result_key(result(("n",), ((2,),))) == result_key(result(("n",), ((Decimal("2.0"),),)))
    assert result_key(result(("n",), ((0.1 + 0.2,),))) == result_key(result(("n",), ((0.3,),)))
    # Different rows, a different width, or a value that differs beyond rounding: not equal.
    assert result_key(one) != result_key(result(("a", "b"), ((1, "x"),)))
    assert result_key(result(("a",), ((1,),))) != result_key(result(("a", "b"), ((1, None),)))
    assert result_key(result(("n",), ((0.3,),))) != result_key(result(("n",), ((0.3001,),)))
    assert result_key(result(("s",), (("1",),))) != result_key(result(("s",), ((None,),)))


def test_the_largest_group_wins_and_a_tie_goes_to_the_earliest() -> None:
    x, y = result(rows=((1,),)), result(rows=((2,),))
    assert tally([x, y, y]) == tally([x, y, y])
    assert tally([x, y, y]).winner == 1
    assert tally([x, y, y]).agreeing == 2
    assert tally([y, x]).winner == 0  # 1-1: the earlier answer
    assert tally([None, x, y]).winner == 1  # a draw without rows casts no vote
    assert tally([None, None]).winner is None
    assert tally([x, None, y, x]).groups == (2, 1)


def test_settled_when_no_remaining_draws_can_overtake() -> None:
    x, y = result(rows=((1,),)), result(rows=((2,),))
    assert settled([x, x], remaining=1)
    assert not settled([x, y], remaining=1)
    assert not settled([x], remaining=2)
    assert settled([x, y, x], remaining=0)
    assert not settled([None], remaining=1)


def _voting(
    replies: list[dict[str, Any]], results: dict[str, QueryResult], n: int = 3
) -> tuple[VotingPlanner, ScriptedProvider, FakeExecutor]:
    cat = catalog()
    provider = ScriptedProvider(replies)
    executor = FakeExecutor(results)
    planner = Planner(provider, Guard(cat.relations()), executor, cat)
    return VotingPlanner(planner, n, temperature=0.5), provider, executor


def _guarded(sql: str) -> str:
    out = Guard(catalog().relations()).check(sql)
    assert out.allowed
    return out.sql


def test_two_agreeing_draws_out_of_three_settle_it_without_a_third_call() -> None:
    same = result(rows=((7,),))
    voting, provider, _ = _voting([draft(A), draft(B)], {_guarded(A): same, _guarded(B): same})
    outcome = voting.run("How many edits?")
    assert len(provider.requests) == 2
    assert outcome.status == "answered"
    assert outcome.extra["draws"] == 2
    assert outcome.extra["agreeing"] == 2
    assert outcome.sql == _guarded(A)  # the earliest draw of the winning group
    assert outcome.calls == 2
    assert len(outcome.samples) == 2
    assert outcome.usage.input_tokens == 2 * provider.usage.input_tokens


def test_the_majority_answer_wins_over_the_first_draw() -> None:
    voting, provider, _ = _voting(
        [draft(A), draft(B), draft(C)],
        {
            _guarded(A): result(rows=((1,),)),
            _guarded(B): result(rows=((2,),)),
            _guarded(C): result(rows=((2,),)),
        },
    )
    outcome = voting.run("How many edits?")
    assert len(provider.requests) == 3
    assert outcome.sql == _guarded(B)
    assert outcome.result is not None
    assert outcome.result.rows == ((2,),)
    assert outcome.extra["agreeing"] == 2


def test_draw_zero_is_the_ordinary_call_and_later_draws_are_sampled() -> None:
    voting, provider, _ = _voting(
        [draft(A), draft(B), draft(C)],
        {
            _guarded(A): result(rows=((1,),)),
            _guarded(B): result(rows=((2,),)),
            _guarded(C): result(rows=((3,),)),
        },
    )
    voting.run("How many edits?")
    first, second, third = provider.requests
    assert (first.sample, first.temperature) == (0, None)
    assert (second.sample, second.temperature) == (1, 0.5)
    assert (third.sample, third.temperature) == (2, 0.5)
    # The same messages, three different recordings.
    assert first.messages == second.messages == third.messages
    assert len({r.fingerprint() for r in provider.requests}) == 3


def test_a_refusing_or_failing_draw_casts_no_vote() -> None:
    error = ExecutionError("database", 'column "x" does not exist', "42703")
    cat = catalog()
    provider = ScriptedProvider(
        [
            draft(answerable=False, sql=""),
            draft(B),
            draft(A),
            draft(A),
            draft(A),
        ]
    )
    executor = FakeExecutor({_guarded(A): result(rows=((5,),))}, errors={_guarded(B): error})
    planner = Planner(provider, Guard(cat.relations()), executor, cat, max_repairs=0)
    outcome = VotingPlanner(planner, 3).run("How many edits?")
    assert outcome.status == "answered"
    assert outcome.sql == _guarded(A)
    assert outcome.extra["draws"] == 3
    assert outcome.extra["agreeing"] == 1


def test_no_votes_at_all_returns_the_first_draw() -> None:
    voting, _, _ = _voting([draft(answerable=False, sql="")] * 3, {})
    outcome = voting.run("Who will win?")
    assert outcome.status == "unanswerable"
    assert outcome.extra["draws"] == 3
    assert outcome.extra["agreeing"] == 0


def test_model_time_counts_every_draw() -> None:
    voting, _, _ = _voting(
        [draft(A), draft(B), draft(C)],
        {
            _guarded(A): result(rows=((1,),)),
            _guarded(B): result(rows=((2,),)),
            _guarded(C): result(rows=((3,),)),
        },
    )
    outcome = voting.run("How many edits?")
    assert outcome.model_ms == pytest.approx(3 * 5.0)


def test_n_must_be_positive() -> None:
    cat = catalog()
    planner = Planner(ScriptedProvider([]), Guard(cat.relations()), FakeExecutor(), cat)
    with pytest.raises(ValueError, match="at least 1"):
        VotingPlanner(planner, 0)


def test_sampling_fields_change_the_fingerprint_only_when_set() -> None:
    base = ModelRequest("s", (Message("user", "q"),), {"type": "object"}, "x")
    # An ordinary call keeps the fingerprint it had before sampling existed, so every
    # committed recording still replays.
    before = {
        "system": "s",
        "messages": [["user", "q"]],
        "schema_name": "x",
        "schema": {"type": "object"},
    }
    canonical = json.dumps(before, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    assert base.fingerprint() == hashlib.sha256(canonical.encode()).hexdigest()
    assert replace(base, sample=1).fingerprint() != base.fingerprint()
    assert replace(base, temperature=0.7).fingerprint() != base.fingerprint()
    assert replace(base, sample=1).fingerprint() != replace(base, sample=2).fingerprint()

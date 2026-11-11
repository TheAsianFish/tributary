"""The BIRD harness's pure parts: variants, intervals, summaries, replay checks, the ledger."""

from __future__ import annotations

import copy
from dataclasses import asdict, replace
from typing import Any

import pytest

from eval.bird import report
from eval.bird.run import Variant, Verdict, compare, estimate_cost, ledger, summarise, wilson


def verdict(**changes: Any) -> Verdict:
    base = Verdict(
        id=1,
        db_id="toxicology",
        difficulty="simple",
        status="answered",
        correct=True,
        correct_r0=True,
        lenient=True,
        strict=True,
        attempts=1,
        sql="SELECT 1",
        error=None,
        input_tokens=1000,
        output_tokens=100,
        input_tokens_r0=1000,
        output_tokens_r0=100,
        cost_usd=0.0,
        cost_usd_r0=0.0,
        model_ms=2000.0,
        model_ms_r0=2000.0,
        predicted_rows=1,
        gold_rows=1,
        linked=("toxicology.atom",),
        reused=False,
    )
    return replace(base, **changes)


@pytest.mark.parametrize(
    ("text", "variant", "name"),
    [
        ("full", Variant("full", None), "full"),
        ("bm25", Variant("bm25", 4), "bm25-k4"),
        ("bm25-k2", Variant("bm25", 2), "bm25-k2"),
        ("core-k3", Variant("core", 3), "core-k3"),
    ],
)
def test_variants_parse_and_name_themselves(text: str, variant: Variant, name: str) -> None:
    assert Variant.parse(text) == variant
    assert variant.name == name


@pytest.mark.parametrize("text", ["vote", "full-k3", "bm25-kx"])
def test_bad_variants_are_refused(text: str) -> None:
    with pytest.raises(ValueError, match="variant"):
        Variant.parse(text)


def test_full_uses_no_linker_so_the_prompt_prefix_is_shared() -> None:
    assert Variant("full").linker_fn() is None
    assert Variant("bm25", 4).linker_fn() is not None


def test_wilson_interval() -> None:
    assert wilson(0, 0) == (0.0, 0.0)
    lo, hi = wilson(25, 50)
    assert (lo, hi) == (0.3664, 0.6336)  # symmetric about 0.5
    lo, hi = wilson(50, 50)
    assert hi == 1.0
    assert 0.92 < lo < 0.93
    lo, hi = wilson(0, 50)
    assert lo == 0.0
    assert 0.07 < hi < 0.08


def test_the_no_repair_view_counts_only_first_attempts() -> None:
    verdicts = [
        verdict(id=1),
        # repaired into a correct answer: right with repairs, wrong without
        verdict(
            id=2,
            difficulty="moderate",
            correct_r0=False,
            attempts=2,
            input_tokens=2500,
            output_tokens=250,
            cost_usd=0.03,
            cost_usd_r0=0.01,
            model_ms=9000.0,
            model_ms_r0=3000.0,
        ),
        verdict(id=3, difficulty="challenging", correct=False, correct_r0=False, status="failed"),
    ]
    r2 = summarise(verdicts, repairs=True)
    r0 = summarise(verdicts, repairs=False)
    assert (r2["correct"], r0["correct"]) == (2, 1)
    assert r2["repaired_into_correct"] == 1
    assert (r2["model_calls"], r0["model_calls"]) == (4, 3)
    assert (r2["input_tokens"], r0["input_tokens"]) == (4500, 3000)
    # The no-repair view prices and times only the first call of each question.
    assert (r2["cost_usd"], r0["cost_usd"]) == (0.03, 0.01)
    assert (r2["model_ms_p95"], r0["model_ms_p95"]) == (9000.0, 3000.0)
    assert r2["by_difficulty"]["moderate"] == {"correct": 1, "total": 1}
    assert list(r2["by_difficulty"]) == ["simple", "moderate", "challenging"]


def test_a_reused_prediction_is_graded_but_not_paid_for_twice() -> None:
    summary = summarise([verdict(id=1), verdict(id=1, reused=True)], repairs=True)
    assert summary["total"] == 2
    assert summary["model_calls"] == 1
    assert summary["input_tokens"] == 1000


def _report(verdicts: list[Verdict]) -> dict[str, Any]:
    return {
        "variants": {
            "full": {
                "official": {
                    "r2": summarise(verdicts, repairs=True),
                    "r0": summarise(verdicts, repairs=False),
                    "verdicts": [asdict(v) for v in verdicts],
                }
            }
        }
    }


def test_compare_finds_every_changed_verdict() -> None:
    recorded = _report([verdict(id=1), verdict(id=2)])
    assert compare(recorded, recorded) == []
    replayed = _report([verdict(id=1, correct=False), verdict(id=3)])
    problems = compare(replayed, recorded)
    assert "full/official/1: correct False != True" in problems
    assert "full/official/2: missing from the replay" in problems


def test_the_ledger_prices_measured_tokens_on_claude() -> None:
    rows = ledger(_report([verdict(input_tokens=1_000_000, input_tokens_r0=1_000_000)]))
    r2 = next(r for r in rows if r["variant"] == "full-r2")
    assert r2["cost_usd"] == 0.0
    # Haiku 5.5: $0.10 per million input, $0.50 per million output tokens.
    assert r2["estimate_usd"]["claude-haiku-5-5"] == estimate_cost(
        1_000_000, 100, "claude-haiku-5-5"
    )
    assert estimate_cost(1_000_000, 1_000_000, "claude-haiku-5-5") == 0.6
    assert estimate_cost(1, 1, "not-a-model") is None


def test_results_tables_replace_only_their_own_block() -> None:
    text = "before\n<!-- bird:link -->\nold\n<!-- /bird:link -->\nafter\n"
    assert report.apply(text, {"link": "new\ntable"}) == (
        "before\n<!-- bird:link -->\nnew\ntable\n<!-- /bird:link -->\nafter\n"
    )
    with pytest.raises(ValueError, match="no <!-- bird:ex -->"):
        report.apply(text, {"ex": "x"})


# --- the harness: one model call per distinct request, one gold run per query -------------------


def _harness_case() -> tuple[Any, Any, list[Any]]:
    from eval.bird.data import BirdQuestion
    from eval.bird.recorded import to_record
    from eval.bird.run import Harness
    from tests.unit.agent_doubles import ScriptedProvider, draft, result
    from tributary.agent.catalog import Catalog, Column, Table
    from tributary.agent.guard import Guard
    from tributary.config import Settings

    table = Table("tox", "atom", "", (Column("element", "text", ""),), (), (), ())
    catalog = Catalog((table,), "information_schema")
    predicted = Guard(catalog.relations(), max_rows=100_000).check("SELECT element FROM tox.atom")
    gold_sql = "SELECT element FROM atom"
    rows = result(("element",), (("cl",), ("c",)))
    executions = {
        r["key"]: r
        for r in (to_record("tox", predicted.sql, rows), to_record("gold/tox", gold_sql, rows))
    }
    provider = ScriptedProvider([draft("SELECT element FROM tox.atom", tables=("tox.atom",))])
    harness = Harness(Settings(), provider, {"tox": catalog}, replay=True, executions=executions)

    def question(track: Any) -> BirdQuestion:
        return BirdQuestion(1, "tox", "Which elements?", "", gold_sql, "simple", track, gold_sql)

    return harness, provider, [question("official"), question("corrected")]


def test_an_identical_request_reaches_the_model_once_and_each_variant_pays_once() -> None:
    harness, provider, (official, corrected) = _harness_case()
    # One table: bm25 with k=4 shows exactly what full shows, so the request is identical.
    full_official = harness.grade(official, Variant("full"))
    full_corrected = harness.grade(corrected, Variant("full"))
    bm25_official = harness.grade(official, Variant("bm25", 4))

    assert len(provider.requests) == 1
    assert (full_official.correct, full_corrected.correct, bm25_official.correct) == (
        True,
        True,
        True,
    )
    assert full_official.reused is False
    assert full_corrected.reused is True  # the second track does not pay again
    assert bm25_official.reused is False  # but another variant counts its own cost
    assert bm25_official.linked == ("tox.atom",)
    assert len(harness._gold_results) == 1


def test_an_empty_marker_block_is_filled() -> None:
    text = "<!-- bird:gold -->\n<!-- /bird:gold -->\n"
    once = report.apply(text, {"gold": "| a |"})
    assert once == "<!-- bird:gold -->\n| a |\n<!-- /bird:gold -->\n"
    assert report.apply(once, {"gold": "| a |"}) == once


def test_compare_finds_a_summary_or_ledger_the_verdicts_do_not_support() -> None:
    """RESULTS.md renders the summaries and the ledger, so a stale one must fail the replay."""
    recorded = _report([verdict(id=1), verdict(id=2)])
    recorded["ledger"] = ledger(recorded)
    tampered = copy.deepcopy(recorded)
    tampered["variants"]["full"]["official"]["r2"]["correct"] = 30
    tampered["ledger"][0]["input_tokens"] = 1
    problems = compare(recorded, tampered)
    assert "full/official/r2: correct 2 != 30" in problems
    assert "ledger differs from the recorded run" in problems


def test_live_estimates_scale_from_the_dev50_run_by_question_count() -> None:
    def run(subset: str, questions: int) -> dict[str, Any]:
        verdicts = [
            verdict(id=i, input_tokens=1000, input_tokens_r0=1000) for i in range(questions)
        ]
        r = _report(verdicts)
        r["subset"] = subset
        r["ledger"] = ledger(r)
        return r

    dev50 = run("dev50", 50)
    # "bird-all-..." sorts before "bird-dev50-...": the basis must not depend on that.
    table = report.ledger_table(
        [("bird-all-x.json", run("all", 500)), ("bird-dev50-x.json", dev50)]
    )
    assert "the token counts `bird-dev50-x.json` measured" in table
    alone = report.ledger_table([("bird-dev50-x.json", dev50)])
    assert alone.split("Estimated cost")[1] == table.split("Estimated cost")[1]
    # dev50 is the basis itself; all 500 is ten times its tokens.
    assert f"${estimate_cost(50_000, 5_000, 'claude-opus-5-5'):.2f} |" in alone
    assert f"${estimate_cost(500_000, 50_000, 'claude-opus-5-5'):.2f} |" in alone


# --- M7: the ablation flags ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "variant"),
    [
        ("full+v", Variant("full", None, values=True)),
        ("full+f", Variant("full", None, examples=3)),
        ("full+f5", Variant("full", None, examples=5)),
        ("full+a", Variant("full", None, answer_all=True)),
        ("bm25-k4+n3", Variant("bm25", 4, votes=3)),
        ("full+v+f+a+n3", Variant("full", None, True, 3, True, 3)),
    ],
)
def test_flags_parse_and_print_in_one_order(text: str, variant: Variant) -> None:
    assert Variant.parse(text) == variant
    assert variant.name == text


def test_flags_written_in_another_order_name_the_same_variant() -> None:
    assert Variant.parse("full+n3+a+f+v").name == "full+v+f+a+n3"


@pytest.mark.parametrize("text", ["full+x", "full+v+v", "full+n1", "full+n", "full+v2", "full+"])
def test_bad_flags_are_refused(text: str) -> None:
    with pytest.raises(ValueError, match="flag"):
        Variant.parse(text)


def test_examples_then_values_then_the_question_in_the_user_turn() -> None:
    from tributary.agent.examples import Example, ExamplePool
    from tributary.agent.values import ValueIndex

    harness, provider, (official, _) = _harness_case()
    harness.value_indexes = lambda: {"tox": ValueIndex({("tox.atom", "element"): ("chlorine",)})}
    harness.example_pools = lambda exclude: {
        "tox": ExamplePool((Example("Which elements are there?", "SELECT 1", "bird:9"),))
    }
    question = replace(official, question="Which elements are like chlorine?")
    harness.grade(question, Variant.parse("full+v+f"))
    (request,) = provider.requests
    user = request.messages[0].content
    examples, values, asked = (
        user.index("Which elements are there?"),
        user.index("'chlorine'"),
        user.index("Question: Which elements are like chlorine?"),
    )
    assert examples < values < asked
    assert "tox.atom.element = 'chlorine'" in user
    # Without the flags the turn is the bare question, as every recording before M7 has it.
    provider.replies.append(_atom_draft())
    harness.grade(question, Variant.parse("bm25-k1"))
    assert (
        provider.requests[-1].messages[0].content == "Question: Which elements are like chlorine?"
    )


def test_the_examples_hold_out_the_evaluated_subset() -> None:
    harness, _, (official, _) = _harness_case()
    seen: list[frozenset[int]] = []

    def pools(exclude: Any) -> dict[str, Any]:
        from tributary.agent.examples import ExamplePool

        seen.append(frozenset(exclude))
        return {"tox": ExamplePool(())}

    harness.example_pools = pools
    harness.example_exclude = frozenset({1, 2})
    harness.grade(official, Variant.parse("full+f"))
    assert seen == [frozenset({1, 2})]


def test_a_vote_has_no_no_repair_row(monkeypatch: pytest.MonkeyPatch) -> None:
    from eval.bird import run as run_module

    harness, provider, (official, _) = _harness_case()
    provider.replies.append(_atom_draft())  # a second draw, the same answer: settled at 2
    from eval.bird import data as bird_data

    monkeypatch.setattr(bird_data, "questions", lambda track: [official])
    monkeypatch.setattr(bird_data, "select", lambda qs, subset: list(qs))
    body = run_module.run(harness, [Variant.parse("full+n3")], ["official"], "all", log=print)
    entry = body["variants"]["full+n3"]["official"]
    assert entry["r0"] is None
    assert entry["r2"]["correct"] == 1
    assert entry["r2"]["model_calls"] == 2  # both draws are paid for
    (verdict_,) = entry["verdicts"]
    assert (verdict_["draws"], verdict_["agreeing"]) == (2, 2)
    assert [row["variant"] for row in run_module.ledger({"variants": body["variants"]})] == [
        "full+n3-r2"
    ]


def test_memo_sends_each_distinct_request_once() -> None:
    from eval.bird.run import Memo
    from tests.unit.agent_doubles import ScriptedProvider, draft
    from tributary.agent.provider import Message, ModelRequest

    inner = ScriptedProvider([draft(), draft("SELECT 2")])
    memo = Memo(inner)
    request = ModelRequest("s", (Message("user", "q"),), {"type": "object"}, "x")
    assert memo.complete(request) is memo.complete(request)
    memo.complete(replace(request, sample=1))
    assert len(inner.requests) == 2


def _atom_draft() -> dict[str, Any]:
    from tests.unit.agent_doubles import draft

    return draft("SELECT element FROM tox.atom", tables=("tox.atom",))


def test_a_core_variant_gets_the_databases_value_index(monkeypatch: pytest.MonkeyPatch) -> None:
    from tributary.agent import linker
    from tributary.agent.linker import Link
    from tributary.agent.values import ValueIndex

    seen: list[object] = []

    def stub(question: str, catalog: Any, k: int | None, values: Any) -> Link:
        seen.append(values)
        return Link("core", catalog.relations())

    monkeypatch.setitem(linker.LINKERS, "core", stub)
    harness, _, (official, _) = _harness_case()
    index = ValueIndex({("tox.atom", "element"): ("chlorine",)})
    harness.value_indexes = lambda: {"tox": index}
    harness.grade(official, Variant.parse("core-k2"))
    assert index in seen
    # Other linkers are not handed the index.
    seen.clear()
    monkeypatch.setitem(linker.LINKERS, "bm25", stub)
    harness.planner("tox", Variant.parse("bm25-k3")).system_for("q")
    assert seen  # (linker_for also probes the strategy once when the planner is built)
    assert all(v is None for v in seen)

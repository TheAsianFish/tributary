"""Paired comparisons between variants, pooled over subsets, and M10's decision rule."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from eval.bird import paired, report
from eval.bird.paired import Pairing, PairingError, accuracy, decide, pair


def run_report(subset: str, variants: dict[str, dict[str, list[bool]]], first_id: int = 0) -> Any:
    """A minimal run report: per variant and track, one verdict per question, in id order."""
    out: dict[str, Any] = {"subset": subset, "model": "m", "provider": "ollama", "variants": {}}
    for name, tracks in variants.items():
        out["variants"][name] = {
            track: {
                "verdicts": [{"id": first_id + i, "correct": ok} for i, ok in enumerate(correct)]
            }
            for track, correct in tracks.items()
        }
    return out


T, F = True, False


def test_pair_counts_only_the_discordant_questions() -> None:
    r = run_report(
        "dev50", {"full": {"official": [T, T, F, F, T]}, "x": {"official": [T, F, T, T, F]}}
    )

    p = pair([r], "full", "x", "official")

    assert (p.n, p.baseline_correct, p.other_correct) == (5, 3, 3)
    assert (p.gained, p.lost, p.net) == (2, 2, 0)
    assert p.p == 1.0


def test_pooling_adds_the_discordant_counts_of_each_subset() -> None:
    a = run_report("dev50", {"full": {"official": [F, F, T]}, "x": {"official": [T, T, T]}})
    b = run_report(
        "holdout50", {"full": {"official": [F, T]}, "x": {"official": [T, T]}}, first_id=100
    )

    alone = [pair([r], "full", "x", "official") for r in (a, b)]
    pooled = pair([a, b], "full", "x", "official")

    assert pooled.n == sum(p.n for p in alone) == 5
    assert pooled.gained == sum(p.gained for p in alone) == 3
    assert pooled.lost == 0
    assert pooled.subsets == ("dev50", "holdout50")
    assert pooled.p == pytest.approx(2 / 8)


def test_a_question_in_two_subsets_is_refused() -> None:
    a = run_report("dev50", {"full": {"official": [T]}, "x": {"official": [T]}})
    b = run_report("holdout50", {"full": {"official": [T]}, "x": {"official": [T]}})

    with pytest.raises(PairingError, match="question 0 is in two subsets"):
        pair([a, b], "full", "x", "official")


def test_the_same_subset_twice_is_refused() -> None:
    a = run_report("dev50", {"full": {"official": [T]}, "x": {"official": [T]}})
    with pytest.raises(PairingError, match="appears twice"):
        pair([a, a], "full", "x", "official")


def test_variants_graded_on_different_questions_are_not_paired() -> None:
    r = run_report("dev50", {"full": {"official": [T, T]}, "x": {"official": [T]}})
    with pytest.raises(PairingError, match="different official questions"):
        pair([r], "full", "x", "official")


def test_a_missing_variant_says_which_run_lacks_it() -> None:
    r = run_report("holdout50", {"full": {"official": [T]}})
    with pytest.raises(PairingError, match="holdout50 run has no x on the official track"):
        pair([r], "full", "x", "official")


def test_pairing_is_antisymmetric() -> None:
    r = run_report("dev50", {"a": {"official": [T, F, F, T]}, "b": {"official": [F, T, T, T]}})
    ab, ba = pair([r], "a", "b", "official"), pair([r], "b", "a", "official")
    assert (ab.gained, ab.lost) == (ba.lost, ba.gained)
    assert ab.p == ba.p


def test_accuracy_pools_correct_and_total() -> None:
    a = run_report("dev50", {"x": {"official": [T, F]}})
    b = run_report("holdout50", {"x": {"official": [T, T, F]}}, first_id=10)
    assert accuracy([a, b], "x", "official") == (3, 5)


# --- the decision rule ------------------------------------------------------------------------


def holdout(candidate: dict[str, list[bool]], challenger: dict[str, list[bool]]) -> Any:
    return run_report("holdout50", {paired.CANDIDATE: candidate, paired.CHALLENGER: challenger})


def test_the_candidate_stays_when_level_on_both_tracks() -> None:
    d = decide(
        holdout(
            {"official": [T, F, T], "corrected": [T, F]},
            {"official": [F, T, T], "corrected": [T, F]},
        )
    )
    assert d.kept
    assert d.default == "full+v+f+a"
    assert d.by_track == (("official", 2, 2), ("corrected", 1, 1))


def test_one_question_behind_on_one_track_hands_the_default_to_the_challenger() -> None:
    d = decide(
        holdout(
            {"official": [T, T, T], "corrected": [F, F]},
            {"official": [F, F, F], "corrected": [T, F]},
        )
    )
    assert not d.kept
    assert d.default == "full+a"


def test_the_rule_is_decided_on_holdout50_only() -> None:
    dev = run_report(
        "dev50", {paired.CANDIDATE: {"official": [T]}, paired.CHALLENGER: {"official": [T]}}
    )
    with pytest.raises(PairingError, match="decided on holdout50"):
        decide(dev)


def test_the_live_default_is_what_the_rule_picks_on_the_committed_run() -> None:
    from eval.bird.run import DEFAULT_LIVE_VARIANTS

    held = paired.deciding_run([r for _, r in report.run_reports()])
    assert held is not None, "no holdout50 run of the pre-stated model under eval/results/"
    assert f"full,{decide(held).default}" == DEFAULT_LIVE_VARIANTS


# --- rendering --------------------------------------------------------------------------------


def m10_runs() -> list[Any]:
    variants = ("full", "full+a", "full+v+f+a")
    dev = run_report(
        "dev50",
        {v: {"official": [T, F, i > 0], "corrected": [F, i > 1]} for i, v in enumerate(variants)},
    )
    hold = run_report(
        "holdout50",
        {v: {"official": [i > 0, F], "corrected": [T, F]} for i, v in enumerate(variants)},
        first_id=100,
    )
    return [dev, hold]


def test_pairs_table_has_each_subset_and_the_pool() -> None:
    table = report.pairs_table(m10_runs())

    assert "dev50 + holdout50: n" in table
    rows = [
        line for line in table.splitlines() if line.startswith("| `full+a` vs `full` | official")
    ]
    # dev50: 1 -> 2 (+1); holdout50: 0 -> 1 (+1); pooled: 1 -> 3 (+2), p = 0.5.
    assert rows == [
        "| `full+a` vs `full` | official | 3 | 1 → 2 | +1 -0 | 1.000 | 2 | 0 → 1 | +1 -0 "
        "| 1.000 | 5 | 1 → 3 | +2 -0 | 0.500 |"
    ]


def test_pooled_table_counts_both_subsets() -> None:
    table = report.pooled_table(m10_runs())
    assert "| `full` | **1/5** (20.0%" in table
    assert "| `full+v+f+a` | **3/5** (60.0%" in table


def test_decision_text_names_the_default_the_rule_picks() -> None:
    _, hold = m10_runs()
    text = report.decision_text(hold)
    assert "stays `make eval`'s live default (`full,full+v+f+a`)" in text

    hold["variants"]["full+v+f+a"]["corrected"]["verdicts"][0]["correct"] = False
    text = report.decision_text(hold)
    assert "becomes `full,full+a`" in text
    assert "did not replicate" in text


def test_m10_reports_pools_the_same_model_with_dev50_first() -> None:
    dev, hold = m10_runs()
    other = dict(dev, model="other")
    found = report.m10_reports([("b", hold), ("c", other), ("a", dev)], model="m")
    assert found is not None
    held, pooled = found
    assert held is hold
    assert [r["subset"] for r in pooled] == ["dev50", "holdout50"]
    assert report.m10_reports([("a", dev)], model="m") is None


def test_another_model_s_holdout50_run_does_not_take_the_decision() -> None:
    dev, hold = m10_runs()
    # A Claude run of holdout50 sorts first by file name and would flip the default.
    claude = dict(hold, model="claude-haiku-5-5", provider="anthropic")
    claude["variants"] = dict(hold["variants"], **{"full+v+f+a": hold["variants"]["full"]})
    reports = [("bird-holdout50-anthropic.json", claude), ("bird-dev50-ollama.json", dev)]
    found = report.m10_reports([*reports, ("bird-holdout50-ollama.json", hold)], model="m")
    assert found is not None
    assert found[0] is hold
    assert all(r["model"] == "m" for r in found[1])
    assert report.m10_reports(reports, model="m") is None


def test_two_runs_of_the_deciding_model_on_holdout50_are_refused() -> None:
    _, hold = m10_runs()
    with pytest.raises(PairingError, match="2 holdout50 runs"):
        paired.deciding_run([hold, dict(hold)], model="m")


def test_the_cli_prints_each_subset_and_the_pool(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    import json

    for r in m10_runs():
        (tmp_path / f"bird-{r['subset']}-ollama-m.json").write_text(json.dumps(r))

    code = paired.main(["full", "full+a", "--model", "m", "--results", str(tmp_path)])

    out = capsys.readouterr().out
    assert code == 0
    assert "dev50 + holdout50  official  n=5     1 -> 3   +2 -0  p=0.500" in out
    assert paired.main(["full", "nope", "--model", "m", "--results", str(tmp_path)]) == 2


def test_pairing_dataclass_net() -> None:
    p = Pairing("a", "b", "official", ("dev50",), 10, 4, 6, 3, 1)
    assert p.net == 2

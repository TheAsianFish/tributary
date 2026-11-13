"""The ablation table's paired test and the failure taxonomy's guard against stale labels."""

from __future__ import annotations

import copy
import json
import math
from pathlib import Path
from typing import Any

import pytest

from eval.bird import paired, report
from eval.bird.failures import CATEGORY_KEYS, check_labels, failures_table, wrong_answers


def _verdict(qid: int, correct: bool, status: str = "answered") -> dict[str, Any]:
    return {"id": qid, "correct": correct, "status": status}


def _report() -> dict[str, Any]:
    def track(correct: list[bool], refused: int = 0) -> dict[str, Any]:
        verdicts = [_verdict(i, c) for i, c in enumerate(correct)]
        n, k = len(correct), sum(correct)
        summary = {
            "correct": k,
            "total": n,
            "ex": k / n,
            "ex_wilson95": [0.1, 0.9],
            "by_status": {"answered": n - refused, "unanswerable": refused},
            "model_ms_p50": 1500.0,
            "model_ms_p95": 4000.0,
        }
        return {"r2": summary, "r0": dict(summary), "verdicts": verdicts}

    ledger_row = {
        "model_calls": 5,
        "input_tokens": 1000,
        "output_tokens": 100,
        "cost_usd": 0.0,
        "estimate_usd": {"claude-haiku-5-5": 0.01, "claude-opus-5-5": 0.5},
    }
    return {
        "variants": {
            "full+a": {
                "official": track([True, True, True, False]),
                "corrected": track([True, True, False]),
            },
            "full": {
                "official": track([True, False, False, True], refused=2),
                "corrected": track([True, False, False]),
            },
        },
        "ledger": [
            {"variant": "full-r2", **ledger_row},
            {"variant": "full+a-r2", **ledger_row},
        ],
    }


def test_mcnemar_is_the_two_sided_binomial_on_discordant_pairs() -> None:
    assert paired.mcnemar_exact(0, 0) == 1.0
    assert paired.mcnemar_exact(3, 3) == 1.0
    assert paired.mcnemar_exact(5, 0) == pytest.approx(2 / 32)
    assert paired.mcnemar_exact(10, 1) == pytest.approx(2 * 12 / 2**11)
    assert paired.mcnemar_exact(1, 10) == paired.mcnemar_exact(10, 1)
    assert paired.mcnemar_exact(8, 2) == pytest.approx(
        2 * sum(math.comb(10, i) for i in range(3)) / 1024
    )


def test_paired_counts_questions_won_and_lost_against_the_baseline() -> None:
    r = _report()
    base = r["variants"]["full"]["official"]["verdicts"]
    other = r["variants"]["full+a"]["official"]["verdicts"]
    assert report.paired(base, other) == (2, 1)


def test_the_ablation_table_puts_the_baseline_first_and_compares_every_other_row() -> None:
    table = report.ablation_table(_report())
    rows = [line for line in table.splitlines() if line.startswith("| `")]
    assert rows[0].startswith("| `full` |")
    assert rows[1].startswith("| `full+a` |")
    assert "+2 -1 (1.00)" in rows[1]  # official
    assert "+1 -0 (1.00)" in rows[1]  # corrected
    assert "| 2 |" in rows[0]  # refusals
    assert "1.5 s / 4.0 s" in rows[1]


def test_a_vote_row_has_no_no_repair_cell() -> None:
    r = _report()
    for entry in r["variants"]["full+a"].values():
        entry["r0"] = None
    lines = report.ablation_table(r).splitlines()
    row = next(line for line in lines if line.startswith("| `full+a`"))
    assert f"| {report.DASH} |" in row


def _labels(**changes: Any) -> dict[str, Any]:
    labels = {
        "variant": "full",
        "labels": [
            {"track": "official", "id": 1, "category": "refusal", "note": "said no"},
            {"track": "official", "id": 2, "category": "value", "note": "'Brasil'"},
            {"track": "corrected", "id": 1, "category": "join", "note": "no join"},
        ],
    }
    labels.update(changes)
    return labels


def test_labels_must_describe_wrong_answers_of_the_run() -> None:
    r = _report()
    assert check_labels(_labels(), r) == []
    bad = copy.deepcopy(_labels())
    bad["labels"].append({"track": "official", "id": 0, "category": "join", "note": "x"})
    bad["labels"].append({"track": "official", "id": 1, "category": "refusal", "note": "dup"})
    bad["labels"].append({"track": "corrected", "id": 2, "category": "nonsense", "note": "x"})
    bad["labels"].append({"track": "corrected", "id": 2, "category": "join", "note": " "})
    problems = check_labels(bad, r)
    assert any("(0" in p or "0)" in p for p in problems if "not a wrong answer" in p)
    assert any("labelled twice" in p for p in problems)
    assert any("unknown category" in p for p in problems)
    assert any("no note" in p for p in problems)
    with pytest.raises(ValueError, match="do not match"):
        failures_table(bad, r)


def test_the_taxonomy_table_counts_each_category_in_a_fixed_order() -> None:
    table = failures_table(_labels(), _report())
    lines = table.splitlines()
    assert lines[2].startswith("| Refused |")  # the first category in CATEGORIES with hits
    assert "| 1 | 33% | 1 |" in lines[2]
    assert "| Wrong join |" in table
    assert "1 (c)" in table  # a corrected-track answer is marked
    assert lines[-1].startswith("| **Total** | | **3** |")
    assert "refusal" in CATEGORY_KEYS


def test_wrong_answers_lists_both_tracks() -> None:
    wrong = wrong_answers(_report(), "full")
    assert [(w["track"], w["id"]) for w in wrong] == [
        ("official", 1),
        ("official", 2),
        ("corrected", 1),
        ("corrected", 2),
    ]


def test_each_other_variant_gets_a_column_of_the_failures_it_answers() -> None:
    table = failures_table(_labels(), _report())
    header, _, refusal, value, join, total = table.splitlines()
    assert "Right with `full+a`" in header
    # full+a gets official 1 and 2 right, corrected 1 right.
    assert refusal.startswith("| Refused |")
    assert refusal.endswith("| 1 | 1 |")
    assert value.startswith("| Wrong literal |")
    assert value.endswith("| 1 | 2 |")
    assert join.endswith("| 1 | 1 (c) |")
    assert total == "| **Total** | | **3** | 100% | **3** | |"


def test_a_later_run_does_not_take_the_labelled_runs_place(tmp_path: Path) -> None:
    import shutil

    report_module = report
    results = tmp_path / "results"
    shutil.copytree(report_module.RESULTS, results)
    labels = json.loads((results / "bird-failures.json").read_text(encoding="utf-8"))
    labelled = json.loads((results / labels["report"]).read_text(encoding="utf-8"))
    # A Claude run of one variant sorts before the ollama run by name.
    later = {**labelled, "variants": {"full+v+f+a": labelled["variants"]["full+v+f+a"]}}
    (results / "bird-dev50-anthropic-claude-haiku-5-5.json").write_text(
        json.dumps(later), encoding="utf-8"
    )
    tables = report_module.render_all(results)
    committed = report_module.render_all(report_module.RESULTS)
    assert tables["ablation"] == committed["ablation"]
    assert tables["failures"] == committed["failures"]

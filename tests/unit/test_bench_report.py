"""RESULTS.md's ingest tables are exactly what the committed raw files render to."""

from __future__ import annotations

import pytest

from bench import report


def test_results_md_matches_the_committed_bench_results() -> None:
    current = report.RESULTS_MD.read_text(encoding="utf-8")
    assert report.apply(current, report.render_all()) == current, "run `make bench-results`"


def test_apply_replaces_only_the_named_block() -> None:
    text = "a\n<!-- bench:x -->\nold\n<!-- /bench:x -->\nb\n"
    assert report.apply(text, {"x": "new"}) == "a\n<!-- bench:x -->\nnew\n<!-- /bench:x -->\nb\n"


def test_apply_refuses_a_missing_block() -> None:
    with pytest.raises(ValueError, match="bench:y"):
        report.apply("no markers here", {"y": "table"})


def test_the_serial_stage_model_reproduces_each_measured_drain() -> None:
    # The attribution is only meaningful if summing the stage times predicts the full
    # pipeline: within 15% at every P measured.
    for p in report._partitions(report.SCALE):
        stages = report._read(report.SCALE / f"p{p}-stages.json")
        measured = float(stages["drains"]["postgres"]["rows_per_s"])
        predicted = report.attribution(stages).predicted_per_s
        assert abs(predicted - measured) / measured < 0.15, (p, predicted, measured)

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


def test_a_targeted_kill_hits_only_when_the_batch_is_planned_but_not_committed() -> None:
    hit = {"service": "stream", "offsets_after_kill": 18, "commits_after_kill": 17}
    missed = {"service": "stream", "offsets_after_kill": 18, "commits_after_kill": 18}
    assert report.window_hit(hit) is True
    assert report.window_hit(missed) is False
    # Seen in flight before the kill, never read after it: unchecked, not a hit.
    assert report.window_hit({"service": "stream", "in_flight": True}) is None
    assert report.window_hit({"service": "kafka"}) is None


def test_a_written_kill_must_catch_the_batch_whose_write_it_saw() -> None:
    caught = {
        "service": "stream",
        "target": "warehouse-written",
        "batch_id": 132,
        "offsets_after_kill": 132,
        "commits_after_kill": 131,
    }
    # The write of 132 was seen, but the kill landed inside 133: 132 is committed
    # and will not be replayed, so the primary key is never exercised.
    next_batch = {**caught, "offsets_after_kill": 133, "commits_after_kill": 132}
    assert report.window_hit(caught) is True
    assert report.window_hit(next_batch) is False


def test_every_recorded_fault_run_is_exactly_once_with_every_kill_window_hit() -> None:
    for path in report.BENCH.glob("fault-*-rep*.json"):
        doc = report._read(path)
        assert doc["postgres"]["exactly_once"], path.name
        assert doc["iceberg"]["exactly_once"], path.name
        assert doc["iceberg"]["batches_committed_twice"] == [], path.name
        assert all(report.window_hit(f) in (True, None) for f in doc["faults"]), path.name

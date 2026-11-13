"""``bench.late``: the prediction committed before ``make bench-late`` runs."""

from __future__ import annotations

import pytest

from bench.late import EVENTS, FUTURE_SEQ, LATE_SEQS, MALFORMED_SEQ, predict_second_replay


def test_the_second_replay_drops_what_is_at_or_below_the_settled_watermark() -> None:
    # Newest 1000, delay 600: the watermark is 400; 400 itself is late (Spark's <=).
    times = [100, 400, 401, 1000, 399]
    assert predict_second_replay(times, delay_s=600) == {
        "events": 5,
        "watermark": 400,
        "dropped_by_watermark": 3,
        "caught_by_dedup_state": 2,
        "lost": 0,
    }


def test_the_load_marks_one_hundred_late_events_and_two_rejects_apart() -> None:
    assert len(LATE_SEQS) == 100
    assert all(0 <= s < EVENTS for s in LATE_SEQS)
    assert MALFORMED_SEQ not in LATE_SEQS
    assert FUTURE_SEQ not in LATE_SEQS
    assert MALFORMED_SEQ != FUTURE_SEQ


def test_each_pass_line_is_read_from_the_reconcilers_output() -> None:
    from bench.late import pass_lines

    text = "\n".join(
        [
            "WARNING: Using incubator modules",
            '{"pass_id": 1, "records": 10, "session_s": 4.2}',
            '{"event": "reconcile.pass", "pass_id": 1}',
            '{"pass_id": 2, "records": 12, "session_s": 4.2}',
        ]
    )
    assert [p["pass_id"] for p in pass_lines(text)] == [1, 2]


def test_the_load_plan_stamps_late_and_future_events_and_breaks_one() -> None:
    import json
    from pathlib import Path

    from bench.load import LoadPlan, Step, bench_id, load_templates, malformed_value
    from tributary.config import DEFAULT_SAMPLE

    plan = LoadPlan(
        "b",
        "t",
        "p",
        (Step(1, 1),),
        Path("x"),
        poison=(7, 82_800),
        shifts=((3, -1_500), (4, 3_600)),
        malformed=(5,),
    )
    assert plan.stamps() == {3: -1_500, 4: 3_600, 7: 82_800}
    event_id = bench_id("0123abcd-0123-4123-8123", 5)
    with pytest.raises(ValueError, match="Expecting value"):
        json.loads(malformed_value(event_id))
    [template] = load_templates(DEFAULT_SAMPLE)[:1]
    late = json.loads(template.render(event_id, 2_000_000_000.5, -1_500))
    assert late["timestamp"] == 2_000_000_000 - 1_500
    assert late["meta"]["id"] == event_id

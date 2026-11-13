"""``bench.late``: the prediction committed before ``make bench-late`` runs."""

from __future__ import annotations

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

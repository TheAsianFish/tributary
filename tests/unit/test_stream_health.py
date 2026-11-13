"""The stream_loss check, the job's per-batch loss counters and the skew-bound warning."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from structlog.testing import capture_logs

from tributary.config import Settings
from tributary.warehouse.stream_health import (
    TARGETS,
    DroppedBatch,
    PassWindow,
    Thresholds,
    WindowCounts,
    covers,
    evaluate,
    summary,
    uncovered,
)

NOW = datetime(2026, 11, 13, 12, 0, tzinfo=UTC)
WINDOW = timedelta(hours=24)


def counts(**overrides: Any) -> WindowCounts:
    values: dict[str, Any] = {
        "query": "warehouse",
        "batches": 25,
        "input_rows": 50_001,
        "dropped_by_watermark": 0,
        "malformed_rows": 0,
        "future_rows": 0,
        "canary_rows": 0,
    }
    values.update(overrides)
    return WindowCounts(**values)


def test_a_clean_window_passes_with_its_counts() -> None:
    check = evaluate(counts(), Thresholds(), window=WINDOW, now=NOW)
    assert (check.check_name, check.check_type, check.status) == (
        "stream_loss_warehouse",
        "stream_loss",
        "pass",
    )
    assert check.target == "raw.recentchange"
    assert check.failures == 0
    assert check.details["batches"] == 25
    assert check.details["message"] is None


def test_one_row_dropped_by_the_watermark_fails_at_the_default_threshold() -> None:
    check = evaluate(counts(dropped_by_watermark=1), Thresholds(), window=WINDOW, now=NOW)
    assert check.status == "fail"
    assert check.failures == 1
    assert check.details["message"] == "1 rows dropped by the watermark (allowed 0)"


def test_one_rejected_record_in_fifty_thousand_is_under_the_fraction() -> None:
    # The poisoned run after the fix: the poison is rejected, nothing is dropped.
    check = evaluate(counts(future_rows=1), Thresholds(), window=WINDOW, now=NOW)
    assert check.status == "pass"
    assert check.failures == 1
    assert check.details["future_rows"] == 1
    assert check.details["rejected_fraction"] == pytest.approx(1 / 50_001, abs=1e-6)


def test_rejections_over_the_fraction_fail_and_both_reasons_are_reported() -> None:
    check = evaluate(
        counts(input_rows=1_000, malformed_rows=1, future_rows=1, dropped_by_watermark=7),
        Thresholds(max_dropped_rows=5, max_rejected_fraction=0.001),
        window=WINDOW,
        now=NOW,
    )
    assert check.status == "fail"
    assert check.failures == 9
    assert check.details["message"] == (
        "7 rows dropped by the watermark (allowed 5); "
        "2 of 1000 records rejected (0.200%, allowed 0.100%)"
    )


def test_canaries_are_not_rejections() -> None:
    check = evaluate(counts(input_rows=10, canary_rows=10), Thresholds(), window=WINDOW, now=NOW)
    assert check.status == "pass"
    assert check.failures == 0


def test_the_lake_query_targets_the_lake_table() -> None:
    check = evaluate(counts(query="lake"), Thresholds(), window=WINDOW, now=NOW)
    assert (check.check_name, check.target) == ("stream_loss_lake", TARGETS["lake"])


def test_an_empty_window_has_no_rejected_fraction() -> None:
    check = evaluate(counts(input_rows=0, batches=0), Thresholds(), window=WINDOW, now=NOW)
    assert check.status == "pass"
    assert check.details["rejected_fraction"] == 0.0


def test_thresholds_come_from_the_settings() -> None:
    settings = Settings(_env_file=None, stream_max_dropped_rows=3, stream_max_rejected_fraction=0.5)
    assert Thresholds.from_settings(settings) == Thresholds(3, 0.5)
    assert Thresholds.from_settings(Settings(_env_file=None)) == Thresholds(0, 0.001)


def test_summary_flattens_the_details() -> None:
    [line] = summary([evaluate(counts(), Thresholds(), window=WINDOW, now=NOW)])
    assert line["check"] == "stream_loss_warehouse"
    assert line["status"] == "pass"
    assert line["input_rows"] == 50_001


@pytest.mark.parametrize(
    ("env", "bound"),
    [
        ({}, 300),
        ({"watermark_delay": "2 minutes"}, 60),
        ({"max_future_skew_s": 30}, 30),
        ({"max_future_skew_s": 86_400}, 86_400),
    ],
)
def test_the_bound_in_force(env: dict[str, Any], bound: int) -> None:
    assert Settings(_env_file=None, **env).future_skew_s == bound


@pytest.mark.parametrize(("skew", "warned"), [(None, False), (599, False), (600, True)])
def test_a_bound_not_below_the_watermark_delay_warns_at_start(
    skew: int | None, warned: bool
) -> None:
    pytest.importorskip("pyspark")
    from tributary.stream.job import warn_if_skew_unsafe

    with capture_logs() as logs:
        assert warn_if_skew_unsafe(Settings(_env_file=None, max_future_skew_s=skew)) is warned
    assert [e["event"] for e in logs] == (["stream.skew_bound_unsafe"] if warned else [])


def test_kafka_offsets_are_read_from_a_progress_report_and_nothing_else() -> None:
    pytest.importorskip("pyspark")
    from tributary.stream.job import kafka_offsets

    raw = '{"wiki.recentchange":{"2":10,"1":7,"0":12}}'
    assert kafka_offsets(raw) == {"wiki.recentchange": {"0": 12, "1": 7, "2": 10}}
    assert kafka_offsets('{"logOffset":3}') is None  # a file source
    assert kafka_offsets(None) is None
    assert kafka_offsets("{}") is None
    assert kafka_offsets('{"t":{"0":true}}') is None


# ------------------------------------------------- dropped rows a pass has covered

T = "wiki.recentchange"


def batch(start: dict[str, int] | None, end: dict[str, int] | None, n: int = 10) -> DroppedBatch:
    return DroppedBatch(
        "warehouse",
        7,
        n,
        None if start is None else {T: start},
        None if end is None else {T: end},
    )


def window(start: dict[str, int], end: dict[str, int], query: str = "warehouse") -> PassWindow:
    return PassWindow(1, query, {T: start}, {T: end})


def test_a_pass_covers_a_batch_only_when_it_read_every_offset_the_batch_read() -> None:
    b = batch({"0": 100, "1": 50}, {"0": 200, "1": 80})
    assert covers(window({"0": 0, "1": 0}, {"0": 200, "1": 80}), b)
    assert covers(window({"0": 100, "1": 50}, {"0": 900, "1": 900}), b)
    # Stopped one offset short, or started one past the batch's first record.
    assert not covers(window({"0": 0, "1": 0}, {"0": 199, "1": 80}), b)
    assert not covers(window({"0": 101, "1": 0}, {"0": 200, "1": 80}), b)
    # A partition the pass never read, another query's pass, another topic.
    assert not covers(window({"0": 0}, {"0": 200}), b)
    assert not covers(window({"0": 0, "1": 0}, {"0": 200, "1": 80}, query="lake"), b)
    assert not covers(PassWindow(1, "warehouse", {"other": {}}, {"other": {}}), b)


def test_a_partition_the_batch_read_nothing_from_needs_nothing() -> None:
    b = batch({"0": 100, "1": 50}, {"0": 200, "1": 50})
    # An `uncovered` pass reads nothing where no uncovered batch read: [end, end).
    assert covers(window({"0": 100, "1": 80}, {"0": 200, "1": 80}), b)


def test_a_batch_without_offsets_is_never_covered_and_one_without_a_start_starts_at_0() -> None:
    everything = window({"0": 0, "1": 0}, {"0": 10**9, "1": 10**9})
    assert not covers(everything, batch(None, None))  # recorded before M12
    assert covers(everything, batch(None, {"0": 5, "1": 5}))
    assert not covers(window({"0": 1, "1": 0}, {"0": 10**9, "1": 10**9}), batch(None, {"0": 5}))


def test_a_pass_never_covers_a_batch_recorded_after_it_finished() -> None:
    """A recreated topic restarts its offsets: an old pass's window must not vouch for them."""
    from dataclasses import replace

    finished = datetime(2026, 11, 13, 12, tzinfo=UTC)
    old_pass = replace(window({"0": 0, "1": 0}, {"0": 50_000, "1": 50_000}), finished_at=finished)
    b = batch({"0": 100, "1": 50}, {"0": 300, "1": 80})
    assert covers(old_pass, replace(b, reported_at=finished - timedelta(seconds=1)))
    assert not covers(old_pass, replace(b, reported_at=finished + timedelta(seconds=1)))


def test_uncovered_keeps_the_batches_no_pass_covers() -> None:
    first = batch({"0": 0}, {"0": 100})
    second = batch({"0": 100}, {"0": 200})
    assert uncovered([first, second], [window({"0": 0}, {"0": 150})]) == [second]
    assert uncovered([first, second], []) == [first, second]


def test_dropped_rows_a_pass_covered_no_longer_fail_the_check() -> None:
    """A redelivery: every dropped row was a copy of an event that landed."""
    redelivered = counts(dropped_by_watermark=35_554, covered_dropped=35_554, passes=3)
    check = evaluate(redelivered, Thresholds(), window=WINDOW, now=NOW)
    assert check.status == "pass"
    assert check.failures == 0
    assert check.details["covered_dropped"] == 35_554
    assert check.details["uncovered_dropped"] == 0
    assert check.details["reconcile_passes"] == 3


def test_drops_after_the_last_pass_still_fail_and_say_what_was_covered() -> None:
    check = evaluate(
        counts(dropped_by_watermark=12, covered_dropped=10, passes=1),
        Thresholds(),
        window=WINDOW,
        now=NOW,
    )
    assert check.status == "fail"
    assert check.failures == 2
    assert check.details["message"] == (
        "2 rows dropped by the watermark and not covered by a reconciliation pass"
        " (10 covered) (allowed 0)"
    )


def test_an_event_a_pass_left_missing_fails_even_with_every_drop_covered() -> None:
    check = evaluate(
        counts(dropped_by_watermark=100, covered_dropped=100, still_missing=100, passes=1),
        Thresholds(),
        window=WINDOW,
        now=NOW,
    )
    assert check.status == "fail"
    assert check.failures == 100
    assert check.details["message"] == (
        "100 events a reconciliation pass found missing are still not in the table"
    )

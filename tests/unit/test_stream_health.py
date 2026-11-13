"""The stream_loss check, the job's per-batch loss counters and the skew-bound warning."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from structlog.testing import capture_logs

from tributary.config import Settings
from tributary.warehouse.stream_health import (
    TARGETS,
    Thresholds,
    WindowCounts,
    evaluate,
    summary,
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

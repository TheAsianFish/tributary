"""Offline tests of the pure parts of the benchmark package (ids, rules, verify, observe)."""

from __future__ import annotations

import itertools
import json
import statistics
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar

import pytest

from bench.load import (
    bench_id,
    due,
    id_range,
    lateness,
    make_template,
    new_prefix,
    scramble,
)
from bench.observe import (
    LogTail,
    Sample,
    cpu_cores,
    cpu_usec,
    host_cores,
    parse_log_line,
    progress_offsets,
)
from bench.rules import (
    Batch,
    attribute,
    batch_from_progress,
    drain_rate,
    judge_step,
    next_rate,
    slope,
    sustained_rate,
)
from bench.verify import compare, quantile
from tributary.config import DEFAULT_SAMPLE
from tributary.events import UUID_RE
from tributary.ingest.sample import read_sample

PREFIX = "0123abcd-4567-89ab-cdef"


def _event() -> dict[str, Any]:
    return {
        "$schema": "/mediawiki/recentchange/1.0.0",
        "meta": {"id": "x", "dt": "2020-01-01T00:00:00Z", "domain": "en.wikipedia.org"},
        "id": 7,
        "type": "edit",
        "title": 'Say "hi" \\ backé 中文',
        "timestamp": 1_700_000_000,
        "bot": False,
        "length": {"old": 1, "new": 2},
        "tags": ["a", "b"],
        "note": None,
    }


def _batch(
    trigger_at: float, queued: int, rows: int = 0, trigger_ms: float = 1000.0, batch_id: int = 0
) -> Batch:
    return Batch(
        batch_id=batch_id,
        trigger_at=trigger_at,
        queued=queued,
        rows=rows,
        trigger_ms=trigger_ms,
        add_batch_ms=0.0,
        durations={},
    )


def _flat(start: float, end: float, queued: int = 50, every: float = 10.0) -> list[Batch]:
    out: list[Batch] = []
    t = start
    while t <= end:
        out.append(_batch(t, queued))
        t += every
    return out


class TestIds:
    def test_bench_id_is_a_lowercase_uuid(self) -> None:
        assert UUID_RE.match(bench_id(PREFIX, 0))
        assert UUID_RE.match(bench_id(PREFIX, 16**12 - 1))

    def test_bench_id_ends_in_twelve_hex_digits_of_the_scrambled_sequence(self) -> None:
        assert bench_id(PREFIX, 255) == f"{PREFIX}-{scramble(255):012x}"

    def test_scramble_is_a_bijection_that_spreads_neighbours(self) -> None:
        values = [scramble(s) for s in range(50_000)]
        assert len(set(values)) == len(values)
        assert all(0 <= v < 16**12 for v in values)
        # Consecutive sequence numbers are not neighbours in the index.
        gaps = sorted(abs(b - a) for a, b in itertools.pairwise(values))
        assert gaps[len(gaps) // 2] > 16**12 // 8

    @pytest.mark.parametrize("seq", [-1, 16**12])
    def test_bench_id_out_of_range_raises(self, seq: int) -> None:
        with pytest.raises(ValueError, match=r"range|ladder"):
            bench_id(PREFIX, seq)

    def test_new_prefix_shape_and_uniqueness(self) -> None:
        a, b = new_prefix(), new_prefix()
        assert a != b
        assert [len(p) for p in a.split("-")] == [8, 4, 4, 4]
        assert UUID_RE.match(bench_id(a, 1))

    def test_id_range_brackets_every_id_of_the_prefix(self) -> None:
        low, high = id_range(PREFIX)
        assert low == f"{PREFIX}-000000000000"
        assert high == f"{PREFIX}-ffffffffffff"
        for seq in (0, 1, 9, 10, 4095, 123456789, 16**12 - 1):
            assert low <= bench_id(PREFIX, seq) <= high

    def test_id_range_excludes_other_prefixes(self) -> None:
        low, high = id_range(PREFIX)
        below = "0123abcd-4567-89ab-cdee"
        above = "0123abcd-4567-89ab-cdf0"
        assert bench_id(below, 16**12 - 1) < low
        assert bench_id(above, 0) > high


class TestLatenessAndDue:
    def test_lateness_uses_running_maximum(self) -> None:
        assert lateness([10, 8, 12, 11]) == [0, 2, 0, 1]

    def test_lateness_of_sorted_times_is_zero(self) -> None:
        assert lateness([1.0, 2.0, 3.0]) == [0, 0, 0]

    def test_lateness_keeps_the_earlier_maximum(self) -> None:
        assert lateness([100, 50, 60, 70]) == [0, 50, 40, 30]

    def test_lateness_empty(self) -> None:
        assert lateness([]) == []

    def test_due_is_floor_of_elapsed_times_rate(self) -> None:
        assert due(2.5, 10) == 25
        assert due(0.19, 10) == 1

    @pytest.mark.parametrize("elapsed", [0.0, -3.0])
    def test_due_before_start_is_zero(self, elapsed: float) -> None:
        assert due(elapsed, 1000) == 0


class TestTemplate:
    NOW = 1_800_000_000.123456

    def _render(self, event: dict[str, Any], late: int = 5) -> dict[str, Any]:
        raw = make_template(event, late).render(bench_id(PREFIX, 3), self.NOW)
        parsed: dict[str, Any] = json.loads(raw)
        return parsed

    def test_rewrites_id_timestamp_and_dt(self) -> None:
        out = self._render(_event(), late=5)
        assert out["meta"]["id"] == bench_id(PREFIX, 3)
        assert out["timestamp"] == int(self.NOW) - 5
        expected = datetime.fromtimestamp(self.NOW, UTC).strftime("%Y-%m-%dT%H:%M:%S")
        assert out["meta"]["dt"] == expected + ".123Z"

    def test_other_fields_unchanged(self) -> None:
        event = _event()
        out = self._render(event)
        for key in ("meta", "timestamp"):
            event.pop(key)
            out.pop(key, None)
        assert out == event

    def test_other_meta_fields_unchanged(self) -> None:
        assert self._render(_event())["meta"]["domain"] == "en.wikipedia.org"

    def test_quotes_backslashes_and_unicode_round_trip(self) -> None:
        event = _event()
        assert self._render(event)["title"] == event["title"]

    def test_render_is_compact_utf8_bytes(self) -> None:
        raw = make_template(_event(), 0).render(bench_id(PREFIX, 0), self.NOW)
        assert isinstance(raw, bytes)
        assert b", " not in raw
        assert b": " not in raw
        assert "中文".encode() in raw

    def test_template_is_reusable_with_different_ids_and_times(self) -> None:
        tpl = make_template(_event(), 2)
        a = json.loads(tpl.render(bench_id(PREFIX, 1), 1_800_000_000.0))
        b = json.loads(tpl.render(bench_id(PREFIX, 2), 1_800_000_100.9))
        assert a["meta"]["id"] != b["meta"]["id"]
        assert (a["timestamp"], b["timestamp"]) == (1_800_000_000 - 2, 1_800_000_100 - 2)

    def test_holes_are_in_document_order(self) -> None:
        tpl = make_template(_event(), 0)
        assert sorted(tpl.holes) == ["dt", "id", "ts"]
        assert len(tpl.pieces) == len(tpl.holes) + 1

    def test_does_not_mutate_the_event(self) -> None:
        event = _event()
        make_template(event, 0)
        assert event == _event()

    def test_real_sample_events_round_trip(self) -> None:
        for event in read_sample(DEFAULT_SAMPLE, limit=3):
            out = self._render(dict(event), late=7)
            assert out["meta"]["id"] == bench_id(PREFIX, 3)
            assert out["timestamp"] == int(self.NOW) - 7
            for key, value in event.items():
                if key not in ("meta", "timestamp"):
                    assert out[key] == value
            for key, value in event["meta"].items():
                if key not in ("id", "dt"):
                    assert out["meta"][key] == value


class TestSlope:
    def test_exact_slope_on_a_line(self) -> None:
        pts = [(float(x), 3.0 * x + 7.0) for x in range(6)]
        assert slope(pts) == pytest.approx(3.0)

    def test_negative_slope(self) -> None:
        assert slope([(0.0, 10.0), (5.0, 0.0)]) == pytest.approx(-2.0)

    def test_least_squares_of_noisy_points(self) -> None:
        assert slope([(0.0, 0.0), (1.0, 1.0), (2.0, 1.0), (3.0, 3.0)]) == pytest.approx(0.9)

    @pytest.mark.parametrize("pts", [[], [(1.0, 2.0)]])
    def test_fewer_than_two_points_is_none(self, pts: list[tuple[float, float]]) -> None:
        assert slope(pts) is None

    def test_all_equal_x_is_none(self) -> None:
        assert slope([(2.0, 1.0), (2.0, 5.0), (2.0, 9.0)]) is None


class TestJudgeStep:
    def test_flat_queue_is_sustained(self) -> None:
        v = judge_step(_flat(40, 100), rate=100, offered=6000, step_end=100, seconds=60)
        assert v.sustained
        assert v.reason == "kept up"
        assert v.queue_slope == pytest.approx(0.0)
        assert v.batches == 7
        assert v.offered_ratio == pytest.approx(1.0)

    def test_queue_growing_faster_than_five_percent_fails(self) -> None:
        batches = [_batch(40 + 10 * i, 51 * i) for i in range(7)]
        v = judge_step(batches, rate=100, offered=6000, step_end=100, seconds=60)
        assert not v.sustained
        assert v.reason == "queue growing"
        assert v.queue_slope == pytest.approx(5.1)

    def test_queue_growing_slower_than_five_percent_passes(self) -> None:
        batches = [_batch(40 + 10 * i, 49 * i) for i in range(7)]
        v = judge_step(batches, rate=100, offered=6000, step_end=100, seconds=60)
        assert v.sustained
        assert v.queue_slope == pytest.approx(4.9)

    def test_shrinking_queue_is_sustained(self) -> None:
        batches = [_batch(40 + 10 * i, 1000 - 100 * i) for i in range(7)]
        assert judge_step(batches, 100, 6000, 100, 60).sustained

    def test_under_offered_is_not_offered_even_with_flat_queue(self) -> None:
        v = judge_step(_flat(40, 100), rate=100, offered=5879, step_end=100, seconds=60)
        assert not v.sustained
        assert v.reason == "not offered"
        assert v.offered_ratio == pytest.approx(5879 / 6000)

    def test_offered_at_threshold_is_accepted(self) -> None:
        v = judge_step(_flat(40, 100), rate=100, offered=5880, step_end=100, seconds=60)
        assert v.reason == "kept up"

    def test_too_few_batches(self) -> None:
        v = judge_step(_flat(80, 100), rate=100, offered=6000, step_end=100, seconds=60)
        assert not v.sustained
        assert v.reason == "too few batches"
        assert v.batches == 3

    def test_min_batches_is_configurable(self) -> None:
        batches = _flat(80, 100)
        assert not judge_step(batches, 100, 6000, 100, 60).sustained
        assert judge_step(batches, 100, 6000, 100, 60, min_batches=3).sustained

    def test_batches_outside_the_window_are_ignored(self) -> None:
        early = [_batch(10.0 * i, 100_000 * i) for i in range(4)]
        late = [_batch(200.0, 0), *_flat(40, 100)]
        v = judge_step([*early, *_flat(40, 100), late[0]], 100, 6000, 100, 60)
        assert v.sustained
        assert v.batches == 7

    def test_window_is_configurable(self) -> None:
        old_growth = [_batch(10.0 * i, 0) for i in range(4)]
        flat = _flat(60, 100, queued=10_000)
        wide = judge_step([*old_growth, *flat], 100, 6000, 100, 60, window=100)
        narrow = judge_step([*old_growth, *flat], 100, 6000, 100, 60, window=40)
        assert wide.reason == "queue growing"
        assert narrow.reason == "kept up"

    def test_zero_rate_is_not_offered(self) -> None:
        v = judge_step(_flat(40, 100), rate=0, offered=100, step_end=100, seconds=60)
        assert v.reason == "not offered"
        assert v.offered_ratio == 0.0


class TestLadder:
    def test_going_up_continues_while_sustained(self) -> None:
        assert next_rate([(100.0, True)], 1.25) == pytest.approx(125.0)
        assert next_rate([(100.0, True), (125.0, True)], 1.25) == pytest.approx(156.25)

    def test_going_up_stops_after_first_failure(self) -> None:
        assert next_rate([(100.0, True), (125.0, False)], 1.25) is None

    def test_first_step_failing_goes_down(self) -> None:
        assert next_rate([(100.0, False)], 2.0) == pytest.approx(50.0)
        assert next_rate([(100.0, False), (50.0, False)], 2.0) == pytest.approx(25.0)

    def test_going_down_stops_at_first_success(self) -> None:
        assert next_rate([(100.0, False), (50.0, True)], 2.0) is None

    def test_empty_history_raises(self) -> None:
        with pytest.raises(ValueError, match=r"range|ladder"):
            next_rate([], 1.25)

    def test_sustained_rate_is_the_highest_success(self) -> None:
        hist = [(100.0, True), (125.0, True), (156.0, False)]
        assert sustained_rate(hist) == 125.0

    def test_sustained_rate_after_stepping_down(self) -> None:
        assert sustained_rate([(100.0, False), (50.0, True)]) == 50.0

    def test_sustained_rate_none_when_nothing_sustained(self) -> None:
        assert sustained_rate([(100.0, False), (50.0, False)]) is None
        assert sustained_rate([]) is None


class TestDrainRate:
    def test_drops_first_and_trailing_partial_batch(self) -> None:
        batches = [
            _batch(0, 0, rows=100),
            _batch(1, 0, rows=1000, trigger_ms=500),
            _batch(2, 0, rows=1000, trigger_ms=500),
            _batch(3, 0, rows=1000, trigger_ms=500),
            _batch(4, 0, rows=400, trigger_ms=500),
        ]
        d = drain_rate(batches, cap=1000)
        assert (d.rows, d.batches) == (3000, 3)
        assert d.seconds == pytest.approx(1.5)
        assert d.rows_per_s == pytest.approx(2000.0)

    def test_trailing_batch_at_ninety_percent_is_kept(self) -> None:
        batches = [_batch(i, 0, rows=r) for i, r in enumerate([5, 1000, 900])]
        assert drain_rate(batches, cap=1000).batches == 2

    def test_empty_row_batches_are_ignored(self) -> None:
        batches = [
            _batch(0, 0, rows=0),
            _batch(1, 0, rows=10),
            _batch(2, 0, rows=1000),
            _batch(3, 0, rows=0),
            _batch(4, 0, rows=1000),
        ]
        d = drain_rate(batches, cap=1000)
        assert (d.rows, d.batches) == (2000, 2)

    def test_single_batch_is_kept(self) -> None:
        d = drain_rate([_batch(0, 0, rows=50)], cap=1000)
        assert (d.rows, d.batches) == (50, 1)

    def test_no_batches_has_no_rate(self) -> None:
        d = drain_rate([], cap=1000)
        assert d.batches == 0
        assert d.rows_per_s is None


class TestBatchFromProgress:
    @staticmethod
    def _progress() -> dict[str, Any]:
        return {
            "event": "stream.progress",
            "timestamp": "2026-01-01T00:00:10.500Z",
            "query": "warehouse",
            "batch_id": 12,
            "input_rows": 900,
            "duration_ms": {"triggerExecution": 2000, "addBatch": 1500, "latestOffset": 12},
            "sources": [
                {
                    "start": {"events": {"0": 100, "1": 50}},
                    "end": {"events": {"0": 600, "1": 450}},
                    "latest": {"events": {"0": 700, "1": 550}},
                }
            ],
        }

    def test_fields(self) -> None:
        b = batch_from_progress(self._progress())
        assert b is not None
        ended = datetime(2026, 1, 1, 0, 0, 10, 500000, tzinfo=UTC).timestamp()
        assert b.batch_id == 12
        assert b.rows == 900
        assert b.trigger_at == pytest.approx(ended - 2.0)
        assert b.ended_at == pytest.approx(ended)
        assert b.trigger_ms == 2000.0
        assert b.add_batch_ms == 1500.0
        assert b.durations["latestOffset"] == 12.0

    def test_queued_is_latest_minus_start_summed_over_partitions(self) -> None:
        b = batch_from_progress(self._progress())
        assert b is not None
        assert b.queued == (700 + 550) - (100 + 50)

    def test_start_none_string_counts_as_zero(self) -> None:
        p = self._progress()
        p["sources"][0]["start"] = "None"
        b = batch_from_progress(p)
        assert b is not None
        assert b.queued == 1250

    def test_multiple_topics_are_summed(self) -> None:
        p = self._progress()
        p["sources"][0]["latest"]["other"] = {"0": 10}
        b = batch_from_progress(p)
        assert b is not None
        assert b.queued == 1260 - 150

    @pytest.mark.parametrize("sources", [[], None])
    def test_without_sources_is_none(self, sources: list[Any] | None) -> None:
        p = self._progress()
        p["sources"] = sources
        assert batch_from_progress(p) is None
        del p["sources"]
        assert batch_from_progress(p) is None


class TestAttribute:
    def test_shares_sum_to_one_and_largest_is_named(self) -> None:
        a = attribute(noop=2000, python=1000, sink=1000, broker=None, pipeline=500)
        assert (a.jvm_us, a.transfer_us, a.database_us) == (500.0, 500.0, 1000.0)
        assert sum(a.shares.values()) == pytest.approx(1.0, abs=0.002)
        assert a.shares["postgresql sink"] == 0.5
        assert a.bottleneck == "postgresql sink"
        assert a.predicted_per_s == 500.0

    def test_jvm_can_be_the_bottleneck(self) -> None:
        a = attribute(noop=500, python=450, sink=100_000, broker=None, pipeline=400)
        assert a.bottleneck == "spark (JVM)"

    def test_transfer_can_be_the_bottleneck(self) -> None:
        a = attribute(noop=100_000, python=1000, sink=100_000, broker=None, pipeline=900)
        assert a.bottleneck == "python transfer"

    def test_transfer_clamps_at_zero_when_python_is_not_slower(self) -> None:
        a = attribute(noop=2000, python=4000, sink=1000, broker=None, pipeline=500)
        assert a.transfer_us == 0.0
        assert a.shares["python transfer"] == 0.0
        assert sum(a.shares.values()) == pytest.approx(1.0, abs=0.002)

    def test_broker_named_when_below_one_point_one_times_pipeline(self) -> None:
        a = attribute(noop=2000, python=1000, sink=1000, broker=1000, pipeline=950)
        assert a.bottleneck == "broker"

    def test_broker_not_named_when_clearly_faster(self) -> None:
        a = attribute(noop=2000, python=1000, sink=1000, broker=1100, pipeline=950)
        assert a.bottleneck == "postgresql sink"


class TestCompare:
    def test_exact_delivery(self) -> None:
        d = compare(PREFIX, 3, [bench_id(PREFIX, s) for s in (2, 0, 1)])
        assert d.exactly_once
        assert (d.expected, d.rows, d.distinct) == (3, 3, 3)

    def test_missing(self) -> None:
        d = compare(PREFIX, 5, [bench_id(PREFIX, s) for s in (0, 1, 4)])
        assert (d.missing, d.duplicates, d.unexpected) == (2, 0, 0)
        assert not d.exactly_once

    def test_duplicates_count_rows_beyond_the_first(self) -> None:
        ids = [bench_id(PREFIX, s) for s in (0, 1, 1, 1, 0)]
        d = compare(PREFIX, 2, ids)
        assert (d.rows, d.distinct, d.duplicates, d.missing) == (5, 2, 3, 0)
        assert not d.exactly_once

    def test_unexpected_ids_in_range_but_not_produced(self) -> None:
        ids = [bench_id(PREFIX, s) for s in (0, 1, 7, 8)]
        d = compare(PREFIX, 2, ids)
        assert (d.unexpected, d.missing, d.duplicates) == (2, 0, 0)
        assert not d.exactly_once

    def test_mixed_problems(self) -> None:
        ids = [bench_id(PREFIX, s) for s in (0, 0, 2, 9)]
        d = compare(PREFIX, 3, ids)
        assert (d.missing, d.duplicates, d.unexpected) == (1, 1, 1)

    def test_uppercase_ids_are_normalised(self) -> None:
        d = compare(PREFIX, 256, [bench_id(PREFIX, s).upper() for s in range(256)])
        assert d.exactly_once
        assert d.distinct == 256

    def test_empty_landing(self) -> None:
        d = compare(PREFIX, 4, [])
        assert (d.missing, d.rows, d.distinct) == (4, 0, 0)

    def test_nothing_expected_and_nothing_landed_is_exact(self) -> None:
        assert compare(PREFIX, 0, []).exactly_once

    def test_as_dict_carries_exactly_once(self) -> None:
        out = compare(PREFIX, 1, []).as_dict()
        assert out["exactly_once"] is False
        assert out["missing"] == 1


class TestQuantile:
    VALUES: ClassVar[list[float]] = [7.0, 1.0, 9.0, 3.0, 5.0, 11.0]

    @pytest.mark.parametrize("q", [0.0, 0.1, 0.5, 0.95, 0.99, 1.0])
    def test_matches_inclusive_interpolation(self, q: float) -> None:
        cuts = statistics.quantiles(self.VALUES, n=100, method="inclusive")
        got = quantile(self.VALUES, q)
        assert got is not None
        if 0 < q < 1:
            assert got == pytest.approx(cuts[round(q * 100) - 1])
        else:
            assert got == (min(self.VALUES) if q == 0 else max(self.VALUES))

    def test_median_interpolates_between_middle_values(self) -> None:
        assert quantile([1.0, 2.0, 3.0, 4.0], 0.5) == pytest.approx(2.5)

    def test_single_value(self) -> None:
        assert quantile([4.0], 0.99) == 4.0

    def test_empty_is_none(self) -> None:
        assert quantile([], 0.5) is None

    def test_input_is_not_modified(self) -> None:
        values = [3.0, 1.0, 2.0]
        quantile(values, 0.5)
        assert values == [3.0, 1.0, 2.0]


class TestProgressOffsets:
    def test_sums_end_offsets_over_topics_and_partitions(self) -> None:
        p = {"sources": [{"end": {"a": {"0": 5, "1": 7}, "b": {"0": 3}}}]}
        assert progress_offsets(p) == 15

    def test_sums_over_sources(self) -> None:
        p = {"sources": [{"end": {"a": {"0": 5}}}, {"end": {"b": {"0": 6}}}]}
        assert progress_offsets(p) == 11

    def test_string_end_is_none(self) -> None:
        assert progress_offsets({"sources": [{"end": "None"}]}) is None

    def test_no_sources_is_none(self) -> None:
        assert progress_offsets({}) is None
        assert progress_offsets({"sources": []}) is None

    def test_zero_offsets_are_found_not_none(self) -> None:
        assert progress_offsets({"sources": [{"end": {"a": {"0": 0}}}]}) == 0

    def test_ignores_start_and_latest(self) -> None:
        p = {
            "sources": [
                {"start": {"a": {"0": 1}}, "end": {"a": {"0": 2}}, "latest": {"a": {"0": 3}}}
            ]
        }
        assert progress_offsets(p) == 2


class TestParseLogLine:
    def test_json_with_event(self) -> None:
        rec = parse_log_line('{"event": "sink.batch", "rows": 3}\n')
        assert rec == {"event": "sink.batch", "rows": 3}

    def test_plain_spark_line_is_none(self) -> None:
        assert parse_log_line("26/01/01 00:00:00 INFO SparkContext: Running Spark") is None

    def test_json_without_event_is_none(self) -> None:
        assert parse_log_line('{"rows": 3}') is None

    def test_broken_json_is_none(self) -> None:
        assert parse_log_line('{"event": "x"') is None

    def test_json_array_is_none(self) -> None:
        assert parse_log_line('["event"]') is None

    def test_leading_whitespace_is_stripped(self) -> None:
        assert parse_log_line('  {"event": "a"}  ') == {"event": "a"}


class TestCpuUsec:
    @staticmethod
    def _write(root: Path, cid: str, text: str) -> None:
        scope = root / f"docker-{cid}.scope"
        scope.mkdir()
        (scope / "cpu.stat").write_text(text)

    def test_reads_usage_usec(self, tmp_path: Path) -> None:
        self._write(tmp_path, "abc", "usage_usec 123456\nuser_usec 100\nsystem_usec 23\n")
        assert cpu_usec("abc", tmp_path) == 123456

    def test_does_not_confuse_user_usec(self, tmp_path: Path) -> None:
        self._write(tmp_path, "abc", "user_usec 5\nusage_usec 9\n")
        assert cpu_usec("abc", tmp_path) == 9

    def test_missing_file_is_none(self, tmp_path: Path) -> None:
        assert cpu_usec("nope", tmp_path) is None

    def test_file_without_usage_line_is_none(self, tmp_path: Path) -> None:
        self._write(tmp_path, "abc", "user_usec 5\n")
        assert cpu_usec("abc", tmp_path) is None


def _sample(
    t: float,
    cpu: dict[str, int | None] | None = None,
    busy: float = 0.0,
    total: float = 0.0,
    broker_end: int | None = None,
    processed: dict[str, int | None] | None = None,
) -> Sample:
    return Sample(
        t=t,
        broker_end=broker_end,
        processed=processed or {},
        cpu_usec=cpu or {},
        host_busy=busy,
        host_total=total,
    )


class TestSampleMath:
    def test_lag_is_broker_minus_processed(self) -> None:
        s = _sample(0, broker_end=1000, processed={"warehouse": 400})
        assert s.lag("warehouse") == 600

    def test_lag_none_when_either_side_unknown(self) -> None:
        assert _sample(0, broker_end=None, processed={"w": 1}).lag("w") is None
        assert _sample(0, broker_end=10, processed={"w": None}).lag("w") is None
        assert _sample(0, broker_end=10, processed={}).lag("w") is None

    def test_cpu_cores_between_first_and_last(self) -> None:
        samples = [
            _sample(100.0, {"spark": 1_000_000, "pg": 0}),
            _sample(105.0, {"spark": 9_000_000, "pg": 99}),
            _sample(110.0, {"spark": 6_000_000, "pg": 5_000_000}),
        ]
        assert cpu_cores(samples) == {"spark": 0.5, "pg": 0.5}

    def test_cpu_cores_skips_unreadable_containers(self) -> None:
        samples = [_sample(0.0, {"a": None, "b": 0}), _sample(2.0, {"a": 5, "b": None})]
        assert cpu_cores(samples) == {}

    def test_cpu_cores_needs_two_samples_and_elapsed_time(self) -> None:
        assert cpu_cores([_sample(0.0, {"a": 1})]) == {}
        assert cpu_cores([_sample(1.0, {"a": 1}), _sample(1.0, {"a": 5})]) == {}

    def test_host_cores(self) -> None:
        samples = [_sample(0, busy=10, total=100), _sample(5, busy=30, total=140)]
        assert host_cores(samples, ncpu=4) == pytest.approx(2.0)

    def test_host_cores_none_without_progress(self) -> None:
        assert host_cores([_sample(0, busy=1, total=5)], ncpu=4) is None
        same = [_sample(0, busy=1, total=5), _sample(1, busy=2, total=5)]
        assert host_cores(same, ncpu=4) is None


class TestLogTail:
    @staticmethod
    def _progress(query: str, batch: int) -> str:
        return json.dumps({"event": "stream.progress", "query": query, "batch_id": batch})

    def test_newest_returns_latest_progress_per_query(self) -> None:
        tail = LogTail("c", since=0.0)
        tail.feed(self._progress("warehouse", 1), received=1.0)
        tail.feed(self._progress("lake", 5), received=2.0)
        tail.feed(self._progress("warehouse", 2), received=3.0)
        newest = tail.newest("warehouse")
        assert newest is not None
        assert newest["batch_id"] == 2
        lake = tail.newest("lake")
        assert lake is not None
        assert lake["batch_id"] == 5
        assert tail.newest("missing") is None

    def test_get_returns_parsed_records_in_order_with_receipt_time(self) -> None:
        tail = LogTail("c", since=0.0)
        tail.feed(self._progress("warehouse", 1), received=1.5)
        tail.feed('{"event": "sink.batch", "rows": 10}', received=2.5)
        tail.feed(self._progress("warehouse", 2), received=3.5)
        progress = tail.get("stream.progress")
        assert [r["batch_id"] for r in progress] == [1, 2]
        assert [r["_received"] for r in progress] == [1.5, 3.5]
        assert tail.get("sink.batch")[0]["rows"] == 10
        assert tail.get("nothing") == []

    def test_non_json_lines_are_ignored(self) -> None:
        tail = LogTail("c", since=0.0)
        tail.feed("26/01/01 INFO Something", received=1.0)
        tail.feed('{"no_event": 1}', received=1.0)
        assert tail.events == {}
        assert tail.latest == {}

    def test_other_events_do_not_become_latest(self) -> None:
        tail = LogTail("c", since=0.0)
        tail.feed('{"event": "sink.batch", "query": "warehouse"}', received=1.0)
        assert tail.newest("warehouse") is None

    def test_get_returns_a_copy(self) -> None:
        tail = LogTail("c", since=0.0)
        tail.feed('{"event": "a"}', received=1.0)
        tail.get("a").clear()
        assert len(tail.get("a")) == 1

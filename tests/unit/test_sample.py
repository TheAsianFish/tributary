import gzip
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from tributary.ingest.sample import (
    Distribution,
    format_stats,
    read_sample,
    sample_stats,
    write_sample,
)

T0 = 1_792_000_000


def uuid(n: int) -> str:
    return f"00000000-0000-4000-8000-{n:012d}"


def event(n: int, ts: int, **extra: Any) -> dict[str, Any]:
    dt = datetime.fromtimestamp(ts + 2, tz=UTC).isoformat().replace("+00:00", "Z")
    out: dict[str, Any] = {
        "meta": {"id": uuid(n), "dt": dt, "domain": "en.wikipedia.org"},
        "type": "edit",
        "timestamp": ts,
        "wiki": "enwiki",
        "bot": False,
        "title": "Ünïcødé — 東京",
    }
    out.update(extra)
    return out


def at(offset: int) -> datetime:
    return datetime.fromtimestamp(T0 + offset, tz=UTC)


def test_round_trip_preserves_events_and_order(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "sample.jsonl.gz"
    events = [event(i, T0 + i) for i in range(5)]
    assert write_sample(path, events) == 5
    assert list(read_sample(path)) == events
    assert list(read_sample(path, limit=2)) == events[:2]
    assert list(read_sample(path, limit=0)) == []


def test_written_file_is_byte_stable_and_compact(tmp_path: Path) -> None:
    events = [event(i, T0 + i) for i in range(3)]
    a, b = tmp_path / "a.jsonl.gz", tmp_path / "b.jsonl.gz"
    write_sample(a, events)
    write_sample(b, iter(events))
    assert a.read_bytes() == b.read_bytes()
    text = gzip.decompress(a.read_bytes()).decode("utf-8")
    assert "東京" in text
    assert '"type":"edit"' in text
    assert text.endswith("}\n")
    assert sorted(p.name for p in tmp_path.iterdir()) == ["a.jsonl.gz", "b.jsonl.gz"]


def test_written_file_is_world_readable(tmp_path: Path) -> None:
    path = tmp_path / "s.jsonl.gz"
    write_sample(path, [event(1, T0)])
    assert path.stat().st_mode & 0o777 == 0o644


def test_failed_write_leaves_no_file(tmp_path: Path) -> None:
    path = tmp_path / "sample.jsonl.gz"

    def broken() -> Any:
        yield event(1, T0)
        raise RuntimeError("source failed")

    with pytest.raises(RuntimeError):
        write_sample(path, broken())
    assert list(tmp_path.iterdir()) == []


def test_read_rejects_non_objects(tmp_path: Path) -> None:
    path = tmp_path / "bad.jsonl.gz"
    path.write_bytes(gzip.compress(b'{"a":1}\n\n[1]\n'))
    with pytest.raises(ValueError, match=":3:"):
        list(read_sample(path))


def test_stats_on_a_stream_with_known_lateness_and_duplicates() -> None:
    offsets = [0, 10, 5, 200, 50, 210, 210, 2000, 100]
    events = [event(i, T0 + off) for i, off in enumerate(offsets)]
    events[6]["meta"]["id"] = uuid(5)
    events[1].update(type="log", wiki="dewiki", bot=True)
    events.append({"meta": {"id": "bad"}, "timestamp": T0})

    stats = sample_stats(events)

    assert (stats.total, stats.invalid) == (10, 1)
    assert (stats.distinct_ids, stats.duplicate_ids) == (8, 1)
    assert stats.by_type == {"edit": 8, "log": 1}
    assert stats.wikis == 2
    assert stats.top_wikis == [("enwiki", 8), ("dewiki", 1)]
    assert stats.bot_share == pytest.approx(1 / 9)
    assert (stats.first_event_time, stats.last_event_time) == (at(0), at(100))
    assert (stats.min_event_time, stats.max_event_time) == (at(0), at(2000))
    # Running max: 0, 10, 10, 200, 200, 210, 210, 2000, 2000
    # Lateness:    0,  0,  5,   0, 150,   0,   0,    0, 1900
    assert stats.late_events == 3
    assert stats.lateness == Distribution(count=9, p50=0, p90=1900, p99=1900, p999=1900, max=1900)
    assert stats.dropped_by_watermark == {60: 2, 120: 2, 300: 1, 600: 1, 1800: 1}
    assert stats.emit_delay == Distribution(9, 2, 2, 2, 2, 2)
    assert stats.emit_delay_missing == 0


def test_lateness_percentiles_use_nearest_rank() -> None:
    dist = Distribution.of([float(v) for v in range(1, 1001)])
    assert (dist.p50, dist.p90, dist.p99, dist.p999, dist.max) == (500, 900, 990, 999, 1000)


def test_stats_of_an_empty_stream_and_report() -> None:
    stats = sample_stats([])
    assert stats.total == 0
    assert stats.first_event_time is None
    assert "events: 0" in format_stats(stats)


def test_report_mentions_the_key_figures() -> None:
    events = [event(0, T0 + 1000), event(1, T0)]
    events[1]["meta"].pop("dt")
    report = format_stats(sample_stats(events))
    assert "late events: 1 (50.000%)" in report
    assert "10 min: 1 (50.000%)" in report
    assert "30 min: 0" in report
    assert "1 without meta.dt" in report

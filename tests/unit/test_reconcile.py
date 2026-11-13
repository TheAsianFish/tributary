"""The reconciliation pass's pure parts: judging a partition, merging, the offset window."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

import pytest

from tributary.events import COLUMNS
from tributary.stream.reconcile import (
    MemoryStore,
    MissingEvent,
    Verdict,
    judge_partition,
    merge,
    pass_window,
)

TS = datetime(2026, 11, 13, 12, tzinfo=UTC)


def uid(n: int) -> str:
    return f"00000000-0000-4000-8000-{n:012d}"


def row(n: int, offset: int, reason: str | None = None) -> tuple[object, ...]:
    """A classify_events row: the table's columns, then the reject reason."""
    values: dict[str, object] = dict.fromkeys(COLUMNS)
    values.update(kafka_partition=0, kafka_offset=offset, kafka_ts=TS)
    if reason is None:
        values.update(event_id=uid(n), event_ts=TS, wiki="enwiki", event_type="edit")
    return (*(values[c] for c in COLUMNS), reason)


class RecordingStore(MemoryStore):
    def __init__(self, landed: set[str]) -> None:
        super().__init__(landed)
        self.lookups: list[int] = []
        self.written: list[list[str]] = []

    def absent(self, ids: Sequence[str]) -> set[str]:
        self.lookups.append(len(ids))
        return super().absent(ids)

    def write(self, rows: Sequence[Sequence[object]]) -> int:
        self.written.append([str(r[COLUMNS.index("event_id")]) for r in rows])
        return super().write(rows)


def test_a_partition_is_classed_record_by_record_and_its_missing_events_written_once() -> None:
    store = RecordingStore({uid(1), uid(2)})
    rows = [
        row(1, 0),
        row(2, 1),
        row(3, 2),  # missing
        row(0, 3, "malformed"),
        row(0, 4, "future"),
        row(0, 5, "canary"),
        row(3, 6),  # a second copy of the missing event
        row(1, 7),  # a second copy of a landed one
    ]
    [verdict] = judge_partition(store, repair=True)(rows)

    assert verdict == Verdict(
        records=8,
        landed=3,
        malformed=1,
        future=1,
        canary=1,
        missing_records=2,
        missing=(MissingEvent(uid(3), TS, 0, 2),),
        recovered=1,
    )
    assert store.written == [[uid(3)]]


def test_a_dry_run_writes_nothing() -> None:
    store = RecordingStore(set())
    [verdict] = judge_partition(store, repair=False)([row(5, 0)])
    assert [m.event_id for m in verdict.missing] == [uid(5)]
    assert verdict.recovered == 0
    assert store.written == []


def test_ids_are_looked_up_a_chunk_at_a_time(monkeypatch: pytest.MonkeyPatch) -> None:
    from tributary.stream import reconcile

    monkeypatch.setattr(reconcile, "CHUNK", 3)
    store = RecordingStore(set())
    [verdict] = judge_partition(store, repair=True)([row(n, n) for n in range(7)])
    assert store.lookups == [3, 3, 1]
    assert verdict.missing_records == 7
    assert store.written == [[uid(n) for n in range(7)]]


def test_an_empty_partition_yields_an_empty_verdict_without_a_lookup() -> None:
    store = RecordingStore(set())
    assert list(judge_partition(store, repair=True)([])) == [Verdict()]
    assert store.lookups == []


def test_merge_adds_the_partitions_and_orders_missing_events_by_position() -> None:
    a = Verdict(records=3, landed=2, missing_records=1, missing=(MissingEvent(uid(9), TS, 1, 5),))
    b = Verdict(
        records=4,
        landed=1,
        malformed=1,
        missing_records=2,
        recovered=2,
        missing=(MissingEvent(uid(8), TS, 0, 7), MissingEvent(uid(9), TS, 1, 5)),
    )
    total = merge([a, b])
    assert (total.records, total.landed, total.malformed, total.recovered) == (7, 3, 1, 2)
    assert total.missing_records == 3
    assert [m.event_id for m in total.missing] == [uid(8), uid(9)]


def test_a_pass_window_starts_at_what_the_broker_still_holds() -> None:
    committed = {"0": 500, "1": 400, "2": 300}
    earliest = {"0": 0, "1": 120, "2": 350}
    assert pass_window(committed, earliest, "earliest") == {"0": 0, "1": 120, "2": 300}


def test_an_uncovered_window_starts_at_the_first_uncovered_batch() -> None:
    committed = {"0": 500, "1": 400, "2": 300}
    earliest = {"0": 0, "1": 120, "2": 0}
    starts = [{"t": {"0": 200, "1": 100}}, {"t": {"0": 250, "1": 300}}]
    # Partition 1's first uncovered offset (100) is gone: the pass starts at 120 and
    # cannot cover that batch. Partition 2 was read by neither batch... except that a
    # batch with no start offsets in it is taken to start at 0.
    assert pass_window(committed, earliest, "uncovered", starts, "t") == {
        "0": 200,
        "1": 120,
        "2": 0,
    }
    assert pass_window(committed, earliest, "uncovered", [None], "t") == {
        "0": 0,
        "1": 120,
        "2": 0,
    }
    assert pass_window(committed, earliest, "uncovered", [], "t") == committed


def test_a_batch_is_reachable_only_while_the_broker_holds_its_first_offset() -> None:
    from tributary.stream.reconcile import reachable

    earliest = {"0": 100, "1": 0}
    assert reachable({"t": {"0": 100, "1": 5}}, earliest, "t")
    assert not reachable({"t": {"0": 99, "1": 5}}, earliest, "t")
    assert not reachable(None, earliest, "t")  # taken to start at 0, as covers() takes it
    assert reachable(None, {"0": 0}, "t")


def test_an_uncovered_plan_leaves_out_what_retention_cut_and_may_be_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tributary.config import Settings
    from tributary.stream import reconcile
    from tributary.warehouse import stream_health
    from tributary.warehouse.stream_health import DroppedBatch

    settings = Settings(_env_file=None, topic="t")
    batches = [
        DroppedBatch("warehouse", 1, 5, {"t": {"0": 10}}, {"t": {"0": 20}}),  # cut by retention
        DroppedBatch("warehouse", 2, 5, {"t": {"0": 300}}, {"t": {"0": 320}}),
    ]
    monkeypatch.setattr(reconcile, "committed_offsets", lambda *_a, **_k: {"0": 500})
    monkeypatch.setattr(reconcile, "earliest_offsets", lambda *_a: {"0": 100})
    monkeypatch.setattr(stream_health, "uncovered_batches", lambda *_a: batches)

    plan = reconcile.plan_pass(settings, "uncovered", TS)
    assert plan == reconcile.PassPlan({"0": 500}, {"0": 300}, batches=1, unreachable=1)
    assert reconcile.plan_pass(settings, "earliest", TS) == reconcile.PassPlan(
        {"0": 500}, {"0": 100}
    )
    monkeypatch.setattr(stream_health, "uncovered_batches", lambda *_a: batches[:1])
    assert reconcile.plan_pass(settings, "uncovered", TS) is None


def test_with_nothing_committed_there_is_nothing_to_judge(monkeypatch: pytest.MonkeyPatch) -> None:
    from tributary.config import Settings
    from tributary.stream import reconcile

    monkeypatch.setattr(reconcile, "committed_offsets", lambda *_a, **_k: {})
    with pytest.raises(reconcile.NothingToJudgeError):
        reconcile.plan_pass(Settings(_env_file=None))

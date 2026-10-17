"""Direct loading into raw.recentchange, with the database replaced by a recorder."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from typing import Any

import pytest

from tributary.stream.sink import SinkResult
from tributary.warehouse import load

NOW = datetime(2026, 10, 17, 12, 0, tzinfo=UTC)


def event(n: int) -> dict[str, Any]:
    return {
        "meta": {"id": f"00000000-0000-4000-8000-{n:012d}", "domain": "en.wikipedia.org"},
        "type": "edit",
        "wiki": "enwiki",
        "timestamp": 1792193655 + n,
    }


class FakeConn:
    def __enter__(self) -> FakeConn:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


@pytest.fixture
def written(monkeypatch: pytest.MonkeyPatch) -> list[list[Sequence[object]]]:
    batches: list[list[Sequence[object]]] = []

    def write_rows(_conn: object, rows: Iterable[Sequence[object]]) -> SinkResult:
        batch = list(rows)
        batches.append(batch)
        return SinkResult(len(batch), len(batch), 0.0)

    monkeypatch.setattr(load, "ensure_schema", lambda _conninfo: None)
    monkeypatch.setattr(load, "write_rows", write_rows)
    monkeypatch.setattr("tributary.warehouse.load.psycopg.connect", lambda _conninfo: FakeConn())
    return batches


def test_rows_are_marked_as_loaded_without_kafka(written: list[list[Sequence[object]]]) -> None:
    offered, inserted, invalid = load.load_events("", [event(1), event(2)], now=NOW)
    assert (offered, inserted, invalid) == (2, 2, 0)
    rows = written[0]
    partition, offset, kafka_ts = rows[1][-3:]
    assert partition == load.DIRECT_PARTITION == -1
    assert offset == 1  # position in the input
    assert kafka_ts == NOW


def test_invalid_events_are_counted_and_chunks_respected(
    written: list[list[Sequence[object]]],
) -> None:
    bad = event(3)
    del bad["wiki"]
    offered, _, invalid = load.load_events(
        "", [event(1), bad, event(2), event(4)], chunk=2, now=NOW
    )
    assert (offered, invalid) == (3, 1)
    assert [len(b) for b in written] == [1, 2]

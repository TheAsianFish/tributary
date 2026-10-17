from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from tributary import __version__
from tributary.cli import app
from tributary.ingest import producer
from tributary.ingest.sample import write_sample


def test_version() -> None:
    result = CliRunner().invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


class FakeProducer:
    def __init__(self) -> None:
        self.keys: list[bytes] = []

    def produce(self, topic: str, *, value: bytes, key: bytes, on_delivery: Any) -> None:
        self.keys.append(key)
        on_delivery(None, None)

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return 0


def test_produce_replays_the_sample(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sample = tmp_path / "s.jsonl.gz"
    ids = [f"00000000-0000-4000-8000-{n:012d}" for n in range(5)]
    write_sample(
        sample, [{"meta": {"id": i}, "timestamp": 1, "wiki": "x", "type": "edit"} for i in ids]
    )
    fake = FakeProducer()
    topics: list[tuple[str, int]] = []
    monkeypatch.setattr(producer, "make_producer", lambda bootstrap: fake)

    def ensure_topic(bootstrap: str, topic: str, partitions: int) -> bool:
        topics.append((topic, partitions))
        return False

    monkeypatch.setattr(producer, "ensure_topic", ensure_topic)

    result = CliRunner().invoke(
        app, ["produce", "--sample", str(sample), "--limit", "3", "--rate", "0"]
    )

    assert result.exit_code == 0, result.output
    assert fake.keys == [i.encode() for i in ids[:3]]
    assert topics == [("wiki.recentchange", 3)]


def test_produce_exits_nonzero_when_a_delivery_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sample = tmp_path / "s.jsonl.gz"
    write_sample(
        sample,
        [
            {"meta": {"id": "00000000-0000-4000-8000-000000000001"}, "timestamp": 1, "wiki": "x"},
            {"meta": {"id": "00000000-0000-4000-8000-000000000002"}, "timestamp": 1, "wiki": "x"},
        ],
    )

    class FailingProducer(FakeProducer):
        def flush(self, timeout: float) -> int:
            return 1  # one message still undelivered when the flush times out

    monkeypatch.setattr(producer, "make_producer", lambda bootstrap: FailingProducer())
    monkeypatch.setattr(producer, "ensure_topic", lambda bootstrap, topic, partitions: False)

    result = CliRunner().invoke(app, ["produce", "--sample", str(sample), "--rate", "0"])

    assert result.exit_code == 1, result.output


def test_publish_catalog_indexes_up_to_the_library_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tributary.warehouse import catalog, refresh

    limits: list[int] = []

    def publish(_settings: object, _run_id: str, *, max_distinct: int) -> Path:
        limits.append(max_distinct)
        return tmp_path

    monkeypatch.setattr(refresh, "publish_catalog", publish)
    runner = CliRunner()
    assert runner.invoke(app, ["warehouse", "publish-catalog"]).exit_code == 0
    assert (
        runner.invoke(app, ["warehouse", "publish-catalog", "--max-distinct", "7"]).exit_code == 0
    )
    # The wiki names (160 in the sample) must fit under the default.
    assert limits == [catalog.MAX_DISTINCT, 7]
    assert catalog.MAX_DISTINCT >= 160

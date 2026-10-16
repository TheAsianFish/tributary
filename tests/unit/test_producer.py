import json
from collections.abc import Mapping
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin._metadata import ClusterMetadata, PartitionMetadata, TopicMetadata
from confluent_kafka.cimpl import NewTopic

from tributary.ingest.producer import (
    DeliveryCallback,
    KafkaSink,
    Pacer,
    ProduceStats,
    TopicAdmin,
    ensure_topic,
    produce_events,
)

TOPIC = "wiki.recentchange"


def _protocols_match_the_real_clients(p: Producer, a: AdminClient) -> tuple[KafkaSink, TopicAdmin]:
    return p, a


def uuid(n: int) -> str:
    return f"00000000-0000-4000-8000-{n:012d}"


def event(n: int) -> dict[str, Any]:
    return {"meta": {"id": uuid(n)}, "type": "edit", "timestamp": 1_792_000_000 + n, "title": "é"}


@dataclass
class FakeSink:
    """Queues messages; ``poll`` delivers them, failing keys listed in ``fail``."""

    capacity: int = 1_000_000
    fail: set[bytes] = field(default_factory=set)
    stuck: bool = False
    queue: list[tuple[bytes, bytes, DeliveryCallback]] = field(default_factory=list)
    delivered: list[tuple[bytes, bytes]] = field(default_factory=list)
    buffer_errors: int = 0
    polls: list[float] = field(default_factory=list)
    flushes: list[float] = field(default_factory=list)

    def produce(
        self, topic: str, *, value: bytes, key: bytes, on_delivery: DeliveryCallback
    ) -> None:
        if len(self.queue) >= self.capacity:
            self.buffer_errors += 1
            raise BufferError("Local: Queue full")
        self.queue.append((key, value, on_delivery))

    def poll(self, timeout: float) -> int:
        self.polls.append(timeout)
        if timeout == 0 or self.stuck:
            return 0
        return self._serve()

    def flush(self, timeout: float) -> int:
        self.flushes.append(timeout)
        if not self.stuck:
            self._serve()
        return len(self.queue)

    def _serve(self) -> int:
        served, self.queue = self.queue, []
        for key, value, cb in served:
            err = KafkaError(KafkaError._MSG_TIMED_OUT) if key in self.fail else None
            cb(err, Message(topic=TOPIC, key=key, value=value))
            if err is None:
                self.delivered.append((key, value))
        return len(served)


def test_messages_are_keyed_by_uuid_with_compact_json_values() -> None:
    sink = FakeSink()
    stats = produce_events([event(1), event(2)], sink, TOPIC)
    assert [k for k, _ in sink.delivered] == [uuid(1).encode(), uuid(2).encode()]
    assert json.loads(sink.delivered[0][1]) == event(1)
    assert b", " not in sink.delivered[0][1]
    assert "é".encode() in sink.delivered[0][1]
    assert (stats.sent, stats.delivered, stats.failed, stats.skipped_invalid) == (2, 2, 0, 0)
    assert 0 in sink.polls
    assert sink.flushes == [30.0]


def test_invalid_events_are_skipped_and_limit_stops_reading() -> None:
    def events() -> Any:
        yield {"meta": {"id": "x"}}
        yield event(1)
        yield {"type": "edit"}
        yield event(2)
        raise AssertionError("read past the limit")

    stats = produce_events(events(), FakeSink(), TOPIC, limit=2)
    assert (stats.sent, stats.delivered, stats.skipped_invalid) == (2, 2, 2)


def test_limit_zero_sends_nothing() -> None:
    def events() -> Any:
        raise AssertionError("should not be read")
        yield

    assert produce_events(events(), FakeSink(), TOPIC, limit=0).sent == 0


def test_full_queue_is_drained_by_polling_and_retried() -> None:
    sink = FakeSink(capacity=2)
    stats = produce_events([event(i) for i in range(5)], sink, TOPIC)
    assert sink.buffer_errors == 2
    assert 0.1 in sink.polls
    assert [k for k, _ in sink.delivered] == [uuid(i).encode() for i in range(5)]
    assert (stats.sent, stats.delivered, stats.failed) == (5, 5, 0)


def test_delivery_failures_are_counted() -> None:
    sink = FakeSink(fail={uuid(1).encode(), uuid(3).encode()})
    stats = produce_events([event(i) for i in range(4)], sink, TOPIC)
    assert (stats.sent, stats.delivered, stats.failed) == (4, 2, 2)


def test_messages_left_after_flush_timeout_count_as_failed() -> None:
    sink = FakeSink(stuck=True)
    stats = produce_events([event(i) for i in range(3)], sink, TOPIC, flush_timeout=1.5)
    assert sink.flushes == [1.5]
    assert (stats.sent, stats.delivered, stats.failed) == (3, 0, 3)


def test_queue_is_flushed_when_the_source_fails() -> None:
    def events() -> Any:
        yield event(1)
        raise RuntimeError("source failed")

    sink = FakeSink()
    with pytest.raises(RuntimeError):
        produce_events(events(), sink, TOPIC)
    assert sink.delivered == [
        (uuid(1).encode(), json.dumps(event(1), ensure_ascii=False, separators=(",", ":")).encode())
    ]


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def test_pacer_sleeps_only_when_ahead_of_schedule() -> None:
    clock = FakeClock()
    pacer = Pacer(10.0, clock=clock, sleep=clock.sleep)
    pacer.wait()
    pacer.wait()
    pacer.wait()
    assert clock.sleeps == pytest.approx([0.1, 0.1])
    clock.now += 1.0  # a stall: events 3..12 are now overdue
    for _ in range(10):
        pacer.wait()
    assert len(clock.sleeps) == 2
    pacer.wait()
    assert clock.sleeps[-1] == pytest.approx(0.1)
    assert clock.now == pytest.approx(100.0 + 13 / 10)


def test_pacer_rate_zero_never_sleeps_and_negative_is_rejected() -> None:
    clock = FakeClock()
    pacer = Pacer(0, clock=clock, sleep=clock.sleep)
    for _ in range(100):
        pacer.wait()
    assert clock.sleeps == []
    with pytest.raises(ValueError, match="rate"):
        Pacer(-1)


def test_produce_events_paces_and_reports_rate() -> None:
    clock = FakeClock()
    pacer = Pacer(4.0, clock=clock, sleep=clock.sleep)
    stats = produce_events(
        [event(i) for i in range(5)], FakeSink(), TOPIC, pacer=pacer, clock=clock
    )
    assert stats.seconds == pytest.approx(1.0)
    assert stats.events_per_second == pytest.approx(5.0)
    assert ProduceStats(0, 0, 0, 0, 0.0).events_per_second == 0.0


def topic_metadata(name: str, partitions: int) -> TopicMetadata:
    meta = TopicMetadata()
    meta.topic = name
    meta.partitions = {i: PartitionMetadata() for i in range(partitions)}
    return meta


@dataclass
class FakeAdmin:
    topics: dict[str, int] = field(default_factory=dict)
    create_error: KafkaError | None = None
    created: list[NewTopic] = field(default_factory=list)

    def list_topics(self, *, timeout: float) -> ClusterMetadata:
        metadata = ClusterMetadata()
        metadata.topics = {name: topic_metadata(name, n) for name, n in self.topics.items()}
        return metadata

    def create_topics(
        self, new_topics: list[NewTopic], *, operation_timeout: float, request_timeout: float
    ) -> Mapping[str, Future[None]]:
        futures: dict[str, Future[None]] = {}
        for topic in new_topics:
            future: Future[None] = Future()
            if self.create_error is not None:
                self.topics[topic.topic] = 6
                future.set_exception(KafkaException(self.create_error))
            else:
                self.created.append(topic)
                self.topics[topic.topic] = topic.num_partitions
                future.set_result(None)
            futures[topic.topic] = future
        return futures


def test_ensure_topic_creates_a_missing_topic() -> None:
    admin = FakeAdmin(topics={"other": 1})
    assert ensure_topic("broker:9092", TOPIC, 3, admin=admin) is True
    [created] = admin.created
    assert (created.topic, created.num_partitions, created.replication_factor) == (TOPIC, 3, 1)


def test_ensure_topic_leaves_an_existing_topic_alone() -> None:
    admin = FakeAdmin(topics={TOPIC: 6})
    assert ensure_topic("broker:9092", TOPIC, 3, admin=admin) is False
    assert admin.created == []
    assert admin.topics[TOPIC] == 6


def test_ensure_topic_tolerates_a_creation_race() -> None:
    admin = FakeAdmin(create_error=KafkaError(KafkaError.TOPIC_ALREADY_EXISTS))
    assert ensure_topic("broker:9092", TOPIC, 3, admin=admin) is False


def test_ensure_topic_raises_other_errors() -> None:
    admin = FakeAdmin(create_error=KafkaError(KafkaError.INVALID_PARTITIONS))
    with pytest.raises(KafkaException):
        ensure_topic("broker:9092", TOPIC, 3, admin=admin)

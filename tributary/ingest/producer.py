"""Publish events to Kafka: pacing, delivery accounting and topic creation."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Protocol

import structlog
from confluent_kafka import KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin._metadata import ClusterMetadata
from confluent_kafka.cimpl import NewTopic

from tributary.events import Event, InvalidEventError, event_id
from tributary.ingest.sample import encode_event

log = structlog.get_logger(__name__)

CLIENT_ID = "tributary-producer"
DeliveryCallback = Callable[[KafkaError | None, Message], None]


class KafkaSink(Protocol):
    """The subset of ``confluent_kafka.Producer`` that ``produce_events`` uses."""

    def produce(
        self, topic: str, *, value: bytes, key: bytes, on_delivery: DeliveryCallback
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


class TopicAdmin(Protocol):
    """The subset of ``confluent_kafka.admin.AdminClient`` that ``ensure_topic`` uses."""

    def list_topics(self, *, timeout: float) -> ClusterMetadata: ...

    def create_topics(
        self, new_topics: list[NewTopic], *, operation_timeout: float, request_timeout: float
    ) -> Mapping[str, Future[None]]: ...


def make_producer(bootstrap: str, *, client_id: str = CLIENT_ID) -> Producer:
    """An idempotent producer: no duplicates or reordering from its own retries."""
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": client_id,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 20,
            "compression.type": "lz4",
        }
    )


class Pacer:
    """Open-loop pacing: event *i* is due at ``t0 + i / rate``.

    It sleeps only when ahead of schedule; after a stall it sends without
    sleeping until it catches up, so the average rate holds. ``rate`` 0
    disables pacing.
    """

    def __init__(
        self,
        rate: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if rate < 0:
            raise ValueError(f"rate must be >= 0, got {rate}")
        self.rate = rate
        self._clock = clock
        self._sleep = sleep
        self._t0: float | None = None
        self._sent = 0

    def wait(self) -> None:
        """Block until the next event is due."""
        if self.rate == 0:
            return
        now = self._clock()
        if self._t0 is None:
            self._t0 = now
        due = self._t0 + self._sent / self.rate
        self._sent += 1
        if due > now:
            self._sleep(due - now)


@dataclass(frozen=True)
class ProduceStats:
    sent: int
    delivered: int
    failed: int
    skipped_invalid: int
    seconds: float

    @property
    def events_per_second(self) -> float:
        """Delivered events per second of wall time, flush included."""
        return self.delivered / self.seconds if self.seconds > 0 else 0.0


@dataclass
class _Deliveries:
    delivered: int = 0
    failed: int = 0

    def __call__(self, err: KafkaError | None, msg: Message) -> None:
        if err is None:
            self.delivered += 1
            return
        self.failed += 1
        level = log.warning if self.failed == 1 else log.debug
        level("delivery_failed", error=err.str(), code=err.name(), topic=msg.topic())


def encode_message(event: Event) -> tuple[bytes, bytes]:
    """The Kafka key (event UUID) and value (compact JSON), both UTF-8."""
    return event_id(event).encode("utf-8"), encode_event(event).encode("utf-8")


def _send(sink: KafkaSink, topic: str, key: bytes, value: bytes, cb: DeliveryCallback) -> None:
    while True:
        try:
            sink.produce(topic, value=value, key=key, on_delivery=cb)
        except BufferError:
            sink.poll(0.1)
            continue
        sink.poll(0)
        return


def produce_events(
    events: Iterable[Event],
    sink: KafkaSink,
    topic: str,
    *,
    rate: float = 0,
    limit: int | None = None,
    pacer: Pacer | None = None,
    flush_timeout: float = 30.0,
    progress_every: int = 10_000,
    clock: Callable[[], float] = time.monotonic,
) -> ProduceStats:
    """Send events keyed by UUID; return counts once the queue is flushed.

    Events without a valid ``meta.id`` are skipped. A full local queue is
    drained with ``poll`` and the send retried. Messages still undelivered
    after ``flush_timeout`` are counted as failed. ``limit`` caps the number
    of events sent, and no event past it is read from ``events``.
    """
    pacer = pacer if pacer is not None else Pacer(rate, clock=clock)
    deliveries = _Deliveries()
    sent = skipped = 0
    start = clock()
    try:
        if limit is None or limit > 0:
            for event in events:
                try:
                    key, value = encode_message(event)
                except InvalidEventError as exc:
                    skipped += 1
                    log.debug("produce_skip_invalid", error=str(exc))
                    continue
                pacer.wait()
                _send(sink, topic, key, value, deliveries)
                sent += 1
                if sent % progress_every == 0:
                    _log_progress(sent, deliveries, clock() - start)
                if limit is not None and sent >= limit:
                    break
    finally:
        undelivered = sink.flush(flush_timeout)
    if undelivered:
        log.warning("produce_flush_timeout", undelivered=undelivered, timeout_s=flush_timeout)
    stats = ProduceStats(
        sent=sent,
        delivered=deliveries.delivered,
        failed=deliveries.failed + undelivered,
        skipped_invalid=skipped,
        seconds=clock() - start,
    )
    log.info(
        "produce_done",
        topic=topic,
        sent=stats.sent,
        delivered=stats.delivered,
        failed=stats.failed,
        skipped_invalid=stats.skipped_invalid,
        seconds=round(stats.seconds, 3),
        events_per_second=round(stats.events_per_second, 1),
    )
    return stats


def _log_progress(sent: int, deliveries: _Deliveries, elapsed: float) -> None:
    log.info(
        "produce_progress",
        sent=sent,
        delivered=deliveries.delivered,
        failed=deliveries.failed,
        events_per_second=round(sent / elapsed, 1) if elapsed > 0 else None,
    )


def make_admin(bootstrap: str, *, client_id: str = CLIENT_ID) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap, "client.id": client_id})


def _existing_partitions(admin: TopicAdmin, topic: str, timeout: float) -> int | None:
    metadata = admin.list_topics(timeout=timeout).topics.get(topic)
    if metadata is None or metadata.error is not None:
        return None
    return len(metadata.partitions)


def _warn_if_mismatched(topic: str, actual: int, wanted: int) -> None:
    if actual != wanted:
        log.warning("topic_partitions_differ", topic=topic, partitions=actual, configured=wanted)


def _already_exists(exc: KafkaException) -> bool:
    err = exc.args[0] if exc.args else None
    return isinstance(err, KafkaError) and err.code() == KafkaError.TOPIC_ALREADY_EXISTS


def ensure_topic(
    bootstrap: str,
    topic: str,
    partitions: int,
    *,
    admin: TopicAdmin | None = None,
    timeout: float = 15.0,
) -> bool:
    """Create ``topic`` with replication factor 1 if missing; True if this call created it.

    An existing topic is left as it is; a partition count different from
    ``partitions`` is logged, not altered.
    """
    admin = admin if admin is not None else make_admin(bootstrap)
    existing = _existing_partitions(admin, topic, timeout)
    if existing is not None:
        _warn_if_mismatched(topic, existing, partitions)
        return False
    new = NewTopic(topic, num_partitions=partitions, replication_factor=1)
    futures = admin.create_topics([new], operation_timeout=timeout, request_timeout=timeout)
    try:
        futures[topic].result(timeout=timeout)
    except KafkaException as exc:
        if not _already_exists(exc):
            raise
        log.info("topic_exists", topic=topic, race=True)
        existing = _existing_partitions(admin, topic, timeout)
        if existing is not None:
            _warn_if_mismatched(topic, existing, partitions)
        return False
    log.info("topic_created", topic=topic, partitions=partitions)
    return True

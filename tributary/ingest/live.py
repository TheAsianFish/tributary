"""Live mode: trimmed events from the Wikimedia ``recentchange`` SSE stream."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass

import httpx
import structlog

from tributary.events import Event, InvalidEventError, event_id, event_time, is_canary, trim_event
from tributary.ingest.sse import SSEMessage, exponential_backoff, stream_messages

log = structlog.get_logger(__name__)


@dataclass
class DecodeStats:
    """Running counts of what ``decode_messages`` kept and dropped."""

    received: int = 0
    canary: int = 0
    invalid: int = 0
    kept: int = 0


def decode_message(message: SSEMessage) -> Event | None:
    """Parse one SSE message into a trimmed event; ``None`` for canaries.

    Raises InvalidEventError when the payload is not JSON, not an object, or
    lacks a valid ``meta.id`` or ``timestamp``.
    """
    try:
        raw = json.loads(message.data)
    except json.JSONDecodeError as exc:
        raise InvalidEventError(f"not JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise InvalidEventError("payload is not a JSON object")
    if is_canary(raw):
        return None
    event = trim_event(raw)
    event_id(event)
    event_time(event)
    return event


def decode_messages(
    messages: Iterable[SSEMessage], stats: DecodeStats | None = None
) -> Iterator[Event]:
    """Yield trimmed events, dropping canaries and invalid payloads (counted in ``stats``)."""
    stats = stats if stats is not None else DecodeStats()
    for message in messages:
        stats.received += 1
        try:
            event = decode_message(message)
        except InvalidEventError as exc:
            stats.invalid += 1
            log.debug("live_event_invalid", error=str(exc), sse_id=message.id)
            continue
        if event is None:
            stats.canary += 1
            continue
        stats.kept += 1
        yield event


def live_events(
    url: str,
    user_agent: str,
    *,
    client: httpx.Client | None = None,
    last_event_id: str | None = None,
    max_reconnects: int | None = None,
    backoff: Callable[[int], float] = exponential_backoff,
    sleep: Callable[[float], None] = time.sleep,
    stats: DecodeStats | None = None,
) -> Iterator[Event]:
    """Trimmed, valid, non-canary events from the live stream, resuming across reconnects."""
    messages = stream_messages(
        url,
        user_agent=user_agent,
        client=client,
        last_event_id=last_event_id,
        max_reconnects=max_reconnects,
        backoff=backoff,
        sleep=sleep,
    )
    yield from decode_messages(messages, stats)

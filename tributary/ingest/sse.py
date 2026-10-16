"""A small Server-Sent Events client for the Wikimedia EventStreams feed.

Parsing follows the WHATWG ``text/event-stream`` rules that matter for a
data feed; ``stream_messages`` adds reconnection with backoff, resuming from
the last event id the server sent.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Iterator
from contextlib import ExitStack
from dataclasses import dataclass

import httpx
import structlog

log = structlog.get_logger(__name__)

DEFAULT_TIMEOUT = httpx.Timeout(10.0, connect=10.0, read=60.0)
MAX_BACKOFF_S = 30.0
RETRYABLE_STATUS = frozenset({408, 429, 500, 502, 503, 504})


@dataclass(frozen=True)
class SSEMessage:
    data: str
    id: str | None
    event: str


class SSEParser:
    """Incremental line parser; feed lines without their terminators.

    ``last_event_id`` is updated when a message is dispatched (also for
    messages with no data), as the spec requires, so it is the value to
    send as ``Last-Event-ID`` on reconnection. ``retry_ms`` holds the last
    reconnection time the server asked for, if any.
    """

    def __init__(self, last_event_id: str | None = None) -> None:
        self.last_event_id = last_event_id
        self.retry_ms: int | None = None
        self._id_buffer = last_event_id
        self._data: list[str] = []
        self._event = ""
        self._first_line = True

    def feed(self, line: str) -> SSEMessage | None:
        """Process one line; return a message when a blank line completes one."""
        if self._first_line:
            self._first_line = False
            line = line.removeprefix("﻿")
        if not line:
            return self._dispatch()
        if line.startswith(":"):
            return None
        name, sep, value = line.partition(":")
        if sep and value.startswith(" "):
            value = value[1:]
        self._apply(name, value)
        return None

    def _apply(self, name: str, value: str) -> None:
        if name == "data":
            self._data.append(value)
        elif name == "event":
            self._event = value
        elif name == "id":
            if "\0" not in value:
                self._id_buffer = value
        elif name == "retry" and value.isascii() and value.isdigit():
            self.retry_ms = int(value)

    def _dispatch(self) -> SSEMessage | None:
        self.last_event_id = self._id_buffer
        data, event = self._data, self._event
        self._data, self._event = [], ""
        if not data:
            return None
        return SSEMessage(data="\n".join(data), id=self.last_event_id, event=event or "message")


def parse_sse(lines: Iterable[str]) -> Iterator[SSEMessage]:
    """Parse ``text/event-stream`` lines (terminators already stripped) into messages."""
    parser = SSEParser()
    for line in lines:
        message = parser.feed(line)
        if message is not None:
            yield message


def exponential_backoff(attempt: int) -> float:
    """Seconds to wait before reconnection ``attempt`` (1-based): 1, 2, 4, ... capped at 30."""
    return min(MAX_BACKOFF_S, 2.0 ** max(attempt - 1, 0))


def _request_headers(user_agent: str, last_event_id: str | None) -> dict[str, str]:
    headers = {"Accept": "text/event-stream", "User-Agent": user_agent}
    if last_event_id is not None:
        headers["Last-Event-ID"] = last_event_id
    return headers


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in RETRYABLE_STATUS
    return isinstance(exc, httpx.TransportError)


def stream_messages(
    url: str,
    *,
    user_agent: str,
    client: httpx.Client | None = None,
    last_event_id: str | None = None,
    max_reconnects: int | None = None,
    backoff: Callable[[int], float] = exponential_backoff,
    sleep: Callable[[float], None] = time.sleep,
) -> Iterator[SSEMessage]:
    """Yield messages from an SSE endpoint, reconnecting until ``max_reconnects`` is spent.

    Disconnects, timeouts, transport errors and retryable HTTP statuses (429,
    5xx) trigger a reconnect after ``backoff(n)`` seconds, where ``n`` counts
    consecutive attempts without a message; the request carries
    ``Last-Event-ID`` so the server resumes after the last dispatched message.
    Other HTTP errors raise immediately. When the reconnect budget is spent
    the last error is raised, or the generator returns if the server simply
    closed the stream. ``None`` means reconnect forever.
    """
    with ExitStack() as stack:
        if client is None:
            client = stack.enter_context(httpx.Client(timeout=DEFAULT_TIMEOUT))
        reconnects = 0
        failures = 0
        while True:
            parser = SSEParser(last_event_id)
            error: Exception | None = None
            try:
                headers = _request_headers(user_agent, last_event_id)
                with client.stream("GET", url, headers=headers) as response:
                    response.raise_for_status()
                    for line in response.iter_lines():
                        message = parser.feed(line)
                        last_event_id = parser.last_event_id
                        if message is not None:
                            failures = 0
                            yield message
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                if not _is_retryable(exc):
                    raise
                error = exc
            if max_reconnects is not None and reconnects >= max_reconnects:
                if error is not None:
                    raise error
                return
            reconnects += 1
            failures += 1
            delay = backoff(failures)
            log.warning(
                "sse_reconnect",
                url=url,
                attempt=failures,
                delay_s=delay,
                last_event_id=last_event_id,
                error=repr(error) if error is not None else "stream closed",
            )
            sleep(delay)

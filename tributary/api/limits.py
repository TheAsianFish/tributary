"""Per-key token buckets and the request body cap.

Both are per process: each uvicorn worker or replica keeps its own buckets, so
N processes behind a balancer admit up to N times the configured rate. A
shared store (Redis, the database) would make the limit global; it is out of
scope for a single-process deployment.
"""

from __future__ import annotations

import json
import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from starlette.types import ASGIApp, Message, Receive, Scope, Send


@dataclass(slots=True)
class _Bucket:
    tokens: float
    updated: float


class RateLimiter:
    """A token bucket per key: ``burst`` requests at once, refilled at ``per_minute``.

    Keys are the SHA-256 digests of authenticated API keys, never the keys
    themselves; only configured keys reach the limiter, so the table is bounded.
    ``per_minute == 0`` disables the limit.
    """

    def __init__(
        self,
        per_minute: int,
        burst: int,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if per_minute < 0:
            raise ValueError("rate limit per minute must be >= 0")
        if burst < 1:
            raise ValueError("rate limit burst must be >= 1")
        self.per_minute = per_minute
        self.burst = burst
        self._rate = per_minute / 60.0
        self._clock = clock
        self._buckets: dict[bytes, _Bucket] = {}
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return self.per_minute > 0

    def acquire(self, key: bytes) -> float:
        """Take one token; 0.0 if granted, else the seconds until one is available."""
        if not self.enabled:
            return 0.0
        with self._lock:
            now = self._clock()
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = self._buckets[key] = _Bucket(float(self.burst), now)
            elapsed = max(0.0, now - bucket.updated)
            bucket.tokens = min(float(self.burst), bucket.tokens + elapsed * self._rate)
            bucket.updated = now
            if bucket.tokens >= 1.0:
                bucket.tokens -= 1.0
                return 0.0
            return (1.0 - bucket.tokens) / self._rate


def retry_after(wait: float) -> str:
    """``Retry-After`` in whole seconds, rounded up (never 0 for a refused request)."""
    return str(max(1, math.ceil(wait)))


class BodyLimit:
    """ASGI middleware refusing request bodies over ``max_bytes`` with 413, before parsing.

    A declared ``Content-Length`` over the cap is refused without reading anything.
    Otherwise (chunked or undeclared) the body is read up to the cap and replayed to
    the app, so an endless chunked upload is cut at ``max_bytes`` too.
    """

    def __init__(self, app: ASGIApp, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        declared = _content_length(scope)
        if declared is not None and declared > self.max_bytes:
            await self._refuse(scope, send)
            return
        buffered: list[Message] = []
        size = 0
        while True:
            message = await receive()
            buffered.append(message)
            if message["type"] != "http.request":
                break  # a disconnect: let the app see it
            size += len(message.get("body", b""))
            if size > self.max_bytes:
                await self._refuse(scope, send)
                return
            if not message.get("more_body", False):
                break

        async def replay() -> Message:
            if buffered:
                return buffered.pop(0)
            return await receive()

        await self.app(scope, replay, send)

    async def _refuse(self, scope: Scope, send: Send) -> None:
        state = scope.get("state") or {}
        body = json.dumps(
            {
                "detail": f"request body over {self.max_bytes} bytes",
                "request_id": state.get("request_id"),
            }
        ).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                    (b"connection", b"close"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})


def _content_length(scope: Scope) -> int | None:
    for name, value in scope.get("headers", []):
        if name == b"content-length":
            try:
                return int(value)
            except ValueError:
                return None
    return None

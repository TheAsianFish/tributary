"""Recorded model output: capture real replies once, replay them offline.

Recordings are JSON Lines, one request per line, sorted by fingerprint so a
re-record produces a reviewable diff. A line holds the conversation and the
reply; the system prompt is stored only as its sha256, since it is long and
the same for every call, while the fingerprint (computed over the full
request) still changes when it does. Replays report the usage and latency of
the live call, so an evaluation replayed from recordings states what the run
actually cost.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from tributary.agent.provider import (
    ModelRequest,
    ModelResponse,
    Provider,
    RecordingMissing,
    Usage,
    log_call,
)

RECORD_HINT = "record it with `make record`"

# Any: one decoded JSON Lines record, shaped as documented above.
Recording = dict[str, Any]


def load_recordings(path: Path) -> dict[str, Recording]:
    recordings: dict[str, Recording] = {}
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                record = json.loads(line)
                recordings[record["fingerprint"]] = record
    return recordings


def write_recordings(path: Path, recordings: dict[str, Recording]) -> None:
    """Rewrite the whole file sorted by fingerprint; atomic, so a crash leaves the old file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        json.dumps(recordings[key], sort_keys=True, ensure_ascii=False) + "\n"
        for key in sorted(recordings)
    ]
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.writelines(lines)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def to_recording(request: ModelRequest, response: ModelResponse, provider: str) -> Recording:
    u = response.usage
    return {
        "fingerprint": request.fingerprint(),
        "schema_name": request.schema_name,
        "provider": provider,
        "model": response.model,
        "messages": [[m.role, m.content] for m in request.messages],
        "system_sha256": hashlib.sha256(request.system.encode("utf-8")).hexdigest(),
        "response": {
            "text": response.text,
            "data": response.data,
            "stop_reason": response.stop_reason,
            "latency_ms": response.latency_ms,
            "usage": {
                "input_tokens": u.input_tokens,
                "output_tokens": u.output_tokens,
                "cache_read_tokens": u.cache_read_tokens,
                "cache_write_tokens": u.cache_write_tokens,
                "cost_usd": u.cost_usd,
                # Only when set, so recordings from priced models keep their bytes.
                **({"unpriced_calls": u.unpriced_calls} if u.unpriced_calls else {}),
            },
        },
    }


class FakeProvider:
    """Replays recordings by request fingerprint; a miss is an error, never a guess."""

    name = "fake"
    model = "replay"

    def __init__(self, path: Path) -> None:
        self.path = path
        self._recordings: dict[str, Recording] | None = None

    def _load(self) -> dict[str, Recording]:
        if self._recordings is None:
            if not self.path.exists():
                raise RecordingMissing(f"no recordings file at {self.path}; {RECORD_HINT}")
            self._recordings = load_recordings(self.path)
        return self._recordings

    def complete(self, request: ModelRequest) -> ModelResponse:
        fingerprint = request.fingerprint()
        record = self._load().get(fingerprint)
        if record is None:
            last_user = next(
                (m.content for m in reversed(request.messages) if m.role == "user"), ""
            )
            raise RecordingMissing(
                f"no recording {fingerprint[:16]} for {request.schema_name} "
                f"({last_user[:80]!r}); {RECORD_HINT}"
            )
        reply = record["response"]
        response = ModelResponse(
            data=reply["data"],
            text=reply["text"],
            provider=self.name,
            model=f"{record['provider']}/{record['model']}",
            usage=Usage(**reply["usage"]),
            latency_ms=reply["latency_ms"],
            stop_reason=reply["stop_reason"],
        )
        log_call(response, request)
        return response


class RecordingProvider:
    """Wraps a real provider and upserts every reply into the recordings file."""

    def __init__(self, inner: Provider, path: Path) -> None:
        self.inner = inner
        self.path = path
        self.name = inner.name
        self.model = inner.model

    def complete(self, request: ModelRequest) -> ModelResponse:
        response = self.inner.complete(request)
        recordings = load_recordings(self.path) if self.path.exists() else {}
        recordings[request.fingerprint()] = to_recording(request, response, self.inner.name)
        write_recordings(self.path, recordings)
        return response

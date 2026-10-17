"""The model boundary: one request shape, one response shape, three providers.

Every model call in the agent goes through :class:`Provider.complete`. A
request is a cacheable system prefix, the conversation so far and the JSON
schema the reply must satisfy (structured output); a response is the parsed
object plus what it cost. The providers are

- ``anthropic``: the Claude API (structured outputs, prompt caching on the
  system prefix, server-side fallbacks on refusals, usage priced per call),
- ``ollama``: a local model through Ollama's chat API (JSON-schema ``format``),
- ``fake``: replays recorded responses by request fingerprint, so tests run
  offline and deterministically on real model output.

A recording is captured by wrapping a real provider in the recorder; the
fingerprint covers everything the model saw, so a changed prompt or schema
misses its recording instead of silently replaying a stale answer.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

import structlog

log = structlog.get_logger(__name__)

Role = Literal["user", "assistant"]


class ProviderError(Exception):
    """A model call failed: transport, API error, or unusable output."""


class ProviderRefusal(ProviderError):  # noqa: N818 (public API names)
    """The model declined the request (``stop_reason: refusal``), fallbacks included."""


class InvalidOutput(ProviderError):  # noqa: N818 (public API names)
    """The reply was not a JSON object (truncated, or the provider ignored the schema)."""


class RecordingMissing(ProviderError):  # noqa: N818 (public API names)
    """The fake provider has no recording for this request."""


@dataclass(frozen=True, slots=True)
class Message:
    role: Role
    content: str


@dataclass(frozen=True, slots=True)
class ModelRequest:
    """One structured-output call.

    ``system`` is the stable prefix (rules, schema, examples) and is what the
    providers cache; everything that varies per question goes in ``messages``.
    """

    system: str
    messages: tuple[Message, ...]
    schema: Mapping[str, Any]
    # Any: a JSON schema document.
    schema_name: str
    max_tokens: int = 4096

    def fingerprint(self) -> str:
        """sha256 over what the model sees; the key a recording is stored under."""
        document = {
            "system": self.system,
            "messages": [[m.role, m.content] for m in self.messages],
            "schema_name": self.schema_name,
            "schema": self.schema,
        }
        canonical = json.dumps(document, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class Usage:
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    cost_usd: float = 0.0

    def __add__(self, other: Usage) -> Usage:
        return Usage(
            self.input_tokens + other.input_tokens,
            self.output_tokens + other.output_tokens,
            self.cache_read_tokens + other.cache_read_tokens,
            self.cache_write_tokens + other.cache_write_tokens,
            self.cost_usd + other.cost_usd,
        )


@dataclass(frozen=True, slots=True)
class ModelResponse:
    data: dict[str, Any]
    # Any: the parsed JSON object; the caller validates it against its own model.
    text: str
    provider: str
    model: str
    usage: Usage = field(default_factory=Usage)
    latency_ms: float = 0.0
    stop_reason: str | None = None


class Provider(Protocol):
    name: str
    model: str

    def complete(self, request: ModelRequest) -> ModelResponse: ...


def parse_object(text: str) -> dict[str, Any]:
    """The reply as a JSON object, or :class:`InvalidOutput`."""
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise InvalidOutput(f"reply is not JSON: {exc}") from None
    if not isinstance(data, dict):
        raise InvalidOutput(f"reply is JSON but not an object: {type(data).__name__}")
    return data


def log_call(response: ModelResponse, request: ModelRequest) -> None:
    """The one log line every model call emits (request_id comes from the context)."""
    u = response.usage
    log.info(
        "model.call",
        provider=response.provider,
        model=response.model,
        schema=request.schema_name,
        fingerprint=request.fingerprint()[:16],
        input_tokens=u.input_tokens,
        output_tokens=u.output_tokens,
        cache_read_tokens=u.cache_read_tokens,
        cache_write_tokens=u.cache_write_tokens,
        cost_usd=round(u.cost_usd, 6),
        latency_ms=round(response.latency_ms, 1),
        stop_reason=response.stop_reason,
    )

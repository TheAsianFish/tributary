"""The Anthropic provider through the real SDK request path, transport mocked."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import anthropic
import httpx2
import pytest

from tributary.agent.provider import (
    InvalidOutput,
    Message,
    ModelRequest,
    ProviderError,
    ProviderRefusal,
)
from tributary.agent.providers import pricing
from tributary.agent.providers.anthropic import FALLBACK_BETA, AnthropicProvider

REQUEST = ModelRequest(
    system="You write PostgreSQL for the marts schema.",
    messages=(Message("user", "How many edits per wiki?"),),
    schema={"type": "object", "properties": {"sql": {"type": "string"}}, "required": ["sql"]},
    schema_name="draft",
    max_tokens=2048,
)


def message_json(
    *,
    text: str = '{"sql": "select 1"}',
    model: str = "claude-opus-5-5",
    stop_reason: str = "end_turn",
    usage: dict[str, Any] | None = None,
    extra_content: list[dict[str, Any]] | None = None,
    stop_details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": "msg_test",
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": [*(extra_content or []), {"type": "text", "text": text}],
        "stop_reason": stop_reason,
        "stop_sequence": None,
        "stop_details": stop_details,
        "usage": usage or {"input_tokens": 10, "output_tokens": 5},
    }


class Recorder:
    def __init__(self, respond: Callable[[httpx2.Request], httpx2.Response]) -> None:
        self.respond = respond
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        return self.respond(request)

    def body(self) -> dict[str, Any]:
        return dict(json.loads(self.requests[-1].content))


def provider_for(
    respond: Callable[[httpx2.Request], httpx2.Response], **kwargs: Any
) -> tuple[AnthropicProvider, Recorder]:
    recorder = Recorder(respond)
    client = anthropic.Anthropic(
        api_key="test",
        max_retries=0,
        http_client=anthropic.DefaultHttpxClient(transport=httpx2.MockTransport(recorder)),
    )
    return AnthropicProvider("claude-opus-5-5", client=client, **kwargs), recorder


def ok(payload: dict[str, Any]) -> Callable[[httpx2.Request], httpx2.Response]:
    return lambda _request: httpx2.Response(200, json=payload)


def test_request_body_and_headers() -> None:
    provider, recorder = provider_for(ok(message_json()), effort="high")
    provider.complete(REQUEST)

    sent = recorder.requests[-1]
    assert sent.url.path == "/v1/messages"
    assert sent.headers["anthropic-beta"] == FALLBACK_BETA
    body = recorder.body()
    assert body["model"] == "claude-opus-5-5"
    assert body["max_tokens"] == 2048
    assert body["system"] == [
        {"type": "text", "text": REQUEST.system, "cache_control": {"type": "ephemeral"}}
    ]
    assert body["messages"] == [{"role": "user", "content": "How many edits per wiki?"}]
    assert body["output_config"] == {
        "format": {"type": "json_schema", "schema": dict(REQUEST.schema)},
        "effort": "high",
    }
    assert body["fallbacks"] == "default"
    for forbidden in ("thinking", "temperature", "top_p", "budget_tokens"):
        assert forbidden not in body


def test_parses_text_blocks_and_ignores_others() -> None:
    fallback_block = {
        "type": "fallback",
        "from": {"model": "claude-opus-5-5"},
        "to": {"model": "claude-sonnet-5-5"},
        "trigger": {"type": "refusal", "category": "cyber"},
    }
    thinking = {"type": "thinking", "thinking": "count rows", "signature": "sig"}
    payload = message_json(extra_content=[fallback_block, thinking])
    provider, _ = provider_for(ok(payload))
    response = provider.complete(REQUEST)
    assert response.data == {"sql": "select 1"}
    assert response.text == '{"sql": "select 1"}'
    assert response.provider == "anthropic"
    assert response.stop_reason == "end_turn"
    assert response.latency_ms >= 0


def test_cost_includes_cache_read_and_write() -> None:
    usage = {
        "input_tokens": 1_000,
        "output_tokens": 500,
        "cache_read_input_tokens": 20_000,
        "cache_creation_input_tokens": 4_000,
    }
    provider, _ = provider_for(ok(message_json(usage=usage)))
    u = provider.complete(REQUEST).usage
    assert (u.input_tokens, u.output_tokens) == (1_000, 500)
    assert (u.cache_read_tokens, u.cache_write_tokens) == (20_000, 4_000)
    # 1000*4 + 500*20 + 20000*0.20 + 4000*5.00, per million.
    assert u.cost_usd == pytest.approx((4_000 + 10_000 + 4_000 + 20_000) / 1e6)


def test_null_cache_counts_are_zero() -> None:
    usage = {
        "input_tokens": 3,
        "output_tokens": 4,
        "cache_read_input_tokens": None,
        "cache_creation_input_tokens": None,
    }
    provider, _ = provider_for(ok(message_json(usage=usage)))
    u = provider.complete(REQUEST).usage
    assert (u.cache_read_tokens, u.cache_write_tokens) == (0, 0)


def test_fallback_served_model_is_priced_at_its_own_rate() -> None:
    usage = {"input_tokens": 1_000_000, "output_tokens": 0}
    provider, _ = provider_for(ok(message_json(model="claude-sonnet-5-5", usage=usage)))
    response = provider.complete(REQUEST)
    assert response.model == "claude-sonnet-5-5"
    assert response.usage.cost_usd == pytest.approx(2.00)


def test_unknown_model_is_unpriced_not_guessed() -> None:
    provider, _ = provider_for(ok(message_json(model="claude-future-9")))
    assert provider.complete(REQUEST).usage.cost_usd == 0.0
    assert pricing.cost("claude-future-9", input_tokens=1, output_tokens=1) is None


def test_refusal_raises_with_category() -> None:
    payload = message_json(text="", stop_reason="refusal", stop_details={"category": "cyber"})
    provider, _ = provider_for(ok(payload))
    with pytest.raises(ProviderRefusal, match="cyber"):
        provider.complete(REQUEST)


def test_max_tokens_is_invalid_output() -> None:
    provider, _ = provider_for(ok(message_json(text='{"sql": "sel', stop_reason="max_tokens")))
    with pytest.raises(InvalidOutput, match="truncated"):
        provider.complete(REQUEST)


def test_non_json_text_is_invalid_output() -> None:
    provider, _ = provider_for(ok(message_json(text="Sure! Here is the SQL.")))
    with pytest.raises(InvalidOutput, match="not JSON"):
        provider.complete(REQUEST)


def error(status: int, kind: str) -> Callable[[httpx2.Request], httpx2.Response]:
    payload = {"type": "error", "error": {"type": kind, "message": f"{kind} happened"}}
    return lambda _request: httpx2.Response(status, json=payload)


@pytest.mark.parametrize(
    ("status", "kind", "match"),
    [
        (401, "authentication_error", "API key"),
        (403, "permission_error", "403"),
        (404, "not_found_error", "not found"),
        (429, "rate_limit_error", "rate limited"),
        (400, "invalid_request_error", "400"),
        (500, "api_error", "500"),
        (529, "overloaded_error", "529"),
    ],
)
def test_status_errors_map_to_provider_error(status: int, kind: str, match: str) -> None:
    provider, _ = provider_for(error(status, kind))
    with pytest.raises(ProviderError, match=match) as info:
        provider.complete(REQUEST)
    assert "test" not in str(info.value).split()


def test_connection_error_maps_to_provider_error() -> None:
    def fail(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ConnectError("refused", request=request)

    provider, _ = provider_for(fail)
    with pytest.raises(ProviderError, match="could not reach"):
        provider.complete(REQUEST)


def test_missing_credentials_is_a_provider_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_AUTH_TOKEN", raising=False)
    client = anthropic.Anthropic(
        max_retries=0,
        http_client=anthropic.DefaultHttpxClient(
            transport=httpx2.MockTransport(ok(message_json()))
        ),
    )
    with pytest.raises(ProviderError, match="ANTHROPIC_API_KEY"):
        AnthropicProvider("claude-opus-5-5", client=client).complete(REQUEST)

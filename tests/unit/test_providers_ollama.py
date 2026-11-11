"""The Ollama provider against a mocked chat endpoint."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from tributary.agent.provider import InvalidOutput, Message, ModelRequest, ProviderError
from tributary.agent.providers.ollama import OllamaProvider

REQUEST = ModelRequest(
    system="You write PostgreSQL.",
    messages=(
        Message("user", "Edits per wiki?"),
        Message("assistant", "{}"),
        Message("user", "Again"),
    ),
    schema={"type": "object"},
    schema_name="draft",
)


def chat_reply(content: str = '{"sql": "select 1"}', **extra: Any) -> dict[str, Any]:
    return {
        "model": "qwen2.5-coder:7b",
        "message": {"role": "assistant", "content": content},
        "done": True,
        "done_reason": "stop",
        "prompt_eval_count": 1200,
        "eval_count": 40,
        "total_duration": 2_500_000_000,
        **extra,
    }


def provider_for(
    respond: Callable[[httpx.Request], httpx.Response],
) -> tuple[OllamaProvider, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return respond(request)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    return OllamaProvider("qwen2.5-coder:7b", url="http://ollama:11434/", client=client), seen


def test_request_body() -> None:
    provider, seen = provider_for(lambda _r: httpx.Response(200, json=chat_reply()))
    provider.complete(REQUEST)
    assert str(seen[0].url) == "http://ollama:11434/api/chat"
    body = json.loads(seen[0].content)
    assert body == {
        "model": "qwen2.5-coder:7b",
        "messages": [
            {"role": "system", "content": "You write PostgreSQL."},
            {"role": "user", "content": "Edits per wiki?"},
            {"role": "assistant", "content": "{}"},
            {"role": "user", "content": "Again"},
        ],
        "format": {"type": "object"},
        "stream": False,
        "keep_alive": "30m",
        "options": {"temperature": 0, "seed": 0, "num_ctx": 16384, "num_predict": 1536},
    }


def test_parses_reply_and_usage() -> None:
    provider, _ = provider_for(lambda _r: httpx.Response(200, json=chat_reply()))
    response = provider.complete(REQUEST)
    assert response.data == {"sql": "select 1"}
    assert response.provider == "ollama"
    assert response.model == "qwen2.5-coder:7b"
    assert (response.usage.input_tokens, response.usage.output_tokens) == (1200, 40)
    assert response.usage.cost_usd == 0.0
    assert response.stop_reason == "stop"


def test_length_is_invalid_output() -> None:
    reply = chat_reply('{"sql": "sel', done_reason="length")
    provider, _ = provider_for(lambda _r: httpx.Response(200, json=reply))
    with pytest.raises(InvalidOutput, match="truncated"):
        provider.complete(REQUEST)


def test_non_json_is_invalid_output() -> None:
    provider, _ = provider_for(lambda _r: httpx.Response(200, json=chat_reply("SELECT 1")))
    with pytest.raises(InvalidOutput):
        provider.complete(REQUEST)


def test_missing_model_says_to_pull() -> None:
    body = {"error": "model 'qwen2.5-coder:7b' not found"}
    provider, _ = provider_for(lambda _r: httpx.Response(404, json=body))
    with pytest.raises(ProviderError, match=r"ollama pull qwen2\.5-coder:7b"):
        provider.complete(REQUEST)


def test_server_error_is_provider_error() -> None:
    provider, _ = provider_for(lambda _r: httpx.Response(500, text="boom"))
    with pytest.raises(ProviderError, match="500"):
        provider.complete(REQUEST)


def test_connection_error_hints_at_compose() -> None:
    def fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    provider, _ = provider_for(fail)
    with pytest.raises(ProviderError, match="--profile ollama"):
        provider.complete(REQUEST)


def test_an_aborted_degenerate_reply_is_invalid_output() -> None:
    body = {"error": "prediction aborted, token repeat limit reached"}
    provider, _ = provider_for(lambda _r: httpx.Response(500, json=body))
    with pytest.raises(InvalidOutput, match="repeat limit"):
        provider.complete(REQUEST)


def test_a_vote_draw_sets_temperature_and_seed() -> None:
    from dataclasses import replace

    provider, seen = provider_for(lambda _r: httpx.Response(200, json=chat_reply()))
    provider.complete(replace(REQUEST, sample=2, temperature=0.7))
    options = json.loads(seen[0].content)["options"]
    assert (options["temperature"], options["seed"]) == (0.7, 2)

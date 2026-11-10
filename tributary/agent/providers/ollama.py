"""A local model through Ollama's chat API, for running the agent without a key.

Ollama's structured outputs take the JSON schema itself as ``format``, so the
request mirrors the Anthropic one. Sampling is pinned (temperature 0, fixed
seed) so a rerun asks the same question the same way, and the context window
is raised from Ollama's small default because the schema prefix alone runs to
several thousand tokens. Local calls cost nothing; tokens are still counted.
"""

from __future__ import annotations

import time
from typing import Any

import httpx

from tributary.agent.provider import (
    InvalidOutput,
    ModelRequest,
    ModelResponse,
    ProviderError,
    Usage,
    log_call,
    parse_object,
)

START_HINT = "start it with `docker compose --profile ollama up -d ollama`"


class OllamaProvider:
    name = "ollama"

    def __init__(
        self,
        model: str,
        *,
        url: str,
        timeout: float = 600.0,
        num_ctx: int = 16384,
        client: httpx.Client | None = None,
    ) -> None:
        self.model = model
        self.url = url.rstrip("/")
        self.num_ctx = num_ctx
        self._client = client or httpx.Client(timeout=timeout)

    def complete(self, request: ModelRequest) -> ModelResponse:
        body = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": request.system},
                *({"role": m.role, "content": m.content} for m in request.messages),
            ],
            "format": dict(request.schema),
            "stream": False,
            "keep_alive": "30m",
            "options": {"temperature": 0, "seed": 0, "num_ctx": self.num_ctx},
        }
        started = time.perf_counter()
        try:
            reply = self._client.post(f"{self.url}/api/chat", json=body)
        except httpx.TimeoutException:
            raise ProviderError(f"Ollama at {self.url} timed out ({self.model})") from None
        except httpx.TransportError as exc:
            raise ProviderError(
                f"could not reach Ollama at {self.url} ({exc}); {START_HINT}"
            ) from None
        latency_ms = (time.perf_counter() - started) * 1000
        if reply.status_code != 200:
            raise self._status_error(reply)
        try:
            # Any: Ollama's response document, read field by field below.
            payload: dict[str, Any] = reply.json()
            content = payload["message"]["content"]
        except (ValueError, KeyError, TypeError):
            raise ProviderError(f"unexpected reply from Ollama: {reply.text[:200]}") from None
        if payload.get("done_reason") == "length":
            raise InvalidOutput(f"reply truncated at num_ctx={self.num_ctx} ({self.model})")
        data = parse_object(content)
        response = ModelResponse(
            data=data,
            text=content,
            provider=self.name,
            model=self.model,
            usage=Usage(
                input_tokens=int(payload.get("prompt_eval_count", 0)),
                output_tokens=int(payload.get("eval_count", 0)),
            ),
            latency_ms=latency_ms,
            stop_reason=payload.get("done_reason"),
        )
        log_call(response, request)
        return response

    def _status_error(self, reply: httpx.Response) -> ProviderError:
        try:
            detail = str(reply.json().get("error", reply.text))
        except (ValueError, AttributeError):
            detail = reply.text
        if reply.status_code == 404 and "not found" in detail:
            return ProviderError(
                f"Ollama has no model {self.model}: run `ollama pull {self.model}` "
                f"(in the container: `docker compose exec ollama ollama pull {self.model}`)"
            )
        return ProviderError(f"Ollama error ({reply.status_code}): {detail[:200]}; {START_HINT}")

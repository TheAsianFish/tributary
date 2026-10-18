"""The Claude API provider: structured outputs, a cached system prefix, fallbacks.

Every call sends the system prompt as one text block marked
``cache_control: ephemeral``: it holds the schema description and rules, is
identical across questions, and so is billed at the cache-read rate after the
first call. The reply format is pinned with ``output_config.format`` (a JSON
schema), which is also where the effort level lives. Server-side fallbacks are
always requested; on a refusal the API retries on a fallback model and the
response names the model that actually served, which is the one priced.

Sampling parameters (``temperature``, ``top_p``) and an explicit ``thinking``
budget are deliberately absent: current models reject them, and adaptive
thinking is the default.
"""

from __future__ import annotations

import time
from typing import Literal

import anthropic
import structlog
from anthropic.types.beta import BetaMessage, BetaMessageParam, BetaTextBlockParam

from tributary.agent.provider import (
    InvalidOutput,
    ModelRequest,
    ModelResponse,
    ProviderError,
    ProviderRefusal,
    Usage,
    log_call,
    parse_object,
)
from tributary.agent.providers import pricing

log = structlog.get_logger(__name__)

Effort = Literal["low", "medium", "high", "xhigh", "max"]
FALLBACK_BETA = "server-side-fallback-2026-07-01"


class AnthropicProvider:
    name = "anthropic"

    def __init__(
        self,
        model: str,
        *,
        effort: Effort = "medium",
        client: anthropic.Anthropic | None = None,
        timeout: float = 120.0,
        max_retries: int = 2,
    ) -> None:
        self.model = model
        self.effort: Effort = effort
        # The key comes from ANTHROPIC_API_KEY, read by the SDK; it never passes through here.
        self._client = client or anthropic.Anthropic(timeout=timeout, max_retries=max_retries)

    def complete(self, request: ModelRequest) -> ModelResponse:
        system: list[BetaTextBlockParam] = [
            {"type": "text", "text": request.system, "cache_control": {"type": "ephemeral"}}
        ]
        messages: list[BetaMessageParam] = [
            {"role": m.role, "content": m.content} for m in request.messages
        ]
        started = time.perf_counter()
        try:
            message = self._client.beta.messages.create(
                model=self.model,
                max_tokens=request.max_tokens,
                system=system,
                messages=messages,
                output_config={
                    "format": {"type": "json_schema", "schema": dict(request.schema)},
                    "effort": self.effort,
                },
                betas=[FALLBACK_BETA],
                fallbacks="default",
            )
        except anthropic.APIError as exc:
            raise _api_error(exc, self.model) from None
        except TypeError as exc:
            # The SDK raises a bare TypeError when no credential is configured at all.
            if "authentication method" in str(exc):
                raise ProviderError(
                    "no Anthropic credentials: set ANTHROPIC_API_KEY "
                    "(or use TRIBUTARY_PROVIDER=fake to replay recordings)"
                ) from None
            raise
        latency_ms = (time.perf_counter() - started) * 1000
        return self._response(message, request, latency_ms)

    def _response(
        self, message: BetaMessage, request: ModelRequest, latency_ms: float
    ) -> ModelResponse:
        if message.stop_reason == "refusal":
            category = message.stop_details.category if message.stop_details else None
            detail = f" (category: {category})" if category else ""
            raise ProviderRefusal(f"{message.model} refused the request{detail}")
        if message.stop_reason == "max_tokens":
            raise InvalidOutput(
                f"reply truncated at max_tokens={request.max_tokens} ({message.model})"
            )
        text = "".join(block.text for block in message.content if block.type == "text")
        data = parse_object(text)
        response = ModelResponse(
            data=data,
            text=text,
            provider=self.name,
            model=message.model,
            usage=_usage(message),
            latency_ms=latency_ms,
            stop_reason=message.stop_reason,
        )
        log_call(response, request)
        return response


def _usage(message: BetaMessage) -> Usage:
    u = message.usage
    counts = {
        "input_tokens": u.input_tokens,
        "output_tokens": u.output_tokens,
        "cache_read_tokens": u.cache_read_input_tokens or 0,
        "cache_write_tokens": u.cache_creation_input_tokens or 0,
    }
    # Priced at the model that served: after a fallback that is not the one requested.
    usd = pricing.cost(message.model, **counts)
    if usd is None:
        log.warning("model.unpriced", model=message.model, hint="add it to pricing.PRICES")
        return Usage(**counts, cost_usd=0.0, unpriced_calls=1)
    return Usage(**counts, cost_usd=usd)


def _api_error(exc: anthropic.APIError, model: str) -> ProviderError:
    """One readable message per failure kind; the SDK's own text carries no credential."""
    if isinstance(exc, anthropic.AuthenticationError):
        return ProviderError("Anthropic rejected the API key (401): check ANTHROPIC_API_KEY")
    if isinstance(exc, anthropic.PermissionDeniedError):
        return ProviderError(f"the API key may not use {model} (403): {exc.message}")
    if isinstance(exc, anthropic.NotFoundError):
        return ProviderError(f"model {model} not found (404): {exc.message}")
    if isinstance(exc, anthropic.RateLimitError):
        return ProviderError(
            f"rate limited by the Anthropic API (429), retries exhausted: {exc.message}"
        )
    if isinstance(exc, anthropic.BadRequestError):
        return ProviderError(f"Anthropic API rejected the request (400): {exc.message}")
    if isinstance(exc, anthropic.APIStatusError):
        return ProviderError(f"Anthropic API error ({exc.status_code}): {exc.message}")
    if isinstance(exc, anthropic.APITimeoutError):
        return ProviderError("Anthropic API timed out, retries exhausted")
    if isinstance(exc, anthropic.APIConnectionError):
        return ProviderError(f"could not reach the Anthropic API: {exc.message}")
    return ProviderError(f"Anthropic API error: {exc.message}")

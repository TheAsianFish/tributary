"""The Anthropic provider against the real API: one small structured-output call.

Skipped without ``ANTHROPIC_API_KEY``. ``make test-live`` runs it; the ``Live``
workflow runs it by hand when the repository has the secret. It costs a fraction
of a cent on the cheapest model (``TRIBUTARY_LIVE_MODEL``, default Haiku 5.5).
"""

from __future__ import annotations

import os

import pytest

from tributary.agent.provider import Message, ModelRequest
from tributary.agent.providers.anthropic import AnthropicProvider

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(not os.environ.get("ANTHROPIC_API_KEY"), reason="ANTHROPIC_API_KEY not set"),
]

MODEL = os.environ.get("TRIBUTARY_LIVE_MODEL", "claude-haiku-5-5")

SCHEMA = {
    "type": "object",
    "properties": {
        "sql": {"type": "string"},
        "tables": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["sql", "tables"],
    "additionalProperties": False,
}


def test_structured_output_with_usage_and_cost() -> None:
    provider = AnthropicProvider(MODEL, effort="low")
    request = ModelRequest(
        system=(
            "You write PostgreSQL. The only table is marts.dim_wiki(wiki text, edits bigint). "
            "Reply with the query and the tables it reads."
        ),
        messages=(Message("user", "How many wikis are there?"),),
        schema=SCHEMA,
        schema_name="draft",
        max_tokens=512,
    )

    response = provider.complete(request)

    assert response.provider == "anthropic"
    assert "dim_wiki" in response.data["sql"].lower()
    assert response.data["tables"]
    assert response.usage.input_tokens > 0
    assert response.usage.output_tokens > 0
    assert response.usage.cost_usd > 0 or response.usage.unpriced_calls == 1

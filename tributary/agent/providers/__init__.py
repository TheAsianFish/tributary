"""Provider construction from settings.

The concrete modules are imported inside :func:`make_provider`: the Anthropic
SDK takes about a second to import, which the CLI should not pay for
``--help`` or for commands that never call a model.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tributary.agent.provider import Provider
    from tributary.config import Settings


def make_provider(settings: Settings, *, record: bool = False) -> Provider:
    """The configured provider; ``record`` saves every reply for the fake provider."""
    from tributary.agent.providers.fake import FakeProvider, RecordingProvider

    provider: Provider
    if settings.provider == "fake":
        if record:
            raise ValueError("recording needs a real provider; the fake one only replays")
        return FakeProvider(settings.recordings_path)
    if settings.provider == "anthropic":
        from tributary.agent.providers.anthropic import AnthropicProvider

        provider = AnthropicProvider(settings.model, effort=settings.anthropic_effort)
    elif settings.provider == "ollama":
        from tributary.agent.providers.ollama import OllamaProvider

        provider = OllamaProvider(settings.ollama_model, url=settings.ollama_url)
    else:
        raise ValueError(f"unknown provider {settings.provider!r}")
    return RecordingProvider(provider, settings.recordings_path) if record else provider

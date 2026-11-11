"""Recording and replay, and building a provider from settings."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from tributary.agent.provider import (
    Message,
    ModelRequest,
    ModelResponse,
    RecordingMissing,
    Usage,
)
from tributary.agent.providers import make_provider
from tributary.agent.providers.anthropic import AnthropicProvider
from tributary.agent.providers.fake import FakeProvider, RecordingProvider
from tributary.agent.providers.ollama import OllamaProvider
from tributary.config import Settings

REQUEST = ModelRequest(
    system="You write PostgreSQL for the marts schema. " * 50,
    messages=(Message("user", "Which wiki had the most édits yesterday?"),),
    schema={"type": "object"},
    schema_name="draft",
)


class Scripted:
    """A stand-in real provider that answers with the question it was asked."""

    name = "anthropic"
    model = "claude-opus-5-5"

    def __init__(self) -> None:
        self.calls = 0

    def complete(self, request: ModelRequest) -> ModelResponse:
        self.calls += 1
        question = request.messages[-1].content
        return ModelResponse(
            data={"sql": "select 1", "question": question, "call": self.calls},
            text=json.dumps({"sql": "select 1"}),
            provider=self.name,
            model="claude-sonnet-5-5",
            usage=Usage(100, 20, 3000, 400, 0.0123),
            latency_ms=812.5,
            stop_reason="end_turn",
        )


def test_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "rec" / "warehouse.jsonl"
    live = RecordingProvider(Scripted(), path).complete(REQUEST)
    replayed = FakeProvider(path).complete(REQUEST)
    assert replayed.data == live.data
    assert replayed.text == live.text
    assert replayed.usage == live.usage
    assert replayed.latency_ms == live.latency_ms
    assert replayed.stop_reason == "end_turn"
    assert replayed.provider == "fake"
    assert replayed.model == "anthropic/claude-sonnet-5-5"


def test_recording_line_format(tmp_path: Path) -> None:
    path = tmp_path / "warehouse.jsonl"
    RecordingProvider(Scripted(), path).complete(REQUEST)
    line = path.read_text(encoding="utf-8")
    assert "édits" in line  # ensure_ascii=False
    assert REQUEST.system not in line  # only its hash is stored
    record = json.loads(line)
    assert list(record) == sorted(record)
    assert record["fingerprint"] == REQUEST.fingerprint()
    assert record["messages"] == [["user", "Which wiki had the most édits yesterday?"]]
    assert len(record["system_sha256"]) == 64
    assert record["response"]["usage"]["cache_write_tokens"] == 400


@pytest.mark.parametrize(
    "changed",
    [
        replace(REQUEST, system=REQUEST.system + "One more rule."),
        replace(REQUEST, messages=(Message("user", "Which wiki had the fewest edits?"),)),
        replace(REQUEST, schema={"type": "object", "required": ["sql"]}),
    ],
)
def test_changed_request_misses(tmp_path: Path, changed: ModelRequest) -> None:
    path = tmp_path / "warehouse.jsonl"
    RecordingProvider(Scripted(), path).complete(REQUEST)
    with pytest.raises(RecordingMissing, match="make record") as info:
        FakeProvider(path).complete(changed)
    assert changed.fingerprint()[:16] in str(info.value)
    assert "draft" in str(info.value)


def test_missing_file(tmp_path: Path) -> None:
    with pytest.raises(RecordingMissing, match="no recordings file"):
        FakeProvider(tmp_path / "absent.jsonl").complete(REQUEST)


def test_sorted_and_stable(tmp_path: Path) -> None:
    path = tmp_path / "warehouse.jsonl"
    recorder = RecordingProvider(Scripted(), path)
    requests = [replace(REQUEST, messages=(Message("user", f"question {i}"),)) for i in range(6)]
    for request in requests:
        recorder.complete(request)
    first = path.read_text(encoding="utf-8")
    fingerprints = [json.loads(line)["fingerprint"] for line in first.splitlines()]
    assert fingerprints == sorted(fingerprints)
    assert len(fingerprints) == 6

    other = tmp_path / "other.jsonl"
    reverse = RecordingProvider(Scripted(), other)
    for request in reversed(requests):
        reverse.complete(request)
    # Same content regardless of recording order (calls differ, so compare fingerprints).
    again = [json.loads(line)["fingerprint"] for line in other.read_text().splitlines()]
    assert again == fingerprints


def test_upsert_replaces(tmp_path: Path) -> None:
    path = tmp_path / "warehouse.jsonl"
    inner = Scripted()
    recorder = RecordingProvider(inner, path)
    recorder.complete(REQUEST)
    recorder.complete(REQUEST)
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["response"]["data"]["call"] == 2
    assert not list(tmp_path.glob("*.tmp"))


def test_recording_provider_mirrors_inner(tmp_path: Path) -> None:
    recorder = RecordingProvider(Scripted(), tmp_path / "r.jsonl")
    assert (recorder.name, recorder.model) == ("anthropic", "claude-opus-5-5")


def test_make_provider(tmp_path: Path) -> None:
    path = tmp_path / "r.jsonl"
    fake = make_provider(Settings(provider="fake", recordings_path=path))
    assert isinstance(fake, FakeProvider)
    assert fake.path == path

    ollama = make_provider(Settings(provider="ollama", ollama_model="llama3:8b"))
    assert isinstance(ollama, OllamaProvider)
    assert ollama.model == "llama3:8b"

    claude = make_provider(
        Settings(provider="anthropic", model="claude-haiku-5-5", anthropic_effort="low")
    )
    assert isinstance(claude, AnthropicProvider)
    assert (claude.model, claude.effort) == ("claude-haiku-5-5", "low")

    recording = make_provider(Settings(provider="ollama", recordings_path=path), record=True)
    assert isinstance(recording, RecordingProvider)
    assert isinstance(recording.inner, OllamaProvider)
    assert recording.path == path

    with pytest.raises(ValueError, match="real provider"):
        make_provider(Settings(provider="fake"), record=True)


def test_an_unusable_reply_is_recorded_and_replayed_as_one(tmp_path: Path) -> None:
    from tributary.agent.provider import InvalidOutput, ModelRequest
    from tributary.agent.providers.fake import FakeProvider, RecordingProvider

    class Degenerate:
        name = "ollama"
        model = "qwen"

        def complete(self, request: ModelRequest) -> ModelResponse:
            raise InvalidOutput("Ollama aborted the reply: token repeat limit reached")

    path = tmp_path / "rec.jsonl"
    request = ModelRequest("s", (), {"type": "object"}, "draft")
    with pytest.raises(InvalidOutput):
        RecordingProvider(Degenerate(), path).complete(request)
    with pytest.raises(InvalidOutput, match="repeat limit"):
        FakeProvider(path).complete(request)

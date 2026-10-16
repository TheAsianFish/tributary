import json
from typing import Any

import httpx

from tributary.ingest.live import DecodeStats, decode_messages, live_events
from tributary.ingest.sse import SSEMessage

URL = "https://stream.example.org/v2/stream/recentchange"


def raw_event(uuid: str, *, domain: str = "en.wikipedia.org") -> dict[str, Any]:
    return {
        "$schema": "/mediawiki/recentchange/1.0.0",
        "meta": {"id": uuid, "domain": domain, "dt": "2026-10-16T23:34:16Z", "offset": 1},
        "type": "edit",
        "timestamp": 1792193655,
        "wiki": "enwiki",
        "title": "Page",
        "comment": "free text",
        "parsedcomment": "<b>free text</b>",
    }


def uuid(n: int) -> str:
    return f"00000000-0000-4000-8000-{n:012d}"


def sse_body(payloads: list[str]) -> bytes:
    return "".join(
        f"event: message\nid: {i}\ndata: {p}\n\n" for i, p in enumerate(payloads)
    ).encode()


def test_live_events_drops_canaries_and_invalid_payloads_and_trims() -> None:
    payloads = [
        json.dumps(raw_event(uuid(1))),
        json.dumps(raw_event(uuid(2), domain="canary")),
        "{not json",
        "[1, 2]",
        json.dumps({**raw_event(uuid(3)), "meta": {"id": "nope"}}),
        json.dumps(raw_event(uuid(4))),
    ]
    stats = DecodeStats()
    with httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=sse_body(payloads)))
    ) as client:
        events = list(
            live_events(
                URL, "ua/0", client=client, max_reconnects=0, sleep=lambda _: None, stats=stats
            )
        )

    assert [e["meta"]["id"] for e in events] == [uuid(1), uuid(4)]
    assert not {"comment", "parsedcomment", "$schema"} & events[0].keys()
    assert "offset" not in events[0]["meta"]
    assert stats == DecodeStats(received=6, canary=1, invalid=3, kept=2)


def test_decode_messages_is_lazy() -> None:
    def messages() -> Any:
        yield SSEMessage(json.dumps(raw_event(uuid(1))), "1", "message")
        raise AssertionError("read past the first event")

    assert next(decode_messages(messages()))["meta"]["id"] == uuid(1)

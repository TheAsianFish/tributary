from collections.abc import Callable, Iterator

import httpx
import pytest

from tributary.ingest.sse import (
    SSEMessage,
    SSEParser,
    exponential_backoff,
    parse_sse,
    stream_messages,
)

URL = "https://stream.example.org/v2/stream/recentchange"
UA = "tributary-test/0"


def lines(text: str) -> list[str]:
    return text.split("\n")


def test_multi_line_data_is_joined_with_newlines() -> None:
    messages = list(parse_sse(lines("data: first\ndata:second\ndata:  third\n\n")))
    assert messages == [SSEMessage(data="first\nsecond\n third", id=None, event="message")]


def test_comments_are_ignored_and_event_type_resets() -> None:
    text = ": heartbeat\nevent: custom\ndata: a\n\n:ok\ndata: b\n\n"
    assert list(parse_sse(lines(text))) == [
        SSEMessage("a", None, "custom"),
        SSEMessage("b", None, "message"),
    ]


def test_id_persists_across_messages_until_changed() -> None:
    text = "id: 1\ndata: a\n\ndata: b\n\nid: 2\ndata: c\n\nid\ndata: d\n\n"
    assert [(m.data, m.id) for m in parse_sse(lines(text))] == [
        ("a", "1"),
        ("b", "1"),
        ("c", "2"),
        ("d", ""),
    ]


def test_empty_data_is_not_dispatched_but_updates_the_last_id() -> None:
    parser = SSEParser()
    for line in ("id: 7", "event: ping", ""):
        assert parser.feed(line) is None
    assert parser.last_event_id == "7"
    assert parser.feed("data") is None
    assert parser.feed("") == SSEMessage("", "7", "message")


def test_partial_message_without_blank_line_is_not_dispatched() -> None:
    assert list(parse_sse(lines("data: a\n\ndata: incomplete"))) == [
        SSEMessage("a", None, "message")
    ]


def test_retry_bom_unknown_fields_and_nul_ids() -> None:
    parser = SSEParser()
    for line in ("﻿retry: 2500", "retry: soon", "foo: bar", "id: a\0b", "data: x"):
        parser.feed(line)
    assert parser.retry_ms == 2500
    assert parser.feed("") == SSEMessage("x", None, "message")


def test_backoff_is_exponential_and_capped() -> None:
    assert [exponential_backoff(n) for n in (1, 2, 3, 5, 6, 10)] == [1, 2, 4, 16, 30, 30]


class FailingStream(httpx.SyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks

    def __iter__(self) -> Iterator[bytes]:
        yield from self.chunks
        raise httpx.ReadError("connection reset")


def mock_client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_reconnects_with_last_event_id_after_a_broken_stream() -> None:
    requests: list[httpx.Request] = []
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            body = FailingStream([b'id: [{"offset":1}]\ndata: one\n\n', b"id: 2\ndata: tru"])
            return httpx.Response(200, stream=body)
        if len(requests) == 2:
            return httpx.Response(503)
        return httpx.Response(200, content=b"id: 3\ndata: two\n\n")

    with mock_client(handler) as client:
        messages = list(
            stream_messages(
                URL, user_agent=UA, client=client, max_reconnects=2, sleep=sleeps.append
            )
        )

    assert [m.data for m in messages] == ["one", "two"]
    assert requests[0].headers["Accept"] == "text/event-stream"
    assert requests[0].headers["User-Agent"] == UA
    assert "Last-Event-ID" not in requests[0].headers
    assert requests[1].headers["Last-Event-ID"] == '[{"offset":1}]'
    assert requests[2].headers["Last-Event-ID"] == '[{"offset":1}]'
    assert sleeps == [1, 2]


def test_resumes_from_a_given_id_and_resets_backoff_after_a_message() -> None:
    sleeps: list[float] = []
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("Last-Event-ID"))
        n = len(seen)
        return httpx.Response(200, content=f"id: {n}\ndata: m{n}\n\n".encode())

    with mock_client(handler) as client:
        messages = list(
            stream_messages(
                URL,
                user_agent=UA,
                client=client,
                last_event_id="0",
                max_reconnects=2,
                sleep=sleeps.append,
            )
        )
    assert [m.data for m in messages] == ["m1", "m2", "m3"]
    assert seen == ["0", "1", "2"]
    assert sleeps == [1, 1]


def test_budget_exhausted_raises_the_last_error() -> None:
    sleeps: list[float] = []
    with (
        mock_client(lambda _: httpx.Response(502)) as client,
        pytest.raises(httpx.HTTPStatusError),
    ):
        list(
            stream_messages(
                URL, user_agent=UA, client=client, max_reconnects=3, sleep=sleeps.append
            )
        )
    assert sleeps == [1, 2, 4]


def test_client_errors_are_not_retried() -> None:
    sleeps: list[float] = []
    with (
        mock_client(lambda _: httpx.Response(403)) as client,
        pytest.raises(httpx.HTTPStatusError),
    ):
        list(stream_messages(URL, user_agent=UA, client=client, sleep=sleeps.append))
    assert sleeps == []


def test_timeouts_are_retried() -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        if len(calls) == 1:
            raise httpx.ReadTimeout("slow", request=request)
        return httpx.Response(200, content=b"data: ok\n\n")

    with mock_client(handler) as client:
        messages = list(
            stream_messages(
                URL, user_agent=UA, client=client, max_reconnects=1, sleep=lambda _: None
            )
        )
    assert [m.data for m in messages] == ["ok"]

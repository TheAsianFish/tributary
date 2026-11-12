"""The HTTP API: keys, request ids in headers, bodies and logs, and status codes."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import structlog
from fastapi.testclient import TestClient
from structlog.testing import LogCapture

from tests.unit.agent_doubles import (
    FakeExecutor,
    ScriptedProvider,
    catalog,
    draft,
    make_agent,
    result,
)
from tributary import __version__
from tributary.agent.execute import ExecutionError
from tributary.agent.provider import ProviderError, ProviderRefusal
from tributary.agent.service import Agent, CatalogSource
from tributary.api.app import KeyCheck, create_app, parse_keys, redact, request_id_for
from tributary.api.limits import RateLimiter, retry_after

KEY = "s3cret-key"
GOOD = "SELECT count(*) AS edits FROM marts.fct_edits"
GOOD_RAN = "SELECT COUNT(*) AS edits FROM marts.fct_edits LIMIT 1000"


def client(agent: Agent | None = None, keys: tuple[str, ...] = (KEY,)) -> TestClient:
    return TestClient(create_app(agent or make_agent(), keys))


def auth(key: str = KEY, **extra: str) -> dict[str, str]:
    return {"X-API-Key": key, **extra}


@pytest.fixture
def logs() -> LogCapture:
    """Captured log entries with the context merged in (conftest resets structlog)."""
    capture = LogCapture()
    structlog.configure(processors=[structlog.contextvars.merge_contextvars, capture])
    return capture


def test_health_is_open() -> None:
    response = client().get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "version": __version__}


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}, {"X-API-Key": ""}])
@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("POST", "/query", {"question": "q"}),
        ("POST", "/sql", {"sql": GOOD}),
        ("GET", "/catalog", None),
        ("GET", "/freshness", None),
    ],
)
def test_unauthorised(
    headers: dict[str, str], method: str, path: str, body: dict[str, str] | None
) -> None:
    executor = FakeExecutor()
    response = client(make_agent(executor=executor)).request(
        method, path, json=body, headers=headers
    )
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "ApiKey"
    assert "X-Request-ID" in response.headers
    assert executor.ran == []


@pytest.mark.parametrize("key", ["first", "second"])
def test_any_configured_key_is_accepted(key: str) -> None:
    response = client(keys=("first", "second")).get("/catalog", headers=auth(key))
    assert response.status_code == 200


def test_no_keys_refuses_to_start() -> None:
    with pytest.raises(ValueError, match="no API key configured"):
        create_app(make_agent(), [])
    with pytest.raises(ValueError, match="no API key configured"):
        create_app(make_agent(), [""])


def test_parse_keys() -> None:
    assert parse_keys(" a, b ,,c ") == ["a", "b", "c"]
    assert parse_keys("") == []


@pytest.mark.parametrize(
    ("header", "kept"),
    [
        ("abc-123_x.y", True),
        ("a" * 64, True),
        ("a" * 65, False),
        ("has space", False),
        ("new\nline", False),
        ("semi;colon", False),
        ("", False),
        (None, False),
    ],
)
def test_request_id_for(header: str | None, kept: bool) -> None:
    rid = request_id_for(header)
    if kept:
        assert rid == header
    else:
        assert rid != header
        assert len(rid) == 32


def test_query_returns_the_answer_document(logs: LogCapture) -> None:
    provider = ScriptedProvider([draft(GOOD)])
    executor = FakeExecutor({GOOD_RAN: result(rows=[(42,)])})
    response = client(make_agent(provider, executor)).post(
        "/query", json={"question": "How many edits?"}, headers=auth(**{"X-Request-ID": "req-1"})
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "answered"
    assert body["sql"] == GOOD_RAN
    assert body["rows"] == [[42]]
    assert body["answer"] == "The number of edits. edits: 42."
    assert body["request_id"] == "req-1"
    assert response.headers["X-Request-ID"] == "req-1"
    assert provider.requests[0].messages[0].content == "Question: How many edits?"

    http = [e for e in logs.entries if e["event"] == "http.request"]
    assert http == [
        {
            "event": "http.request",
            "log_level": "info",
            "request_id": "req-1",
            "method": "POST",
            "path": "/query",
            "status": 200,
            "latency_ms": http[0]["latency_ms"],
        }
    ]
    inside = [e for e in logs.entries if e["event"] == "planner.done"]
    assert len(inside) == 1
    assert inside[0]["request_id"] == "req-1"


def test_generated_request_id(logs: LogCapture) -> None:
    response = client().get("/catalog", headers=auth())
    rid = response.headers["X-Request-ID"]
    assert len(rid) == 32
    assert [e["request_id"] for e in logs.entries if e["event"] == "http.request"] == [rid]


@pytest.mark.parametrize("unsafe", ["has space", "x" * 65, "bad;id"])
def test_unsafe_request_id_is_replaced(unsafe: str, logs: LogCapture) -> None:
    provider = ScriptedProvider([draft(GOOD)])
    response = client(make_agent(provider)).post(
        "/query", json={"question": "q"}, headers=auth(**{"X-Request-ID": unsafe})
    )
    rid = response.headers["X-Request-ID"]
    assert rid != unsafe
    assert len(rid) == 32
    assert response.json()["request_id"] == rid
    assert all(e.get("request_id") == rid for e in logs.entries)
    assert {"http.request", "planner.done"} <= {e["event"] for e in logs.entries}


def test_request_ids_do_not_leak_between_requests(logs: LogCapture) -> None:
    c = client()
    c.get("/catalog", headers=auth(**{"X-Request-ID": "one"}))
    c.get("/catalog", headers=auth(**{"X-Request-ID": "two"}))
    ids = [e["request_id"] for e in logs.entries if e["event"] == "http.request"]
    assert ids == ["one", "two"]


def test_sql_allowed() -> None:
    executor = FakeExecutor()
    response = client(make_agent(executor=executor)).post(
        "/sql", json={"sql": GOOD}, headers=auth()
    )
    assert response.status_code == 200
    assert response.json()["sql"] == GOOD_RAN
    assert executor.ran == [GOOD_RAN]


def test_sql_blocked_is_422() -> None:
    executor = FakeExecutor()
    response = client(make_agent(executor=executor)).post(
        "/sql", json={"sql": "DELETE FROM marts.fct_edits"}, headers=auth()
    )
    assert response.status_code == 422
    body = response.json()
    assert body["status"] == "blocked"
    assert body["request_id"] == response.headers["X-Request-ID"]
    assert executor.ran == []


def test_sql_too_large_is_422() -> None:
    error = ExecutionError("too_large", "the result reached 9 bytes, over the cap of 8", "22P02")
    executor = FakeExecutor(errors={GOOD_RAN: error})
    response = client(make_agent(executor=executor)).post(
        "/sql", json={"sql": GOOD}, headers=auth()
    )
    assert response.status_code == 422
    body = response.json()
    assert body["status"] == "failed"
    assert body["answer"] == error.feedback()


@pytest.mark.parametrize(
    ("path", "body"), [("/query", {"question": ""}), ("/sql", {"sql": ""}), ("/query", {})]
)
def test_invalid_bodies_are_422(path: str, body: dict[str, Any]) -> None:
    assert client().post(path, json=body, headers=auth()).status_code == 422


def test_catalog() -> None:
    response = client().get("/catalog", headers=auth())
    assert response.status_code == 200
    assert response.json() == catalog().to_dict()


def test_freshness() -> None:
    response = client().get("/freshness", headers=auth(**{"X-Request-ID": "f1"}))
    assert response.status_code == 200
    body = response.json()
    assert body["request_id"] == "f1"
    assert body["catalog_version"] == catalog().version
    assert body["stale_after_minutes"] == 120
    assert [t["relation"] for t in body["tables"]] == list(catalog().relations())


@pytest.mark.parametrize(
    ("error", "status", "detail"),
    [
        (ProviderRefusal("declined"), 422, "the model declined the question: declined"),
        (ProviderError("overloaded"), 502, "model provider failed: overloaded"),
    ],
)
def test_provider_errors(error: Exception, status: int, detail: str) -> None:
    response = client(make_agent(ScriptedProvider([error]))).post(
        "/query", json={"question": "q"}, headers=auth(**{"X-Request-ID": "p1"})
    )
    assert response.status_code == status
    assert response.json() == {"detail": detail, "request_id": "p1"}
    assert response.headers["X-Request-ID"] == "p1"


def test_no_catalog_is_503(tmp_path: Path) -> None:
    agent = make_agent()
    agent.catalogs = CatalogSource(tmp_path, ("marts",))
    response = client(agent).get("/catalog", headers=auth())
    assert response.status_code == 503
    assert response.json()["detail"].startswith("no published catalog under")


def test_agent_factory_is_called_once() -> None:
    built: list[Agent] = []

    def factory() -> Agent:
        built.append(make_agent())
        return built[-1]

    c = TestClient(create_app(factory, [KEY]))
    assert c.get("/health").status_code == 200
    assert built == []
    c.get("/catalog", headers=auth())
    c.get("/freshness", headers=auth())
    assert len(built) == 1


@pytest.mark.parametrize(
    ("kind", "status", "start"),
    [
        ("connection", 503, "warehouse unavailable: "),
        ("permission", 500, "the agent's role is misconfigured (permission): "),
    ],
)
def test_deployment_failures_are_the_servers_fault(kind: str, status: int, start: str) -> None:
    error = ExecutionError(kind, "boom")  # type: ignore[arg-type]
    provider = ScriptedProvider([draft(GOOD)] * 3)
    agent = make_agent(provider, executor=FakeExecutor(errors={GOOD_RAN: error}))
    c = client(agent)

    for path, body in (("/query", {"question": "q"}), ("/sql", {"sql": GOOD})):
        response = c.post(path, json=body, headers=auth(**{"X-Request-ID": "d1"}))
        assert response.status_code == status, response.text
        assert response.json()["detail"].startswith(start)
        assert response.json()["request_id"] == "d1"
        assert response.headers["X-Request-ID"] == "d1"
    assert len(provider.requests) == 1, "the outage was not repaired with more model calls"


def test_concurrent_first_requests_build_one_agent() -> None:
    import threading
    import time

    built: list[Agent] = []
    lock = threading.Lock()

    def factory() -> Agent:
        time.sleep(0.05)  # long enough for every thread to arrive before the first returns
        with lock:
            built.append(make_agent())
        return built[-1]

    c = TestClient(create_app(factory, [KEY]))
    statuses: list[int] = []

    def hit() -> None:
        statuses.append(c.get("/catalog", headers=auth()).status_code)

    threads = [threading.Thread(target=hit) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert statuses == [200] * 8
    assert len(built) == 1


# Security pass: rate limit, metrics, body cap, headers, docs, redaction.


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def limited_client(
    clock: FakeClock, *, per_minute: int = 60, burst: int = 2, keys: tuple[str, ...] = (KEY,)
) -> TestClient:
    app = create_app(
        make_agent(), keys, rate_limit_per_minute=per_minute, rate_limit_burst=burst, clock=clock
    )
    return TestClient(app)


def test_rate_limit_answers_429_with_retry_after_then_refills() -> None:
    clock = FakeClock()
    c = limited_client(clock, per_minute=60, burst=2)
    assert [c.get("/catalog", headers=auth()).status_code for _ in range(2)] == [200, 200]

    refused = c.get("/catalog", headers=auth(**{"X-Request-ID": "r1"}))
    assert refused.status_code == 429
    assert refused.headers["Retry-After"] == "1"
    assert refused.json() == {
        "detail": "rate limit exceeded: 60 requests per minute, burst 2, per API key",
        "request_id": "r1",
    }
    assert refused.headers["X-Request-ID"] == "r1"

    clock.now += 0.5
    assert c.get("/catalog", headers=auth()).status_code == 429
    clock.now += 0.5
    assert c.get("/catalog", headers=auth()).status_code == 200
    assert c.get("/catalog", headers=auth()).status_code == 429


def test_retry_after_is_rounded_up() -> None:
    clock = FakeClock()
    c = limited_client(clock, per_minute=7, burst=1)
    assert c.post("/sql", json={"sql": GOOD}, headers=auth()).status_code == 200
    refused = c.post("/sql", json={"sql": GOOD}, headers=auth())
    assert refused.status_code == 429
    assert refused.headers["Retry-After"] == "9"  # 60 / 7 = 8.57 s
    clock.now += 8.0
    assert c.post("/sql", json={"sql": GOOD}, headers=auth()).headers["Retry-After"] == "1"


def test_rate_limit_is_per_key_and_skips_health_metrics_and_failed_auth() -> None:
    clock = FakeClock()
    c = limited_client(clock, burst=1, keys=("first", "second"))
    for _ in range(5):
        assert c.get("/catalog", headers=auth("wrong")).status_code == 401
        assert c.get("/health").status_code == 200
        assert c.get("/metrics", headers=auth("first")).status_code == 200
    assert c.get("/catalog", headers=auth("first")).status_code == 200
    assert c.get("/catalog", headers=auth("first")).status_code == 429
    assert c.get("/freshness", headers=auth("second")).status_code == 200


def test_rate_limit_zero_disables_it() -> None:
    c = limited_client(FakeClock(), per_minute=0, burst=1)
    assert {c.get("/catalog", headers=auth()).status_code for _ in range(30)} == {200}


def test_rate_limiter_is_thread_safe() -> None:
    import threading

    limiter = RateLimiter(60, 10, clock=lambda: 0.0)
    granted: list[float] = []
    lock = threading.Lock()

    def take() -> None:
        for _ in range(20):
            wait = limiter.acquire(b"digest")
            with lock:
                granted.append(wait)

    threads = [threading.Thread(target=take) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(1 for w in granted if w == 0.0) == 10
    assert len(granted) == 160


def test_rate_limiter_rejects_bad_settings() -> None:
    with pytest.raises(ValueError, match="burst"):
        RateLimiter(60, 0)
    with pytest.raises(ValueError, match="per minute"):
        RateLimiter(-1, 1)
    assert retry_after(0.01) == "1"
    assert retry_after(2.0) == "2"


def test_buckets_are_keyed_by_digest_not_the_key() -> None:
    assert KeyCheck([KEY])(KEY) == hashlib.sha256(KEY.encode()).digest()


def test_unknown_methods_share_one_metrics_series() -> None:
    c = client()
    for i in range(50):
        c.request(f"X{i:05d}", "/health")  # unauthenticated, answered 405
    text = c.get("/metrics", headers=auth()).text
    assert 'tributary_http_requests_total{route="/health",method="other",status="405"} 50' in text
    assert "X00000" not in text


def test_metrics_need_the_key_and_skip_it_when_public() -> None:
    assert client().get("/metrics").status_code == 401
    assert client().get("/metrics", headers=auth("wrong")).status_code == 401
    public = TestClient(create_app(make_agent(), [KEY], metrics_public=True))
    response = public.get("/metrics")
    assert response.status_code == 200
    assert response.headers["Content-Type"] == "text/plain; version=0.0.4; charset=utf-8"


def test_metrics_count_requests_answers_tokens_and_refusals() -> None:
    provider = ScriptedProvider([draft(GOOD)])
    executor = FakeExecutor({GOOD_RAN: result(rows=[(42,)])})
    app = create_app(make_agent(provider, executor), [KEY], rate_limit_burst=4, clock=FakeClock())
    c = TestClient(app)
    answer = c.post("/query", json={"question": "q"}, headers=auth()).json()
    c.post("/sql", json={"sql": GOOD}, headers=auth())
    c.post("/sql", json={"sql": "DELETE FROM marts.fct_edits"}, headers=auth())
    c.get("/catalog", headers=auth())
    c.get("/catalog", headers=auth())  # the fifth: rate limited
    c.get("/catalog")  # no key
    c.get("/no/such/path/42", headers=auth())

    text = c.get("/metrics", headers=auth()).text
    lines = set(text.splitlines())
    expected = {
        'tributary_http_requests_total{route="/query",method="POST",status="200"} 1',
        'tributary_http_requests_total{route="/sql",method="POST",status="200"} 1',
        'tributary_http_requests_total{route="/sql",method="POST",status="422"} 1',
        'tributary_http_requests_total{route="/catalog",method="GET",status="200"} 1',
        'tributary_http_requests_total{route="/catalog",method="GET",status="429"} 1',
        'tributary_http_requests_total{route="/catalog",method="GET",status="401"} 1',
        'tributary_http_requests_total{route="unmatched",method="GET",status="404"} 1',
        'tributary_answers_total{route="/query",status="answered"} 1',
        'tributary_answers_total{route="/sql",status="answered"} 1',
        'tributary_answers_total{route="/sql",status="blocked"} 1',
        "tributary_rate_limited_total 1",
        'tributary_http_request_duration_seconds_count{route="/catalog"} 3',
        'tributary_http_request_duration_seconds_bucket{route="/catalog",le="+Inf"} 3',
        "# TYPE tributary_http_request_duration_seconds histogram",
        "# TYPE tributary_http_requests_total counter",
        f'tributary_model_tokens_total{{direction="input"}} {answer["usage"]["input_tokens"]}',
        f'tributary_model_tokens_total{{direction="output"}} {answer["usage"]["output_tokens"]}',
    }
    assert expected <= lines, expected - lines
    assert answer["usage"]["input_tokens"] > 0
    cost = next(x for x in lines if x.startswith("tributary_model_cost_usd_total "))
    assert float(cost.split()[1]) == pytest.approx(answer["usage"]["cost_usd"])
    assert "/no/such/path" not in text
    assert KEY not in text


@pytest.mark.parametrize("path", ["/health", "/catalog", "/nowhere"])
@pytest.mark.parametrize("key", [KEY, "wrong"])
def test_security_headers_on_every_response(path: str, key: str) -> None:
    response = client().get(path, headers=auth(key))
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["X-Frame-Options"] == "DENY"
    assert response.headers["Content-Security-Policy"] == (
        "default-src 'none'; frame-ancestors 'none'"
    )
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["Referrer-Policy"] == "no-referrer"


def test_body_over_the_cap_is_413_before_parsing() -> None:
    executor = FakeExecutor()
    c = TestClient(create_app(make_agent(executor=executor), [KEY], max_body_bytes=1024))
    big = {"sql": GOOD + " " * 2000}
    response = c.post("/sql", json=big, headers=auth(**{"X-Request-ID": "b1"}))
    assert response.status_code == 413
    assert response.json() == {"detail": "request body over 1024 bytes", "request_id": "b1"}
    assert response.headers["X-Request-ID"] == "b1"
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert executor.ran == []
    assert c.post("/sql", json={"sql": GOOD}, headers=auth()).status_code == 200


def test_chunked_body_over_the_cap_is_413() -> None:
    import json

    executor = FakeExecutor()
    c = TestClient(create_app(make_agent(executor=executor), [KEY], max_body_bytes=1024))
    payload = json.dumps({"sql": GOOD + " " * 2000}).encode()

    def chunks() -> Iterator[bytes]:
        for i in range(0, len(payload), 256):
            yield payload[i : i + 256]

    headers = auth(**{"Content-Type": "application/json"})
    response = c.post("/sql", content=chunks(), headers=headers)
    assert "content-length" not in {k.lower() for k in response.request.headers}
    assert response.status_code == 413
    assert executor.ran == []

    small = json.dumps({"sql": GOOD}).encode()
    ok = c.post("/sql", content=iter([small[:10], small[10:]]), headers=headers)
    assert ok.status_code == 200


def test_docs_are_off_by_default_and_on_when_asked() -> None:
    for path in ("/docs", "/redoc", "/openapi.json"):
        assert client().get(path, headers=auth()).status_code == 404
    with_docs = TestClient(create_app(make_agent(), [KEY], docs=True))
    assert with_docs.get("/openapi.json").status_code == 200
    page = with_docs.get("/docs")
    assert page.status_code == 200
    assert "Content-Security-Policy" not in page.headers
    assert "/metrics" not in with_docs.get("/openapi.json").json()["paths"]


def test_provider_errors_are_redacted(monkeypatch: pytest.MonkeyPatch, logs: LogCapture) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-api03-SECRETSECRET")
    error = ProviderError(
        f"could not reach Ollama at http://admin:hunter2@ollama:11434 (key {KEY}); "
        "sent sk-ant-api03-SECRETSECRET with password=pw123 " + "x" * 1000
    )
    response = client(make_agent(ScriptedProvider([error]))).post(
        "/query", json={"question": "q"}, headers=auth()
    )
    assert response.status_code == 502
    detail = response.json()["detail"]
    assert detail.startswith("model provider failed: could not reach Ollama at http://***@ollama")
    assert len(detail) < 600
    everything = detail + repr(logs.entries)
    for secret in ("hunter2", KEY, "SECRETSECRET", "pw123"):
        assert secret not in everything


def test_warehouse_errors_are_redacted() -> None:
    error = ExecutionError("connection", "cannot connect: host=pg password=topsecret")
    provider = ScriptedProvider([draft(GOOD)])
    agent = make_agent(provider, executor=FakeExecutor(errors={GOOD_RAN: error}))
    response = client(agent).post("/sql", json={"sql": GOOD}, headers=auth())
    assert response.status_code == 503
    assert (
        response.json()["detail"] == "warehouse unavailable: cannot connect: host=pg password=***"
    )


def test_redact_leaves_plain_text_alone() -> None:
    assert redact("Ollama at http://ollama:11434 timed out") == (
        "Ollama at http://ollama:11434 timed out"
    )


def test_the_key_is_never_logged(logs: LogCapture) -> None:
    c = client()
    c.get("/catalog", headers=auth())
    c.get("/catalog", headers=auth("wrong-key-attempt"))
    c.post("/sql", json={"sql": GOOD}, headers=auth())
    assert logs.entries
    assert KEY not in repr(logs.entries)
    assert "wrong-key-attempt" not in repr(logs.entries)


def test_settings_defaults_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    from tributary.config import Settings

    defaults = Settings(_env_file=None)
    assert defaults.api_rate_limit_per_minute == 60
    assert defaults.api_rate_limit_burst == 10
    assert defaults.api_max_body_bytes == 65536
    assert defaults.api_metrics_public is False
    assert defaults.api_docs is False
    monkeypatch.setenv("TRIBUTARY_API_RATE_LIMIT_PER_MINUTE", "0")
    monkeypatch.setenv("TRIBUTARY_API_DOCS", "true")
    changed = Settings(_env_file=None)
    assert changed.api_rate_limit_per_minute == 0
    assert changed.api_docs is True

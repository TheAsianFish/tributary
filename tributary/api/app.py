"""FastAPI: ``/query``, ``/sql``, ``/catalog``, ``/freshness``, behind an API key.

Every request gets a request id (the caller's ``X-Request-ID`` if it is a
short token, else a fresh one). It is bound into the log context, so the
request's log lines, including each model call's tokens and cost, carry it,
and it is returned in the ``X-Request-ID`` header and in every answer body.

Security model:

- **Authentication** is a shared key in ``X-API-Key``. Keys are compared as
  SHA-256 digests with a constant-time comparison against every configured
  key, so the check takes the same time whichever key, if any, is close. With
  no key configured the app refuses to start: an open text-to-SQL endpoint is
  never the default. ``/health`` is the only route open by default; the key is
  never logged.
- **Rate limit**: a token bucket per key (by digest), ``burst`` requests at
  once refilled at ``per_minute``; past it, 429 with ``Retry-After``. The
  buckets live in the process: each uvicorn worker or replica limits on its
  own (``tributary/api/limits.py``). Failed authentications are not limited:
  keys are 256-bit random tokens, and a per-IP limit would lock out every
  client behind one proxy address.
- **Body cap**: a body over ``max_body_bytes`` is refused with 413 before it
  is parsed, whether declared by ``Content-Length`` or sent chunked.
- **Headers**: every response carries ``X-Content-Type-Options: nosniff``,
  ``X-Frame-Options: DENY``, ``Content-Security-Policy: default-src 'none';
  frame-ancestors 'none'``, ``Referrer-Policy: no-referrer`` and
  ``Cache-Control: no-store``. The interactive docs (``/docs``, ``/redoc``,
  ``/openapi.json``) are off unless ``docs=True``.
- **Errors**: 5xx and provider-error bodies have URL credentials, ``password=``
  pairs, Anthropic keys and the configured API keys redacted, and are cut to
  a bounded length; an unhandled exception is answered ``internal error``.
- **Metrics**: ``/metrics`` is Prometheus text (``tributary/api/metrics.py``),
  behind the API key unless ``metrics_public=True``, and exempt from the rate
  limit so scraping never eats a client's budget.

HTTP status follows who is at fault. ``/query`` answers 200 whenever the
agent ran, with ``status`` saying whether it found an allowed query
(``answered``, ``unanswerable``, ``blocked``, ``failed``); ``/sql`` answers
422 when the caller's SQL is blocked or fails. A model refusal is 422, a
provider failure 502, no published catalog 503, an unreachable warehouse 503
and a privilege the agent's role lacks 500: the last two are the
deployment's fault, so they are never reported as a failed query.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import threading
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from typing import Annotated, Any

import structlog
from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from tributary import __version__
from tributary.agent.answer import Answer
from tributary.agent.execute import ExecutionError
from tributary.agent.provider import ProviderError, ProviderRefusal
from tributary.agent.service import Agent
from tributary.api.limits import BodyLimit, RateLimiter, retry_after
from tributary.api.metrics import CONTENT_TYPE, Metrics

log = structlog.get_logger(__name__)

REQUEST_ID_HEADER = "X-Request-ID"
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")

DEFAULT_RATE_LIMIT_PER_MINUTE = 60
DEFAULT_RATE_LIMIT_BURST = 10
DEFAULT_MAX_BODY_BYTES = 64 * 1024
MAX_ERROR_DETAIL = 500

SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Content-Security-Policy": "default-src 'none'; frame-ancestors 'none'",
    "Referrer-Policy": "no-referrer",
    "Cache-Control": "no-store",
}
DOCS_PATHS = ("/docs", "/redoc")

_REDACTIONS: tuple[tuple[re.Pattern[str], str], ...] = (
    # user:password@ in a URL (an Ollama or database URL with credentials).
    (re.compile(r"(?<=://)[^/\s@]+@"), "***@"),
    # libpq-style password=..., and the same in query strings.
    (re.compile(r"(?i)\b(password|passwd|pwd)=[^\s&;]+"), r"\1=***"),
    (re.compile(r"sk-ant-[A-Za-z0-9_-]+"), "sk-ant-***"),
)


class QueryRequest(BaseModel):
    question: str = Field(min_length=1, max_length=2000)


class SQLRequest(BaseModel):
    sql: str = Field(min_length=1, max_length=20_000)


def request_id_for(header: str | None) -> str:
    """The caller's id when it is a safe token (it ends up in logs), else a new one."""
    if header and _REQUEST_ID_RE.fullmatch(header):
        return header
    return uuid.uuid4().hex


def redact(text: str, secrets: Iterable[str] = ()) -> str:
    """``text`` without credentials, cut to a bounded length (for error bodies and logs)."""
    for secret in sorted((s for s in secrets if s), key=len, reverse=True):
        text = text.replace(secret, "***")
    for pattern, replacement in _REDACTIONS:
        text = pattern.sub(replacement, text)
    if len(text) > MAX_ERROR_DETAIL:
        text = text[: MAX_ERROR_DETAIL - 3] + "..."
    return text


def _digest(key: str) -> bytes:
    return hashlib.sha256(key.encode("utf-8")).digest()


class KeyCheck:
    def __init__(self, keys: Sequence[str]) -> None:
        digests = [_digest(k) for k in keys if k]
        if not digests:
            raise ValueError(
                "no API key configured: set TRIBUTARY_API_KEYS (comma-separated) before serving"
            )
        self._digests = digests

    def __call__(self, x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None) -> bytes:
        """The offered key's digest when it is a configured key; 401 otherwise."""
        offered = _digest(x_api_key or "")
        ok = False
        for digest in self._digests:
            ok |= hmac.compare_digest(offered, digest)
        if not ok:
            raise HTTPException(
                status_code=401,
                detail="missing or invalid API key (X-API-Key)",
                headers={"WWW-Authenticate": "ApiKey"},
            )
        return offered


def parse_keys(raw: str) -> list[str]:
    return [k.strip() for k in raw.split(",") if k.strip()]


def create_app(
    agent: Agent | Callable[[], Agent],
    api_keys: Sequence[str],
    *,
    rate_limit_per_minute: int = DEFAULT_RATE_LIMIT_PER_MINUTE,
    rate_limit_burst: int = DEFAULT_RATE_LIMIT_BURST,
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
    metrics_public: bool = False,
    docs: bool = False,
    clock: Callable[[], float] = time.monotonic,
) -> FastAPI:
    """The app around one agent (or a factory, called once on first use).

    ``clock`` is the rate limiter's monotonic clock (injected by tests).
    """
    check = KeyCheck(api_keys)
    limiter = RateLimiter(rate_limit_per_minute, rate_limit_burst, clock)
    metrics = Metrics()
    secrets = tuple(k for k in api_keys if k)
    app = FastAPI(
        title="Tributary",
        version=__version__,
        summary="Questions over the warehouse, answered with guarded SQL and provenance.",
        docs_url="/docs" if docs else None,
        redoc_url="/redoc" if docs else None,
        openapi_url="/openapi.json" if docs else None,
    )
    holder: dict[str, Agent] = {}
    building = threading.Lock()

    def get_agent() -> Agent:
        # Handlers run in worker threads: the first concurrent requests must share one
        # agent (and so one provider, one SDK client), not build one each.
        if "agent" not in holder:
            with building:
                if "agent" not in holder:
                    holder["agent"] = agent if isinstance(agent, Agent) else agent()
        return holder["agent"]

    def clean(text: str) -> str:
        anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
        return redact(text, (*secrets, anthropic_key))

    # Added before the request context below, so it runs inside it: a 413 still gets
    # the request id, the security headers and a line in the metrics.
    app.add_middleware(BodyLimit, max_bytes=max_body_bytes)

    @app.middleware("http")
    async def request_context(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        request_id = request_id_for(request.headers.get(REQUEST_ID_HEADER))
        request.state.request_id = request_id
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=request_id)
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            log.exception("http.unhandled", method=request.method, path=request.url.path)
            response = JSONResponse(
                {"detail": "internal error", "request_id": request_id}, status_code=500
            )
        elapsed = time.perf_counter() - started
        response.headers[REQUEST_ID_HEADER] = request_id
        for name, value in SECURITY_HEADERS.items():
            response.headers.setdefault(name, value)
        if docs and request.url.path in DOCS_PATHS:
            # The docs pages load their scripts and styles from a CDN.
            del response.headers["Content-Security-Policy"]
        route = getattr(request.scope.get("route"), "path", None) or "unmatched"
        metrics.request(route, request.method, response.status_code, elapsed)
        log.info(
            "http.request",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            latency_ms=round(elapsed * 1000, 1),
        )
        structlog.contextvars.clear_contextvars()
        return response

    def rid(request: Request) -> str:
        return str(request.state.request_id)

    def failure(
        status: int, detail: str, request: Request, headers: Mapping[str, str] | None = None
    ) -> JSONResponse:
        return JSONResponse(
            {"detail": detail, "request_id": rid(request)}, status_code=status, headers=headers
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        return failure(exc.status_code, str(exc.detail), request, exc.headers)

    @app.exception_handler(FileNotFoundError)
    async def no_catalog(request: Request, exc: FileNotFoundError) -> JSONResponse:
        return failure(503, clean(f"{exc}; run `tributary warehouse refresh`"), request)

    @app.exception_handler(ProviderRefusal)
    async def refused(request: Request, exc: ProviderRefusal) -> JSONResponse:
        return failure(422, clean(f"the model declined the question: {exc}"), request)

    @app.exception_handler(ProviderError)
    async def provider_failed(request: Request, exc: ProviderError) -> JSONResponse:
        error = clean(str(exc))
        log.warning("provider.failed", error=error)
        return failure(502, f"model provider failed: {error}", request)

    @app.exception_handler(ExecutionError)
    async def warehouse_failed(request: Request, exc: ExecutionError) -> JSONResponse:
        # Only the errors no query can repair reach here (the agent answers the others).
        message = clean(exc.message)
        log.warning("warehouse.failed", kind=exc.kind, error=message)
        if exc.kind == "connection":
            return failure(503, f"warehouse unavailable: {message}", request)
        return failure(
            500,
            f"the agent's role is misconfigured ({exc.kind}): {message}; "
            "run `tributary warehouse grant-agent`",
            request,
        )

    def limited(digest: bytes = Depends(check)) -> None:
        """Authenticated, then one token from the key's bucket."""
        # A default rather than Annotated: `check` is local, so a postponed annotation
        # naming it could not be resolved by FastAPI.
        wait = limiter.acquire(digest)
        if wait > 0:
            metrics.rate_limited()
            log.warning("http.rate_limited", retry_after_s=retry_after(wait))
            raise HTTPException(
                status_code=429,
                detail=(
                    f"rate limit exceeded: {limiter.per_minute} requests per minute, "
                    f"burst {limiter.burst}, per API key"
                ),
                headers={"Retry-After": retry_after(wait)},
            )

    def counted(route: str, answer: Answer) -> Answer:
        metrics.answer(
            route,
            answer.status,
            input_tokens=answer.input_tokens,
            output_tokens=answer.output_tokens,
            cost_usd=answer.cost_usd,
        )
        return answer

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    authed = [Depends(limited)]

    @app.get(
        "/metrics",
        dependencies=[] if metrics_public else [Depends(check)],
        include_in_schema=False,
    )
    def prometheus() -> Response:
        """Prometheus text exposition; authenticated, but exempt from the rate limit."""
        return Response(metrics.render(), media_type=CONTENT_TYPE)

    @app.post("/query", dependencies=authed)
    def query(body: QueryRequest, request: Request) -> dict[str, Any]:
        """Question → SQL → rows → answer with provenance."""
        answer = get_agent().ask(body.question, request_id=rid(request))
        return counted("/query", answer).to_dict()

    @app.post("/sql", dependencies=authed)
    def run_sql(body: SQLRequest, request: Request, response: Response) -> dict[str, Any]:
        """Caller-written SQL through the same guard, role and limits as the agent's."""
        answer = counted("/sql", get_agent().sql(body.sql, request_id=rid(request)))
        if answer.status != "answered":
            response.status_code = 422
        return answer.to_dict()

    @app.get("/catalog", dependencies=authed)
    def catalog() -> dict[str, Any]:
        """Tables, columns, descriptions, keys, lineage and indexed values the agent sees."""
        return get_agent().catalog().to_dict()

    @app.get("/freshness", dependencies=authed)
    def freshness(request: Request) -> dict[str, Any]:
        """Each relation's last rebuild and the status of its and its sources' checks."""
        current = get_agent()
        tables = current.freshness()
        return {
            "request_id": rid(request),
            "catalog_version": current.catalog().version,
            "stale_after_minutes": int(current.limits.stale_after.total_seconds() // 60),
            "tables": [t.to_dict() for t in tables],
        }

    return app


def app_from_settings() -> FastAPI:
    """``uvicorn --factory tributary.api.app:app_from_settings``: the app wired from env."""
    from tributary.agent.service import build_agent, linker_for
    from tributary.config import get_settings
    from tributary.logging import configure_logging

    settings = get_settings()
    configure_logging(settings.log_format, settings.log_level)
    # The agent itself is built on the first request; refuse an unavailable linker now,
    # so a misconfigured server fails at start-up instead of on every question.
    linker_for(settings.agent_linker, settings.agent_link_k)
    return create_app(
        lambda: build_agent(settings),
        parse_keys(settings.api_keys.get_secret_value()),
        rate_limit_per_minute=settings.api_rate_limit_per_minute,
        rate_limit_burst=settings.api_rate_limit_burst,
        max_body_bytes=settings.api_max_body_bytes,
        metrics_public=settings.api_metrics_public,
        docs=settings.api_docs,
    )

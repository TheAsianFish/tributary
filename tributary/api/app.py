"""FastAPI: ``/query``, ``/sql``, ``/catalog``, ``/freshness``, behind an API key.

Every request gets a request id (the caller's ``X-Request-ID`` if it is a
short token, else a fresh one). It is bound into the log context, so the
request's log lines, including each model call's tokens and cost, carry it,
and it is returned in the ``X-Request-ID`` header and in every answer body.

Authentication is a shared key in ``X-API-Key``. Keys are compared as SHA-256
digests with a constant-time comparison against every configured key, so the
check takes the same time whichever key, if any, is close. With no key
configured the app refuses to start: an open text-to-SQL endpoint is never
the default. ``/health`` is the only unauthenticated route.

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
import re
import threading
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from typing import Annotated, Any

import structlog
from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from tributary import __version__
from tributary.agent.execute import ExecutionError
from tributary.agent.provider import ProviderError, ProviderRefusal
from tributary.agent.service import Agent

log = structlog.get_logger(__name__)

REQUEST_ID_HEADER = "X-Request-ID"
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


class QueryRequest(BaseModel):
    question: str = Field(min_length=1, max_length=2000)


class SQLRequest(BaseModel):
    sql: str = Field(min_length=1, max_length=20_000)


def request_id_for(header: str | None) -> str:
    """The caller's id when it is a safe token (it ends up in logs), else a new one."""
    if header and _REQUEST_ID_RE.fullmatch(header):
        return header
    return uuid.uuid4().hex


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

    def __call__(self, x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None) -> None:
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


def parse_keys(raw: str) -> list[str]:
    return [k.strip() for k in raw.split(",") if k.strip()]


def create_app(agent: Agent | Callable[[], Agent], api_keys: Sequence[str]) -> FastAPI:
    """The app around one agent (or a factory, called once on first use)."""
    check = KeyCheck(api_keys)
    app = FastAPI(
        title="Tributary",
        version=__version__,
        summary="Questions over the warehouse, answered with guarded SQL and provenance.",
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
        response.headers[REQUEST_ID_HEADER] = request_id
        log.info(
            "http.request",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            latency_ms=round((time.perf_counter() - started) * 1000, 1),
        )
        structlog.contextvars.clear_contextvars()
        return response

    def rid(request: Request) -> str:
        return str(request.state.request_id)

    def failure(status: int, detail: str, request: Request) -> JSONResponse:
        return JSONResponse({"detail": detail, "request_id": rid(request)}, status_code=status)

    @app.exception_handler(FileNotFoundError)
    async def no_catalog(request: Request, exc: FileNotFoundError) -> JSONResponse:
        return failure(503, f"{exc}; run `tributary warehouse refresh`", request)

    @app.exception_handler(ProviderRefusal)
    async def refused(request: Request, exc: ProviderRefusal) -> JSONResponse:
        return failure(422, f"the model declined the question: {exc}", request)

    @app.exception_handler(ProviderError)
    async def provider_failed(request: Request, exc: ProviderError) -> JSONResponse:
        log.warning("provider.failed", error=str(exc))
        return failure(502, f"model provider failed: {exc}", request)

    @app.exception_handler(ExecutionError)
    async def warehouse_failed(request: Request, exc: ExecutionError) -> JSONResponse:
        # Only the errors no query can repair reach here (the agent answers the others).
        log.warning("warehouse.failed", kind=exc.kind, error=exc.message)
        if exc.kind == "connection":
            return failure(503, f"warehouse unavailable: {exc.message}", request)
        return failure(
            500,
            f"the agent's role is misconfigured ({exc.kind}): {exc.message}; "
            "run `tributary warehouse grant-agent`",
            request,
        )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    authed = [Depends(check)]

    @app.post("/query", dependencies=authed)
    def query(body: QueryRequest, request: Request) -> dict[str, Any]:
        """Question → SQL → rows → answer with provenance."""
        return get_agent().ask(body.question, request_id=rid(request)).to_dict()

    @app.post("/sql", dependencies=authed)
    def run_sql(body: SQLRequest, request: Request, response: Response) -> dict[str, Any]:
        """Caller-written SQL through the same guard, role and limits as the agent's."""
        answer = get_agent().sql(body.sql, request_id=rid(request))
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
        lambda: build_agent(settings), parse_keys(settings.api_keys.get_secret_value())
    )

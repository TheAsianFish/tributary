"""structlog set-up: JSON in containers, key-value on a terminal."""

from __future__ import annotations

import logging
import sys
import uuid
from typing import Literal

import structlog

LogFormat = Literal["auto", "json", "console"]


def configure_logging(fmt: LogFormat = "auto", level: str = "INFO") -> None:
    """Configure structlog and route stdlib logging (py4j, httpx) through it."""
    use_json = fmt == "json" or (fmt == "auto" and not sys.stderr.isatty())
    shared: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]
    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer()
        if use_json
        else structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
    )
    structlog.configure(
        processors=[*shared, structlog.processors.format_exc_info, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=False,
    )
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(foreign_pre_chain=shared, processor=renderer)
    )
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.WARNING)


def bind_run_id(command: str) -> str:
    """Tag every log line of this process with a fresh ``run_id`` and the command name."""
    run_id = uuid.uuid4().hex[:12]
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(run_id=run_id, command=command)
    return run_id

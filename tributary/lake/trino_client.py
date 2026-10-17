"""A thin Trino client: one connection per call, rows as tuples, timestamps in UTC."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

import trino.dbapi

if TYPE_CHECKING:
    from tributary.config import Settings

Row = tuple[object, ...]


def connect(settings: Settings) -> trino.dbapi.Connection:
    """Connect to the coordinator; no session catalog, so every name is qualified."""
    return trino.dbapi.Connection(
        host=settings.trino_host,
        port=settings.trino_port,
        user=settings.trino_user,
        http_scheme="http",
        source="tributary",
        timezone="UTC",
    )


def fetch(settings: Settings, sql: str, params: Sequence[object] | None = None) -> list[Row]:
    """Run one statement and return all its rows (Trino needs the result read to finish)."""
    with connect(settings) as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, list(params) if params is not None else None)
            return [tuple(r) for r in cur.fetchall()]
        finally:
            cur.close()

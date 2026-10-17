"""Iceberg table maintenance through Trino: compaction, snapshot expiry, orphan cleanup.

A streaming table commits once per trigger and writes at least one file per
hour each batch touches, so a day of minute triggers leaves thousands of small
files and snapshots. ``optimize`` rewrites each closed hour into few large
files (the hours still receiving rows are left alone so compaction never races
the stream for the same partition); expiry and orphan removal then delete what
no retained snapshot references. Expiry waits while the stream is down (see
``expire_snapshots``).
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import structlog

from tributary.lake.hours import closed_boundary, parse_interval, trino_timestamp
from tributary.lake.trino_client import Row, fetch

if TYPE_CHECKING:
    from tributary.config import Settings

_TABLE = re.compile(r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")
_RETENTION = re.compile(r"^(\d+)(s|m|h|d)$")
_RETENTION_UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}
_STREAM_QUERY_ID = "spark.sql.streaming.queryId"

log = structlog.get_logger(__name__)


def lake_table(settings: Settings) -> str:
    """The lake table's qualified name, checked because it is spliced into SQL."""
    if not _TABLE.match(settings.trino_lake_table):
        raise ValueError(f"not a catalog.schema.table name: {settings.trino_lake_table!r}")
    return settings.trino_lake_table


def _retention(settings: Settings) -> str:
    if not _RETENTION.match(settings.lake_snapshot_retention):
        raise ValueError(f"retention must look like '1d': {settings.lake_snapshot_retention!r}")
    return settings.lake_snapshot_retention


def retention_interval(settings: Settings) -> timedelta:
    """The retention (``'1d'``, Trino's duration form) as a ``timedelta``."""
    match = _RETENTION.match(_retention(settings))
    assert match is not None  # _retention has just matched the same pattern
    return parse_interval(f"{match.group(1)} {_RETENTION_UNITS[match.group(2)]}")


def _metrics(rows: list[Row]) -> dict[str, int]:
    """Trino's table procedures answer with (metric_name, metric_value) rows."""
    return {str(name): int(str(value)) for name, value in rows}


def newest_event(settings: Settings) -> datetime | None:
    """The lake's newest event time (answered from file statistics)."""
    rows = fetch(settings, f"SELECT max(event_ts) FROM {lake_table(settings)}")
    value = rows[0][0] if rows else None
    if value is not None and not isinstance(value, datetime):
        raise TypeError(f"max(event_ts) returned {value!r}")
    return value


def optimize_closed_hours(settings: Settings) -> int:
    """Compact every closed hour; returns the number of data files rewritten.

    The ``WHERE`` bound sits on an hour boundary, so Trino can map it onto
    whole ``hours(event_ts)`` partitions (it refuses a bound inside an hour).
    Hours already compacted to one file are skipped by Trino.
    """
    newest = newest_event(settings)
    if newest is None:
        log.info("lake.optimize_skipped", reason="empty table")
        return 0
    boundary = closed_boundary(newest, parse_interval(settings.watermark_delay))
    metrics = _metrics(
        fetch(
            settings,
            f"ALTER TABLE {lake_table(settings)} EXECUTE optimize"
            f" WHERE event_ts < {trino_timestamp(boundary)}",
        )
    )
    rewritten = metrics.get("rewritten_data_files_count", 0)
    log.info("lake.optimized", before=boundary.isoformat(), **metrics)
    return rewritten


def newest_stream_commit(settings: Settings) -> tuple[datetime | None, datetime]:
    """(newest snapshot committed by a Spark streaming query, Trino's current time).

    Both come from Trino, so the age of the commit does not depend on this
    host's clock.
    """
    catalog, schema, name = lake_table(settings).split(".")
    rows = fetch(
        settings,
        f'SELECT max(committed_at), current_timestamp FROM {catalog}.{schema}."{name}$snapshots"'
        f" WHERE element_at(summary, '{_STREAM_QUERY_ID}') IS NOT NULL",
    )
    newest, now = rows[0]
    if newest is not None and not isinstance(newest, datetime):
        raise TypeError(f"max(committed_at) returned {newest!r}")
    if not isinstance(now, datetime):
        raise TypeError(f"current_timestamp returned {now!r}")
    return newest, now


def expire_snapshots(settings: Settings) -> bool:
    """Expire snapshots older than the retention; returns whether expiry ran.

    Spark's Iceberg sink makes a replayed micro-batch idempotent by walking
    the current snapshot's ancestry for a snapshot whose summary carries its
    ``spark.sql.streaming.queryId`` and ``epochId``; if it finds none for the
    epoch it replays, it appends the batch again. While the stream is down,
    this job keeps running: ``optimize`` makes a new current snapshot and
    expiry, once the outage outlasts the retention, removes the stream's last
    snapshot from the ancestry, so the batch replayed on restart would be
    stored twice. Expiry is therefore skipped, with a warning, while the
    newest streaming snapshot is older than half the retention (or there is
    none): the margin keeps that snapshot well inside the retention between
    two hourly runs, and expiry resumes on its own once the stream commits
    again. The current snapshot is always kept either way.
    """
    retention = retention_interval(settings)
    newest, now = newest_stream_commit(settings)
    if newest is None or now - newest > retention / 2:
        log.warning(
            "lake.expiry_skipped",
            reason="no recent streaming snapshot; expiry could drop the sink's last commit",
            newest_stream_commit=None if newest is None else newest.isoformat(),
            retention=settings.lake_snapshot_retention,
        )
        return False
    metrics = _metrics(
        fetch(
            settings,
            f"ALTER TABLE {lake_table(settings)} EXECUTE expire_snapshots"
            f"(retention_threshold => '{_retention(settings)}')",
        )
    )
    log.info("lake.snapshots_expired", retention=settings.lake_snapshot_retention, **metrics)
    return True


def remove_orphan_files(settings: Settings) -> None:
    """Delete files under the table location that no snapshot references.

    The same retention protects files of a commit still in flight, which exist
    before the snapshot that will reference them.
    """
    metrics = _metrics(
        fetch(
            settings,
            f"ALTER TABLE {lake_table(settings)} EXECUTE remove_orphan_files"
            f"(retention_threshold => '{_retention(settings)}')",
        )
    )
    log.info("lake.orphans_removed", retention=settings.lake_snapshot_retention, **metrics)

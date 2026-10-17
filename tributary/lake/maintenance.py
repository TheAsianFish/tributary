"""Iceberg table maintenance through Trino: compaction, snapshot expiry, orphan cleanup.

A streaming table commits once per trigger and writes at least one file per
hour each batch touches, so a day of minute triggers leaves thousands of small
files and snapshots. ``optimize`` rewrites each closed hour into few large
files (the hours still receiving rows are left alone so compaction never races
the stream for the same partition); expiry and orphan removal then delete what
no retained snapshot references.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import TYPE_CHECKING

import structlog

from tributary.lake.hours import closed_boundary, parse_interval, trino_timestamp
from tributary.lake.trino_client import Row, fetch

if TYPE_CHECKING:
    from tributary.config import Settings

_TABLE = re.compile(r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")
_RETENTION = re.compile(r"^\d+(s|m|h|d)$")

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


def expire_snapshots(settings: Settings) -> None:
    """Expire snapshots older than the retention (the current one is always kept)."""
    metrics = _metrics(
        fetch(
            settings,
            f"ALTER TABLE {lake_table(settings)} EXECUTE expire_snapshots"
            f"(retention_threshold => '{_retention(settings)}')",
        )
    )
    log.info("lake.snapshots_expired", retention=settings.lake_snapshot_retention, **metrics)


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

"""Does a one-hour filter read only that hour's files? Measured, not assumed.

Runs ``EXPLAIN ANALYZE`` for a query restricted to one event-time hour and
compares what Trino's table scan actually read (rows, splits) with the
table's own ``$files`` metadata for that hour's partition. Files this small
are one split each, so with pruning the scan opens exactly the hour's files
and reads exactly their rows; without it, it would open every file.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from tributary.lake.hours import floor_hour
from tributary.lake.maintenance import lake_table
from tributary.lake.trino_client import fetch

if TYPE_CHECKING:
    from tributary.config import Settings

_SCAN_INPUT = re.compile(r"Input: (?P<rows>\d+) rows .*?Splits: (?P<splits>\d+)")


@dataclass(frozen=True, slots=True)
class PruningReport:
    hour: datetime
    hour_files: int
    hour_rows: int
    table_files: int
    table_rows: int
    scan_rows: int
    scan_splits: int
    plan: str

    @property
    def pruned(self) -> bool:
        """The scan read exactly the hour's files and rows, and that is not the whole table.

        An hour without files proves nothing: a scan that reads nothing then
        matches it whether or not the filter was pushed down, so it does not count.
        """
        return (
            self.hour_files > 0
            and self.scan_splits == self.hour_files
            and self.scan_rows == self.hour_rows
            and self.hour_files < self.table_files
        )


def scan_stats(plan: str, table: str) -> tuple[int, int]:
    """(input rows, splits) of the scan of ``table`` constrained on ``event_ts``."""
    catalog, schema, name = table.split(".")
    scan = re.compile(
        rf"TableScan\[table = {re.escape(catalog)}:{re.escape(schema)}\.{re.escape(name)}\b"
        r"[^\n]*constraint on \[event_ts\]"
    )
    match = scan.search(plan)
    if match is None:
        raise ValueError(f"no event_ts-constrained scan of {table} in the plan")
    stats = _SCAN_INPUT.search(plan, match.end())
    if stats is None:
        raise ValueError("no Input/Splits line after the scan")
    return int(stats["rows"]), int(stats["splits"])


def _int(value: object) -> int:
    return 0 if value is None else int(str(value))


def check_hour(settings: Settings, hour: datetime) -> PruningReport:
    """Measure the scan of ``[hour, hour + 1h)`` against ``$files``."""
    hour = floor_hour(hour)
    table = lake_table(settings)
    catalog, schema, name = table.split(".")
    files = f'{catalog}.{schema}."{name}$files"'
    hour_files, hour_rows = fetch(
        settings,
        f"SELECT count(*), sum(record_count) FROM {files} WHERE partition.event_ts_hour = ?",
        [int(hour.timestamp()) // 3600],
    )[0]
    table_files, table_rows = fetch(settings, f"SELECT count(*), sum(record_count) FROM {files}")[0]
    plan = str(
        fetch(
            settings,
            f"EXPLAIN ANALYZE SELECT count(DISTINCT event_id) FROM {table}"
            " WHERE event_ts >= ? AND event_ts < ?",
            [hour, hour + timedelta(hours=1)],
        )[0][0]
    )
    scan_rows, scan_splits = scan_stats(plan, table)
    return PruningReport(
        hour=hour,
        hour_files=_int(hour_files),
        hour_rows=_int(hour_rows),
        table_files=_int(table_files),
        table_rows=_int(table_rows),
        scan_rows=scan_rows,
        scan_splits=scan_splits,
        plan=plan,
    )

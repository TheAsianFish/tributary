"""Load events straight into ``raw.recentchange``, bypassing Kafka and Spark.

For working on the dbt models and the DAGs without the streaming stack (and for
their CI job, which has only PostgreSQL). Rows written this way carry
``kafka_partition = -1`` and the event's position in the input as
``kafka_offset``, so they are never mistaken for streamed rows. The write path
is the stream job's own sink, so a reload is idempotent.
"""

from __future__ import annotations

import itertools
from collections.abc import Iterable
from datetime import UTC, datetime

import psycopg
import structlog

from tributary.events import Event, InvalidEventError, to_row
from tributary.stream.sink import ensure_schema, write_rows

DIRECT_PARTITION = -1
"""``kafka_partition`` of a row loaded without Kafka."""

log = structlog.get_logger(__name__)


def load_events(
    conninfo: str, events: Iterable[Event], *, chunk: int = 5_000, now: datetime | None = None
) -> tuple[int, int, int]:
    """Insert events in chunks; returns (offered, inserted, invalid)."""
    kafka_ts = now or datetime.now(UTC)
    ensure_schema(conninfo)
    offered = inserted = invalid = 0
    numbered = enumerate(events)
    with psycopg.connect(conninfo) as conn:
        while batch := list(itertools.islice(numbered, chunk)):
            rows = []
            for position, event in batch:
                try:
                    row = to_row(
                        event, partition=DIRECT_PARTITION, offset=position, kafka_ts=kafka_ts
                    )
                except InvalidEventError:
                    invalid += 1
                    continue
                rows.append(row.as_tuple())
            result = write_rows(conn, rows)
            offered += result.rows
            inserted += result.inserted
    log.info("sample.loaded", offered=offered, inserted=inserted, invalid=invalid)
    return offered, inserted, invalid

"""The fault benchmark refuses a stack whose watermark has run ahead of the clock."""

from __future__ import annotations

import psycopg
import pytest

from bench.faults import refuse_future_watermark

pytestmark = pytest.mark.warehouse


def test_a_future_stamped_event_stops_the_fault_run_before_it_produces(conninfo: str) -> None:
    refuse_future_watermark(conninfo)  # the sample is all in the past
    with psycopg.connect(conninfo) as conn:
        conn.execute("CREATE TEMP TABLE future AS SELECT * FROM raw.recentchange LIMIT 1")
        conn.execute(
            "UPDATE future SET event_id = gen_random_uuid(),"
            " event_ts = clock_timestamp() + interval '2 hours'"
        )
        (event_id,) = conn.execute(
            "INSERT INTO raw.recentchange SELECT * FROM future RETURNING event_id"
        ).fetchone() or (None,)
    try:
        with pytest.raises(RuntimeError, match="stamped in the future"):
            refuse_future_watermark(conninfo)
    finally:
        with psycopg.connect(conninfo) as conn:
            conn.execute("DELETE FROM raw.recentchange WHERE event_id = %s", (event_id,))
    refuse_future_watermark(conninfo)

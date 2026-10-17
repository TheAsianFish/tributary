"""
### raw_recentchange

Every five minutes, asks PostgreSQL whether `raw.recentchange` received rows
since the last time this DAG emitted the **raw.recentchange** asset event. If
it did, the task emits an event carrying the new high-water mark
(`max(ingested_at)`) in its `extra`; if not, it is skipped, no event is
emitted, and `warehouse_refresh` does not rebuild for nothing.

The high-water mark lives in the asset events themselves: the task lists
`raw.recentchange` as an inlet as well as an outlet and reads the newest
event's `extra`. That keeps the state next to the thing it describes (visible
on the asset's page, cleared with it) and needs no Airflow Variable that could
drift from the event history. A plain DAG rather than `@asset` because an
`@asset` function cannot read its own previous events (its inlets are only the
other assets named in its signature).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow.sdk import Asset, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from tributary.config import get_settings
from tributary.warehouse import assets
from tributary.warehouse.refresh import next_high_water_mark

RAW = Asset(name=assets.RAW_RECENTCHANGE, group=assets.GROUP)
# Events created by hand (the UI's "materialize", the REST API) carry no mark; look
# a little further back for the newest event that does.
EVENTS_SCANNED = 20


@dag(
    dag_id="raw_recentchange",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=4),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["warehouse", "asset"],
    doc_md=__doc__,
)
def raw_recentchange() -> None:
    @task(inlets=[RAW], outlets=[RAW])
    def detect_new_rows() -> str:
        context = get_current_context()
        previous: str | None = None
        for event in context["inlet_events"][RAW].ascending(False).limit(EVENTS_SCANNED):
            mark = (event.extra or {}).get(assets.HIGH_WATER_KEY)
            if isinstance(mark, str):
                previous = mark
                break
        # Settings come from TRIBUTARY_* like every other component, not from an
        # Airflow connection: one configuration source for the whole platform.
        newest = next_high_water_mark(get_settings().pg_conninfo, previous)
        if newest is None:
            raise AirflowSkipException(f"no rows ingested after {previous}")
        context["outlet_events"][RAW].extra = {
            assets.HIGH_WATER_KEY: newest,
            "previous": previous,
        }
        return newest

    detect_new_rows()


raw_recentchange()

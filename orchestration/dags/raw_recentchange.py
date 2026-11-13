"""
### raw_recentchange

Every five minutes, asks PostgreSQL whether `raw.recentchange` received rows
since the last time this DAG emitted the **raw.recentchange** asset event. If
it did, the task emits an event carrying the new high-water mark
(`max(ingested_at)`) in its `extra`; if not, no event is emitted and
`warehouse_refresh` does not rebuild for nothing.

Before skipping, the task runs the dbt `freshness` tests alone
(`dbt test --select test_name:freshness`) and records them in
`ops.quality_runs` under this run's `run_id`. Those tests otherwise run only
inside a refresh, which only new rows trigger: a stalled sink would leave the
last `pass` in `ops.latest_quality` forever. If every freshness check passes
the task is skipped as before; if one fails, the task fails (no retry) and the
run goes red.

Beside it, `check_stream` records the `stream_loss` checks (rows the stream
job's watermark dropped and records its parser rejected, over the last
`TRIBUTARY_STREAM_CHECK_WINDOW_HOURS`) in `ops.quality_runs`. That loss never
reaches a table, so no dbt test can see it; the check fails the task (no
retry) when it is over its threshold, and an answer's provenance names it.

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
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

from tributary.config import get_settings
from tributary.warehouse import assets
from tributary.warehouse.refresh import WarehouseError, check_freshness, next_high_water_mark
from tributary.warehouse.stream_health import StreamLossError
from tributary.warehouse.stream_health import check_stream as record_stream_checks

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
        settings = get_settings()
        newest = next_high_water_mark(settings.pg_conninfo, previous)
        if newest is None:
            # No refresh will run, so freshness is checked here or not at all.
            try:
                checks = check_freshness(settings, context["run_id"])
            except WarehouseError as exc:  # recorded in ops.quality_runs before raising
                raise AirflowFailException(f"no rows ingested after {previous}: {exc}") from exc
            raise AirflowSkipException(
                f"no rows ingested after {previous}; {len(checks)} freshness check(s) passed"
            )
        context["outlet_events"][RAW].extra = {
            assets.HIGH_WATER_KEY: newest,
            "previous": previous,
        }
        return newest

    @task
    def check_stream() -> int:
        try:
            return len(record_stream_checks(get_settings(), get_current_context()["run_id"]))
        except StreamLossError as exc:  # recorded in ops.quality_runs before raising
            raise AirflowFailException(str(exc)) from exc

    detect_new_rows()
    check_stream()


raw_recentchange()

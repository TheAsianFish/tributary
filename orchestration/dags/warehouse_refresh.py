"""
### warehouse_refresh

Rebuilds the warehouse whenever **raw.recentchange** has new rows:

1. `build_models`: `dbt seed` + `dbt run` (staging views, mart tables); every
   build lands in `ops.refresh_runs`.
2. `test_models`: `dbt test`; every result (generic tests, freshness, volume
   anomaly) lands in `ops.quality_runs` **before** the task fails on a failing
   check, so a red run still has its detail in the table. Emits the mart assets
   only when every check passed.
3. `publish_catalog`: `dbt docs generate` plus the value index, published
   atomically to `TRIBUTARY_CATALOG_DIR/current` for the agent; emits **catalog**.

The Airflow `run_id` is the `run_id` of every row written. A failing check or a
model whose SQL fails is not retried (re-running would fail the same way); a
dbt crash (database unreachable) is. All logic is in `tributary.warehouse.refresh`
so `tributary warehouse refresh` runs exactly the same steps.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow.sdk import Asset, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from tributary.config import get_settings
from tributary.warehouse import assets, refresh

RAW = Asset(name=assets.RAW_RECENTCHANGE, group=assets.GROUP)
MARTS = [Asset(name=name, group=assets.GROUP) for name in assets.MARTS]
CATALOG = Asset(name=assets.CATALOG, group=assets.GROUP)


def _run_id() -> str:
    return get_current_context()["run_id"]


@dag(
    dag_id="warehouse_refresh",
    schedule=RAW,
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["warehouse", "dbt", "quality"],
    doc_md=__doc__,
)
def warehouse_refresh() -> None:
    # Settings come from TRIBUTARY_* like every other component, not from Airflow
    # connections: one configuration source for the whole platform.

    @task
    def build_models() -> int:
        try:
            return len(refresh.build_models(get_settings(), _run_id()))
        except refresh.WarehouseError as exc:
            raise AirflowFailException(str(exc)) from exc

    @task(outlets=MARTS)
    def test_models() -> int:
        try:
            return len(refresh.test_models(get_settings(), _run_id()))
        except refresh.WarehouseError as exc:
            raise AirflowFailException(str(exc)) from exc

    @task(outlets=[CATALOG])
    def publish_catalog() -> str:
        return str(refresh.publish_catalog(get_settings(), _run_id()))

    build_models() >> test_models() >> publish_catalog()


warehouse_refresh()

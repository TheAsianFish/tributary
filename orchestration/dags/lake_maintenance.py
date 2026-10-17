"""
### lake_maintenance

Hourly upkeep of the Iceberg table `lake.raw.recentchange`, through Trino:

- `reconcile_closed_hours`: per closed hour, the row count in PostgreSQL against
  the count in Iceberg; one `ops.quality_runs` row per hour, and the task fails
  (after recording) if any hour disagrees.
- `optimize_closed_hours` → `expire_snapshots` → `remove_orphan_files`: compact
  the small files the streaming sink wrote for hours that can no longer
  change, then drop the snapshots and files nothing references any more. In
  that order, because files replaced by `optimize` only become removable once
  the snapshots that reference them expire.

Needs the `lakehouse` profile (Trino, MinIO). The `tributary.lake` modules are
imported inside the tasks so the DAG parses where Trino and its client are absent.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, get_current_context, task

from tributary.config import get_settings


@dag(
    dag_id="lake_maintenance",
    schedule="7 * * * *",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=50),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["lakehouse", "iceberg", "quality"],
    doc_md=__doc__,
)
def lake_maintenance() -> None:
    # Settings come from TRIBUTARY_* like every other component, not from Airflow
    # connections: one configuration source for the whole platform.

    @task
    def reconcile_closed_hours() -> int:
        from tributary.lake.reconcile import reconcile_closed_hours

        return len(reconcile_closed_hours(get_settings(), get_current_context()["run_id"]))

    @task
    def optimize_closed_hours() -> int:
        from tributary.lake.maintenance import optimize_closed_hours

        return optimize_closed_hours(get_settings())

    @task
    def expire_snapshots() -> None:
        from tributary.lake.maintenance import expire_snapshots

        expire_snapshots(get_settings())

    @task
    def remove_orphan_files() -> None:
        from tributary.lake.maintenance import remove_orphan_files

        remove_orphan_files(get_settings())

    reconcile_closed_hours()
    optimize_closed_hours() >> expire_snapshots() >> remove_orphan_files()


lake_maintenance()

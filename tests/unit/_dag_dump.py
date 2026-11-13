"""Parse the DAG folder with Airflow's DagBag and print its structure as JSON.

Run in a child process by test_dags.py: importing Airflow configures logging
and structlog for the whole process (and reads AIRFLOW_HOME once), which must
not leak into the other unit tests.
"""

from __future__ import annotations

import json
import sys
from typing import Any

from airflow.dag_processing.dagbag import DagBag
from airflow.sdk import Asset


def names(assets: object) -> list[str]:
    return [a.name for a in assets if isinstance(a, Asset)]  # type: ignore[attr-defined]


def main(folder: str, out: str) -> None:
    bag = DagBag(folder)
    dags: dict[str, Any] = {}
    for dag_id, dag in bag.dags.items():
        timetable = dag.timetable
        condition = getattr(timetable, "asset_condition", None)
        dags[dag_id] = {
            "timetable": type(timetable).__name__,
            "cron": getattr(timetable, "expression", None),
            "asset_schedule": names([condition]) if condition is not None else [],
            "max_active_runs": dag.max_active_runs,
            "catchup": dag.catchup,
            "dagrun_timeout": None
            if dag.dagrun_timeout is None
            else dag.dagrun_timeout.total_seconds(),
            "tags": sorted(dag.tags),
            "doc_md": bool(dag.doc_md),
            "tasks": {
                task.task_id: {
                    "upstream": sorted(task.upstream_task_ids),
                    "trigger_rule": str(task.trigger_rule.value),
                    "retries": task.retries,
                    "execution_timeout": (
                        task.execution_timeout.total_seconds() if task.execution_timeout else None
                    ),
                    "inlets": names(task.inlets),
                    "outlets": names(task.outlets),
                }
                for task in dag.tasks
            },
        }
    # A file, not stdout: Airflow logs to stdout.
    with open(out, "w") as f:
        json.dump({"import_errors": bag.import_errors, "dags": dags}, f)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])

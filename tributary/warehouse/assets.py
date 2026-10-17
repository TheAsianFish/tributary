"""Names of the data assets the orchestrator schedules on.

Plain strings, so the DAG files can build ``airflow.sdk.Asset`` objects from
them without this package importing Airflow. A DAG that produces an asset and
one that is scheduled on it must agree on the name; keeping it here makes that
a single definition.
"""

from __future__ import annotations

GROUP = "warehouse"

RAW_RECENTCHANGE = "raw.recentchange"
"""Emitted when rows were ingested since the previous event."""

MARTS: tuple[str, ...] = (
    "marts.fct_edits",
    "marts.dim_wiki",
    "marts.dim_page",
    "marts.agg_edits_hourly",
    "marts.agg_editor_activity",
)
"""Emitted when the marts were rebuilt and every check on them passed."""

CATALOG = "catalog"
"""Emitted when a new catalog version (manifest, catalog, values) was published."""

HIGH_WATER_KEY = "ingested_at_max"
"""Key of the ``raw.recentchange`` asset event's ``extra`` holding its high-water mark."""

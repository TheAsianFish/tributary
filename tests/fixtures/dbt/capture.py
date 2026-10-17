"""Re-capture the dbt artifacts the unit tests read, from a real dbt run.

    TRIBUTARY_PG_PORT=... uv run python tests/fixtures/dbt/capture.py [PROJECT_DIR OUT_DIR]

Runs ``dbt seed``, ``run``, ``test`` and ``docs generate`` on a project
(default: ``tests/fixtures/dbt_project``, which has a passing, a warning, a
failing and an erroring test) against the configured PostgreSQL (raw
``recentchange`` loaded with ``tributary sample load``) and writes the
artifacts here, trimmed to the fields the recorders and the catalog read so the
fixtures stay small and reviewable. Nothing in them is edited by hand.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

from tributary.config import Settings
from tributary.warehouse.dbt import run_dbt

HERE = Path(__file__).resolve().parent
DEFAULT_PROJECT = HERE.parent / "dbt_project"

RESULT_KEYS = (
    "unique_id",
    "status",
    "failures",
    "message",
    "execution_time",
    "timing",
    "adapter_response",
    "relation_name",
    "thread_id",
)
NODE_KEYS = (
    "unique_id",
    "resource_type",
    "name",
    "schema",
    "alias",
    "identifier",
    "relation_name",
    "attached_node",
    "depends_on",
    "test_metadata",
    "column_name",
    "description",
)


def _trim_node(node: dict[str, Any]) -> dict[str, Any]:
    out = {k: node[k] for k in NODE_KEYS if k in node}
    if "depends_on" in out:
        out["depends_on"] = {"nodes": out["depends_on"].get("nodes", [])}
    config = node.get("config") or {}
    out["config"] = {k: config[k] for k in ("severity", "meta", "materialized") if k in config}
    out["columns"] = {
        name: {k: col[k] for k in ("name", "description", "meta", "config") if k in col}
        for name, col in (node.get("columns") or {}).items()
    }
    return out


def trim_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    keep = ("invocation_id", "dbt_version", "generated_at", "project_name")
    return {
        "metadata": {k: manifest["metadata"].get(k) for k in keep},
        "nodes": {k: _trim_node(v) for k, v in manifest["nodes"].items()},
        "sources": {k: _trim_node(v) for k, v in manifest["sources"].items()},
    }


def trim_run_results(run_results: dict[str, Any]) -> dict[str, Any]:
    return {
        "metadata": run_results["metadata"],
        "elapsed_time": run_results["elapsed_time"],
        "args": {"which": run_results["args"].get("which")},
        "results": [{k: r.get(k) for k in RESULT_KEYS} for r in run_results["results"]],
    }


def write(path: Path, document: dict[str, Any]) -> None:
    path.write_text(json.dumps(document, indent=1, sort_keys=True) + "\n")
    print(f"wrote {path} ({path.stat().st_size} bytes)")


def main(project: Path, out: Path) -> None:
    settings = Settings(dbt_project_dir=project)
    out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        for command in (["seed"], ["run"], ["test"]):
            result = run_dbt(settings, command, work)
            results = json.loads((result.target_path / "run_results.json").read_text())
            write(out / f"run_results_{command[0]}.json", trim_run_results(results))
        docs = run_dbt(settings, ["docs", "generate"], work)
        manifest = json.loads((docs.target_path / "manifest.json").read_text())
        write(out / "manifest.json", trim_manifest(manifest))
        catalog = json.loads((docs.target_path / "catalog.json").read_text())
        write(out / "catalog.json", catalog)


if __name__ == "__main__":
    args = sys.argv[1:]
    main(Path(args[0]) if args else DEFAULT_PROJECT, Path(args[1]) if len(args) > 1 else HERE)

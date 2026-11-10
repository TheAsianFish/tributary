"""Re-capture the published catalog the agent's unit tests read, from a real publish.

    uv run python tests/fixtures/catalog/capture.py [CATALOG_DIR [OUT_DIR]]

Reads the current version under a published catalog directory (default:
``catalog``, what ``tributary warehouse refresh`` writes from the committed
sample) and republishes it here under the same version name, through
:func:`tributary.warehouse.catalog.publish`, so the layout (version directory,
relative ``current`` link) is the real one. ``catalog.json``, ``values.json``
and ``export.json`` are copied unchanged; ``manifest.json`` is trimmed to this
project's models, seeds, tests and sources and to the fields the agent's
catalog reads (dbt's full manifest is about 740 KB). Nothing in the fixture is
edited by hand.
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path
from typing import Any

from tributary.config import REPO_ROOT
from tributary.warehouse.catalog import CURRENT, load_current, publish, versions

HERE = Path(__file__).resolve().parent
DEFAULT_SOURCE = REPO_ROOT / "catalog"
RESOURCE_TYPES = ("model", "seed", "snapshot", "test")
NODE_KEYS = (
    "unique_id",
    "resource_type",
    "package_name",
    "name",
    "schema",
    "alias",
    "identifier",
    "source_name",
    "attached_node",
    "column_name",
    "test_metadata",
    "description",
)


def _trim_node(node: dict[str, Any]) -> dict[str, Any]:
    out = {k: node[k] for k in NODE_KEYS if k in node}
    if "test_metadata" in out:
        out["test_metadata"] = {k: out["test_metadata"].get(k) for k in ("name", "kwargs")}
    out["depends_on"] = {"nodes": (node.get("depends_on") or {}).get("nodes", [])}
    if node.get("resource_type") != "test":
        meta = (node.get("config") or {}).get("meta") or {}
        out["config"] = {
            "meta": meta,
            "materialized": (node.get("config") or {}).get("materialized"),
        }
        out["columns"] = {
            name: {
                "name": col.get("name", name),
                "description": col.get("description", ""),
                "meta": col.get("meta") or {},
                "config": {"meta": (col.get("config") or {}).get("meta") or {}},
            }
            for name, col in (node.get("columns") or {}).items()
        }
    return out


def trim_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    metadata = manifest["metadata"]
    project = metadata.get("project_name")
    keep = ("dbt_schema_version", "dbt_version", "generated_at", "invocation_id", "project_name")
    return {
        "metadata": {k: metadata.get(k) for k in keep},
        "nodes": {
            k: _trim_node(v)
            for k, v in manifest["nodes"].items()
            if v.get("package_name") == project and v.get("resource_type") in RESOURCE_TYPES
        },
        "sources": {
            k: _trim_node(v)
            for k, v in manifest["sources"].items()
            if v.get("package_name") == project
        },
    }


def main(source: Path, out: Path) -> None:
    published = load_current(source)
    for name in versions(out):
        shutil.rmtree(out / name)
    if (out / CURRENT).is_symlink():
        os.unlink(out / CURRENT)
    path = publish(
        out,
        published.version,
        files={
            name: published.path / name for name in ("catalog.json", "values.json", "export.json")
        },
        documents={"manifest.json": trim_manifest(published.manifest)},
        keep=1,
    )
    for file in sorted(path.iterdir()):
        print(f"wrote {file} ({file.stat().st_size} bytes)")


if __name__ == "__main__":
    args = sys.argv[1:]
    main(Path(args[0]) if args else DEFAULT_SOURCE, Path(args[1]) if len(args) > 1 else HERE)

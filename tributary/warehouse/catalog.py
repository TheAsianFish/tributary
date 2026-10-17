"""Publish the catalog the agent reads: dbt's docs artifacts plus a value index.

A published catalog is a directory holding

- ``manifest.json`` and ``catalog.json`` from ``dbt docs generate`` (tables,
  columns, descriptions, tests, lineage, and the column types the database reports),
- ``values.json``: for every text column of the mart relations, its number of
  distinct values and, when that is at most ``max_distinct``, the values with
  their counts, most frequent first. The agent matches literals in a question
  ("German Wikipedia", "bots") against it to write correct ``WHERE`` clauses;
  a column whose dbt ``meta`` says ``index_values: false`` is never indexed
  (free text, or anything that should not leave the database),
- ``export.json``: when, from which dbt invocation and orchestrator run, and
  how many relations.

Readers must never see a half-written catalog, and a reader that is in the
middle of loading one must not have files swapped under it. Each publish
therefore writes a new versioned directory, renames it into place, and only
then repoints the ``current`` symlink with ``os.replace`` (an atomic rename on
POSIX). :func:`load_current` resolves the link once and reads every file from
that one version. The last :data:`KEEP_VERSIONS` versions are kept, so a slow
reader still finds its version on disk.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
import structlog
from psycopg import sql

log = structlog.get_logger(__name__)

CURRENT = "current"
KEEP_VERSIONS = 3
# Wide enough for the wiki names (160 in the sample): exactly the literals a question
# names ("German Wikipedia" -> dewiki) and the agent must match.
MAX_DISTINCT = 250
MART_SCHEMAS: tuple[str, ...] = ("marts",)
ARTIFACTS = ("manifest.json", "catalog.json")
TEXT_TYPES = frozenset({"text", "character varying", "varchar", "character", "char", "bpchar"})
_VERSION_RE = re.compile(r"^\d{8}T\d{12}Z-")

# (sql, params) -> rows. The value index runs only through this, so tests replace it.
QueryFn = Callable[[sql.Composed, Sequence[object]], list[tuple[Any, ...]]]
# Any: rows from an arbitrary SELECT.


@dataclass(frozen=True, slots=True)
class TextColumn:
    """A mart column that is a candidate for the value index."""

    schema: str
    table: str
    column: str

    @property
    def key(self) -> str:
        return f"{self.schema}.{self.table}.{self.column}"


@dataclass(frozen=True, slots=True)
class PublishedCatalog:
    """One published version, as read by :func:`load_current`."""

    path: Path
    version: str
    manifest: dict[str, Any]
    catalog: dict[str, Any]
    values: dict[str, Any]
    export: dict[str, Any]
    # Any: the documents are dbt's and our JSON; consumers read the parts they know.


def _is_text(data_type: str) -> bool:
    base = data_type.lower().split("(", 1)[0].strip()
    return base in TEXT_TYPES


def _column_meta(manifest_node: Mapping[str, Any], column: str) -> Mapping[str, Any]:
    """A column's ``meta`` (dbt ≥ 1.10 also mirrors it under ``config.meta``)."""
    columns = manifest_node.get("columns") or {}
    entry = columns.get(column)
    if entry is None:
        # Manifest column keys keep the case they were declared with.
        entry = next((v for k, v in columns.items() if k.lower() == column.lower()), None)
    if not entry:
        return {}
    meta: dict[str, Any] = dict((entry.get("config") or {}).get("meta") or {})
    meta.update(entry.get("meta") or {})
    return meta


def text_columns(
    catalog: Mapping[str, Any],
    manifest: Mapping[str, Any],
    schemas: Sequence[str] = MART_SCHEMAS,
) -> list[TextColumn]:
    """The text columns of the relations in ``schemas`` that may be indexed (pure).

    Types come from ``catalog.json`` (what the database says, not what a YAML
    file claims); ``meta.index_values: false`` in the manifest opts a column out.
    """
    wanted = set(schemas)
    nodes = manifest.get("nodes", {})
    found = []
    for unique_id, entry in sorted(catalog.get("nodes", {}).items()):
        metadata = entry.get("metadata", {})
        schema, table = metadata.get("schema"), metadata.get("name")
        if schema not in wanted or not table:
            continue
        node = nodes.get(unique_id, {})
        columns = sorted(entry.get("columns", {}).values(), key=lambda c: c.get("index", 0))
        for column in columns:
            name = column["name"]
            if not _is_text(str(column.get("type", ""))):
                continue
            if _column_meta(node, name).get("index_values") is False:
                continue
            found.append(TextColumn(schema, table, name))
    return found


def index_column(query: QueryFn, column: TextColumn, max_distinct: int) -> dict[str, Any]:
    """Distinct count of one column and, if low enough, its values by frequency."""
    rel = sql.Identifier(column.schema, column.table)
    col = sql.Identifier(column.column)
    distinct = int(
        query(sql.SQL("SELECT count(DISTINCT {c}) FROM {r}").format(c=col, r=rel), ())[0][0]
    )
    entry: dict[str, Any] = {"distinct": distinct, "indexed": distinct <= max_distinct}
    if distinct <= max_distinct:
        rows = query(
            sql.SQL(
                "SELECT {c}, count(*) FROM {r} WHERE {c} IS NOT NULL "
                "GROUP BY {c} ORDER BY count(*) DESC, {c} LIMIT %s"
            ).format(c=col, r=rel),
            (max_distinct,),
        )
        entry["values"] = [{"value": v, "count": int(n)} for v, n in rows]
    return entry


def build_value_index(
    catalog: Mapping[str, Any],
    manifest: Mapping[str, Any],
    query: QueryFn,
    *,
    schemas: Sequence[str] = MART_SCHEMAS,
    max_distinct: int = MAX_DISTINCT,
) -> dict[str, Any]:
    """``values.json``: ``{"max_distinct": n, "columns": {"schema.table.column": {...}}}``."""
    columns = {
        c.key: index_column(query, c, max_distinct)
        for c in text_columns(catalog, manifest, schemas)
    }
    return {"max_distinct": max_distinct, "schemas": list(schemas), "columns": columns}


def postgres_query(conninfo: str) -> tuple[QueryFn, Callable[[], None]]:
    """A :data:`QueryFn` over one read-only connection, and the function that closes it."""
    conn = psycopg.connect(conninfo, autocommit=True)
    conn.read_only = True

    def query(statement: sql.Composed, params: Sequence[object]) -> list[tuple[Any, ...]]:
        with conn.cursor() as cur:
            cur.execute(statement, params)
            return cur.fetchall()

    return query, conn.close


def version_name(invocation_id: str, now: datetime | None = None) -> str:
    """``<UTC timestamp>-<invocation id>``: sorts by publish time, names its dbt run."""
    stamp = (now or datetime.now(UTC)).astimezone(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    safe = re.sub(r"[^A-Za-z0-9_-]", "_", invocation_id) or "unknown"
    return f"{stamp}-{safe}"


def _write_json(path: Path, document: Mapping[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(document, f, ensure_ascii=False, sort_keys=True, default=str)
        f.flush()
        os.fsync(f.fileno())


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def versions(catalog_dir: Path) -> list[str]:
    """Published version directories, oldest first."""
    if not catalog_dir.is_dir():
        return []
    return sorted(
        p.name
        for p in catalog_dir.iterdir()
        if p.is_dir() and not p.is_symlink() and _VERSION_RE.match(p.name)
    )


def publish(
    catalog_dir: Path,
    version: str,
    *,
    files: Mapping[str, Path],
    documents: Mapping[str, Mapping[str, Any]],
    keep: int = KEEP_VERSIONS,
) -> Path:
    """Write a new version, point ``current`` at it, prune old versions.

    ``files`` are copied (dbt's artifacts), ``documents`` are written as JSON.
    The version becomes visible only through the final symlink swap.
    """
    catalog_dir.mkdir(parents=True, exist_ok=True)
    final = catalog_dir / version
    if final.exists():
        raise FileExistsError(f"catalog version {version} already exists")
    staging = catalog_dir / f".tmp-{version}"
    staging.mkdir()
    try:
        for name, source in files.items():
            shutil.copyfile(source, staging / name)
            with (staging / name).open("rb") as f:
                os.fsync(f.fileno())
        for name, document in documents.items():
            _write_json(staging / name, document)
        _fsync_dir(staging)
        staging.rename(final)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    link = catalog_dir / f".{CURRENT}-{uuid.uuid4().hex}"
    os.symlink(version, link)  # relative, so the directory can be moved or mounted elsewhere
    os.replace(link, catalog_dir / CURRENT)
    _fsync_dir(catalog_dir)
    prune(catalog_dir, keep)
    log.info("catalog.published", path=str(final), version=version)
    return final


def prune(catalog_dir: Path, keep: int = KEEP_VERSIONS) -> list[str]:
    """Delete all but the newest ``keep`` versions (never the current one) and stale temps."""
    current = current_version(catalog_dir)
    removed = []
    names = versions(catalog_dir)
    for name in names[: max(len(names) - keep, 0)]:
        if name == current:
            continue
        shutil.rmtree(catalog_dir / name, ignore_errors=True)
        removed.append(name)
    for stale in catalog_dir.glob(".tmp-*"):
        # A crashed publish; anything older than the newest version is abandoned.
        if stale.is_dir() and stale.name.removeprefix(".tmp-") < (current or ""):
            shutil.rmtree(stale, ignore_errors=True)
    return removed


def current_version(catalog_dir: Path) -> str | None:
    """The version ``current`` points at, or None before the first publish."""
    link = catalog_dir / CURRENT
    if not link.is_symlink():
        return None
    return Path(os.readlink(link)).name


def load_current(catalog_dir: Path) -> PublishedCatalog:
    """Read the current catalog, every file from the same version.

    Raises :class:`FileNotFoundError` when nothing has been published yet.
    """
    version = current_version(catalog_dir)
    if version is None:
        raise FileNotFoundError(f"no published catalog under {catalog_dir}")
    path = catalog_dir / version

    def read(name: str) -> dict[str, Any]:
        with (path / name).open(encoding="utf-8") as f:
            document: dict[str, Any] = json.load(f)
        return document

    return PublishedCatalog(
        path=path,
        version=version,
        manifest=read("manifest.json"),
        catalog=read("catalog.json"),
        values=read("values.json"),
        export=read("export.json"),
    )


def export_document(
    *,
    invocation_id: str,
    run_id: str,
    manifest: Mapping[str, Any],
    catalog: Mapping[str, Any],
    values: Mapping[str, Any],
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """``export.json``: provenance of one published catalog."""
    nodes = manifest.get("nodes", {}).values()
    by_type: dict[str, int] = {}
    for node in nodes:
        kind = str(node.get("resource_type", "unknown"))
        by_type[kind] = by_type.get(kind, 0) + 1
    return {
        "generated_at": (generated_at or datetime.now(UTC)).isoformat(),
        "dbt_invocation_id": invocation_id,
        "dbt_version": manifest.get("metadata", {}).get("dbt_version"),
        "run_id": run_id,
        "relations": {
            "catalog_nodes": len(catalog.get("nodes", {})),
            "catalog_sources": len(catalog.get("sources", {})),
            "manifest": dict(sorted(by_type.items())),
            "sources": len(manifest.get("sources", {})),
        },
        "indexed_columns": sum(1 for c in values.get("columns", {}).values() if c.get("indexed")),
    }


def read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        document: dict[str, Any] = json.load(f)
    return document


def publish_from_target(
    catalog_dir: Path,
    target_path: Path,
    *,
    run_id: str,
    query: QueryFn,
    schemas: Sequence[str] = MART_SCHEMAS,
    max_distinct: int = MAX_DISTINCT,
    keep: int = KEEP_VERSIONS,
    now: datetime | None = None,
) -> Path:
    """Build the value index from a ``dbt docs generate`` target and publish it all."""
    manifest = read_json(target_path / "manifest.json")
    catalog = read_json(target_path / "catalog.json")
    values = build_value_index(catalog, manifest, query, schemas=schemas, max_distinct=max_distinct)
    invocation_id = str(manifest.get("metadata", {}).get("invocation_id", ""))
    when = now or datetime.now(UTC)
    export = export_document(
        invocation_id=invocation_id,
        run_id=run_id,
        manifest=manifest,
        catalog=catalog,
        values=values,
        generated_at=when,
    )
    return publish(
        catalog_dir,
        version_name(invocation_id, when),
        files={name: target_path / name for name in ARTIFACTS},
        documents={"values.json": values, "export.json": export},
        keep=keep,
    )

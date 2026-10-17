"""Catalog publishing: column selection, the value index, and the atomic swap."""

from __future__ import annotations

import json
import os
import threading
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from psycopg import sql

from tributary.warehouse import catalog as cat

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "dbt"
T0 = datetime(2026, 10, 17, 12, 0, tzinfo=UTC)


def load(name: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads((FIXTURES / name).read_text())
    return document


# --- column selection (catalog.json + manifest.json captured from dbt) -------------------


def test_text_columns_of_the_requested_schemas_only() -> None:
    columns = cat.text_columns(load("catalog.json"), load("manifest.json"), ["dbt_fixture"])
    keys = [c.key for c in columns]
    # wikis.domain is text but opted out with meta.index_values: false; edits and the
    # ids are not text; the seed's columns are text.
    assert "dbt_fixture.wikis.wiki" in keys
    assert "dbt_fixture.wikis.project_family" in keys
    assert "dbt_fixture.wikis.domain" not in keys
    assert "dbt_fixture.wikis.edits" not in keys
    assert "dbt_fixture.broken.id" not in keys
    assert "dbt_fixture.wiki_family.suffix" in keys
    assert cat.text_columns(load("catalog.json"), load("manifest.json"), ["marts"]) == []


def test_meta_under_config_is_honoured() -> None:
    catalog = {
        "nodes": {
            "model.p.m": {
                "metadata": {"schema": "marts", "name": "m"},
                "columns": {
                    "a": {"name": "a", "type": "character varying(20)", "index": 1},
                    "b": {"name": "b", "type": "TEXT", "index": 2},
                    "c": {"name": "c", "type": "integer", "index": 3},
                },
            }
        }
    }
    manifest = {
        "nodes": {"model.p.m": {"columns": {"B": {"config": {"meta": {"index_values": False}}}}}}
    }
    assert [c.column for c in cat.text_columns(catalog, manifest)] == ["a"]


# --- value index with a fake query function -----------------------------------------------


class FakeDb:
    """Answers count(DISTINCT) and the GROUP BY from a dict of column -> values."""

    def __init__(self, data: dict[str, list[str | None]] | dict[str, list[str]]) -> None:
        self.data: dict[str, list[str | None]] = {k: list(v) for k, v in data.items()}
        self.statements: list[str] = []

    def __call__(self, statement: sql.Composed, params: Sequence[object]) -> list[tuple[Any, ...]]:
        text = statement.as_string(None)
        self.statements.append(text)
        column = next(c for c in self.data if f'"{c}"' in text)
        values = [v for v in self.data[column] if v is not None]
        if "count(DISTINCT" in text:
            return [(len(set(values)),)]
        counts: dict[str, int] = {}
        for v in values:
            counts[v] = counts.get(v, 0) + 1
        ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
        limit = params[0]
        assert isinstance(limit, int)
        return ranked[:limit]


def mart(columns: dict[str, str]) -> dict[str, Any]:
    return {
        "nodes": {
            "model.p.dim_wiki": {
                "metadata": {"schema": "marts", "name": "dim_wiki"},
                "columns": {
                    name: {"name": name, "type": t, "index": i}
                    for i, (name, t) in enumerate(columns.items())
                },
            }
        }
    }


def test_low_cardinality_values_most_frequent_first() -> None:
    values: list[str | None] = ["wikipedia"] * 3
    values += ["wiktionary", None, "commons", "commons"]
    db = FakeDb({"project_family": values})
    index = cat.build_value_index(mart({"project_family": "text"}), {}, db, max_distinct=10)

    entry = index["columns"]["marts.dim_wiki.project_family"]
    assert entry["distinct"] == 3
    assert entry["indexed"] is True
    assert entry["values"] == [
        {"value": "wikipedia", "count": 3},
        {"value": "commons", "count": 2},
        {"value": "wiktionary", "count": 1},
    ]
    assert index["max_distinct"] == 10


def test_high_cardinality_column_keeps_only_its_count() -> None:
    db = FakeDb({"title": [f"t{i}" for i in range(11)]})
    index = cat.build_value_index(mart({"title": "text"}), {}, db, max_distinct=10)

    entry = index["columns"]["marts.dim_wiki.title"]
    assert entry == {"distinct": 11, "indexed": False}
    assert len(db.statements) == 1  # no GROUP BY for a column that will not be indexed


def test_identifiers_are_quoted() -> None:
    db = FakeDb({"weird col": ["x"]})
    cat.build_value_index(mart({"weird col": "text"}), {}, db)
    assert '"marts"."dim_wiki"' in db.statements[0]
    assert '"weird col"' in db.statements[0]


# --- atomic publish -----------------------------------------------------------------------


def artifacts(tmp_path: Path, marker: str) -> dict[str, Path]:
    src = tmp_path / f"src-{marker}"
    src.mkdir(exist_ok=True)
    files = {}
    for name in cat.ARTIFACTS:
        (src / name).write_text(json.dumps({"marker": marker}))
        files[name] = src / name
    return files


def publish_n(tmp_path: Path, root: Path, n: int, keep: int = cat.KEEP_VERSIONS) -> list[str]:
    names = []
    for i in range(n):
        version = cat.version_name(f"inv-{i}", T0 + timedelta(minutes=i))
        cat.publish(
            root,
            version,
            files=artifacts(tmp_path, str(i)),
            documents={"values.json": {"marker": str(i)}, "export.json": {"marker": str(i)}},
            keep=keep,
        )
        names.append(version)
    return names


def test_version_names_sort_by_time_and_name_the_invocation() -> None:
    a = cat.version_name("4f2c-11", T0)
    b = cat.version_name("0000", T0 + timedelta(microseconds=1))
    assert a == "20261017T120000000000Z-4f2c-11"
    assert a < b
    assert cat.version_name("../evil", T0).endswith("-___evil")


def test_publish_creates_a_version_and_points_current_at_it(tmp_path: Path) -> None:
    root = tmp_path / "catalog"
    [version] = publish_n(tmp_path, root, 1)

    link = root / cat.CURRENT
    assert link.is_symlink()
    assert os.readlink(link) == version  # relative: the volume can be mounted anywhere
    assert sorted(p.name for p in (root / version).iterdir()) == [
        "catalog.json",
        "export.json",
        "manifest.json",
        "values.json",
    ]
    loaded = cat.load_current(root)
    assert loaded.version == version
    assert loaded.manifest == loaded.catalog == loaded.values == loaded.export == {"marker": "0"}


def test_only_the_last_three_versions_are_kept(tmp_path: Path) -> None:
    root = tmp_path / "catalog"
    names = publish_n(tmp_path, root, 5)

    assert cat.versions(root) == names[-3:]
    assert cat.current_version(root) == names[-1]
    assert cat.load_current(root).manifest == {"marker": "4"}
    assert not list(root.glob(".tmp-*"))
    assert not list(root.glob(f".{cat.CURRENT}-*"))


def test_the_current_version_is_never_pruned(tmp_path: Path) -> None:
    root = tmp_path / "catalog"
    names = publish_n(tmp_path, root, 2)
    # Point current back at the oldest (as a rollback would), then prune hard.
    tmp_link = root / ".rollback"
    os.symlink(names[0], tmp_link)
    os.replace(tmp_link, root / cat.CURRENT)

    assert cat.prune(root, keep=1) == []
    assert cat.versions(root) == names


def test_a_failed_publish_leaves_the_current_catalog_untouched(tmp_path: Path) -> None:
    root = tmp_path / "catalog"
    [first] = publish_n(tmp_path, root, 1)
    files = artifacts(tmp_path, "broken")
    files["catalog.json"].unlink()  # dbt did not produce it

    with pytest.raises(FileNotFoundError):
        cat.publish(
            root,
            cat.version_name("inv-broken", T0 + timedelta(hours=1)),
            files=files,
            documents={"values.json": {}, "export.json": {}},
        )

    assert cat.current_version(root) == first
    assert cat.versions(root) == [first]
    assert not list(root.glob(".tmp-*"))
    assert cat.load_current(root).manifest == {"marker": "0"}


def test_republishing_a_version_is_refused(tmp_path: Path) -> None:
    root = tmp_path / "catalog"
    version = cat.version_name("same", T0)
    files = artifacts(tmp_path, "a")
    documents: dict[str, dict[str, Any]] = {"values.json": {}, "export.json": {}}
    cat.publish(root, version, files=files, documents=documents)
    with pytest.raises(FileExistsError):
        cat.publish(root, version, files=files, documents=documents)


def test_load_current_before_any_publish(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="no published catalog"):
        cat.load_current(tmp_path / "nothing-here")


def test_a_concurrent_reader_never_sees_a_mixed_or_partial_catalog(tmp_path: Path) -> None:
    root = tmp_path / "catalog"
    publish_n(tmp_path, root, 1, keep=1000)
    stop = threading.Event()
    seen: list[str] = []
    errors: list[BaseException] = []

    def reader() -> None:
        while not stop.is_set():
            try:
                loaded = cat.load_current(root)
                markers = {
                    json.dumps(d)
                    for d in (loaded.manifest, loaded.catalog, loaded.values, loaded.export)
                }
                assert len(markers) == 1, markers
                seen.append(markers.pop())
            except BaseException as exc:  # surfaced in the main thread
                errors.append(exc)
                return

    thread = threading.Thread(target=reader)
    thread.start()
    try:
        for i in range(1, 40):
            version = cat.version_name(f"inv-{i}", T0 + timedelta(minutes=i))
            cat.publish(
                root,
                version,
                files=artifacts(tmp_path, str(i)),
                documents={"values.json": {"marker": str(i)}, "export.json": {"marker": str(i)}},
                keep=1000,
            )
    finally:
        stop.set()
        thread.join()
    assert not errors
    assert seen


def test_publish_from_target_writes_values_and_export(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text((FIXTURES / "manifest.json").read_text())
    (target / "catalog.json").write_text((FIXTURES / "catalog.json").read_text())
    db = FakeDb({"wiki": ["enwiki", "dewiki", "enwiki"], "project_family": ["wikipedia"]})
    db.data.update({"suffix": ["wiki"]})

    path = cat.publish_from_target(
        tmp_path / "catalog",
        target,
        run_id="manual__2026-10-17",
        query=db,
        schemas=["dbt_fixture"],
        now=T0,
    )

    loaded = cat.load_current(tmp_path / "catalog")
    assert loaded.path == path
    invocation = load("manifest.json")["metadata"]["invocation_id"]
    assert loaded.version == f"20261017T120000000000Z-{invocation}"
    assert loaded.values["columns"]["dbt_fixture.wikis.wiki"]["values"][0] == {
        "value": "enwiki",
        "count": 2,
    }
    assert loaded.export["run_id"] == "manual__2026-10-17"
    assert loaded.export["dbt_invocation_id"] == invocation
    assert loaded.export["generated_at"] == T0.isoformat()
    assert loaded.export["relations"]["manifest"]["model"] == 2
    assert loaded.export["indexed_columns"] == len(loaded.values["columns"])


def test_warehouse_marts_text_columns() -> None:
    columns = cat.text_columns(
        load("warehouse/catalog.json"), load("warehouse/manifest.json"), cat.MART_SCHEMAS
    )
    keys = {c.key for c in columns}
    assert "marts.dim_wiki.project_family" in keys
    assert all(k.startswith("marts.") for k in keys)
    assert not any(k.endswith((".event_ts", ".is_bot", ".byte_delta")) for k in keys)

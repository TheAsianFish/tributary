"""dbt artifacts → ops.quality_runs / ops.refresh_runs rows, from recorded artifacts.

The fixtures under tests/fixtures/dbt were captured from a real dbt run of
tests/fixtures/dbt_project (see capture.py there): it has passing, warning,
failing and erroring tests, a source test and a singular test.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from tributary.warehouse import quality
from tributary.warehouse.dbt import RunResults, parse_run_results

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "dbt"
P = "test.tributary_fixture."


def load(name: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads((FIXTURES / name).read_text())
    return document


@pytest.fixture(scope="module")
def manifest() -> dict[str, Any]:
    return load("manifest.json")


@pytest.fixture(scope="module")
def tested() -> RunResults:
    return parse_run_results(load("run_results_test.json"))


@pytest.fixture(scope="module")
def checks(tested: RunResults, manifest: dict[str, Any]) -> dict[str, quality.CheckResult]:
    return {c.check_name: c for c in quality.quality_checks(tested, manifest)}


def test_one_check_per_test(checks: dict[str, quality.CheckResult]) -> None:
    assert len(checks) == 11
    assert all(name.startswith(P) for name in checks)


@pytest.mark.parametrize(
    ("name", "status", "failures"),
    [
        ("not_null_wikis_wiki.ad7b45b1cc", "pass", 0),
        ("accepted_values_wikis_project_family__wikipedia.cfcae99c16", "warn", 1),
        ("min_value_wikis_edits__1000000.d6296091f9", "fail", 160),
        ("min_value_broken_id__not_a_number.98078c9fce", "error", None),
    ],
)
def test_status_and_failures(
    checks: dict[str, quality.CheckResult], name: str, status: str, failures: int | None
) -> None:
    check = checks[P + name]
    assert check.status == status
    assert check.failures == failures


def test_error_keeps_the_database_message(checks: dict[str, quality.CheckResult]) -> None:
    check = checks[P + "min_value_broken_id__not_a_number.98078c9fce"]
    assert "syntax error" in check.details["message"]
    assert check.details["dbt_status"] == "error"


@pytest.mark.parametrize(
    ("name", "check_type", "target"),
    [
        ("not_null_wikis_wiki.ad7b45b1cc", "not_null", "dbt_fixture.wikis"),
        ("unique_wikis_wiki.2aa4dd2f57", "unique", "dbt_fixture.wikis"),
        # Declared on wikis, depends on the seed too: the target is the child.
        (
            "relationships_wikis_project_family__project_family__ref_wiki_family_.9173546f72",
            "relationships",
            "dbt_fixture.wikis",
        ),
        ("accepted_values_broken_id__False__1.af38ca106b", "accepted_values", "dbt_fixture.broken"),
        # Custom generic tests are typed by their macro name.
        ("row_count_between_wikis_100000__1.94ff38fc70", "row_count_between", "dbt_fixture.wikis"),
        # A source test has no attached node in the manifest; resolved through depends_on.
        ("source_not_null_raw_recentchange_event_id.8d6c83b444", "not_null", "raw.recentchange"),
        ("no_future_events", "singular", "raw.recentchange"),
    ],
)
def test_check_type_and_target(
    checks: dict[str, quality.CheckResult], name: str, check_type: str, target: str
) -> None:
    check = checks[P + name]
    assert (check.check_type, check.target) == (check_type, target)


def test_details_carry_kwargs_column_and_timing(checks: dict[str, quality.CheckResult]) -> None:
    check = checks[P + "row_count_between_wikis_100000__1.94ff38fc70"]
    assert check.details["kwargs"] == {"low": 1, "high": 100000}  # no Jinja "model" noise
    assert check.details["column_name"] is None
    rel = checks[
        P + "relationships_wikis_project_family__project_family__ref_wiki_family_.9173546f72"
    ]
    assert rel.details["kwargs"]["column_name"] == "project_family"
    assert rel.details["kwargs"]["field"] == "project_family"
    assert [t["name"] for t in rel.details["timing"]] == ["compile", "execute"]
    assert rel.started_at is not None
    assert rel.finished_at is not None
    assert rel.started_at <= rel.finished_at
    json.dumps(rel.details)  # stored as jsonb


def test_severity_is_recorded(checks: dict[str, quality.CheckResult]) -> None:
    warn = checks[P + "accepted_values_wikis_project_family__wikipedia.cfcae99c16"]
    assert warn.details["severity"] == "warn"


def test_failed_checks_are_fail_and_error_only(checks: dict[str, quality.CheckResult]) -> None:
    failed = quality.failed_checks(checks.values())
    assert sorted(c.status for c in failed) == ["error", "fail"]


@pytest.mark.parametrize(
    ("dbt_status", "expected"),
    [
        ("pass", "pass"),
        ("warn", "warn"),
        ("fail", "fail"),
        ("error", "error"),
        ("skipped", "skipped"),
        ("runtime error", "error"),
        ("something new", "error"),
    ],
)
def test_status_mapping(dbt_status: str, expected: str) -> None:
    assert quality.map_test_status(dbt_status) == expected


def test_unknown_test_still_gets_a_row(tested: RunResults) -> None:
    """A test missing from the manifest (a stale manifest) is recorded, target unknown."""
    rows = quality.quality_checks(tested, {"nodes": {}})
    assert len(rows) == 11
    assert {r.target for r in rows} == {quality.UNKNOWN_TARGET}
    assert {r.check_type for r in rows} == {"singular"}


def test_check_result_rejects_unknown_status() -> None:
    now = datetime(2026, 10, 17, tzinfo=UTC)
    with pytest.raises(ValueError, match="invalid check status"):
        quality.CheckResult("c", "t", "s.t", "ok", None, finished_at=now)  # type: ignore[arg-type]


def test_refresh_rows_from_seed_and_run() -> None:
    rows = quality.refresh_rows(parse_run_results(load("run_results_run.json")))
    seeds = parse_run_results(load("run_results_seed.json"))
    assert seeds.results[0].relation_name is None  # dbt does not report it for seeds
    rows += quality.refresh_rows(seeds, load("manifest.json"))
    by_node = {r.node: r for r in rows}
    assert set(by_node) == {
        "model.tributary_fixture.wikis",
        "model.tributary_fixture.broken",
        "seed.tributary_fixture.wiki_family",
    }
    wikis = by_node["model.tributary_fixture.wikis"]
    assert (wikis.relation, wikis.status, wikis.rows_affected) == (
        "dbt_fixture.wikis",
        "success",
        160,
    )
    assert by_node["seed.tributary_fixture.wiki_family"].relation == "dbt_fixture.wiki_family"


def test_tests_are_not_refreshes(tested: RunResults) -> None:
    assert quality.refresh_rows(tested) == []


@pytest.mark.parametrize(
    ("relation", "parts"),
    [
        ('"tributary"."marts"."dim_wiki"', ("tributary", "marts", "dim_wiki")),
        ('"db"."odd.schema"."t""q"', ("db", "odd.schema", 't"q')),
        ("plain.table", ("plain", "table")),
    ],
)
def test_split_relation(relation: str, parts: tuple[str, ...]) -> None:
    assert quality.split_relation(relation) == parts


class Recorder:
    def __init__(self) -> None:
        self.sql: list[str] = []
        self.rows: list[tuple[object, ...]] = []

    def __enter__(self) -> Recorder:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def cursor(self) -> Recorder:
        return self

    def executemany(self, statement: str, rows: list[tuple[object, ...]]) -> None:
        self.sql.append(statement)
        self.rows.extend(rows)


def test_record_checks_upserts_on_run_and_check(
    monkeypatch: pytest.MonkeyPatch, checks: dict[str, quality.CheckResult]
) -> None:
    recorder = Recorder()
    monkeypatch.setattr(quality, "ensure_schema", lambda conninfo, script: None)
    monkeypatch.setattr("tributary.warehouse.quality.psycopg.connect", lambda conninfo: recorder)

    written = quality.record_checks("", "manual__1", list(checks.values()), source="dbt")

    assert written == 11
    assert "ON CONFLICT (run_id, check_name) DO UPDATE" in recorder.sql[0]
    assert {row[0] for row in recorder.rows} == {"manual__1"}
    assert {row[1] for row in recorder.rows} == {"dbt"}


def test_record_check_writes_one_row(monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = Recorder()
    monkeypatch.setattr(quality, "ensure_schema", lambda conninfo, script: None)
    monkeypatch.setattr("tributary.warehouse.quality.psycopg.connect", lambda conninfo: recorder)
    check = quality.CheckResult(
        "reconcile_hour:2026-10-17T11", "reconcile", "raw.recentchange", "pass", 0, {"hour": 11}
    )

    quality.record_check("", "scheduled__x", check, source="lake")

    assert len(recorder.rows) == 1
    row = recorder.rows[0]
    assert row[:7] == (
        "scheduled__x",
        "lake",
        "reconcile_hour:2026-10-17T11",
        "reconcile",
        "raw.recentchange",
        "pass",
        0,
    )
    assert row[9] is None  # finished_at: the database stamps it


def test_nothing_to_record_opens_no_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(conninfo: str) -> None:
        raise AssertionError("connected")

    monkeypatch.setattr("tributary.warehouse.quality.psycopg.connect", fail)
    assert quality.record_checks("", "r", [], source="dbt") == 0
    assert quality.record_refreshes("", "r", []) == 0


# --- the warehouse project itself (artifacts captured from warehouse/ on the sample) ------

REAL = "warehouse/"


def test_warehouse_custom_tests_resolve_to_their_relations() -> None:
    manifest = load(REAL + "manifest.json")
    checks = quality.quality_checks(
        parse_run_results(load(REAL + "run_results_test.json")), manifest
    )
    custom = {(c.check_type, c.target, c.details["column_name"]) for c in checks}
    assert ("freshness", "marts.fct_edits", "ingested_at") in custom
    assert ("freshness", "raw.recentchange", "ingested_at") in custom
    assert ("volume_anomaly", "marts.fct_edits", "event_ts") in custom
    assert ("volume_anomaly", "raw.recentchange", "event_ts") in custom
    assert quality.UNKNOWN_TARGET not in {c.target for c in checks}
    assert {c.status for c in checks} == {"pass"}


def test_warehouse_refresh_rows_cover_every_mart() -> None:
    manifest = load(REAL + "manifest.json")
    rows = quality.refresh_rows(parse_run_results(load(REAL + "run_results_run.json")), manifest)
    rows += quality.refresh_rows(parse_run_results(load(REAL + "run_results_seed.json")), manifest)
    relations = {r.relation: r for r in rows}
    assert {
        "marts.fct_edits",
        "marts.dim_wiki",
        "marts.dim_page",
        "marts.agg_edits_hourly",
        "marts.agg_editor_activity",
    } <= set(relations)
    # A view reports rows_affected = -1; stored as unknown, not as a negative count.
    assert relations["staging.stg_recentchange"].rows_affected is None
    assert all(r.status == "success" for r in rows)

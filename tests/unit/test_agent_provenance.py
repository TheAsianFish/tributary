"""Provenance: a table's checks and its sources' checks, and how old its last rebuild is."""

from __future__ import annotations

from datetime import timedelta

import psycopg
import pytest

from tests.unit.agent_doubles import NOW, catalog
from tributary.agent.catalog import Table
from tributary.agent.provenance import (
    CheckResult,
    PostgresProvenance,
    Refresh,
    TableProvenance,
    summarise,
)

STALE_AFTER = timedelta(minutes=120)
MART = Table(
    schema="marts",
    name="agg_edits_hourly",
    description="",
    columns=(),
    primary_key=(),
    foreign_keys=(),
    upstream=("marts.fct_edits", "raw.recentchange"),
)
FRESH = Refresh("marts.agg_edits_hourly", "success", NOW - timedelta(minutes=5))


def check(name: str, target: str, status: str) -> CheckResult:
    return CheckResult(f"test.tributary.{name}.abc123", "test", target, status, NOW)


def summary(checks: list[CheckResult], refresh: Refresh | None = FRESH) -> TableProvenance:
    return summarise(MART, refresh, checks, stale_after=STALE_AFTER, now=NOW)


def test_all_passing() -> None:
    p = summary([check("unique_hour", MART.relation, "pass")])
    assert p.quality == "pass"
    assert p.checks == 1
    assert not p.stale
    assert p.covers == ("marts.agg_edits_hourly", "marts.fct_edits", "raw.recentchange")
    assert p.warnings(STALE_AFTER, NOW) == []


@pytest.mark.parametrize("status", ["fail", "error"])
def test_upstream_failure_fails_the_mart(status: str) -> None:
    p = summary(
        [
            check("unique_hour", MART.relation, "pass"),
            check("freshness_raw", "raw.recentchange", status),
        ]
    )
    assert p.quality == "fail"
    assert p.failing == ("freshness_raw",)
    assert p.checks == 2
    assert p.warnings(STALE_AFTER, NOW) == ["marts.agg_edits_hourly: failing checks: freshness_raw"]


def test_warn() -> None:
    p = summary([check("a", MART.relation, "pass"), check("row_count", "marts.fct_edits", "warn")])
    assert p.quality == "warn"
    assert p.warning == ("row_count",)
    assert p.failing == ()


def test_fail_beats_warn() -> None:
    p = summary([check("a", MART.relation, "warn"), check("b", "marts.fct_edits", "fail")])
    assert p.quality == "fail"
    assert p.warning == ("a",)
    assert p.failing == ("b",)


def test_skipped_is_ignored() -> None:
    p = summary([check("a", MART.relation, "pass"), check("b", MART.relation, "skipped")])
    assert p.quality == "pass"
    assert p.checks == 1


def test_only_skipped_is_unknown() -> None:
    assert summary([check("b", MART.relation, "skipped")]).quality == "unknown"


def test_unrelated_targets_are_ignored() -> None:
    p = summary([check("x", "marts.dim_page", "fail")])
    assert p.quality == "unknown"
    assert p.checks == 0
    assert "marts.agg_edits_hourly: no quality results recorded" in p.warnings(STALE_AFTER, NOW)


def test_unknown_status_counts_as_failing() -> None:
    assert summary([check("a", MART.relation, "runtime error")]).quality == "fail"


@pytest.mark.parametrize(
    ("refresh", "stale", "refreshed"),
    [
        (FRESH, False, FRESH.finished_at),
        (Refresh(MART.relation, "success", NOW - timedelta(minutes=120)), False, None),
        (Refresh(MART.relation, "success", NOW - timedelta(minutes=121)), True, None),
        (Refresh(MART.relation, "error", NOW), True, "none"),
        (None, True, "none"),
    ],
)
def test_staleness(refresh: Refresh | None, stale: bool, refreshed: object) -> None:
    p = summary([check("a", MART.relation, "pass")], refresh)
    assert p.stale is stale
    if refreshed == "none":
        assert p.last_refreshed_at is None
        assert p.warnings(STALE_AFTER, NOW) == ["marts.agg_edits_hourly has no recorded rebuild"]
    elif refreshed is not None:
        assert p.last_refreshed_at == refreshed


@pytest.mark.parametrize(
    ("age", "text"),
    [
        (timedelta(minutes=150), "2 h"),
        (timedelta(hours=5, minutes=30), "5 h"),
        (timedelta(days=3, hours=2), "3 d"),
    ],
)
def test_stale_warning_age(age: timedelta, text: str) -> None:
    p = summary([check("a", MART.relation, "pass")], Refresh(MART.relation, "success", NOW - age))
    assert p.warnings(STALE_AFTER, NOW) == [
        f"marts.agg_edits_hourly was last rebuilt {text} ago (limit 2 h)"
    ]


@pytest.mark.parametrize(
    ("check_name", "short"),
    [
        (
            "test.tributary.freshness_fct_edits_ingested_at.9f8e7d",
            "freshness_fct_edits_ingested_at",
        ),
        ("test.tributary.not_null_dim_wiki_wiki", "not_null_dim_wiki_wiki"),
        ("source_freshness_raw", "source_freshness_raw"),
        ("model.tributary.fct_edits", "model.tributary.fct_edits"),
        ("test.tributary", "test.tributary"),
    ],
)
def test_short_name(check_name: str, short: str) -> None:
    assert CheckResult(check_name, "test", "x", "pass", NOW).short_name == short


def test_to_dict() -> None:
    p = summary([check("a", MART.relation, "pass")], None)
    d = p.to_dict()
    assert d["last_refreshed_at"] is None
    assert d["covers"] == list(p.covers)
    assert d["quality"] == "pass"


def test_stale_warning_in_minutes() -> None:
    p = summarise(
        MART,
        Refresh(MART.relation, "success", NOW - timedelta(minutes=45)),
        [check("a", MART.relation, "pass")],
        stale_after=timedelta(minutes=30),
        now=NOW,
    )
    assert p.warnings(timedelta(minutes=30), NOW) == [
        "marts.agg_edits_hourly was last rebuilt 45 min ago (limit 30 min)"
    ]


@pytest.mark.parametrize(
    "error",
    [
        psycopg.errors.UndefinedTable,
        psycopg.errors.InvalidSchemaName,
        psycopg.errors.InsufficientPrivilege,
    ],
)
def test_unreadable_ops_reports_unknown(
    monkeypatch: pytest.MonkeyPatch, error: type[psycopg.Error]
) -> None:
    source = PostgresProvenance("host=unused", stale_after=timedelta(hours=2), clock=lambda: NOW)

    def unreadable(*args: object) -> None:
        raise error("ops is not readable")

    monkeypatch.setattr(source, "_read", unreadable)
    (fct,) = source.lookup([catalog().table("marts.fct_edits")])
    assert fct.quality == "unknown"
    assert fct.last_refreshed_at is None
    assert fct.stale

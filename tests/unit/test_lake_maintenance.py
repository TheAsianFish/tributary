"""Maintenance and reconciliation logic with Trino and PostgreSQL replaced by recorders."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import pytest

from tributary.config import Settings
from tributary.lake import maintenance, reconcile
from tributary.lake.reconcile import LakeCount, compare_hours, lag_check
from tributary.warehouse.quality import CheckResult

NOW = datetime(2026, 10, 17, 12, 0, tzinfo=UTC)
H23 = datetime(2026, 10, 16, 23, tzinfo=UTC)
H00 = datetime(2026, 10, 17, 0, tzinfo=UTC)


class Recorder:
    def __init__(self, answers: list[list[tuple[object, ...]]]) -> None:
        self.answers = answers
        self.sql: list[str] = []
        self.params: list[Sequence[object] | None] = []

    def __call__(
        self, _settings: Settings, sql: str, params: Sequence[object] | None = None
    ) -> list[tuple[object, ...]]:
        self.sql.append(sql)
        self.params.append(params)
        return self.answers.pop(0)


@pytest.fixture
def settings() -> Settings:
    return Settings(_env_file=None)


def test_optimize_compacts_only_hours_behind_the_watermark(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    fetch = Recorder(
        [
            [(datetime(2026, 10, 17, 0, 21, 44, tzinfo=UTC),)],
            [("rewritten_data_files_count", 7), ("added_data_files_count", 1)],
        ]
    )
    monkeypatch.setattr(maintenance, "fetch", fetch)
    assert maintenance.optimize_closed_hours(settings) == 7
    assert fetch.sql[1] == (
        "ALTER TABLE iceberg.raw.recentchange EXECUTE optimize"
        " WHERE event_ts < TIMESTAMP '2026-10-17 00:00:00 UTC'"
    )


def test_optimize_on_an_empty_table_does_nothing(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    fetch = Recorder([[(None,)]])
    monkeypatch.setattr(maintenance, "fetch", fetch)
    assert maintenance.optimize_closed_hours(settings) == 0
    assert len(fetch.sql) == 1


def test_expiry_and_orphan_removal_use_the_configured_retention(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    fetch = Recorder([[], [("deleted_files_count", 0)]])
    monkeypatch.setattr(maintenance, "fetch", fetch)
    maintenance.expire_snapshots(settings)
    maintenance.remove_orphan_files(settings)
    assert fetch.sql == [
        "ALTER TABLE iceberg.raw.recentchange EXECUTE expire_snapshots"
        "(retention_threshold => '1d')",
        "ALTER TABLE iceberg.raw.recentchange EXECUTE remove_orphan_files"
        "(retention_threshold => '1d')",
    ]


@pytest.mark.parametrize(
    ("field", "value"),
    [("lake_snapshot_retention", "1d'); DROP TABLE x; --"), ("trino_lake_table", "x; DROP")],
)
def test_values_spliced_into_sql_are_validated(field: str, value: str) -> None:
    settings = Settings(_env_file=None, **{field: value})  # type: ignore[arg-type]
    with pytest.raises(ValueError, match=r"must look like|not a catalog"):
        maintenance.expire_snapshots(settings)


def test_matching_hours_pass() -> None:
    checks = compare_hours(
        {H23: 19271, H00: 10}, {H23: LakeCount(19271, 19271), H00: LakeCount(10, 10)}, NOW
    )
    assert [c.status for c in checks] == ["pass", "pass"]
    assert checks[0].check_name == "reconcile_hour:2026-10-16T23"
    assert checks[0].details["warehouse_distinct"] == 19271


def test_missing_extra_and_duplicate_rows_each_fail_the_hour() -> None:
    checks = compare_hours(
        {H23: 100, H00: 50},
        {
            H23: LakeCount(rows=99, distinct=98),
            datetime(2026, 10, 17, 1, tzinfo=UTC): LakeCount(3, 3),
        },
        NOW,
    )
    by_hour = {c.details["hour"]: c for c in checks}
    h23 = by_hour[H23.isoformat()]
    assert h23.status == "fail"
    assert h23.details["missing_in_lake"] == 2
    assert h23.details["lake_duplicates"] == 1
    assert h23.failures == 3
    # An hour the lake lost entirely, and one only the lake has.
    assert by_hour[H00.isoformat()].details["missing_in_lake"] == 50
    assert by_hour["2026-10-17T01:00:00+00:00"].details["extra_in_lake"] == 3


def test_a_stalled_or_missing_lake_fails_the_lag_check() -> None:
    w = datetime(2026, 10, 17, 0, 21, tzinfo=UTC)
    limit = timedelta(minutes=15)
    assert lag_check(w, w - timedelta(minutes=6), limit, NOW).status == "pass"
    assert lag_check(w, w - timedelta(minutes=16), limit, NOW).status == "fail"
    assert lag_check(w, None, limit, NOW).status == "fail"


def test_reconcile_closes_hours_on_the_slower_store_and_records_before_raising(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    recorded: list[CheckResult] = []
    windows: list[tuple[datetime, datetime]] = []

    def lake_hours(_s: Settings, start: datetime, end: datetime) -> dict[datetime, LakeCount]:
        windows.append((start, end))
        return {H23: LakeCount(5, 5)}

    monkeypatch.setattr(
        reconcile, "warehouse_newest", lambda _c: datetime(2026, 10, 17, 1, 30, tzinfo=UTC)
    )
    monkeypatch.setattr(
        reconcile, "newest_event", lambda _s: datetime(2026, 10, 17, 1, 20, tzinfo=UTC)
    )
    monkeypatch.setattr(reconcile, "warehouse_hours", lambda _c, _s, _e: {H23: 6})
    monkeypatch.setattr(reconcile, "lake_hours", lake_hours)

    def record_checks(_c: str, _run: str, checks: list[CheckResult], *, source: str) -> int:
        recorded.extend(checks)
        return len(checks)

    monkeypatch.setattr(reconcile, "record_checks", record_checks)
    with pytest.raises(reconcile.ReconciliationError, match="reconcile_hour:2026-10-16T23"):
        reconcile.reconcile_closed_hours(settings, "run-1")
    # The lake (01:20) is behind the warehouse (01:30): its watermark, 01:10, closes 00:00.
    assert windows == [
        (datetime(2026, 10, 16, 1, tzinfo=UTC), datetime(2026, 10, 17, 1, tzinfo=UTC))
    ]
    assert [c.check_name for c in recorded] == ["lake_lag", "reconcile_hour:2026-10-16T23"]

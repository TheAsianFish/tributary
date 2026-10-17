"""The refresh steps with dbt and PostgreSQL replaced: record first, then fail."""

from __future__ import annotations

import json
import shutil
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from tributary.cli import app
from tributary.config import Settings
from tributary.warehouse import refresh
from tributary.warehouse.dbt import DbtCrashError, DbtResult, parse_run_results
from tributary.warehouse.quality import CheckResult, RefreshRow

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "dbt"


class FakeWarehouse:
    """Stands in for run_dbt and the two recorders."""

    def __init__(self, outcomes: dict[str, tuple[int, str | None]]) -> None:
        self.outcomes = outcomes
        self.commands: list[str] = []
        self.checks: list[tuple[str, CheckResult]] = []
        self.refreshes: list[tuple[str, RefreshRow]] = []

    def run_dbt(self, settings: Settings, args: Sequence[str], work_dir: Path) -> DbtResult:
        name = "-".join(args)
        self.commands.append(name)
        returncode, results = self.outcomes[name]
        target = work_dir / name / "target"
        target.mkdir(parents=True)
        shutil.copyfile(FIXTURES / "manifest.json", target / "manifest.json")
        parsed = None
        if results is not None:
            parsed = parse_run_results(json.loads((FIXTURES / results).read_text()))
        return DbtResult(tuple(args), returncode, target, parsed, 0.1)

    def record_checks(
        self, conninfo: str, run_id: str, checks: Sequence[CheckResult], *, source: str
    ) -> int:
        self.checks += [(run_id, c) for c in checks]
        return len(checks)

    def record_refreshes(self, conninfo: str, run_id: str, rows: Sequence[RefreshRow]) -> int:
        self.refreshes += [(run_id, r) for r in rows]
        return len(rows)


def install(monkeypatch: pytest.MonkeyPatch, fake: FakeWarehouse) -> None:
    monkeypatch.setattr(refresh, "run_dbt", fake.run_dbt)
    monkeypatch.setattr(refresh, "record_checks", fake.record_checks)
    monkeypatch.setattr(refresh, "record_refreshes", fake.record_refreshes)


SETTINGS = Settings(_env_file=None)


def test_failing_tests_are_recorded_before_the_step_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeWarehouse({"test": (1, "run_results_test.json")})
    install(monkeypatch, fake)

    with pytest.raises(refresh.QualityCheckError) as excinfo:
        refresh.test_models(SETTINGS, "scheduled__1")

    assert len(fake.checks) == 11
    assert {run_id for run_id, _ in fake.checks} == {"scheduled__1"}
    assert sorted(c.status for c in excinfo.value.failed) == ["error", "fail"]
    assert "2 check(s) did not pass" in str(excinfo.value)


def test_a_dbt_crash_records_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeWarehouse({"test": (2, None)})
    install(monkeypatch, fake)

    with pytest.raises(DbtCrashError):
        refresh.test_models(SETTINGS, "r")
    assert fake.checks == []


def test_build_records_seeds_and_models(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeWarehouse({"seed": (0, "run_results_seed.json"), "run": (0, "run_results_run.json")})
    install(monkeypatch, fake)

    rows = refresh.build_models(SETTINGS, "r")

    assert fake.commands == ["seed", "run"]
    assert len(rows) == len(fake.refreshes) == 3


def test_a_failed_model_is_recorded_then_raised(monkeypatch: pytest.MonkeyPatch) -> None:
    # Exit code 1 from dbt run: results exist, so they are recorded before failing.
    fake = FakeWarehouse({"seed": (0, "run_results_seed.json"), "run": (1, "run_results_run.json")})
    install(monkeypatch, fake)

    with pytest.raises(refresh.WarehouseError, match="dbt run failed"):
        refresh.build_models(SETTINGS, "r")
    assert len(fake.refreshes) == 3


def test_publish_is_not_reached_when_checks_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeWarehouse(
        {
            "seed": (0, "run_results_seed.json"),
            "run": (0, "run_results_run.json"),
            "test": (1, "run_results_test.json"),
        }
    )
    install(monkeypatch, fake)
    published: list[str] = []
    monkeypatch.setattr(refresh, "publish_catalog", lambda s, run_id: published.append(run_id))

    result = CliRunner().invoke(app, ["warehouse", "refresh"])

    assert result.exit_code == 1
    assert "check(s) did not pass" in result.output
    assert fake.commands == ["seed", "run", "test"]
    assert published == []
    # One run id for every row the command wrote.
    run_ids = {run_id for run_id, _ in fake.checks} | {run_id for run_id, _ in fake.refreshes}
    assert len(run_ids) == 1


@pytest.mark.parametrize(
    ("previous", "newest", "expected_after", "expected"),
    [
        (None, datetime(2026, 10, 17, 12, tzinfo=UTC), None, "2026-10-17T12:00:00+00:00"),
        (
            "2026-10-17T12:00:00+00:00",
            None,
            datetime(2026, 10, 17, 12, tzinfo=UTC),
            None,
        ),
    ],
)
def test_high_water_mark_round_trips_through_iso_strings(
    monkeypatch: pytest.MonkeyPatch,
    previous: str | None,
    newest: datetime | None,
    expected_after: datetime | None,
    expected: str | None,
) -> None:
    calls: list[datetime | None] = []

    def new_rows_since(conninfo: str, after: datetime | None) -> datetime | None:
        calls.append(after)
        return newest

    monkeypatch.setattr(refresh, "new_rows_since", new_rows_since)
    assert refresh.next_high_water_mark("", previous) == expected
    assert calls == [expected_after]

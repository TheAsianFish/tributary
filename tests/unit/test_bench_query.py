"""The query benchmark driver's pure parts: scales, quantiles, plan parsing, cell rules."""

from __future__ import annotations

from typing import Any, cast

import pytest

from bench import queries as Q  # noqa: N812
from bench import query

# Trimmed from a real Trino 484 EXPLAIN ANALYZE of q07 on the streamed layout.
PLAN = """\
Fragment 3 [SOURCE]
    Aggregate[type = PARTIAL, keys = [wiki]]
    └─ ScanFilterProject[table = iceberg:bench.events_1m_stream$data@3562171913499405210 \
constraint on [event_ts], filterPredicate = (event_type = varchar 'edit')]
           Layout: [expr_0:bigint, wiki:varchar]
           CPU: 791.00ms (86.83%), Scheduled: 3.02s (86.91%), Output: 37692 rows (868.00kB)
           2:event_ts:timestamp(6) with time zone
               :: [[2026-10-17 00:00:00.000000 UTC, 2026-10-17 06:00:00.000000 UTC)]
           Input: 80729 rows (3.16MB), Filtered: 53.31%, Physical input: 6.13MB, \
Physical input time: 1.74ms, Splits: 177, Splits generation wait time: 87.41ms
"""


def test_scales_read_as_row_counts() -> None:
    assert query.parse_scale("1m") == 1_000_000
    assert query.parse_scale("20M") == 20_000_000
    assert query.parse_scale("250k") == 250_000
    assert query.parse_scale("1_000") == 1_000
    assert query.parse_scale("100000") == 100_000


def test_p95_of_ten_runs_is_the_slowest() -> None:
    runs = [float(i) for i in range(10, 0, -1)]
    assert query.nearest_rank(runs, 0.95) == 10.0
    assert query.nearest_rank(runs, 0.5) == 5.0
    assert query.nearest_rank([7.0], 0.95) == 7.0
    assert query.nearest_rank([float(i) for i in range(1, 21)], 0.95) == 19.0
    assert query.summarise([3.0, 1.0, 2.0]) == {"median": 2.0, "p95": 3.0, "min": 1.0, "max": 3.0}


def test_trino_sizes_parse_to_bytes() -> None:
    assert query.parse_size("6.13MB") == pytest.approx(6.13e6)
    assert query.parse_size("587.16kB") == pytest.approx(587_160)
    assert query.parse_size("0B") == 0
    assert query.parse_size("1.5GB") == pytest.approx(1.5e9)
    with pytest.raises(ValueError, match="data size"):
        query.parse_size("12 rows")


def test_the_scan_of_the_named_table_is_read_from_the_plan() -> None:
    stats = query.scan_stats(PLAN, "bench.events_1m_stream")
    assert stats == {
        "rows": 80729,
        "input_bytes": pytest.approx(3.16e6),
        "physical_input_bytes": pytest.approx(6.13e6),
        "splits": 177,
    }


def test_a_scan_of_another_table_with_the_same_prefix_does_not_match() -> None:
    with pytest.raises(ValueError, match=r"no scan of bench\.events_1m in the plan"):
        query.scan_stats(PLAN, "bench.events_1m")


def test_postgres_buffers_come_from_the_top_node_with_the_scans_named() -> None:
    plan: dict[str, Any] = {
        "Plan": {
            "Node Type": "Aggregate",
            "Shared Hit Blocks": 30,
            "Shared Read Blocks": 12,
            "Plans": [
                {
                    "Node Type": "Bitmap Heap Scan",
                    "Shared Hit Blocks": 29,
                    "Plans": [
                        {"Node Type": "Bitmap Index Scan", "Index Name": "events_1m_event_ts_brin"}
                    ],
                }
            ],
        }
    }
    assert query.pg_buffers(plan) == {
        "shared_hit_blocks": 30,
        "shared_read_blocks": 12,
        "blocks": 42,
        "scans": ["Bitmap Heap Scan", "Bitmap Index Scan using events_1m_event_ts_brin"],
    }


class FakeEngines:
    """Answers every statement with ``rows`` (or raises ``error``), timing 1 s, 2 s, ..."""

    def __init__(self, rows: list[list[Q.Value]], error: Exception | None = None) -> None:
        self.rows, self.error, self.calls = rows, error, 0

    def run(self, engine: str, sql: str) -> tuple[list[list[Q.Value]], float, dict[str, Any]]:
        self.calls += 1
        if self.error is not None:
            raise self.error
        stats = {"physicalInputBytes": 100} if engine != "postgres" else {}
        return self.rows, float(self.calls), stats


def _measure(
    fake: FakeEngines, reference: list[list[Q.Value]] | None, engine: str = "trino-iceberg"
) -> dict[str, Any]:
    return query.measure(cast(query.Engines, fake), engine, "SELECT 1", 3, reference)


def test_a_matching_cell_discards_the_first_run_and_times_the_rest() -> None:
    fake = FakeEngines([["enwiki", 3]])
    cell = _measure(fake, [["enwiki", 3]])
    assert fake.calls == 4
    assert cell["warmup_seconds"] == 1.0
    assert cell["seconds"] == [2.0, 3.0, 4.0]
    assert cell["summary"]["median"] == 3.0
    assert len(cell["stats"]) == 3
    assert "mismatch" not in cell


def test_a_result_that_differs_from_postgres_voids_the_cell() -> None:
    fake = FakeEngines([["enwiki", 4]])
    cell = _measure(fake, [["enwiki", 3]])
    assert fake.calls == 1
    assert "seconds" not in cell
    assert "differs" in cell["mismatch"]


def test_a_failing_query_is_recorded_as_an_error() -> None:
    cell = _measure(
        FakeEngines([], error=RuntimeError("Query exceeded per-node memory limit")), None
    )
    assert cell["error"].startswith("RuntimeError: Query exceeded")
    assert "seconds" not in cell


def test_postgres_cells_carry_no_engine_statistics() -> None:
    cell = _measure(FakeEngines([[1]]), None, engine="postgres")
    assert "stats" not in cell
    assert cell["result"] == [[1]]


def test_each_engine_restarts_the_containers_it_reads_through() -> None:
    assert query.ENGINE_SERVICES == {
        "postgres": ("postgres",),
        "trino-postgres": ("postgres", "trino"),
        "trino-iceberg": ("minio", "trino"),
    }
    assert set(query.LAYOUTS) == {"partitioned", "unpartitioned", "uncompacted"}

"""The query set: templates, per-engine rendering, and the cross-engine result check."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from bench import queries as Q  # noqa: N812

WINDOW = Q.Window(datetime(2026, 10, 21, 5, tzinfo=UTC), "6f0c3b4e-1d2a-5b3c-8d4e-5f6a7b8c9d0e")


def test_the_set_is_ten_queries_of_known_kinds() -> None:
    queries = Q.load_queries()
    assert [q.name.split("_", 1)[0] for q in queries] == [f"q{i:02d}" for i in range(1, 11)]
    kinds = {q.name: q.kind for q in queries}
    assert sorted(n for n, k in kinds.items() if k == "window") == [
        "q05_hour_pages",
        "q06_hour_editors",
        "q07_six_hours_growth",
    ]
    assert [n for n, k in kinds.items() if k == "lookup"] == ["q08_lookup"]
    assert all(q.title and not q.title.startswith("q") for q in queries)


def test_only_the_percentile_has_a_trino_variant() -> None:
    variants = [q.name for q in Q.load_queries() if q.has_variant]
    assert variants == ["q09_emit_delay"]
    q09 = next(q for q in Q.load_queries() if q.has_variant)
    assert "percentile_disc" in Q.render(q09, "postgres", "t", WINDOW)
    assert "percentile_disc" not in Q.render(q09, "trino-iceberg", "t", WINDOW)
    assert "row_number()" in Q.render(q09, "trino-iceberg", "t", WINDOW)
    assert q09.sql("trino-postgres") == q09.sql("trino-iceberg")


@pytest.mark.parametrize("engine", Q.ENGINES)
def test_every_query_renders_on_every_engine(engine: str) -> None:
    for query in Q.load_queries():
        sql = Q.render(query, engine, Q.table_ref(engine, "events_1m"), WINDOW)
        assert "{" not in sql
        assert "--" not in sql  # header and comment lines are dropped
        assert Q.table_ref(engine, "events_1m") in sql


def test_timestamps_and_ids_are_literals_in_each_dialect() -> None:
    q05 = next(q for q in Q.load_queries() if q.name == "q05_hour_pages")
    pg = Q.render(q05, "postgres", "bench.events_1m", WINDOW)
    assert "event_ts >= TIMESTAMPTZ '2026-10-21 05:00:00+00'" in pg
    assert "event_ts < TIMESTAMPTZ '2026-10-21 06:00:00+00'" in pg
    trino = Q.render(q05, "trino-iceberg", "iceberg.bench.events_1m", WINDOW)
    assert "event_ts >= TIMESTAMP '2026-10-21 05:00:00 UTC'" in trino
    q07 = next(q for q in Q.load_queries() if q.name == "q07_six_hours_growth")
    assert "TIMESTAMPTZ '2026-10-21 11:00:00+00'" in Q.render(q07, "postgres", "t", WINDOW)
    q08 = next(q for q in Q.load_queries() if q.name == "q08_lookup")
    assert f"event_id = '{WINDOW.event_id}'" in Q.render(q08, "postgres", "t", WINDOW)
    assert f"event_id = UUID '{WINDOW.event_id}'" in Q.render(q08, "trino-postgres", "t", WINDOW)
    assert f"event_id = '{WINDOW.event_id}'" in Q.render(q08, "trino-iceberg", "t", WINDOW)


def test_an_id_that_is_not_a_uuid_is_refused() -> None:
    q08 = next(q for q in Q.load_queries() if q.name == "q08_lookup")
    bad = Q.Window(WINDOW.hour_start, "x' OR '1'='1")
    with pytest.raises(ValueError, match="badly formed"):
        Q.render(q08, "postgres", "t", bad)


def test_an_unknown_placeholder_is_an_error(tmp_path: Path) -> None:
    (tmp_path / "q01_x.sql").write_text("-- title: x\n-- kind: scan\nSELECT {nope} FROM {events}\n")
    (query,) = Q.load_queries(tmp_path)
    with pytest.raises(KeyError, match="nope"):
        Q.render(query, "postgres", "t", WINDOW)


def test_a_query_without_a_known_kind_is_refused(tmp_path: Path) -> None:
    (tmp_path / "q01_x.sql").write_text("-- title: x\nSELECT 1\n")
    with pytest.raises(ValueError, match="kind"):
        Q.load_queries(tmp_path)


def test_layouts_exist_only_in_iceberg() -> None:
    assert Q.table_ref("trino-iceberg", "events_1m", layout="_flat") == (
        "iceberg.bench.events_1m_flat"
    )
    assert Q.table_ref("trino-postgres", "events_1m") == "postgresql.bench.events_1m"
    with pytest.raises(ValueError, match="layouts"):
        Q.table_ref("postgres", "events_1m", layout="_flat")


def test_the_window_is_the_hour_holding_the_midpoint() -> None:
    first = datetime(2026, 10, 16, 23, 45, 26, tzinfo=UTC)
    last = first + timedelta(hours=12, minutes=5)
    assert Q.middle_hour(first, last) == datetime(2026, 10, 17, 5, tzinfo=UTC)
    assert WINDOW.hour_end - WINDOW.hour_start == timedelta(hours=1)
    assert WINDOW.six_hours_end - WINDOW.hour_start == timedelta(hours=6)


def test_the_federated_query_joins_the_warehouse_mart() -> None:
    fed = Q.load_federated()
    assert "postgresql.marts.dim_wiki" in fed.sql("trino-iceberg")
    assert "JOIN marts.dim_wiki" in fed.sql("postgres")
    sql = Q.render(fed, "trino-iceberg", "iceberg.bench.events_1m", WINDOW)
    assert "FROM iceberg.bench.events_1m AS e" in sql


def test_values_are_normalised_across_drivers() -> None:
    eastern = timezone(timedelta(hours=-5))
    assert Q.normalise(Decimal("0.395")) == 0.395
    assert Q.normalise(datetime(2026, 10, 17, 5)) == "2026-10-17T05:00:00+00:00"
    assert Q.normalise(datetime(2026, 10, 17, 0, tzinfo=eastern)) == "2026-10-17T05:00:00+00:00"
    assert Q.normalise(uuid.UUID(WINDOW.event_id)) == WINDOW.event_id
    assert Q.normalise(None) is None
    assert Q.normalise(True) is True
    with pytest.raises(TypeError):
        Q.normalise(b"bytes")


def test_results_compare_as_multisets_with_a_float_tolerance() -> None:
    expected: list[list[Q.Value]] = [["enwiki", 3, 0.25], ["dewiki", 1, None]]
    assert Q.compare(expected, [["dewiki", 1, None], ["enwiki", 3, 0.25 * (1 + 1e-12)]]) is None
    assert Q.compare([[3]], [[3.0]]) is None
    assert Q.compare([[1_000_000_000]], [[1_000_000_001]]) is not None  # counts compare exactly
    assert Q.compare(expected, expected[:1]) == "1 rows, expected 2"
    differs = Q.compare(expected, [["enwiki", 3, 0.2501], ["dewiki", 1, None]])
    assert differs is not None
    assert "differs" in differs
    assert Q.compare([[True]], [[1]]) is not None
    assert Q.compare([["a", 1], ["a", 1]], [["a", 1], ["a", 2]]) is not None  # duplicates count


def test_the_description_records_both_texts_only_where_they_differ() -> None:
    described = Q.describe(Q.load_queries())
    assert len(described) == 10
    assert [d["name"] for d in described if d["trino"] is not None] == ["q09_emit_delay"]

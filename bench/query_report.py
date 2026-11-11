"""RESULTS.md's query-engine tables, rendered from ``bench/results/m6-query/``.

Called by ``bench.report`` (``make bench-results``), whose ``--check`` and unit
test then cover these tables too. Every cell is read from a raw file; a cell
whose result did not match PostgreSQL's, or whose query failed, shows that
instead of a time.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
QUERY = ROOT / "bench/results/m6-query"
DASH = "\u2013"  # en dash, for empty cells
TIE = 1.1
"""Medians closer than this ratio are not called a win (run-to-run spread is about that size)."""
ENGINE_NAMES = {
    "postgres": "PostgreSQL",
    "trino-postgres": "Trino over PostgreSQL",
    "trino-iceberg": "Trino over Iceberg",
}
LAYOUT_NAMES = {
    "partitioned": "Iceberg, by hour, compacted",
    "unpartitioned": "Iceberg, unpartitioned",
    "uncompacted": "Iceberg, by hour, as streamed",
}


def _read(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(path.read_text())
    return data


def scale_label(rows: int) -> str:
    if rows % 1_000_000 == 0:
        return f"{rows // 1_000_000}M"
    if rows % 1_000 == 0:
        return f"{rows // 1_000}k"
    return f"{rows:,}"


def fmt_seconds(seconds: float | None) -> str:
    """Milliseconds below a second (one decimal under 10 ms), seconds above."""
    if seconds is None:
        return DASH
    if seconds >= 1:
        return f"{seconds:.2f} s"
    ms = seconds * 1000
    return f"{ms:.1f} ms" if ms < 10 else f"{ms:.0f} ms"


def fmt_bytes(value: float | None) -> str:
    if value is None:
        return DASH
    for unit, size in (("GB", 1e9), ("MB", 1e6), ("kB", 1e3)):
        if value >= size:
            return f"{value / size:.1f} {unit}"
    return f"{value:.0f} B"


def _n(value: float | None) -> str:
    return DASH if value is None else f"{value:,.0f}"


def cell_text(cell: Mapping[str, Any] | None) -> str:
    """``median / p95`` of a warm cell, or why there is none."""
    if cell is None:
        return DASH
    if "error" in cell:
        return "error"
    if "mismatch" in cell:
        return "result differs"
    summary = cell.get("summary")
    if not summary:
        return DASH
    return f"{fmt_seconds(summary['median'])} / {fmt_seconds(summary['p95'])}"


def _scales(results: Path, prefix: str) -> list[tuple[str, dict[str, Any], int]]:
    """(label, data, rows) per scale that has a ``<prefix>-<label>.json``, smallest first."""
    found = []
    for path in results.glob(f"{prefix}-*.json"):
        label = path.stem.removeprefix(f"{prefix}-")
        load = results / f"load-{label}.json"
        if not load.exists():
            continue
        found.append((label, _read(path), int(_read(load)["rows"])))
    return sorted(found, key=lambda item: item[2])


def _query_names(cells: Mapping[str, Any]) -> list[str]:
    return sorted({key.split("/", 1)[0] for key in cells})


def _titles(results: Path) -> dict[str, str]:
    from bench.queries import load_queries

    return {q.name: q.title for q in load_queries()}


def fastest(cells: Mapping[str, Any], query: str) -> str:
    """The engine with the lowest median among cells that matched, with its lead."""
    medians = {
        engine: cell["summary"]["median"]
        for engine in ENGINE_NAMES
        if (cell := cells.get(f"{query}/{engine}")) and cell.get("summary")
    }
    if not medians:
        return DASH
    ordered = sorted(medians.items(), key=lambda kv: kv[1])
    best, best_s = ordered[0]
    if len(ordered) == 1:
        return ENGINE_NAMES[best]
    ratio = ordered[1][1] / best_s if best_s > 0 else float("inf")
    if ratio < TIE:
        return f"{ENGINE_NAMES[best]} and {ENGINE_NAMES[ordered[1][0]]} within 10%"
    return f"{ENGINE_NAMES[best]} ({ratio:.1f}x)"


def latency_table(results: Path = QUERY) -> str:
    titles = _titles(results)
    lines = [
        "| Query | Rows | PostgreSQL | Trino over PostgreSQL | Trino over Iceberg | Fastest "
        "(lead over the next) |",
        "|---|---|---|---|---|---|",
    ]
    scales = _scales(results, "warm")
    if not scales:
        return "No run recorded yet."
    for query in _query_names(scales[0][1]["cells"]):
        for _, data, rows in scales:
            cells = data["cells"]
            lines.append(
                f"| {query.split('_', 1)[0]} {titles.get(query, query)} | {scale_label(rows)} | "
                + " | ".join(cell_text(cells.get(f"{query}/{e}")) for e in ENGINE_NAMES)
                + f" | {fastest(cells, query)} |"
            )
    return "\n".join(lines)


def cold_table(results: Path = QUERY) -> str:
    titles = _titles(results)
    scales = _scales(results, "cold")
    if not scales:
        return "No run recorded yet."
    lines = [
        "| Query | Rows | PostgreSQL | Trino over PostgreSQL | Trino over Iceberg |",
        "|---|---|---|---|---|",
    ]

    def one(cell: Mapping[str, Any] | None) -> str:
        if cell is None:
            return DASH
        if "error" in cell:
            return "error"
        if "mismatch" in cell:
            return "result differs"
        return fmt_seconds(cell.get("seconds"))

    for query in _query_names(scales[0][1]["cells"]):
        for _, data, rows in scales:
            cells = data["cells"]
            lines.append(
                f"| {query.split('_', 1)[0]} {titles.get(query, query)} | {scale_label(rows)} | "
                + " | ".join(one(cells.get(f"{query}/{e}")) for e in ENGINE_NAMES)
                + " |"
            )
    return "\n".join(lines)


def _median_stat(cell: Mapping[str, Any], key: str) -> float | None:
    values = sorted(s[key] for s in cell.get("stats", []) if s.get(key) is not None)
    if not values:
        return None
    mid = len(values) // 2
    return float(values[mid]) if len(values) % 2 else (values[mid - 1] + values[mid]) / 2


def pruning_table(results: Path = QUERY) -> str:
    titles = _titles(results)
    scales = _scales(results, "pruning")
    if not scales:
        return "No run recorded yet."
    lines = [
        "| Query | Rows | Layout | Data files in the table | Files the window's bounds match "
        "| Splits read | Rows read | Bytes read | Median / p95 |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for _, data, rows in scales:
        label = scale_label(rows)
        load = _read(results / f"load-{data['name'].removeprefix('events_')}.json")
        warm_path = results / f"warm-{data['name'].removeprefix('events_')}.json"
        warm = _read(warm_path)["cells"] if warm_path.exists() else {}
        cells = data["cells"]
        for query in _query_names(cells):
            name = f"{query.split('_', 1)[0]} {titles.get(query, query)}"
            for layout, layout_name in LAYOUT_NAMES.items():
                cell = cells.get(f"{query}/{layout}")
                if cell is None:
                    continue
                scan = cell.get("scan", {})
                window = cell.get("window_files", {})
                lines.append(
                    f"| {name} | {label} | {layout_name} | "
                    f"{_n(load['iceberg'][layout]['files'])} | {_n(window.get('files'))} | "
                    f"{_n(scan.get('splits'))} | {_n(scan.get('rows'))} | "
                    f"{fmt_bytes(_median_stat(cell, 'physicalInputBytes'))} | {cell_text(cell)} |"
                )
            pg = cells.get(f"{query}/postgres")
            if pg is not None:
                buffers = pg["buffers"]
                heap_blocks = load["postgres"]["heap_bytes"] / 8192
                lines.append(
                    f"| {name} | {label} | PostgreSQL heap, BRIN on `event_ts` | {DASH} | {DASH} | "
                    f"{_n(buffers['blocks'])} of {_n(heap_blocks)} blocks | "
                    f"{DASH} | {fmt_bytes(buffers['blocks'] * 8192)} | "
                    f"{cell_text(warm.get(f'{query}/postgres'))} |"
                )
    return "\n".join(lines)


def storage_table(results: Path = QUERY) -> str:
    loads = sorted((_read(p) for p in results.glob("load-*.json")), key=lambda d: int(d["rows"]))
    if not loads:
        return "No run recorded yet."
    lines = [
        "| Rows | Store | Data files | Size | Median file | Written in |",
        "|---|---|---|---|---|---|",
    ]
    for load in loads:
        label = scale_label(int(load["rows"]))
        pg = load["postgres"]
        gen = load["generator"]["seconds"]
        lines.append(
            f"| {label} | PostgreSQL heap (primary key {fmt_bytes(pg['pkey_bytes'])}, "
            f"BRIN {fmt_bytes(pg['brin_bytes'])}) | {DASH} | {fmt_bytes(pg['table_bytes'])} | "
            f"{DASH} | {fmt_seconds(gen.get('postgres'))} (COPY, keys, VACUUM) |"
        )
        written = {
            "partitioned": f"{fmt_seconds(gen.get('iceberg'))} + optimize "
            f"{fmt_seconds(load['optimize_seconds'])}",
            "unpartitioned": f"{fmt_seconds(load['flat_ctas_seconds'])} (Trino CTAS)",
            "uncompacted": f"{fmt_seconds(gen.get('stream'))}",
        }
        for layout, layout_name in LAYOUT_NAMES.items():
            ice = load["iceberg"][layout]
            lines.append(
                f"| {label} | {layout_name} | {_n(ice['files'])} | {fmt_bytes(ice['bytes'])} | "
                f"{fmt_bytes(ice['median_file_bytes'])} | {written[layout]} |"
            )
    return "\n".join(lines)


def federated_table(results: Path = QUERY) -> str:
    scales = _scales(results, "federated")
    if not scales:
        return "No run recorded yet."
    lines = [
        "| Rows | Trino: Iceberg ⋈ PostgreSQL | The same join inside PostgreSQL | Result |",
        "|---|---|---|---|",
    ]
    for _, data, rows in scales:
        trino, pg = data["trino"], data["postgres"]
        result = "matches" if trino.get("summary") else cell_text(trino)
        lines.append(
            f"| {scale_label(rows)} | {cell_text(trino)} | {cell_text(pg)} | "
            f"{result}, {trino.get('result_rows', 0)} rows |"
        )
    return "\n".join(lines)


def federated_result(results: Path = QUERY) -> str:
    """The largest scale's federated result as a table."""
    scales = _scales(results, "federated")
    if not scales:
        return "No run recorded yet."
    _, data, rows = scales[-1]
    out = [
        f"Result at {scale_label(rows)} rows, hour starting {data['window']['hour_start']}:",
        "",
        "| project_family | events | wikis |",
        "|---|---|---|",
    ]
    for family, events, wikis in data["trino"].get("result", []):
        out.append(f"| {family if family is not None else '(none)'} | {events:,} | {wikis} |")
    return "\n".join(out)


def environment_line(results: Path = QUERY) -> str:
    path = results / "environment.json"
    if not path.exists():
        return "No run recorded yet."
    env = _read(path)
    res = env["resources"]
    limits = ", ".join(
        f"{name} {c['mem_limit_bytes'] / 2**30:g} GiB"
        for name, c in sorted(res["containers"].items())
        if c.get("mem_limit_bytes") and name in {"postgres", "trino", "minio"}
    )
    pg = env["postgres_settings"]
    return (
        f"{res['nproc']} vCPU ({res['cpu_model']}), {res['mem_total_kb'] / 2**20:.1f} GiB RAM, "
        f"Linux {res['kernel']}, Docker {res['docker']}. Memory limits: {limits}; no CPU limits. "
        f"PostgreSQL {pg['server_version'].split()[0]} (shared_buffers {pg['shared_buffers']}, "
        f"session work_mem {pg['work_mem']}, max_parallel_workers_per_gather "
        f"{pg['max_parallel_workers_per_gather']}, jit {pg['jit']}); Trino {env['trino_version']}. "
        f"Host page cache dropped before cold runs: "
        f"{'yes' if env.get('page_cache_droppable') else 'no'}."
    )


def render(results: Path = QUERY) -> dict[str, str]:
    if not results.exists():
        return {}
    return {
        "query-environment": environment_line(results),
        "query-storage": storage_table(results),
        "query-latency": latency_table(results),
        "query-cold": cold_table(results),
        "query-pruning": pruning_table(results),
        "query-federated": federated_table(results),
        "query-federated-result": federated_result(results),
    }


def summaries(cells: Mapping[str, Any], keys: Sequence[str]) -> dict[str, float | None]:
    """Medians of the named cells (for prose checks in tests)."""
    return {k: (cells.get(k) or {}).get("summary", {}).get("median") for k in keys}

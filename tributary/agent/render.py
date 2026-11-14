"""An answer document (``Answer.to_dict()`` or the API's JSON) for a terminal."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

MAX_ROWS = 20
MAX_WIDTH = 40


def _cell(value: object) -> str:
    text = "NULL" if value is None else str(value)
    return text if len(text) <= MAX_WIDTH else text[: MAX_WIDTH - 1] + "…"


def table(columns: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    """A plain aligned table of at most :data:`MAX_ROWS` rows."""
    shown = [[_cell(v) for v in row] for row in rows[:MAX_ROWS]]
    widths = [len(c) for c in columns]
    for row in shown:
        widths = [max(w, len(v)) for w, v in zip(widths, row, strict=True)]
    lines = [
        "  ".join(c.ljust(w) for c, w in zip(columns, widths, strict=True)),
        "  ".join("-" * w for w in widths),
    ]
    lines += ["  ".join(v.ljust(w) for v, w in zip(row, widths, strict=True)) for row in shown]
    if len(rows) > MAX_ROWS:
        lines.append(f"... {len(rows) - MAX_ROWS} more rows")
    return "\n".join(line.rstrip() for line in lines)


def render_answer(document: Mapping[str, object]) -> str:
    out = [str(document.get("answer", ""))]
    sql = document.get("sql")
    if sql:
        out += ["", "SQL:", f"  {sql}"]
    columns = document.get("columns")
    rows = document.get("rows")
    if isinstance(columns, list) and isinstance(rows, list) and columns and rows:
        out += ["", table([str(c) for c in columns], rows)]
    provenance = document.get("provenance")
    if isinstance(provenance, list) and provenance:
        out += ["", "Provenance:"]
        for item in provenance:
            if isinstance(item, Mapping):
                out.append(
                    f"  {item.get('relation')}: rebuilt {item.get('last_refreshed_at') or 'never'}"
                    f", quality {item.get('quality')} ({item.get('checks')} checks)"
                    + (", STALE" if item.get("stale") else "")
                )
    warnings = document.get("warnings")
    if isinstance(warnings, list) and warnings:
        out += ["", "Warnings:", *(f"  - {w}" for w in warnings)]
    agreement = document.get("agreement")
    if agreement == "agreed":
        out += ["", "agreement: agreed (a second prompt variant returned the same rows)"]
    elif agreement == "differed":
        out += [
            "",
            "agreement: differed (a second prompt variant did not return the same rows): "
            "treat this answer with more caution",
        ]
    usage = document.get("usage")
    if document.get("model") and isinstance(usage, Mapping):
        out += [
            "",
            f"{document.get('model')}, {document.get('attempts')} attempt(s), "
            f"{usage.get('input_tokens')} in / {usage.get('output_tokens')} out tokens, "
            f"${usage.get('cost_usd')}, {document.get('latency_ms')} ms",
        ]
    return "\n".join(out)

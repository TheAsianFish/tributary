"""The guard evaluation: every unsafe statement blocked, no benign query blocked.

Two sets go through :class:`tributary.agent.guard.Guard`: the adversarial suite
in ``eval/guard/unsafe.yaml`` and a benign set, the gold SQL of the warehouse
question set (``eval/warehouse_questions.yaml``, or any ``--benign`` files).
A guard that blocks everything would pass the first half, so the second half
is the check that it still lets the agent answer questions.

The run is pure and offline (no database); the verdicts are written to
``eval/results/guard.json`` with sorted keys and no timestamps, so a rerun on
the same inputs is byte-identical and a diff shows exactly what changed. The
exit code is 1 unless every unsafe item is blocked and no benign item is.

    uv run python -m eval.guard_eval [--benign PATH ...] [--catalog DIR | --relations a.b,c.d]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

import yaml

from tributary.agent.guard import Guard, GuardResult

ROOT = Path(__file__).resolve().parent.parent
UNSAFE_PATH = ROOT / "eval" / "guard" / "unsafe.yaml"
BENIGN_PATH = ROOT / "eval" / "warehouse_questions.yaml"
RESULT_PATH = ROOT / "eval" / "results" / "guard.json"

# The agent's allowlist is the catalog's relations; the committed fixture is the
# published catalog of the sample, so the eval needs no database.
CATALOG_PATH = ROOT / "tests" / "fixtures" / "catalog"


def catalog_relations(catalog_dir: Path = CATALOG_PATH) -> tuple[str, ...]:
    from tributary.agent.catalog import load_dbt

    return load_dbt(catalog_dir).relations()


CATEGORIES = frozenset(
    {
        "dml",
        "ddl",
        "stacked",
        "comment_trick",
        "dangerous_function",
        "file_access",
        "system_catalog",
        "outside_allowlist",
        "session",
        "locking",
        "privilege",
        "exfiltration",
        "encoding",
        "procedural",
    }
)


@dataclass(frozen=True, slots=True)
class Item:
    """One statement to judge. ``category`` is "benign" for the benign set."""

    id: str
    category: str
    sql: str
    source: str


@dataclass(frozen=True, slots=True)
class Verdict:
    item: Item
    result: GuardResult

    @property
    def correct(self) -> bool:
        """Unsafe items must be blocked, benign ones allowed."""
        return self.result.allowed == (self.item.category == "benign")


@dataclass(frozen=True, slots=True)
class GuardReport:
    unsafe: tuple[Verdict, ...]
    benign: tuple[Verdict, ...]

    @property
    def unsafe_blocked(self) -> int:
        return sum(not v.result.allowed for v in self.unsafe)

    @property
    def benign_blocked(self) -> tuple[Verdict, ...]:
        return tuple(v for v in self.benign if not v.result.allowed)

    @property
    def unsafe_missed(self) -> tuple[Verdict, ...]:
        return tuple(v for v in self.unsafe if v.result.allowed)

    @property
    def passed(self) -> bool:
        return not self.unsafe_missed and not self.benign_blocked

    def by_category(self) -> dict[str, tuple[int, int]]:
        """category -> (blocked, total) over the unsafe set."""
        totals: Counter[str] = Counter(v.item.category for v in self.unsafe)
        blocked: Counter[str] = Counter(
            v.item.category for v in self.unsafe if not v.result.allowed
        )
        return {c: (blocked[c], totals[c]) for c in sorted(totals)}

    def by_code(self) -> dict[str, int]:
        """reason code -> number of unsafe items it fired on (an item can have several)."""
        counts: Counter[str] = Counter(
            code for v in self.unsafe for code in dict.fromkeys(v.result.codes)
        )
        return dict(sorted(counts.items()))

    def to_json(self) -> dict[str, object]:
        def verdict(v: Verdict) -> dict[str, object]:
            return {
                "allowed": v.result.allowed,
                "category": v.item.category,
                "correct": v.correct,
                "id": v.item.id,
                "reasons": list(v.result.reasons),
                "source": v.item.source,
                "sql": v.result.sql,
                "tables": list(v.result.tables),
            }

        return {
            "benign": {
                "blocked": len(self.benign_blocked),
                "items": [verdict(v) for v in self.benign],
                "total": len(self.benign),
            },
            "passed": self.passed,
            "unsafe": {
                "blocked": self.unsafe_blocked,
                "by_category": {c: list(n) for c, n in self.by_category().items()},
                "by_code": self.by_code(),
                "items": [verdict(v) for v in self.unsafe],
                "total": len(self.unsafe),
            },
        }


def evaluate(guard: Guard, unsafe: Iterable[Item], benign: Iterable[Item]) -> GuardReport:
    """Judge every item; pure, no I/O."""
    return GuardReport(
        unsafe=tuple(Verdict(item, guard.check(item.sql)) for item in unsafe),
        benign=tuple(Verdict(item, guard.check(item.sql)) for item in benign),
    )


def load_unsafe(path: Path) -> list[Item]:
    data = yaml.safe_load(path.read_text())
    entries = data.get("unsafe") if isinstance(data, dict) else None
    if not isinstance(entries, list):
        raise ValueError(f"{path}: expected a list under 'unsafe'")
    items: list[Item] = []
    for entry in entries:
        category = str(entry["category"])
        if category not in CATEGORIES:
            raise ValueError(f"{path}: {entry['id']}: unknown category {category!r}")
        items.append(Item(str(entry["id"]), category, str(entry["sql"]), path.name))
    _check_unique(items, path)
    return items


def load_benign(path: Path) -> list[Item]:
    data = yaml.safe_load(path.read_text())
    entries = data.get("questions") if isinstance(data, dict) else None
    if not isinstance(entries, list):
        raise ValueError(f"{path}: expected a list under 'questions'")
    items = [Item(str(e["id"]), "benign", str(e["sql"]), path.name) for e in entries]
    _check_unique(items, path)
    return items


def _check_unique(items: Sequence[Item], path: Path) -> None:
    seen: Counter[str] = Counter(item.id for item in items)
    duplicates = sorted(i for i, n in seen.items() if n > 1)
    if duplicates:
        raise ValueError(f"{path}: duplicate ids: {', '.join(duplicates)}")


def render(report: GuardReport) -> str:
    lines: list[str] = []
    total = len(report.unsafe)
    share = 100.0 * report.unsafe_blocked / total if total else 0.0
    lines.append(f"unsafe blocked     {report.unsafe_blocked}/{total} ({share:.1f}%)")
    for category, (caught, n) in report.by_category().items():
        lines.append(f"  {category:<20} {caught}/{n}")
    lines.append("reason codes (unsafe items)")
    for code, n in report.by_code().items():
        lines.append(f"  {code:<20} {n}")
    for v in report.unsafe_missed:
        lines.append(f"  MISSED {v.item.id}: {v.item.sql.strip()}")
    false_blocks = report.benign_blocked
    lines.append(f"benign false blocks {len(false_blocks)}/{len(report.benign)}")
    for v in false_blocks:
        lines.append(f"  {v.item.source}:{v.item.id}: {'; '.join(v.result.reasons)}")
    lines.append("PASS" if report.passed else "FAIL")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m eval.guard_eval", description=__doc__)
    parser.add_argument("--unsafe", type=Path, default=UNSAFE_PATH)
    parser.add_argument("--benign", type=Path, action="append", help="question YAML (repeatable)")
    parser.add_argument("--relations", help="comma-separated schema.table allowlist")
    parser.add_argument(
        "--catalog", type=Path, default=CATALOG_PATH, help="published catalog for the allowlist"
    )
    parser.add_argument("--max-rows", type=int, default=1000)
    parser.add_argument("--output", type=Path, default=RESULT_PATH)
    args = parser.parse_args(argv)

    # sqlglot warns on every statement it falls back to exp.Command for; those are the
    # suite's point, and the verdicts already say what happened.
    logging.getLogger("sqlglot").setLevel(logging.ERROR)

    relations = (
        tuple(r.strip() for r in args.relations.split(",") if r.strip())
        if args.relations
        else catalog_relations(args.catalog)
    )
    benign_paths: list[Path] = args.benign or [BENIGN_PATH]
    benign: list[Item] = []
    for path in benign_paths:
        if path.exists():
            benign.extend(load_benign(path))
        else:
            print(f"WARNING: benign set {path} does not exist", file=sys.stderr)
    if not benign:
        print(
            "WARNING: the benign set is EMPTY; false blocks are not being measured",
            file=sys.stderr,
        )

    report = evaluate(Guard(relations, max_rows=args.max_rows), load_unsafe(args.unsafe), benign)
    print(render(report))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report.to_json(), indent=2, sort_keys=True) + "\n")
    print(f"wrote {_display(args.output)}")
    return 0 if report.passed else 1


def _display(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


if __name__ == "__main__":
    sys.exit(main())

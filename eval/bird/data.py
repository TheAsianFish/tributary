"""Where BIRD Mini-Dev comes from, the two label tracks, and the fixed subsets.

Sources (each pinned by sha256, downloaded once into the cache):

- **Questions, official labels**: the 2025-07-04 release of BIRD Mini-Dev in
  PostgreSQL (500 questions, 148 simple, 250 moderate, 102 challenging), as
  archived on Hugging Face. The copy inside ``minidev.zip`` is older and
  numbers two financial questions twice (137 and 138 stand where 119 and 120
  belong), so it is not used for questions.
- **Questions, corrected labels**: Arcwise-Plat-SQL from the VLDB 2026
  annotation-error study (``uiuc-kang-lab/text_to_sql_benchmarks``): 498 of
  the 500 questions (119 and 120 missing) with corrected SQLite gold. Its
  question text is the original BIRD dev wording, which differs from the
  Mini-Dev wording for 81 questions, so the track asks its own questions.
- **Databases**: ``minidev.zip`` from BIRD's object store: the eleven SQLite
  databases with their column-description CSVs and a PostgreSQL dump of all
  eleven in one schema.

Both question files are committed under ``eval/bird/data/`` (CC BY-SA 4.0,
unmodified) so that ``make eval-link`` and the replayed ``make eval`` run
offline in CI; the databases are never committed.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import zipfile
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import httpx

from tributary.config import REPO_ROOT

DATA_DIR = REPO_ROOT / "eval" / "bird" / "data"
DEFAULT_CACHE = Path.home() / ".cache" / "tributary" / "bird"

Track = Literal["official", "corrected"]
TRACKS: tuple[Track, ...] = ("official", "corrected")
Difficulty = Literal["simple", "moderate", "challenging"]
DIFFICULTIES: tuple[Difficulty, ...] = ("simple", "moderate", "challenging")

DATABASES: tuple[str, ...] = (
    "california_schools",
    "card_games",
    "codebase_community",
    "debit_card_specializing",
    "european_football_2",
    "financial",
    "formula_1",
    "student_club",
    "superhero",
    "thrombosis_prediction",
    "toxicology",
)


@dataclass(frozen=True, slots=True)
class Source:
    name: str
    url: str
    sha256: str
    size: int


OFFICIAL = Source(
    "mini_dev_pg-2025-07-04.json",
    "https://huggingface.co/datasets/birdsql/bird_mini_dev/resolve/"
    "5b256b5a9c17478f06ed6912612e9a14d17065c8/archive/"
    "bird_mini_dev_2025-07-04/data/mini_dev_pg-00000-of-00001.json",
    "7fa740ef9225389cff6c34432120e8325d0ca3008d73db1ae38731234bc10da7",
    283_551,
)
CORRECTED = Source(
    "arcwise_plat_sql_only_with_diff.json",
    # URLs name the commit the file was taken from, not a branch.
    "https://raw.githubusercontent.com/uiuc-kang-lab/text_to_sql_benchmarks/"
    "b83d8a899c3e0a771feccfa9ae0ece89885f2898/data/arcwise_plat_sql_only_with_diff.json",
    "e7bf76408f99266506ea558d84982c69422e5db3dfecab5f08a3a8d3900e8395",
    287_718,
)
MINIDEV_ZIP = Source(
    "minidev.zip",
    "https://bird-bench.oss-cn-beijing.aliyuncs.com/minidev.zip",
    "cc48ba16838204e4e214512030cb572eeb5f7bcdd999bae4b9b6ff12ec13b92f",
    800_943_648,
)


class SourceError(Exception):
    """A download failed or a file does not match its pinned checksum."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def fetch(source: Source, cache: Path, *, client: httpx.Client | None = None) -> Path:
    """The file in the cache, downloaded first if missing; a checksum mismatch is fatal.

    The download goes to a temporary file in the cache directory and is renamed
    only once its checksum matches, so an interrupted or tampered download never
    looks like a cached one.
    """
    target = cache / source.name
    if target.exists():
        if sha256_file(target) != source.sha256:
            raise SourceError(
                f"{target} does not match its pinned sha256; delete it to download again"
            )
        return target
    cache.mkdir(parents=True, exist_ok=True)
    own = client is None
    http = client or httpx.Client(timeout=httpx.Timeout(60.0, read=300.0), follow_redirects=True)
    fd, tmp = tempfile.mkstemp(dir=cache, prefix=f".{source.name}.", suffix=".part")
    try:
        digest = hashlib.sha256()
        with os.fdopen(fd, "wb") as out, http.stream("GET", source.url) as reply:
            if reply.status_code != 200:
                raise SourceError(f"GET {source.url}: HTTP {reply.status_code}")
            for chunk in reply.iter_bytes(1 << 20):
                digest.update(chunk)
                out.write(chunk)
        if digest.hexdigest() != source.sha256:
            raise SourceError(
                f"{source.url} has sha256 {digest.hexdigest()}, expected {source.sha256}"
            )
        os.replace(tmp, target)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    finally:
        if own:
            http.close()
    return target


def unpack(zip_path: Path, cache: Path) -> Path:
    """``minidev.zip`` extracted once; returns the directory holding ``MINIDEV``."""
    root = cache / "minidev"
    marker = root / ".complete"
    if marker.exists():
        return root
    shutil.rmtree(root, ignore_errors=True)
    with zipfile.ZipFile(zip_path) as archive:
        for member in archive.namelist():
            if member.startswith("/") or ".." in Path(member).parts:
                raise SourceError(f"refusing to extract {member!r} from {zip_path}")
        # Only what the loader reads: the SQLite files, descriptions and the PostgreSQL dump.
        wanted = [
            m
            for m in archive.namelist()
            if m.startswith(("minidev/MINIDEV/dev_databases/", "minidev/MINIDEV_postgresql/"))
            or m == "minidev/MINIDEV/dev_tables.json"
        ]
        archive.extractall(cache, members=wanted)
    marker.write_text("ok\n")
    return root


@dataclass(frozen=True, slots=True)
class BirdQuestion:
    id: int
    db_id: str
    question: str
    evidence: str
    sql: str
    difficulty: str
    track: Track
    # The gold as its source wrote it (SQLite for the corrected track); equal to sql otherwise.
    source_sql: str

    @property
    def prompt(self) -> str:
        """The question as the agent receives it: BIRD's external knowledge follows it."""
        text = self.question.strip()
        evidence = self.evidence.strip()
        return f"{text}\nEvidence: {evidence}" if evidence else text


def _read(path: Path) -> list[dict[str, object]]:
    with path.open(encoding="utf-8") as fh:
        document = json.load(fh)
    if not isinstance(document, list):
        raise SourceError(f"{path}: expected a JSON list")
    return document


def official_questions(path: Path | None = None) -> list[BirdQuestion]:
    """The 500 official questions with their PostgreSQL gold."""
    rows = _read(path or DATA_DIR / OFFICIAL.name)
    return [
        BirdQuestion(
            id=int(str(r["question_id"])),
            db_id=str(r["db_id"]),
            question=str(r["question"]),
            evidence=str(r.get("evidence") or ""),
            sql=str(r["SQL"]),
            difficulty=str(r["difficulty"]),
            track="official",
            source_sql=str(r["SQL"]),
        )
        for r in rows
    ]


def corrected_questions(
    path: Path | None = None,
    transpiled: Path | None = None,
    official: Sequence[BirdQuestion] | None = None,
) -> list[BirdQuestion]:
    """The corrected track: verified PostgreSQL gold only (``make bird`` writes it).

    Questions whose transpiled gold failed verification are left out, and the
    loader's report says how many. Difficulty comes from the official release
    (Arcwise does not carry it).
    """
    rows = _read(path or DATA_DIR / CORRECTED.name)
    verified = load_transpiled(transpiled)
    difficulty = {q.id: q.difficulty for q in (official or official_questions())}
    out = []
    for r in rows:
        qid = int(str(r["question_id"]))
        entry = verified.get(qid)
        if entry is None or entry.get("status") != "verified":
            continue
        out.append(
            BirdQuestion(
                id=qid,
                db_id=str(r["db_id"]),
                question=str(r["question"]),
                evidence=str(r.get("evidence") or ""),
                sql=str(entry["postgres"]),
                difficulty=difficulty.get(qid, "unknown"),
                track="corrected",
                source_sql=str(r["SQL"]),
            )
        )
    return out


TRANSPILED = DATA_DIR / "corrected_pg.json"


def load_transpiled(path: Path | None = None) -> dict[int, dict[str, object]]:
    target = path or TRANSPILED
    if not target.exists():
        return {}
    with target.open(encoding="utf-8") as fh:
        document = json.load(fh)
    return {int(k): v for k, v in document["questions"].items()}


def questions(track: Track) -> list[BirdQuestion]:
    return official_questions() if track == "official" else corrected_questions()


# --- subsets ----------------------------------------------------------------------------------

# dev50: 50 questions in both tracks, in the official release's difficulty mix (30/50/20).
# holdout50 (M10): 50 more in the same mix, drawn by the same rule from the questions dev50
# did not take, so a variant chosen on dev50 can be checked on questions that did not pick it.
SUBSETS: dict[str, dict[str, int]] = {
    "dev50": {"simple": 15, "moderate": 25, "challenging": 10},
    "holdout50": {"simple": 15, "moderate": 25, "challenging": 10},
}
# Subsets a subset is drawn disjoint from (each named one is drawn first).
DISJOINT_FROM: dict[str, tuple[str, ...]] = {"holdout50": ("dev50",)}
# The ids of each subset, committed when the subset was drawn (before any model saw it):
# a test fails if the rule above ever draws anything else.
SUBSET_IDS = DATA_DIR / "subsets.json"


def _rank(subset: str, qid: int) -> str:
    return hashlib.sha256(f"tributary:{subset}:{qid}".encode()).hexdigest()


def subset_ids(
    name: str, pool: Iterable[BirdQuestion], *, also_in: Iterable[int] = ()
) -> list[int]:
    """A fixed stratified sample: per difficulty, the ids with the smallest seeded hash.

    No random generator is involved, so the subset is the same on every
    machine and Python version. ``also_in`` restricts it to ids the corrected
    source covers (Arcwise's 498), a fixed input: the subset does not move
    when the transpiler improves. The corrected track then grades the
    subset's questions whose transpiled gold verified (45 of dev50's 50).
    A subset listed in ``DISJOINT_FROM`` first removes the ids of the subsets
    it names, drawn by the same rule, and then draws with its own seed.
    """
    quota = SUBSETS[name]
    pool = list(pool)
    allowed = set(also_in)
    taken: set[int] = set()
    for other in DISJOINT_FROM.get(name, ()):
        taken |= set(subset_ids(other, pool, also_in=allowed))
    chosen: list[int] = []
    for difficulty, count in quota.items():
        ids = sorted(
            (
                q.id
                for q in pool
                if q.difficulty == difficulty
                and (not allowed or q.id in allowed)
                and q.id not in taken
            ),
            key=lambda i: _rank(name, i),
        )
        if len(ids) < count:
            raise ValueError(f"subset {name}: only {len(ids)} {difficulty} questions")
        chosen += ids[:count]
    return sorted(chosen)


def committed_subsets(path: Path | None = None) -> dict[str, list[int]]:
    """The committed ids of every named subset (``eval/bird/data/subsets.json``)."""
    document = json.loads((path or SUBSET_IDS).read_text(encoding="utf-8"))
    return {name: [int(i) for i in ids] for name, ids in document["subsets"].items()}


def select(questions_: Sequence[BirdQuestion], subset: str | None) -> list[BirdQuestion]:
    """``questions_`` restricted to a named subset (``None`` or ``all`` keeps every one)."""
    if subset in (None, "all"):
        return list(questions_)
    if subset not in SUBSETS:
        raise ValueError(f"unknown subset {subset!r}: one of {', '.join(SUBSETS)} or all")
    official = official_questions()
    corrected = json.loads((DATA_DIR / CORRECTED.name).read_text(encoding="utf-8"))
    ids = set(subset_ids(subset, official, also_in=(int(r["question_id"]) for r in corrected)))
    return [q for q in questions_ if q.id in ids]


def by_database(questions_: Iterable[BirdQuestion]) -> dict[str, list[BirdQuestion]]:
    grouped: dict[str, list[BirdQuestion]] = {}
    for q in questions_:
        grouped.setdefault(q.db_id, []).append(q)
    return dict(sorted(grouped.items()))

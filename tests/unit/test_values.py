"""Value retrieval: matching question text against a column value index."""

from __future__ import annotations

from pathlib import Path

import pytest

from eval.bird import data
from eval.bird.values import VALUES, encode, load_document, load_value_indexes, skip_reason
from tributary.agent.catalog import Catalog, Column, Table
from tributary.agent.values import _QUOTED, ValueIndex, ValueMatch, hint_lines, indexable

SCHOOLS = "california_schools.schools"
CARDS = "card_games.cards"


def schools() -> ValueIndex:
    return ValueIndex(
        {
            (SCHOOLS, "County"): ("Alameda", "Los Angeles", "San Diego"),
            (SCHOOLS, "City"): ("Alameda", "San Diego", "Ana"),
            (CARDS, "artist"): ("Kev Walker", "C++", "Kev"),
        }
    )


def spans(matches: tuple[ValueMatch, ...]) -> list[tuple[str, str, str]]:
    return [(m.span, m.column, m.value) for m in matches]


def test_whole_words_only() -> None:
    index = schools()
    assert index.match("An Analysis of Ana's results") == (
        ValueMatch(SCHOOLS, "City", "Ana", "Ana"),
    )
    assert index.match("Analysis of Banana") == ()


def test_case_insensitive_match_keeps_the_stored_value_and_the_written_span() -> None:
    (match, *_) = schools().match("schools in alameda county")
    assert match.value == "Alameda"
    assert match.span == "alameda"


def test_longest_span_wins_over_its_parts() -> None:
    matches = schools().match("cards drawn by Kev Walker")
    assert spans(matches) == [("Kev Walker", "artist", "Kev Walker")]


def test_overlapping_spans_keep_the_longest() -> None:
    index = ValueIndex({("t.x", "a"): ("New York", "York City Hall")})
    assert spans(index.match("New York City Hall")) == [("York City Hall", "a", "York City Hall")]


def test_quoted_string_matches_with_punctuation() -> None:
    index = schools()
    assert index.match("written in C++ by someone") == ()
    assert spans(index.match("artist named 'C++' please")) == [("C++", "artist", "C++")]
    assert spans(index.match('artist "c++"')) == [("c++", "artist", "C++")]


def test_apostrophe_in_a_word_opens_no_quote() -> None:
    assert _QUOTED.findall("the driver's best lap and the team's car") == []
    found = _QUOTED.findall("""named 'O'Brien' or "Kev Walker".""")
    assert [single or double for single, double in found] == ["O'Brien", "Kev Walker"]
    index = ValueIndex({("t.x", "a"): ("O'Brien",)})
    assert spans(index.match("the coach 'o'brien'")) == [("o'brien", "a", "O'Brien")]


def test_markup_values_match_only_when_quoted() -> None:
    index = ValueIndex({("m.match", "goal"): ("<goal />",)})
    assert index.match("the most goal scored") == ()
    assert spans(index.match("goal = '<goal />'")) == [("<goal />", "goal", "<goal />")]


@pytest.mark.parametrize(
    "value", ["ab", "1998", "2015/2016", "12 345", "the", "Of The", "  x ", "a" * 101]
)
def test_noise_values_are_not_indexed(value: str) -> None:
    assert not indexable(value)
    index = ValueIndex({("t.x", "a"): (value,)})
    assert index.match(f"text with {value} and '{value}'") == ()


@pytest.mark.parametrize("value", ["Ana", "R2D2", "Kev Walker", "C++", "a" * 100])
def test_real_values_are_indexed(value: str) -> None:
    assert indexable(value)


def test_one_span_in_several_columns_keeps_all_capped_per_span() -> None:
    index = ValueIndex({("t.x", c): ("Alameda",) for c in ("a", "b", "c", "d", "e")})
    matches = index.match("in Alameda")
    assert [m.column for m in matches] == ["a", "b", "c"]
    assert {m.span for m in matches} == {"Alameda"}


def test_exact_case_comes_first_within_a_span() -> None:
    index = ValueIndex({("t.a", "x"): ("ALAMEDA",), ("t.b", "x"): ("Alameda",)})
    assert [m.value for m in index.match("in Alameda")] == ["Alameda", "ALAMEDA"]
    assert [m.value for m in index.match("in ALAMEDA")] == ["ALAMEDA", "Alameda"]
    # The per-span cap keeps the exact-case column even when it sorts last by name.
    crowded = ValueIndex(
        {("t.a", "x"): ("alameda",), ("t.b", "x"): ("ALAMEDA",), ("t.c", "x"): ("aLameda",)}
        | {("t.z", "x"): ("Alameda",)}
    )
    assert [m.relation for m in crowded.match("in Alameda")] == ["t.z", "t.a", "t.b"]


def test_output_in_text_order_and_limit() -> None:
    index = schools()
    text = "San Diego, Los Angeles and alameda"
    assert [m.span for m in index.match(text)] == [
        "San Diego",
        "San Diego",
        "Los Angeles",
        "alameda",
        "alameda",
    ]
    limited = index.match(text, limit=3)
    # Under the limit, each span's best column first: one per span, still in text order.
    assert spans(limited) == [
        ("San Diego", "City", "San Diego"),
        ("Los Angeles", "County", "Los Angeles"),
        ("alameda", "City", "Alameda"),
    ]
    assert index.match(text, limit=0) == ()


def test_a_value_named_twice_is_reported_once() -> None:
    index = ValueIndex({("t.x", "a"): ("Aaron Mooy",)})
    matches = index.match("rating of Aaron Mooy?\nEvidence: Aaron Mooy refers to player_name")
    assert len(matches) == 1
    assert matches[0].value == "Aaron Mooy"


def test_deterministic_regardless_of_insertion_order() -> None:
    columns = {
        ("t.b", "x"): ("Alameda", "Fresno"),
        ("t.a", "y"): ("Fresno", "Alameda"),
        ("t.a", "x"): ("alameda",),
    }
    forward = ValueIndex(columns)
    backward = ValueIndex(dict(reversed(list(columns.items()))))
    text = "Fresno or Alameda"
    assert forward.match(text) == backward.match(text)
    assert [(m.relation, m.column, m.value) for m in forward.match(text)] == [
        ("t.a", "y", "Fresno"),
        ("t.b", "x", "Fresno"),
        ("t.a", "y", "Alameda"),
        ("t.b", "x", "Alameda"),
        ("t.a", "x", "alameda"),
    ]


def test_to_dict_from_dict_round_trip() -> None:
    index = schools()
    again = ValueIndex.from_dict(index.to_dict())
    assert again == index
    assert again.match("Kev Walker in 'C++'") == index.match("Kev Walker in 'C++'")
    assert len(again) == 9


def test_from_catalog_takes_indexed_columns_only() -> None:
    table = Table(
        schema="marts",
        name="edits",
        description="",
        columns=(
            Column("wiki", "text", "", distinct=2, values=("dewiki", "enwiki")),
            Column("title", "text", "", values=None),
            Column("Bot Name", "text", "", values=()),
        ),
        primary_key=(),
        foreign_keys=(),
        upstream=(),
    )
    index = ValueIndex.from_catalog(Catalog(tables=(table,), source="dbt"))
    assert dict(index.columns) == {("marts.edits", "wiki"): ("dewiki", "enwiki")}
    assert index.match("edits on dewiki") == (
        ValueMatch("marts.edits", "wiki", "dewiki", "dewiki"),
    )


def test_hint_lines_quote_identifiers_and_literals() -> None:
    matches = (
        ValueMatch(SCHOOLS, "County", "Alameda", "alameda"),
        ValueMatch("california_schools.frpm", "School Name", "O'Brien High", "O'Brien High"),
        ValueMatch("marts.edits", "wiki", "dewiki", "dewiki"),
    )
    assert hint_lines(matches) == "\n".join(
        [
            '- "alameda" matches california_schools.schools."County" = \'Alameda\'',
            "- \"O'Brien High\" matches california_schools.frpm.\"School Name\" = 'O''Brien High'",
            "- \"dewiki\" matches marts.edits.wiki = 'dewiki'",
        ]
    )
    assert hint_lines(()) == ""


def test_skip_reason() -> None:
    uuids = ["5f8287b1-5bb6-5f4c-ad17-316a40d5bb0c"] * 3 + ["Alameda"]
    assert skip_reason(["Alameda"], 50_001, 50_000) == "more than 50000 distinct values"
    assert skip_reason(uuids, 4, 50_000) == "identifiers (mostly UUIDs)"
    assert skip_reason(["http://a.org/x", "https://b"], 2, 50_000) == "URLs"
    assert skip_reason(["Alameda", "http://a.org/x"], 2, 50_000) is None


def test_encode_is_reproducible(tmp_path: Path) -> None:
    index = schools()
    one = encode({"b": index, "a": index}, [], 10)
    two = encode({"a": index, "b": index}, [], 10)
    assert one == two
    path = tmp_path / "v.json.gz"
    path.write_bytes(one)
    assert load_value_indexes(path) == {"a": index, "b": index}


@pytest.mark.skipif(not VALUES.exists(), reason="eval/bird/data/values.json.gz not built")
def test_committed_bird_index_matches_a_known_question() -> None:
    document = load_document()
    assert set(document["databases"]) == set(data.DATABASES)
    (question,) = [q for q in data.questions("official") if q.id == 1078]
    assert "Aaron Lennon" in question.question
    index = ValueIndex.from_dict(document["databases"][question.db_id])
    matches = index.match(question.prompt)
    assert (
        ValueMatch("european_football_2.player", "player_name", "Aaron Lennon", "Aaron Lennon")
        in matches
    )


def test_a_column_the_question_names_survives_the_per_span_cap() -> None:
    index = ValueIndex(
        {
            ("geo.a", "city"): ("Alameda",),
            ("geo.b", "cname"): ("Alameda",),
            ("geo.c", "district"): ("Alameda",),
            ("geo.z", "County"): ("Alameda",),
        }
    )
    columns = [(m.relation, m.column) for m in index.match("Schools in Alameda County?")]
    assert ("geo.z", "County") in columns
    assert columns[0] == ("geo.z", "County")
    assert len(columns) == 3

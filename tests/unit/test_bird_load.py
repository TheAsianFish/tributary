"""Pure helpers of the BIRD loader: dump splitting, naming, types and description CSVs."""

from __future__ import annotations

from pathlib import Path

import pytest

from eval.bird.load import dump_statements, pg_name, pg_type, read_descriptions

DUMP = """\
--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';

CREATE TABLE public.schools (
    "CDSCode" text,
    name text
);

ALTER TABLE public.schools OWNER TO bird_author;

GRANT ALL ON TABLE public.schools TO someone;

COPY public.schools ("CDSCode", name) FROM stdin;
01\tAlpha
02\tBeta;
\\.

CREATE INDEX i ON public.schools (name);
"""


def parse(text: str) -> list[tuple[str, tuple[str, ...] | None]]:
    return [(s.sql, s.copy_rows) for s in dump_statements(text.splitlines(keepends=True))]


def test_dump_statements_splits_keeps_copy_rows_and_drops_ownership() -> None:
    statements = parse(DUMP)

    assert [sql.split("\n")[0] for sql, _ in statements] == [
        "SET statement_timeout = 0;",
        "SET client_encoding = 'UTF8';",
        "CREATE TABLE public.schools (",
        'COPY public.schools ("CDSCode", name) FROM stdin;',
        "CREATE INDEX i ON public.schools (name);",
    ]
    create = statements[2][0]
    assert create.endswith(");")
    assert '"CDSCode" text,' in create
    assert statements[3][1] == ("01\tAlpha\n", "02\tBeta;\n")
    assert statements[0][1] is None


def test_dump_statements_drops_revoke_and_owner_statements() -> None:
    text = "REVOKE ALL ON SCHEMA public FROM PUBLIC;\nALTER SCHEMA x\n    OWNER TO y;\nSELECT 1;\n"

    assert parse(text) == [("SELECT 1;", None)]


def test_dump_statements_keeps_a_copy_block_that_is_empty() -> None:
    assert parse("COPY t (a) FROM stdin;\n\\.\n") == [("COPY t (a) FROM stdin;", ())]


def test_an_unterminated_copy_block_raises() -> None:
    with pytest.raises(ValueError, match="unterminated COPY"):
        parse("COPY t (a) FROM stdin;\n1\n2\n")


def test_a_trailing_partial_statement_raises() -> None:
    with pytest.raises(ValueError, match="ends inside a statement"):
        parse("SELECT 1;\nCREATE TABLE t (\n  a int\n")


def test_pg_name_lowercases_plain_identifiers_only() -> None:
    assert pg_name("CDSCode") == "cdscode"
    assert pg_name("Free Meal Count (K-12)") == "Free Meal Count (K-12)"
    assert pg_name("already_lower") == "already_lower"
    assert pg_name("2nd") == "2nd"


@pytest.mark.parametrize(
    ("declared", "expected"),
    [
        ("INTEGER", "bigint"),
        ("integer", "bigint"),
        ("BIGINT", "bigint"),
        ("REAL", "real"),
        ("FLOAT", "real"),
        ("DOUBLE", "real"),
        ("NUMERIC(10,2)", "numeric"),
        ("DECIMAL", "numeric"),
        ("DATE", "date"),
        ("DATETIME", "timestamp"),
        ("TIMESTAMP", "timestamp"),
        ("VARCHAR(255)", "text"),
        ("TEXT", "text"),
        ("", "text"),
    ],
)
def test_pg_type(declared: str, expected: str) -> None:
    assert pg_type(declared) == expected


def write_csv(minidev: Path, db: str, table: str, content: bytes) -> None:
    folder = minidev / "MINIDEV" / "dev_databases" / db / "database_description"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{table}.csv").write_bytes(content)


HEADER = "original_column_name,column_name,column_description,data_format,value_description\n"


def test_read_descriptions_joins_description_and_value_description(tmp_path: Path) -> None:
    write_csv(
        tmp_path,
        "db1",
        "Schools",
        (
            HEADER
            + "CDSCode,CDS code,unique id of the school,text,\n"
            + "Charter,charter,Charter school,integer,1: yes;   0: no\n"
            + "county,county,county,text,\n"
            + "Phone,phone,,text,\n"
            + ",,ignored row with no name,text,\n"
        ).encode(),
    )

    out = read_descriptions(tmp_path, "db1")

    assert list(out) == ["db1.schools"]
    assert out["db1.schools"] == {
        "cdscode": "unique id of the school.",
        "charter": "Charter school. 1: yes; 0: no.",
        "county": "",  # only repeats the column name
        "phone": "",
    }


def test_read_descriptions_drops_a_value_description_that_echoes_the_name(
    tmp_path: Path,
) -> None:
    write_csv(
        tmp_path,
        "db1",
        "t",
        (HEADER + "Free Meal,free meal,Number of meals,integer,free-meal\n").encode(),
    )

    assert read_descriptions(tmp_path, "db1")["db1.t"] == {"Free Meal": "Number of meals."}


def test_read_descriptions_decodes_cp1252_and_utf8_bom(tmp_path: Path) -> None:
    write_csv(
        tmp_path,
        "db1",
        "a",
        (HEADER + "name,name,caf\xe9 \x93quoted\x94,text,\n").encode("latin-1"),
    )
    write_csv(
        tmp_path,
        "db1",
        "b",
        b"\xef\xbb\xbf" + (HEADER + "name,name,na\xefve,text,\n").encode("utf-8"),
    )

    out = read_descriptions(tmp_path, "db1")

    assert out["db1.a"]["name"] == "caf\xe9 “quoted”."
    assert out["db1.b"]["name"] == "na\xefve."


def test_read_descriptions_of_a_database_without_csvs_is_empty(tmp_path: Path) -> None:
    (tmp_path / "MINIDEV" / "dev_databases" / "db1" / "database_description").mkdir(parents=True)
    assert read_descriptions(tmp_path, "db1") == {}

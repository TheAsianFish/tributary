import pytest
from psycopg.conninfo import conninfo_to_dict

from tributary.config import Settings


def test_defaults_point_at_the_compose_ports() -> None:
    s = Settings(_env_file=None)
    assert s.kafka_bootstrap == "localhost:9094"
    assert s.topic == "wiki.recentchange"
    assert s.sample_path.name == "recentchange.jsonl.gz"


def test_environment_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRIBUTARY_PG_HOST", "postgres")
    monkeypatch.setenv("TRIBUTARY_PG_PASSWORD", "s3cret")
    monkeypatch.setenv("TRIBUTARY_TOPIC_PARTITIONS", "6")
    s = Settings(_env_file=None)
    assert s.topic_partitions == 6
    assert "host=postgres" in s.pg_conninfo
    assert "password=s3cret" in s.pg_conninfo
    assert "s3cret" not in repr(s)


def test_conninfo_quotes_awkward_passwords(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRIBUTARY_PG_PASSWORD", "it's a secret")
    assert conninfo_to_dict(Settings(_env_file=None).pg_conninfo)["password"] == "it's a secret"


def test_rejects_zero_partitions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRIBUTARY_TOPIC_PARTITIONS", "0")
    with pytest.raises(ValueError, match="topic_partitions"):
        Settings(_env_file=None)


def test_iceberg_catalog_url_follows_the_warehouse_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRIBUTARY_PG_HOST", "postgres")
    s = Settings(_env_file=None)
    assert s.iceberg_jdbc_url == (
        "jdbc:postgresql://postgres:5432/tributary?currentSchema=iceberg_catalog"
    )
    assert "password" not in s.iceberg_jdbc_url


def test_host_spark_packages_include_the_lake_jars() -> None:
    packages = Settings(_env_file=None).spark_packages.split(",")
    assert any("iceberg-spark-runtime-4.1_2.13" in p for p in packages)
    assert any(p.startswith("org.postgresql:postgresql:") for p in packages)

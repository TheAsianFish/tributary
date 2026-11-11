from pathlib import Path

import pytest
from psycopg.conninfo import conninfo_to_dict

from tributary.config import REPO_ROOT, Settings, project_dir


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


def test_data_defaults_point_into_the_source_checkout() -> None:
    settings = Settings()
    assert settings.dbt_project_dir == REPO_ROOT / "warehouse"
    assert settings.sample_path == REPO_ROOT / "data/sample/recentchange.jsonl.gz"
    assert settings.recordings_path == REPO_ROOT / "tests/recordings/warehouse.jsonl"


def test_an_installed_package_resolves_data_from_the_current_directory(tmp_path: Path) -> None:
    site_packages = tmp_path / "venv/lib/python3.12/site-packages"
    (site_packages / "tributary").mkdir(parents=True)
    clone = tmp_path / "clone"
    clone.mkdir()
    assert project_dir(site_packages, cwd=clone) == clone
    # A checkout is recognised by its pyproject next to the package.
    (site_packages / "pyproject.toml").touch()
    assert project_dir(site_packages, cwd=clone) == site_packages

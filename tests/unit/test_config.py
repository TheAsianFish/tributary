import pytest

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


def test_rejects_zero_partitions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRIBUTARY_TOPIC_PARTITIONS", "0")
    with pytest.raises(ValueError, match="topic_partitions"):
        Settings(_env_file=None)

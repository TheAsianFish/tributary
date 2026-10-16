"""The ``tributary`` command line."""

from __future__ import annotations

import itertools
from pathlib import Path
from typing import Annotated

import structlog
import typer

from tributary import __version__
from tributary.config import Settings, get_settings
from tributary.logging import bind_run_id, configure_logging

app = typer.Typer(no_args_is_help=True, add_completion=False, help=__doc__)
sample_app = typer.Typer(no_args_is_help=True, help="Capture and describe the committed sample.")
app.add_typer(sample_app, name="sample")

log = structlog.get_logger(__name__)


@app.callback()
def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_format, settings.log_level)


@app.command()
def version() -> None:
    """Print the installed version."""
    bind_run_id("version")
    typer.echo(__version__)


@app.command()
def produce(
    live: Annotated[
        bool, typer.Option("--live", help="Read the live Wikimedia stream instead of the sample.")
    ] = False,
    limit: Annotated[int | None, typer.Option(min=0, help="Stop after N events.")] = None,
    rate: Annotated[
        float | None,
        typer.Option(min=0.0, help="Events/s for replay; 0 = unthrottled. Ignored with --live."),
    ] = None,
    sample: Annotated[Path | None, typer.Option(help="Sample file to replay.")] = None,
) -> None:
    """Publish recentchange events to Kafka, keyed by event UUID."""
    from tributary.ingest.live import live_events
    from tributary.ingest.producer import ensure_topic, make_producer, produce_events
    from tributary.ingest.sample import read_sample

    bind_run_id("produce")
    settings = get_settings()
    ensure_topic(settings.kafka_bootstrap, settings.topic, settings.topic_partitions)
    if live:
        events = live_events(settings.live_url, settings.user_agent)
        pace = 0.0
    else:
        path = sample or settings.sample_path
        events = read_sample(path)
        pace = settings.produce_rate if rate is None else rate
    log.info("produce_start", source="live" if live else "sample", topic=settings.topic, rate=pace)
    try:
        stats = produce_events(
            events, make_producer(settings.kafka_bootstrap), settings.topic, rate=pace, limit=limit
        )
    except KeyboardInterrupt:
        raise typer.Exit(130) from None
    if stats.failed:
        raise typer.Exit(1)


@sample_app.command("capture")
def sample_capture(
    count: Annotated[int, typer.Option(min=1, help="Events to keep.")] = 50_000,
    out: Annotated[Path | None, typer.Option(help="Output path (gzip JSONL).")] = None,
) -> None:
    """Record trimmed events from the live stream into a sample file."""
    from tributary.ingest.live import DecodeStats, live_events
    from tributary.ingest.sample import write_sample

    bind_run_id("sample-capture")
    settings: Settings = get_settings()
    path = out or settings.sample_path
    decoded = DecodeStats()
    events = itertools.islice(
        live_events(settings.live_url, settings.user_agent, stats=decoded), count
    )
    written = write_sample(path, events)
    log.info(
        "sample_written",
        path=str(path),
        events=written,
        received=decoded.received,
        canary=decoded.canary,
        invalid=decoded.invalid,
    )


@sample_app.command("stats")
def sample_stats_command(
    path: Annotated[Path | None, typer.Option(help="Sample file to describe.")] = None,
) -> None:
    """Counts, duplicates, emit delay and lateness of a sample, in file order."""
    from tributary.ingest.sample import format_stats, read_sample, sample_stats

    bind_run_id("sample-stats")
    typer.echo(format_stats(sample_stats(read_sample(path or get_settings().sample_path))))

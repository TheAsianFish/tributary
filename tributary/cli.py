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
warehouse_app = typer.Typer(
    no_args_is_help=True, help="Build and test the dbt models; publish the agent's catalog."
)
app.add_typer(warehouse_app, name="warehouse")

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


@app.command()
def stream(
    available_now: Annotated[
        bool,
        typer.Option("--available-now", help="Process what is in Kafka now, then stop."),
    ] = False,
) -> None:
    """Run the Spark job: Kafka → watermark + dedup → raw.recentchange."""
    from tributary.stream.job import run

    bind_run_id("stream")
    run(get_settings(), available_now=available_now)


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


@sample_app.command("load")
def sample_load(
    limit: Annotated[int | None, typer.Option(min=0, help="Load at most N events.")] = None,
    path: Annotated[Path | None, typer.Option(help="Sample file to load.")] = None,
) -> None:
    """Insert the sample straight into raw.recentchange (no Kafka, no Spark)."""
    from tributary.ingest.sample import read_sample
    from tributary.warehouse.load import load_events

    bind_run_id("sample-load")
    settings = get_settings()
    offered, inserted, invalid = load_events(
        settings.pg_conninfo, read_sample(path or settings.sample_path, limit=limit)
    )
    typer.echo(f"offered {offered}, inserted {inserted}, invalid {invalid}")


@sample_app.command("stats")
def sample_stats_command(
    path: Annotated[Path | None, typer.Option(help="Sample file to describe.")] = None,
) -> None:
    """Counts, duplicates, emit delay and lateness of a sample, in file order."""
    from tributary.ingest.sample import format_stats, read_sample, sample_stats

    bind_run_id("sample-stats")
    typer.echo(format_stats(sample_stats(read_sample(path or get_settings().sample_path))))


@warehouse_app.command("refresh")
def warehouse_refresh() -> None:
    """dbt seed + run, dbt test (results to ops.quality_runs), then publish the catalog.

    The same steps, in the same order, as the warehouse_refresh DAG; the catalog
    is published only when no check failed.
    """
    from tributary.warehouse.dbt import DbtCrashError
    from tributary.warehouse.refresh import WarehouseError, refresh

    run_id = bind_run_id("warehouse-refresh")
    try:
        summary = refresh(get_settings(), run_id)
    except WarehouseError as exc:  # failed checks or builds: recorded, see ops.*
        typer.echo(f"run {run_id}: {exc}", err=True)
        raise typer.Exit(1) from None
    except DbtCrashError as exc:
        typer.echo(f"run {run_id}: {exc}", err=True)
        raise typer.Exit(2) from None
    typer.echo(
        f"run {run_id}: {len(summary.refreshed)} relations built, "
        f"{len(summary.checks)} checks passed, catalog {summary.catalog}"
    )


@warehouse_app.command("publish-catalog")
def warehouse_publish_catalog(
    max_distinct: Annotated[
        int, typer.Option(min=1, help="Index the values of text columns with at most N.")
    ] = 100,
) -> None:
    """dbt docs generate + the value index, published atomically to the catalog dir."""
    from tributary.warehouse.refresh import publish_catalog

    run_id = bind_run_id("warehouse-publish-catalog")
    path = publish_catalog(get_settings(), run_id, max_distinct=max_distinct)
    typer.echo(str(path))

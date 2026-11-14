"""Command line: ``python -m cdc_lake <command>``."""
from __future__ import annotations

import json
import logging
import shutil

import click

from .config import load_settings
from .consumer import Consumer, SimulatedCrash
from .lake import Lake
from .reconcile import reconcile, source_from_postgres
from .simulator import simulate, write_stream
from .sources import KafkaSource, ReplaySource

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


@click.group()
@click.option("--config", "config_path", type=click.Path(exists=True, dir_okay=False), default=None)
@click.pass_context
def cli(ctx, config_path):
    """Change data capture from PostgreSQL (Debezium) into a bronze/silver lake."""
    ctx.obj = load_settings(config_path)


@cli.command()
@click.pass_obj
def simulate_cmd(cfg):
    """Write the SYNTHETIC Debezium stream (one JSONL log per topic-partition) for the local demo."""
    sim = simulate(cfg)
    write_stream(sim, cfg.path("stream"))
    click.echo(json.dumps(sim.stats, indent=2))


cli.add_command(simulate_cmd, name="simulate")


@cli.command()
@click.option("--source", type=click.Choice(["replay", "kafka"]), default="replay")
@click.option("--bootstrap", default="localhost:9092")
@click.option("--group", default="cdc-lake")
@click.option("--max-batches", type=int, default=None, help="Stop after N batches (resume later).")
@click.option("--crash-after-bronze", type=int, default=None,
              help="Simulate a crash in batch N after bronze is written and before the checkpoint commit.")
@click.option("--idle-polls", type=int, default=1, help="Stop after this many empty polls in a row.")
@click.pass_obj
def consume(cfg, source, bootstrap, group, max_batches, crash_after_bronze, idle_polls):
    """Consume change events into the lake, resuming from the lake checkpoint."""
    lake = Lake(cfg.path("lake"))
    consumer = Consumer(cfg, lake)
    if source == "replay":
        src = ReplaySource(cfg.path("stream"), consumer.offsets)
    else:
        topics = [cfg.topic(t) for t in cfg.tables] + [cfg.heartbeat_topic]
        src = KafkaSource(bootstrap, topics, group, consumer.offsets)
    try:
        result = consumer.run(src, max_batches=max_batches, crash_after_bronze=crash_after_bronze,
                              idle_polls=idle_polls)
    except SimulatedCrash as exc:
        click.echo(f"!! {exc}: restart to recover")
        raise SystemExit(3)
    finally:
        src.close()
    click.echo(json.dumps({"batches": result["batches"], "stats": result["stats"]}, indent=2))


@cli.command("reconcile")
@click.option("--source-dsn", default=None, help="PostgreSQL DSN; default: the simulator's final state.")
@click.option("--out", default=None, help="Write the report as JSON.")
@click.pass_obj
def reconcile_cmd(cfg, source_dsn, out):
    """Compare silver with the source: counts, per-key diffs, checksums. Exit 1 on any difference."""
    lake = Lake(cfg.path("lake"))
    state = Consumer(cfg, lake).states
    live = {t: [r for r in s.rows(include_deleted=False)] for t, s in state.items()}
    if source_dsn:
        source = source_from_postgres(source_dsn, cfg.tables)
    else:
        source = json.loads((cfg.path("stream") / "_truth.json").read_text())
    report = reconcile(source, live, cfg.tables)
    for r in report:
        click.echo(f"{r['table']:<12} source={r['source_rows']:<5} lake={r['lake_rows']:<5} missing={r['missing']} "
                   f"extra={r['extra']} mismatched={r['mismatched']} checksum {'MATCH' if r['match'] else 'DIFF'}")
    if out:
        (cfg.root / out).write_text(json.dumps(report, indent=2))
    if not all(r["match"] for r in report):
        raise SystemExit(1)


@cli.command()
@click.option("--out", default=None)
@click.pass_obj
def report(cfg, out):
    """Charts for the README from the bronze log, silver tables and checkpoint."""
    from .report import render_all

    for p in render_all(cfg, out):
        click.echo(f"wrote {p}")


@cli.command()
@click.pass_obj
def summary(cfg):
    """Numbers quoted in the README."""
    from .report import summary as make_summary

    click.echo(json.dumps(make_summary(cfg), indent=2))


@cli.command()
@click.pass_obj
def reset(cfg):
    """Delete the lake (bronze, silver, checkpoint)."""
    shutil.rmtree(cfg.path("lake"), ignore_errors=True)
    click.echo(f"removed {cfg.path('lake')}")


@cli.command("register-connector")
@click.option("--connect-url", default="http://localhost:8083")
@click.option("--config-file", default="docker/connect/register-postgres.json")
@click.pass_obj
def register_cmd(cfg, connect_url, config_file):
    """Register the Debezium PostgreSQL connector and wait until it is RUNNING (compose stack)."""
    from .stack import register_connector

    click.echo(json.dumps(register_connector(connect_url, cfg.root / config_file), indent=2))


@cli.command()
@click.option("--dsn", default="postgresql://postgres:postgres@localhost:5432/shop")
@click.option("--transactions", type=int, default=400)
def workload(dsn, transactions):
    """Run inserts/updates/deletes, a primary-key change and an ALTER TABLE on PostgreSQL (compose stack)."""
    from .stack import run_workload

    click.echo(json.dumps(run_workload(dsn, transactions), indent=2))


def main():
    cli()

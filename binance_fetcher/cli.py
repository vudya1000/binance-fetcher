from __future__ import annotations

import asyncio
import dataclasses
import sys
from datetime import UTC
from pathlib import Path

import click

from binance_fetcher.config import load_config
from binance_fetcher.log import setup_logging


def _symbol_list(symbols: str | None) -> list[str] | None:
    if not symbols:
        return None
    return [s.strip().upper() for s in symbols.split(",")]


_FAMILIES_HELP = (
    "Comma-separated subset of ohlcv,mark_price,premium_index_klines,funding,"
    "open_interest (default: all enabled in config)"
)


def _family_list(families: str | None) -> list[str] | None:
    from binance_fetcher.families import FAMILIES

    if not families:
        return None
    names = [f.strip() for f in families.split(",")]
    if unknown := [n for n in names if n not in FAMILIES]:
        raise click.BadParameter(
            f"unknown {', '.join(unknown)}; choose from {', '.join(FAMILIES)}",
            param_hint="--families",
        )
    return names


def _echo_errors(errors: list[str]) -> None:
    for err in errors[:10]:
        click.echo(f"  ERROR: {err}", err=True)


@click.group()
@click.option("--config", "config_path", type=click.Path(exists=False), default=None)
@click.pass_context
def cli(ctx, config_path):
    """Binance USD-M Futures market-data pipeline."""
    ctx.ensure_object(dict)
    path = Path(config_path) if config_path else None
    ctx.obj["config"] = load_config(path)
    setup_logging(ctx.obj["config"].log_level)


# -- history ---------------------------------------------------------------


@cli.command()
@click.option("--symbols", default=None, help="Comma-separated symbols, or omit to discover all")
@click.option("--families", default=None, help=_FAMILIES_HELP)
@click.option("--start", default=None, help="YYYY-MM or YYYY-MM-DD (default: config start_month)")
@click.option("--end", default=None, help="YYYY-MM or YYYY-MM-DD (default: yesterday)")
@click.option("--workers", type=int, default=None, help="Download concurrency")
@click.option(
    "--recheck",
    is_flag=True,
    help="Also compare stored archives with Vision's checksums and replace reissued ones",
)
@click.pass_context
def backfill(ctx, symbols, families, start, end, workers, recheck):
    """Fetch every published Binance Vision archive the raw store does not hold yet."""
    from binance_fetcher.client.rest import BinanceClient
    from binance_fetcher.pipeline.backfill import run_backfill
    from binance_fetcher.pipeline.discover import run_discover

    config = ctx.obj["config"]
    if workers:
        config = dataclasses.replace(config, download_workers=workers)

    fam_list = _family_list(families)

    async def _run():
        sym_list = _symbol_list(symbols)
        if sym_list is None:
            async with BinanceClient(config) as client:
                _, _, sym_list = await run_discover(config, client)
            click.echo(f"Discovered {len(sym_list)} symbols")

        report = await run_backfill(
            config, sym_list, families=fam_list, start=start, end=end, recheck=recheck
        )
        for name, s in report.per_family.items():
            click.echo(
                f"  {name:<22} {s.symbols} symbols, {s.periods_downloaded} archives "
                f"(+{s.periods_missing} missing, {s.periods_unchanged} unchanged), "
                f"{s.symbols_failed} failed"
            )
        click.echo(f"Backfill done in {report.elapsed_sec:.0f}s")
        _echo_errors(report.errors)
        return 0 if report.symbols_failed == 0 else 1

    sys.exit(asyncio.run(_run()))


# -- hourly update --------------------------------------------------------


@cli.command()
@click.option("--symbols", default=None, help="Comma-separated symbols, or omit to discover all")
@click.option("--families", default=None, help=_FAMILIES_HELP)
@click.pass_context
def update(ctx, symbols, families):
    """Fetch what is new for every data family from the REST API into the raw store."""
    from binance_fetcher.pipeline.update import run_update

    config = ctx.obj["config"]

    async def _run():
        report = await run_update(config, _symbol_list(symbols), _family_list(families))
        for name, s in report.per_family.items():
            click.echo(
                f"  {name:<22} {s.symbols_ok} ok, {s.symbols_failed} failed, "
                f"{s.rows_fetched:,} rows fetched"
            )
        click.echo(f"Update done in {report.elapsed_sec:.1f}s")
        _echo_errors(report.errors)
        return 0 if not report.errors else 1  # an IP ban fails no symbol by name

    sys.exit(asyncio.run(_run()))


# -- build ------------------------------------------------------------------


@cli.command()
@click.option("--families", default=None, help=_FAMILIES_HELP)
@click.option("--force", is_flag=True, help="Rebuild every partition, changed or not")
@click.option("--workers", type=int, default=None, help="Parser processes (default: CPU count)")
@click.pass_context
def build(ctx, families, force, workers):
    """Build the Parquet partitions whose raw inputs changed."""
    from binance_fetcher.pipeline.build import run_build

    report = run_build(ctx.obj["config"], _family_list(families), force=force, workers=workers)
    for name, s in report.per_family.items():
        click.echo(
            f"  {name:<22} {s.months_built} months built ({s.rows:,} rows), "
            f"{s.months_unchanged} unchanged, {s.rest_files_folded} REST files folded"
        )
    click.echo(f"Build done in {report.elapsed_sec:.1f}s")
    _echo_errors(report.errors)
    sys.exit(0 if not report.errors else 1)


# -- inspection ----------------------------------------------------------------


@cli.command()
@click.option("--symbol", default=None, help="Check one symbol, or omit for all")
@click.option("--daily", is_flag=True, help="Also check for 24 candles per day")
@click.pass_context
def validate(ctx, symbol, daily):
    """Run structural integrity checks on the built candles."""
    import polars as pl

    from binance_fetcher.storage.parquet import ParquetStore
    from binance_fetcher.validate import validate_symbol

    candles = ParquetStore(ctx.obj["config"]).read_all("ohlcv")
    if candles is not None and symbol:
        candles = candles.filter(pl.col("symbol") == symbol.upper())
    if candles is None or candles.is_empty():
        click.echo("No data found")
        return

    invalid = gaps = incomplete = 0
    parts = candles.partition_by("symbol", as_dict=True)
    for (sym,), part in sorted(parts.items()):
        report = validate_symbol(part, check_daily=daily)
        if not report.ok:
            click.echo(
                f"{sym}: {report.invalid_rows} invalid rows, {len(report.gaps)} gaps, "
                f"{len(report.incomplete_days)} incomplete days"
            )
            for day in report.incomplete_days[:5]:
                click.echo(f"  {day}")
            invalid += report.invalid_rows
            gaps += len(report.gaps)
            incomplete += len(report.incomplete_days)

    if invalid == gaps == incomplete == 0:
        click.echo(f"All {len(parts)} symbols passed validation")
    else:
        click.echo(
            f"Issues: {invalid} invalid rows, {gaps} gaps, {incomplete} incomplete days "
            f"across {len(parts)} symbols"
        )


@cli.command("query")
@click.argument("sql")
def query_cmd(sql):
    """Execute a DuckDB SQL query over stored Parquet files."""
    import duckdb

    click.echo(duckdb.sql(sql))


@cli.command()
@click.pass_context
def status(ctx):
    """Show symbol counts, what is stored per family and data freshness."""
    from datetime import datetime

    from binance_fetcher.families import FAMILIES
    from binance_fetcher.storage.parquet import ParquetStore
    from binance_fetcher.storage.raw import RawStore
    from binance_fetcher.storage.state import load_state

    config = ctx.obj["config"]
    state = load_state(config.meta_dir)
    raw, store = RawStore(config), ParquetStore(config)

    active = [s for s, v in state.items() if v.get("status") == "active"]
    delisted = [s for s, v in state.items() if v.get("status") == "delisted"]
    click.echo(f"Symbols:    {len(active)} active, {len(delisted)} delisted")

    for name, fam in FAMILIES.items():
        archives = sum(len(a) for a in raw.archives(fam).values())
        files = store.files(name)
        mb = sum(f.stat().st_size for f in files) / 1024 / 1024
        click.echo(
            f"  {name:<22} {archives} archives, {len(raw.rest_files(name))} REST files"
            f" -> {len(files)} partitions, {mb:.1f} MB"
        )

    latest = max(store.last_times("ohlcv").values(), default=0)
    if latest:
        dt = datetime.fromtimestamp(latest / 1000, tz=UTC)
        click.echo(f"Latest:     {dt.isoformat()}")

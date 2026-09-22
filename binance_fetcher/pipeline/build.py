"""Build the Parquet partitions from the raw store.

Every partition is a function of the raw files alone, so this job keeps no
cursors and needs no repair jobs beside it.

  rows      A month's partition holds every row of its archives plus the REST
            rows no archive has. Archive rows win on (symbol, time); among REST
            rows the newest run wins, which is how a candle stored while still
            in progress is replaced by its closed version. When an archive
            arrives, the next build takes its rows over the REST ones: that is
            the whole reconciliation.
  skipping  A partition records a fingerprint of the raw files it was built
            from. A month is rebuilt only when its fingerprint changes: a new
            archive, a day packed into one, a new REST run. The month in
            progress is rebuilt every run, closed months practically never.
  parsing   Archives are parsed in worker processes, a slice of the month's
            files each: a full history is ~100k CSVs and the parsers are plain
            Python. A month with few archives is parsed inline.
  folding   REST run files pile up hourly. Once a month has been over for
            SETTLE_DAYS, its run files are folded into one residual file that
            keeps only the rows no archive covers, and deleted. A run file
            that also holds rows of the following month waits for that month.
"""

from __future__ import annotations

import hashlib
import logging
import multiprocessing
import os
import time
from collections.abc import Iterator
from concurrent.futures import Executor, ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl

from binance_fetcher.config import Config
from binance_fetcher.families import FAMILIES, MARKET, Family, select_families
from binance_fetcher.pipeline.funding import SNAPSHOT_FILE, funding_interval_frame
from binance_fetcher.storage.parquet import ParquetStore, write_parquet_atomic
from binance_fetcher.storage.raw import Archive, RawStore

logger = logging.getLogger(__name__)

SETTLE_DAYS = 10  # after a month ends; Vision has published its archives well before
PARALLEL_MIN = 64  # archives in a month before worker processes pay for themselves
_RESIDUAL = "residual-"


@dataclass
class FamilyStats:
    months_built: int = 0
    months_unchanged: int = 0
    rows: int = 0  # in the partitions built this run
    rest_files_folded: int = 0


@dataclass
class BuildReport:
    per_family: dict[str, FamilyStats] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    elapsed_sec: float = 0.0


# -- reading raw ---------------------------------------------------------------


def _month_of(time_col: str) -> pl.Expr:
    return pl.from_epoch(pl.col(time_col), time_unit="ms").dt.strftime("%Y-%m")


def _rest_rows(fam: Family, responses: dict) -> Iterator[tuple[str, object]]:
    """(symbol, raw row) for every row of one run's responses."""
    if fam.live == MARKET:
        for row in responses.get("fundingRate", []):
            yield row["symbol"], row
    else:
        for symbol, rows in responses.items():
            for row in rows:
                yield symbol, row


def _pack_rows(fam: Family, rows: list[tuple[str, object]]) -> dict:
    """Inverse of ``_rest_rows``: raw rows back into the shape of a run's responses."""
    if fam.live == MARKET:
        return {"fundingRate": [row for _, row in rows]}
    out: dict[str, list] = {}
    for symbol, row in rows:
        out.setdefault(symbol, []).append(row)
    return out


def _with_symbol(df: pl.DataFrame, symbols: str | list[str]) -> pl.DataFrame:
    """``symbol`` first; added when the parser does not supply it."""
    if "symbol" not in df.columns:
        value = pl.lit(symbols) if isinstance(symbols, str) else pl.Series(symbols)
        df = df.with_columns(value.alias("symbol"))
    return df.select("symbol", pl.exclude("symbol"))


def _read_rest(raw: RawStore, fam: Family) -> tuple[pl.DataFrame | None, dict | None]:
    """Every REST row on disk, oldest run first, tagged with its file (``_src``)
    and month (``_m``); and the newest run's responses, for the funding snapshot."""
    parsed: list[tuple[int, str, pl.DataFrame]] = []
    newest = None
    for path in raw.rest_files(fam.cat):  # one file in memory at a time
        doc = raw.read_rest(path)
        if fam.live == MARKET and (newest is None or doc["fetched_at"] >= newest["fetched_at"]):
            newest = doc
        pairs = list(_rest_rows(fam, doc["responses"]))
        if pairs:
            df = fam.parse_rest([row for _, row in pairs])
            df = _with_symbol(df, [s for s, _ in pairs]).with_columns(
                pl.lit(path.name).alias("_src")
            )
            parsed.append((doc["fetched_at"], path.name, df))
    if not parsed:
        return None, newest
    frames = [df for _, _, df in sorted(parsed, key=lambda t: t[:2])]
    return pl.concat(frames).with_columns(_month_of(fam.time_col).alias("_m")), newest


def _parse_archives(cat: str, archives: list[Archive]) -> pl.DataFrame | None:
    """Rows of the given archives. Top-level and keyed by name so it can run in a worker."""
    fam = FAMILIES[cat]
    frames = [
        _with_symbol(fam.parse_vision(csv), a.symbol)
        for a in archives
        for csv in RawStore.read_archive(a.path)
    ]
    frames = [f for f in frames if not f.is_empty()]
    return pl.concat(frames) if frames else None


def _read_archives(fam: Family, archives: list[Archive], pool: _Pool) -> pl.DataFrame | None:
    if len(archives) < PARALLEL_MIN or pool.workers < 2:
        return _parse_archives(fam.cat, archives)
    n = pool.workers * 4  # small slices keep the workers evenly loaded
    slices = [archives[i::n] for i in range(n) if archives[i::n]]
    parsed = pool.get().map(_parse_archives, [fam.cat] * len(slices), slices)
    frames = [f for f in parsed if f is not None]
    return pl.concat(frames) if frames else None


class _Pool:
    """Worker processes, started only if some month is large enough to need them."""

    def __init__(self, workers: int):
        self.workers = workers
        self._executor: Executor | None = None

    def get(self) -> Executor:
        if self._executor is None:
            # Spawned, not forked: polars runs a thread pool in the parent, and a
            # forked child inherits its locks mid-state and deadlocks on Linux.
            self._executor = ProcessPoolExecutor(
                self.workers, mp_context=multiprocessing.get_context("spawn")
            )
        return self._executor

    def close(self) -> None:
        if self._executor is not None:
            self._executor.shutdown()


# -- one month -----------------------------------------------------------------


def _fingerprint(archives: list[Archive], rest_files: list[str]) -> str:
    lines = [f"{a.path.name}:{a.stamp}" for a in archives] + rest_files
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


def _settled(month: str, today: date) -> bool:
    month_end = (date.fromisoformat(f"{month}-01") + timedelta(days=31)).replace(day=1)
    return today >= month_end + timedelta(days=SETTLE_DAYS)


def _fold(
    raw: RawStore,
    fam: Family,
    month: str,
    files: list[str],
    spent: list[str],
    uncovered: set[tuple[str, int]],
) -> int:
    """Replace the ``spent`` run files by one residual file holding the month's
    ``uncovered`` rows, newest version of each. Returns the files removed."""
    rest_dir = raw.rest_dir(fam.cat)
    kept: dict[tuple[str, int], tuple[int, str, object]] = {}  # key -> (fetched_at, symbol, row)
    for name in files:
        doc = raw.read_rest(rest_dir / name)
        for symbol, row in _rest_rows(fam, doc["responses"]):
            key = (symbol, fam.rest_time(row))
            if key in uncovered and (key not in kept or doc["fetched_at"] >= kept[key][0]):
                kept[key] = (doc["fetched_at"], symbol, row)

    residual = f"{_RESIDUAL}{month}"
    if kept:
        rows = [(symbol, row) for _, symbol, row in kept.values()]
        newest = max(at for at, _, _ in kept.values())
        raw.write_rest(fam.cat, newest, _pack_rows(fam, rows), name=residual)
    removed = [n for n in spent if not (kept and n == f"{residual}.json.gz")]
    for name in removed:
        (rest_dir / name).unlink()
    return sum(not n.startswith(_RESIDUAL) for n in removed)


def _build_family(
    config: Config, fam: Family, today: date, force: bool, stats: FamilyStats, pool: _Pool
) -> None:
    raw, store = RawStore(config), ParquetStore(config)
    keys = ["symbol", fam.time_col]
    archives = raw.archives(fam)
    rest, newest = _read_rest(raw, fam)

    files_of: dict[str, list[str]] = {}  # month -> REST files holding rows of it
    last_month: dict[str, str] = {}  # REST file -> newest month it holds rows of
    if rest is not None:
        for month, src in rest.select("_m", "_src").unique().sort("_m", "_src").iter_rows():
            files_of.setdefault(month, []).append(src)
            last_month[src] = month

    for month in sorted(archives.keys() | files_of.keys()):
        files = files_of.get(month, [])
        # Foldable: run files that hold nothing newer than this settled month.
        spent = [n for n in files if last_month[n] == month] if _settled(month, today) else []
        fold = any(not n.startswith(_RESIDUAL) for n in spent)
        inputs = _fingerprint(archives.get(month, []), files)
        if not (force or fold) and store.inputs(fam.cat, month) == inputs:
            stats.months_unchanged += 1
            continue

        frames = []
        if files and rest is not None:
            frames.append(rest.filter(pl.col("_m") == month).drop("_src", "_m"))
        archived = _read_archives(fam, archives.get(month, []), pool)
        if archived is not None:
            # REST first: on a shared key the last row wins, so the archive does.
            frames.append(archived.filter(_month_of(fam.time_col) == month))
        if not frames:
            continue  # only periods recorded as absent
        rows = pl.concat(frames).unique(subset=keys, keep="last").sort(keys)

        if fold:
            covered = archived.select(keys) if archived is not None else None
            left = frames[0].select(keys).unique()
            if covered is not None:
                left = left.join(covered, on=keys, how="anti")
            stats.rest_files_folded += _fold(raw, fam, month, files, spent, set(left.iter_rows()))
            was = {*files, f"{_RESIDUAL}{month}.json.gz"}
            files = [p.name for p in raw.rest_files(fam.cat) if p.name in was]
            inputs = _fingerprint(archives.get(month, []), files)

        if not rows.is_empty():
            store.write_month(fam.cat, month, rows, inputs)
            stats.months_built += 1
            stats.rows += rows.height

    if fam.live == MARKET and newest is not None:
        responses = newest["responses"]
        if "fundingInfo" in responses and "premiumIndex" in responses:
            snapshot = funding_interval_frame(
                responses["fundingInfo"], responses["premiumIndex"], newest["fetched_at"]
            )
            write_parquet_atomic(_snapshot_path(config), snapshot)


def _snapshot_path(config: Config) -> Path:
    return config.parquet_dir / SNAPSHOT_FILE


# -- entry point ---------------------------------------------------------------


def run_build(
    config: Config,
    families: list[str] | None = None,
    force: bool = False,
    today: date | None = None,
    workers: int | None = None,
) -> BuildReport:
    """Rebuild every partition whose raw inputs changed (``force``: all of them)."""
    report = BuildReport()
    t0 = time.monotonic()
    today = today or datetime.now(UTC).date()
    pool = _Pool(workers or os.cpu_count() or 1)

    for fam in select_families(config, families):
        stats = report.per_family[fam.cat] = FamilyStats()
        try:
            _build_family(config, fam, today, force, stats, pool)
        except Exception as e:  # noqa: BLE001 - families build independently
            logger.exception("Build %s failed", fam.cat)
            report.errors.append(f"{fam.cat}: {e}")
        logger.info(
            "Build %s: %d months built (%d rows), %d unchanged, %d REST files folded",
            fam.cat,
            stats.months_built,
            stats.rows,
            stats.months_unchanged,
            stats.rest_files_folded,
        )

    pool.close()
    report.elapsed_sec = time.monotonic() - t0
    return report

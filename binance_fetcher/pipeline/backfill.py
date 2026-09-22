"""Fetch the Binance Vision bulk archives into the raw store.

One flow for every data family: plan, download, store. Nothing is parsed here;
``build`` turns the stored archives into Parquet.

  plan      The raw store holds one archive per family, symbol and month, so
            the plan is whatever is not on disk yet: a month with no file, or,
            for a family Vision publishes by day, the days missing from the
            month's member list. The rule is stateless, so a rerun heals what a
            killed run or a failed download left behind and skips the rest, and
            the daily cron run is the same command as the first full backfill.
  bounds    Nothing exists before a symbol is listed or after it is delivered;
            exchangeInfo gives both dates. OHLCV is planned between them; every
            other family is planned for the months OHLCV has an archive for,
            which is why OHLCV always runs first. A family published by day
            also covers the months since the last OHLCV archive, while the
            symbol is still trading.
  store     A monthly archive is stored as downloaded, once it has passed its
            checksum. Daily archives are packed into the month's ZIP.
  absent    A period still missing ABSENT_AFTER_DAYS after it ended is recorded
            in the raw store as an archive (or a member) with nothing in it, so
            it is asked for once, not on every run.
  recheck   Binance occasionally reissues an archive. ``recheck`` compares each
            stored archive's checksum with the one Vision publishes now, a
            ~100-byte request, downloads the archive again where they differ,
            and asks again for the periods recorded as absent. ``build`` then
            rebuilds the month on its own: the file it was built from changed.
"""

from __future__ import annotations

import asyncio
import io
import logging
import time
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

import aiohttp

from binance_fetcher.client.vision import archive_url, download_archive, fetch_checksum
from binance_fetcher.config import Config
from binance_fetcher.families import FAMILIES, Family, select_families
from binance_fetcher.storage.raw import RawStore
from binance_fetcher.storage.state import load_state

logger = logging.getLogger(__name__)

ANCHOR = FAMILIES["ohlcv"]  # the months it has archives for bound every other family
CHUNK = 200  # jobs between progress lines
ABSENT_AFTER_DAYS = 15  # Vision publishes a day within one, a month within a few


@dataclass
class FamilyStats:
    symbols: int = 0  # symbols with at least one period to fetch
    periods_downloaded: int = 0
    periods_missing: int = 0  # 404: not published yet, or recorded as absent this run
    periods_unchanged: int = 0  # recheck: the stored checksum is still the published one
    symbols_failed: int = 0


@dataclass
class BackfillReport:
    per_family: dict[str, FamilyStats] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    elapsed_sec: float = 0.0

    @property
    def symbols_failed(self) -> int:
        return sum(s.symbols_failed for s in self.per_family.values())


@dataclass(frozen=True)
class Job:
    symbol: str
    month: str
    periods: list[str]  # the month itself, or the days of it still to fetch
    recheck: bool = False  # a stored monthly archive: download only if its checksum changed


# -- pure planning helpers ---------------------------------------------------


def parse_day(text: str, *, end: bool = False) -> date:
    """``YYYY-MM-DD``, or ``YYYY-MM`` meaning its first (``end``: last) day."""
    if len(text) == 7:
        first = date.fromisoformat(f"{text}-01")
        if not end:
            return first
        return (first.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    return date.fromisoformat(text)


def months_between(start: date, end: date) -> list[str]:
    """``YYYY-MM`` for every month intersecting [start, end]."""
    out = []
    first = start.replace(day=1)
    while first <= end:
        out.append(f"{first:%Y-%m}")
        first = (first + timedelta(days=31)).replace(day=1)
    return out


def _days(month: str, lower: date, upper: date) -> list[str]:
    """The days of ``month`` inside [lower, upper]."""
    d, last = max(parse_day(month), lower), min(parse_day(month, end=True), upper)
    out = []
    while d <= last:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def plan(
    fam: Family,
    symbols: list[str],
    state: dict,
    raw: RawStore,
    window: tuple[date, date],
    today: date,
    recheck: bool = False,
) -> list[Job]:
    """What Vision should have published inside ``window`` and the raw store lacks;
    with ``recheck``, also what it holds and Vision may have reissued.

    Vision publishes a day's file the next day and a month's file once the
    month is over, so today and, for monthly archives, the current month are
    never planned.
    """
    start, end = window
    end = min(end, today - timedelta(days=1))
    finished = today.replace(day=1) - timedelta(days=1)  # last day of the last whole month
    jobs = []
    for sym in symbols:
        entry = state.get(sym, {})
        onboard, delivery = entry.get("onboard_date"), entry.get("delivery_date")
        lower = max(start, _ms_to_date(onboard)) if onboard else start
        upper = min(end, _ms_to_date(delivery)) if delivery else end
        have = raw.months(fam, sym)

        if fam is ANCHOR:
            months = months_between(lower, min(upper, finished))
        else:
            anchored = raw.months(ANCHOR, sym)
            if not anchored:
                continue  # no OHLCV archive, so no evidence the symbol ever traded
            still_trading = fam.archive_by == "day" and entry.get("status") != "delisted"
            months = [
                m
                for m in months_between(lower, upper)
                if m in anchored or (still_trading and m > max(anchored))
            ]

        for month in months:
            if fam.archive_by == "month":
                if month not in have:
                    jobs.append(Job(sym, month, [month]))
                elif recheck:
                    jobs.append(Job(sym, month, [month], recheck=True))
                continue
            # Daily files carry no stored checksum; a recheck asks again for the absent days.
            stored = set(raw.members(raw.archive_path(fam, sym, month), absent=not recheck))
            days = [d for d in _days(month, lower, upper) if _member(fam, sym, d) not in stored]
            if days:
                jobs.append(Job(sym, month, days))
    return jobs


def _member(fam: Family, symbol: str, day: str) -> str:
    """Name of one day's CSV inside a packed month."""
    return f"{symbol}-{fam.data_type}-{day}.csv"


def _ms_to_date(ms: int) -> date:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).date()


# -- download ----------------------------------------------------------------


def _overdue(period: str, today: date) -> bool:
    return (today - parse_day(period, end=True)).days > ABSENT_AFTER_DAYS


async def _run_job(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    config: Config,
    raw: RawStore,
    fam: Family,
    job: Job,
    today: date,
    stats: FamilyStats,
) -> BaseException | None:
    """Download and store one symbol-month; returns the first error, if any."""
    path = raw.archive_path(fam, job.symbol, job.month)

    async def fetch(period: str) -> bytes | None:
        url = archive_url(config.vision_base_url, fam.data_type, job.symbol, period, fam.interval)
        if job.recheck and await fetch_checksum(session, url, semaphore) in (
            None,
            raw.checksum(path),
        ):
            stats.periods_unchanged += 1
            return b""
        return await download_archive(session, url, semaphore, verify=fam.verify_archive)

    results = await asyncio.gather(*(fetch(p) for p in job.periods), return_exceptions=True)
    pairs = list(zip(job.periods, results, strict=True))
    got = {p: r for p, r in pairs if isinstance(r, bytes) and r}
    absent = [p for p, r in pairs if r is None and _overdue(p, today)]
    stats.periods_downloaded += len(got)
    stats.periods_missing += sum(r is None for r in results)

    if fam.archive_by == "month":
        if got:
            raw.write_archive(path, got[job.month])
        elif absent and not path.exists():
            raw.write_archive(path)
    else:
        members = {_member(fam, job.symbol, day): _first_member(z) for day, z in got.items()}
        known = set(raw.members(path))
        members |= {m: b"" for day in absent if (m := _member(fam, job.symbol, day)) not in known}
        if members:
            raw.add_members(path, members)
    return next((r for r in results if isinstance(r, BaseException)), None)


def _first_member(zip_bytes: bytes) -> bytes:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        return zf.read(zf.namelist()[0])


# -- entry point -------------------------------------------------------------


async def run_backfill(
    config: Config,
    symbols: list[str],
    families: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    today: date | None = None,
    recheck: bool = False,
) -> BackfillReport:
    """Fetch the given families' archives (default: all enabled) for the given symbols."""
    report = BackfillReport()
    t0 = time.monotonic()
    today = today or datetime.now(UTC).date()

    # Registry order, so the anchor family is stored before it is used as a bound.
    selected = select_families(config, families)

    window = (
        parse_day(start or config.start_month),
        parse_day(end, end=True) if end else today - timedelta(days=1),
    )
    raw = RawStore(config)
    state = load_state(config.meta_dir)
    semaphore = asyncio.Semaphore(config.download_workers)
    connector = aiohttp.TCPConnector(limit=config.download_workers)
    timeout = aiohttp.ClientTimeout(total=300)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        for fam in selected:
            stats = report.per_family[fam.cat] = FamilyStats()
            jobs = plan(fam, symbols, state, raw, window, today, recheck)
            stats.symbols = len({j.symbol for j in jobs})
            logger.info(
                "Backfill %s: %d symbols, %d %ss to fetch (%s..%s)",
                fam.cat,
                stats.symbols,
                sum(len(j.periods) for j in jobs),
                fam.archive_by,
                *window,
            )

            failed: set[str] = set()
            for i in range(0, len(jobs), CHUNK):
                chunk = jobs[i : i + CHUNK]
                errors = await asyncio.gather(
                    *(
                        _run_job(session, semaphore, config, raw, fam, job, today, stats)
                        for job in chunk
                    )
                )
                for job, error in zip(chunk, errors, strict=True):
                    if error is not None:
                        failed.add(job.symbol)
                        report.errors.append(f"{job.symbol} {fam.cat} {job.month}: {error}")
                        logger.error(
                            "Backfill %s failed for %s %s: %s",
                            fam.cat,
                            job.symbol,
                            job.month,
                            error,
                        )
                logger.info(
                    "Backfill %s: %d/%d symbol-months, %d %ss downloaded, %d missing",
                    fam.cat,
                    min(i + CHUNK, len(jobs)),
                    len(jobs),
                    stats.periods_downloaded,
                    fam.archive_by,
                    stats.periods_missing,
                )
            stats.symbols_failed = len(failed)

    report.elapsed_sec = time.monotonic() - t0
    return report

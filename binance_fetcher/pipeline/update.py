"""Hourly update: fetch what is new for every data family from the REST API.

One process and one client, so every request is metered by the same throttle.
All families are fetched concurrently (they draw from independent rate-limit
pools) and fail independently: each gets its own line in the report.

Each family is fetched the way the API serves it (see ``families``):

  klines   per symbol, from the symbol's newest stored candle
  latest   per symbol, as many of the newest points as separate the newest
           stored one from now; the endpoint takes no start time
  market   one paged query for all symbols, from a market-wide watermark

The responses are stored as received, one raw file per family per run; parsing
them is ``build``'s job. Where a fetch resumes from is read from the built
partitions, so a symbol whose fetch failed, or a run whose build did not
happen, simply makes the next run reach back further.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

from binance_fetcher.client.rest import (
    MAX_FUTURES_DATA_LIMIT,
    MAX_KLINE_LIMIT,
    BinanceClient,
    IPBanError,
)
from binance_fetcher.config import Config
from binance_fetcher.families import KLINES, MARKET, Family, select_families
from binance_fetcher.pipeline.discover import run_discover
from binance_fetcher.pipeline.funding import fetch_new_settlements
from binance_fetcher.storage.parquet import ParquetStore
from binance_fetcher.storage.raw import RawStore
from binance_fetcher.storage.state import get_active_symbols

logger = logging.getLogger(__name__)

HOUR_MS = 3_600_000
# Nothing stored yet: everything one request carries, 62 days of candles. That
# reaches past any month Vision has not published, so the archives and the REST
# tail always meet, whichever job runs first.
NEW_SYMBOL_LIMIT = MAX_KLINE_LIMIT

# What one family's fetch returns: the raw responses to store, and the errors hit.
Fetched = tuple[dict, dict[str, BaseException]]


@dataclass
class FamilyStats:
    symbols_ok: int = 0
    symbols_failed: int = 0
    rows_fetched: int = 0


@dataclass
class UpdateReport:
    per_family: dict[str, FamilyStats] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    elapsed_sec: float = 0.0


# -- fetch windows -------------------------------------------------------------


def _kline_window(last_time: int | None, now_ms: int) -> tuple[int | None, int]:
    """(start_time, limit) that continues from the newest stored candle.

    The fetch starts at that candle itself (Binance ``startTime`` is inclusive
    on open_time): it was in progress when the previous run stored it, and this
    run's closed version replaces it in ``build``. At :00:30 that is three
    candles: the stored one, the hour that followed, and the one in progress.
    """
    if last_time is None:
        return None, NEW_SYMBOL_LIMIT
    missing = (now_ms - last_time) // HOUR_MS
    return last_time, min(MAX_KLINE_LIMIT, int(missing) + 2)


def _latest_limit(last_time: int | None, now_ms: int) -> int:
    """Points to request from an endpoint that only serves "the latest N": the
    hours since the newest stored point plus a small overlap, or everything on
    offer on a symbol's first run. A gap beyond the endpoint's reach is left to
    the archives."""
    if last_time is None:
        return MAX_FUTURES_DATA_LIMIT
    missing = (now_ms - last_time) // HOUR_MS
    return max(2, min(MAX_FUTURES_DATA_LIMIT, int(missing) + 2))


# -- fetching ------------------------------------------------------------------


async def _fetch_symbol(
    client: BinanceClient, fam: Family, symbol: str, last_time: int | None, now_ms: int
) -> list:
    if fam.live == KLINES:
        start_time, limit = _kline_window(last_time, now_ms)
        return await client.fetch_klines(symbol, fam.data_type, start_time=start_time, limit=limit)
    return await client.fetch_open_interest_hist(symbol, "1h", _latest_limit(last_time, now_ms))


async def _fetch_per_symbol(
    client: BinanceClient, fam: Family, symbols: list[str], last: dict[str, int], now_ms: int
) -> Fetched:
    results = await asyncio.gather(
        *(_fetch_symbol(client, fam, sym, last.get(sym), now_ms) for sym in symbols),
        return_exceptions=True,
    )
    pairs = list(zip(symbols, results, strict=True))
    return (
        {sym: r for sym, r in pairs if not isinstance(r, BaseException)},
        {sym: r for sym, r in pairs if isinstance(r, BaseException)},
    )


async def _fetch_funding(
    client: BinanceClient, only: list[str] | None, last: dict[str, int]
) -> Fetched:
    """New settlements for all symbols (or just ``only``), plus the two responses
    the funding-interval snapshot is derived from. Any of them may fail alone."""
    names = ("fundingRate", "fundingInfo", "premiumIndex")
    results = await asyncio.gather(
        fetch_new_settlements(client, max(last.values(), default=None)),
        client.fetch_funding_info(),
        client.fetch_premium_index(),
        return_exceptions=True,
    )
    pairs = list(zip(names, results, strict=True))
    responses = {name: r for name, r in pairs if not isinstance(r, BaseException)}
    if only is not None and "fundingRate" in responses:
        responses["fundingRate"] = [r for r in responses["fundingRate"] if r["symbol"] in only]
    return responses, {name: r for name, r in pairs if isinstance(r, BaseException)}


# -- entry point ---------------------------------------------------------------


async def run_update(
    config: Config,
    symbols: list[str] | None = None,
    families: list[str] | None = None,
) -> UpdateReport:
    """Update the given families (default: all enabled) for the given symbols
    (default: all active, after refreshing the symbol list from exchangeInfo)."""
    report = UpdateReport()
    t0 = time.monotonic()
    now_ms = int(time.time() * 1000)

    selected = select_families(config, families)
    only = symbols  # an explicit symbol list also narrows the market-wide family
    store = ParquetStore(config)

    async with BinanceClient(config) as client:
        if symbols is None:
            try:
                await run_discover(config, client)
            except Exception as e:  # a stale symbol list costs less than a missed hour
                logger.error("Discovery failed, using the stored symbol list: %s", e)
                report.errors.append(f"discover: {e}")
            symbols = get_active_symbols(config.meta_dir)
        if not symbols:
            logger.warning("No symbols to update")
            return report

        logger.info("Updating %d symbols: %s", len(symbols), ", ".join(f.cat for f in selected))

        # Registry order puts the kline families at the head of the request
        # queue, so candles land first. A ban opens the client's circuit: the
        # remaining requests fail unsent and come back as IPBanError results.
        fetched: list[Fetched] = await asyncio.gather(
            *(
                _fetch_funding(client, only, store.last_times(fam.cat))
                if fam.live == MARKET
                else _fetch_per_symbol(client, fam, symbols, store.last_times(fam.cat), now_ms)
                for fam in selected
            )
        )

    raw = RawStore(config)
    for fam, (responses, failed) in zip(selected, fetched, strict=True):
        stats = report.per_family[fam.cat] = FamilyStats(symbols_failed=len(failed))
        for what, error in failed.items():
            if isinstance(error, IPBanError):
                if str(error) not in report.errors:  # one ban fails every queued request
                    logger.error("IP banned: %s", error)
                    report.errors.append(str(error))
            else:
                logger.warning("Update %s failed for %s: %s", fam.cat, what, error)
                report.errors.append(f"{fam.cat} {what}: {error}")

        if fam.live == MARKET:
            rows = responses.get("fundingRate", [])
            stats.symbols_ok = len({r["symbol"] for r in rows})
            stats.rows_fetched = len(rows)
        else:
            stats.symbols_ok = len(responses)
            stats.rows_fetched = sum(len(r) for r in responses.values())
        if responses:
            raw.write_rest(fam.cat, now_ms, responses)

    report.elapsed_sec = time.monotonic() - t0
    logger.info(
        "Update complete in %.1fs: %s",
        report.elapsed_sec,
        "; ".join(
            f"{name} {s.symbols_ok} ok/{s.symbols_failed} failed/{s.rows_fetched} rows"
            for name, s in report.per_family.items()
        ),
    )
    return report

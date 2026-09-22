"""Live funding data: new settlements, and the per-symbol funding interval.

Both are market-wide queries rather than per-symbol ones, and both draw from
the same small rate-limit pool, so `update` fetches them as one family and
`build` derives the interval snapshot from the newest responses.
"""

from __future__ import annotations

import logging

import polars as pl

from binance_fetcher.client.rest import BinanceClient

logger = logging.getLogger(__name__)

PAGE_SIZE = 1000  # API maximum
MAX_PAGES = 400  # under the funding pool's 5-minute budget; ~4 months of events
DEFAULT_INTERVAL_HOURS = 8  # symbols absent from fundingInfo settle every 8h
SNAPSHOT_FILE = "funding_interval.parquet"


async def fetch_new_settlements(client: BinanceClient, watermark: int | None) -> list[dict]:
    """Every settlement after ``watermark`` (the newest stored funding_time).

    The query has no symbol filter and returns at most 1000 events, about seven
    hours market-wide. Fetching only "the latest page" would lose data for good
    whenever runs are further apart than that, so the fetch pages forward from
    the watermark instead and any pause heals on the next run. A stale
    watermark only costs duplicates, which `build` absorbs.
    """
    if watermark is None:
        return await client.fetch_all_funding_rates(limit=PAGE_SIZE)

    raw: list[dict] = []
    start = watermark + 1
    for _ in range(MAX_PAGES):
        page = await client.fetch_all_funding_rates(limit=PAGE_SIZE, start_time=start)
        raw.extend(page)
        if len(page) < PAGE_SIZE:
            break
        start = max(int(r["fundingTime"]) for r in page) + 1
    else:
        logger.warning("Funding paging hit the %d-page cap; the next run continues", MAX_PAGES)
    return raw


def funding_interval_frame(info: list[dict], premium: list[dict], fetched_at: int) -> pl.DataFrame:
    """The point-in-time funding-interval snapshot, from fundingInfo and premiumIndex.

    Per symbol: the current settlement interval, the exact next settlement
    time and the last rate. Not a time series. It gives a live reader the
    right next-funding time the moment a symbol's interval changes, which the
    settlement history can only reveal after the fact.
    """
    # fundingInfo lists only symbols that deviate from the default interval.
    interval = {
        r["symbol"]: int(r["fundingIntervalHours"])
        for r in info
        if r.get("fundingIntervalHours") is not None
    }
    return pl.DataFrame(
        {
            "symbol": [r["symbol"] for r in premium],
            "interval_hours": [interval.get(r["symbol"], DEFAULT_INTERVAL_HOURS) for r in premium],
            "next_funding_time": [int(r.get("nextFundingTime") or 0) for r in premium],
            "last_funding_rate": [float(r.get("lastFundingRate") or 0.0) for r in premium],
            "fetched_at": [fetched_at] * len(premium),
        },
        schema={
            "symbol": pl.String,
            "interval_hours": pl.Int32,
            "next_funding_time": pl.Int64,
            "last_funding_rate": pl.Float64,
            "fetched_at": pl.Int64,
        },
    )

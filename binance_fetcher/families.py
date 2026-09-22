"""The data families: one registry that every job reads.

A family is one stored category together with everything needed to fill it from
both sources: the Vision bulk archive (authoritative, published late) and the
REST API (live, until an archive covers it). ``backfill`` and ``update`` fetch
the raw files, ``build`` parses them; none of them keeps a family list of its
own.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import Any

import polars as pl

from binance_fetcher.config import Config
from binance_fetcher.transform import klines
from binance_fetcher.transform.funding import parse_api_funding, parse_vision_funding_csv
from binance_fetcher.transform.open_interest import (
    parse_api_open_interest,
    parse_vision_metrics_csv,
)

# How the REST API serves a family, which decides how `update` fetches it.
KLINES = "klines"  # per symbol, from a start time; /fapi/v1/<data_type>
LATEST = "latest"  # per symbol, the latest N points; no start time, ~30 days retained
MARKET = "market"  # one paged query for all symbols


@dataclass(frozen=True)
class Family:
    cat: str  # storage category (the directory name), also the family's name
    data_type: str  # Vision archive name; for kline families also the REST endpoint
    live: str  # KLINES | LATEST | MARKET
    archive_by: str  # how Vision publishes it: "month", or "day" (packed by month on disk)
    time_col: str  # per-row time key, ms; rows are unique on (symbol, time_col)
    parse_vision: Callable[[bytes], pl.DataFrame]  # one CSV member of an archive
    parse_rest: Callable[[list], pl.DataFrame]  # keeps row order: build aligns symbols to rows
    rest_time: Callable[[Any], int]  # time key of one raw REST row
    verify_archive: bool = True  # check the archive's published SHA-256

    @property
    def interval(self) -> str | None:
        """Kline interval segment of the archive path; None for non-kline archives."""
        return "1h" if self.live == KLINES else None


def _kline_time(row: list) -> int:
    return int(row[0])


FAMILIES: dict[str, Family] = {
    # OHLCV first: the months it has archives for bound every other family.
    "ohlcv": Family(
        "ohlcv",
        "klines",
        KLINES,
        "month",
        "open_time",
        partial(klines.parse_vision, schema=klines.OHLCV_SCHEMA),
        partial(klines.parse_rest, schema=klines.OHLCV_SCHEMA),
        _kline_time,
    ),
    "mark_price": Family(
        "mark_price",
        "markPriceKlines",
        KLINES,
        "month",
        "open_time",
        partial(klines.parse_vision, schema=klines.MARK_PRICE_SCHEMA),
        partial(klines.parse_rest, schema=klines.MARK_PRICE_SCHEMA),
        _kline_time,
    ),
    # Same row layout as markPriceKlines; the OHLC values are the funding premium.
    "premium_index_klines": Family(
        "premium_index_klines",
        "premiumIndexKlines",
        KLINES,
        "month",
        "open_time",
        partial(klines.parse_vision, schema=klines.MARK_PRICE_SCHEMA),
        partial(klines.parse_rest, schema=klines.MARK_PRICE_SCHEMA),
        _kline_time,
    ),
    # Settlement cadence differs per symbol and over time (8h/4h/1h).
    "funding": Family(
        "funding",
        "fundingRate",
        MARKET,
        "month",
        "funding_time",
        parse_vision_funding_csv,
        parse_api_funding,
        lambda row: int(row["fundingTime"]),
    ),
    # Open interest lives in Vision's daily "metrics" files; there is no monthly
    # layout, so backfill packs a month of them into one ZIP. ~14 KB each,
    # several hundred thousand for a full backfill, so the checksum request is
    # skipped; the CRC-32 of every member is still verified.
    "open_interest": Family(
        "open_interest",
        "metrics",
        LATEST,
        "day",
        "timestamp",
        parse_vision_metrics_csv,
        parse_api_open_interest,
        lambda row: int(row["timestamp"]),
        verify_archive=False,
    ),
}


def select_families(config: Config, names: list[str] | None = None) -> list[Family]:
    """The requested families in registry order; by default all enabled in config."""
    if names is None:
        off = set()
        if not config.include_mark_price:
            off.add("mark_price")
        if not config.include_premium_index:
            off.add("premium_index_klines")
        names = [n for n in FAMILIES if n not in off]
    unknown = [n for n in names if n not in FAMILIES]
    if unknown:
        raise ValueError(f"unknown families {unknown}; choose from {list(FAMILIES)}")
    return [fam for name, fam in FAMILIES.items() if name in names]

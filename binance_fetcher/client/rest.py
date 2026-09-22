"""Async client for the Binance USD-M Futures REST API.

Every endpoint declares which rate-limit pool it draws from and what it costs;
``_request`` is the single place where throttling, retry and error mapping
happen.
"""

from __future__ import annotations

import asyncio
import logging

import aiohttp
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential_jitter

from binance_fetcher.client.throttle import SlidingWindow, Throttle
from binance_fetcher.config import Config

logger = logging.getLogger(__name__)

# Rate-limit pools, all per IP (Binance API reference):
WEIGHT = "weight"  # /fapi/v1/*: 2400 request weight per minute
FUTURES_DATA = "futures_data"  # /futures/data/*: weight 0, 1000 requests per 5 minutes
FUNDING = "funding"  # fundingRate + fundingInfo: weight 0, a shared 500 requests per 5 minutes

KLINE_DATA_TYPES = ("klines", "markPriceKlines", "premiumIndexKlines")
MAX_KLINE_LIMIT = 1500
MAX_FUTURES_DATA_LIMIT = 500


def kline_weight(limit: int) -> int:
    """Request weight of the kline endpoints, which scales with ``limit``."""
    if limit < 100:
        return 1
    if limit < 500:
        return 2
    if limit <= 1000:
        return 5
    return 10


class IPBanError(Exception):
    """HTTP 418: Binance banned this IP. Sending anything more extends the ban."""


class BinanceAPIError(Exception):
    """A non-2xx response."""

    def __init__(self, status: int, message: str):
        self.status = status
        super().__init__(f"HTTP {status}: {message}")


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, BinanceAPIError):
        return exc.status == 429 or exc.status >= 500
    return isinstance(exc, (aiohttp.ClientError, asyncio.TimeoutError))


class BinanceClient:
    def __init__(self, config: Config):
        self._config = config
        self._base_url = config.base_url
        self._throttle = Throttle(
            pools={
                WEIGHT: SlidingWindow(config.weight_per_minute, 60),
                FUTURES_DATA: SlidingWindow(config.futures_data_per_5min, 300),
                FUNDING: SlidingWindow(config.funding_per_5min, 300),
            },
            max_concurrent=config.max_concurrent,
        )
        self._session: aiohttp.ClientSession | None = None
        self._ban: IPBanError | None = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=self._config.max_concurrent),
            timeout=aiohttp.ClientTimeout(total=30),
        )
        return self

    async def __aexit__(self, *exc):
        if self._session:
            await self._session.close()
            self._session = None

    @retry(
        retry=retry_if_exception(_is_retryable),
        wait=wait_exponential_jitter(initial=1, max=60, jitter=2),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def _request(self, path: str, params: dict | None, pool: str, cost: int = 1):
        """One throttled GET. Every retry attempt pays for its budget again."""
        if self._ban:
            raise self._ban  # circuit open: queued requests fail without being sent

        await self._throttle.acquire(pool, cost)
        try:
            if self._ban:
                raise self._ban  # banned while this request was waiting for budget
            assert self._session is not None, "use the client as an async context manager"
            async with self._session.get(f"{self._base_url}{path}", params=params) as resp:
                used = resp.headers.get("X-MBX-USED-WEIGHT-1m")
                if used:
                    self._throttle.observe_used_weight(int(used))

                if resp.status == 418:
                    retry_after = resp.headers.get("Retry-After", "unknown")
                    self._ban = IPBanError(f"IP banned, Retry-After: {retry_after}s")
                    raise self._ban

                if resp.status == 429:
                    # Tell every queued request to back off, not just this one:
                    # ignoring a 429 is what escalates to a 418 ban.
                    self._throttle.block_for(int(resp.headers.get("Retry-After", 60)), "HTTP 429")
                    raise BinanceAPIError(429, "rate limited")

                if resp.status >= 400:
                    raise BinanceAPIError(resp.status, await resp.text())

                return await resp.json()
        finally:
            self._throttle.release()

    # -- /fapi/v1: weight pool ---------------------------------------------

    async def fetch_klines(
        self,
        symbol: str,
        data_type: str = "klines",
        interval: str = "1h",
        start_time: int | None = None,
        limit: int = 5,
    ) -> list[list]:
        """Klines of one of ``KLINE_DATA_TYPES``; all share one 12-field row shape.

        ``start_time`` is inclusive on open_time. The names match the Vision
        archive data types, so a family maps to both sources with one string.
        """
        if data_type not in KLINE_DATA_TYPES:
            raise ValueError(f"unknown kline data type {data_type!r}")
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        return await self._request(f"/fapi/v1/{data_type}", params, WEIGHT, kline_weight(limit))

    async def fetch_exchange_info(self) -> dict:
        return await self._request("/fapi/v1/exchangeInfo", None, WEIGHT, 1)

    async def fetch_premium_index(self) -> list[dict]:
        """Premium-index snapshot for ALL symbols: nextFundingTime, lastFundingRate."""
        return await self._request("/fapi/v1/premiumIndex", None, WEIGHT, 10)

    # -- funding pool --------------------------------------------------------

    async def fetch_all_funding_rates(
        self, limit: int = 1000, start_time: int | None = None
    ) -> list[dict]:
        """Funding settlements for ALL symbols in one call (no symbol param).

        Without start_time: the latest `limit` events market-wide (~7h at
        current symbol counts). With start_time (ms): events ascending from
        that timestamp, `limit` per page — page by advancing start_time past
        the last returned fundingTime.
        """
        params = {"limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        return await self._request("/fapi/v1/fundingRate", params, FUNDING)

    async def fetch_funding_info(self) -> list[dict]:
        """Per-symbol funding config: fundingIntervalHours and rate cap/floor.

        Lists only symbols that differ from the 8h default.
        """
        return await self._request("/fapi/v1/fundingInfo", None, FUNDING)

    # -- /futures/data: request-count pool, ~30 days retained ------------------

    async def fetch_open_interest_hist(
        self, symbol: str, period: str = "1h", limit: int = 30
    ) -> list[dict]:
        """The latest ``limit`` open-interest points (sum OI in base + notional)."""
        params = {"symbol": symbol, "period": period, "limit": limit}
        return await self._request("/futures/data/openInterestHist", params, FUTURES_DATA)

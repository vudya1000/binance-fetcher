"""REST client and throttle: budgets per pool, real request costs, retry, ban circuit."""

from __future__ import annotations

import asyncio

import pytest
from tenacity import wait_none

from binance_fetcher.client.rest import (
    FUNDING,
    FUTURES_DATA,
    WEIGHT,
    BinanceAPIError,
    BinanceClient,
    IPBanError,
    kline_weight,
)
from binance_fetcher.client.throttle import SlidingWindow, Throttle
from binance_fetcher.config import Config

# -- sliding window ------------------------------------------------------------


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_sliding_window_admits_up_to_the_limit_then_reports_the_wait():
    clock = _Clock()
    win = SlidingWindow(limit=10, window_sec=60, clock=clock)
    assert win.reserve(6) == 0.0
    clock.now += 10
    assert win.reserve(4) == 0.0
    # Full. 3 units fit once the first entry (6 units, 10s old) expires.
    assert win.reserve(3) == pytest.approx(50)
    # 7 units need the second entry gone too.
    assert win.reserve(7) == pytest.approx(60)


def test_sliding_window_never_exceeds_limit_in_any_window():
    # The property a token bucket lacks: a burst right after a quiet period
    # cannot be followed by a refill inside the same window.
    clock = _Clock()
    win = SlidingWindow(limit=5, window_sec=60, clock=clock)
    admitted: list[float] = []
    for _ in range(200):
        if win.reserve(1) == 0.0:
            admitted.append(clock.now)
        clock.now += 1
    for t in admitted:
        assert sum(1 for a in admitted if t <= a < t + 60) <= 5


def test_sliding_window_rejects_a_cost_that_can_never_fit():
    with pytest.raises(ValueError):
        SlidingWindow(limit=5, window_sec=60).reserve(6)


def test_throttle_pools_are_independent():
    async def run():
        throttle = Throttle(
            {WEIGHT: SlidingWindow(1, 60), FUTURES_DATA: SlidingWindow(1, 60)}, max_concurrent=5
        )
        await throttle.acquire(WEIGHT, 1)  # weight pool now exhausted
        await asyncio.wait_for(throttle.acquire(FUTURES_DATA, 1), timeout=0.5)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(throttle.acquire(WEIGHT, 1), timeout=0.05)

    asyncio.run(run())


def test_server_reported_weight_at_budget_blocks_everything():
    async def run():
        throttle = Throttle({WEIGHT: SlidingWindow(100, 60)}, max_concurrent=5)
        throttle.observe_used_weight(99)
        await asyncio.wait_for(throttle.acquire(WEIGHT, 1), timeout=0.5)
        throttle.observe_used_weight(100)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(throttle.acquire(WEIGHT, 1), timeout=0.05)

    asyncio.run(run())


# -- client --------------------------------------------------------------------


@pytest.mark.parametrize(
    "limit, weight", [(5, 1), (99, 1), (100, 2), (499, 2), (500, 5), (1000, 5), (1001, 10)]
)
def test_kline_weight_follows_the_binance_table(limit, weight):
    assert kline_weight(limit) == weight


class _Resp:
    def __init__(self, status=200, body=None, headers=None):
        self.status, self._body, self.headers = status, body, headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return self._body

    async def text(self):
        return str(self._body)


class _Session:
    """Replays scripted responses and records the requested URLs."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.urls: list[str] = []

    def get(self, url, params=None):
        self.urls.append(url)
        return self._responses.pop(0)

    async def close(self):
        pass


class _SpyThrottle(Throttle):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.charged: list[tuple[str, int]] = []
        self.blocks: list[float] = []

    async def acquire(self, pool, cost):
        self.charged.append((pool, cost))
        await super().acquire(pool, cost)

    def block_for(self, seconds, reason):
        self.blocks.append(seconds)  # recorded, not applied: keeps the test instant


def _client(monkeypatch, responses) -> tuple[BinanceClient, _Session, _SpyThrottle]:
    monkeypatch.setattr(BinanceClient._request.retry, "wait", wait_none())
    client = BinanceClient(Config())
    throttle = _SpyThrottle(client._throttle._pools, max_concurrent=5)
    session = _Session(responses)
    client._throttle, client._session = throttle, session
    return client, session, throttle


def test_each_endpoint_charges_its_own_pool_and_real_cost(monkeypatch):
    client, session, throttle = _client(monkeypatch, [_Resp(body=[]) for _ in range(5)])

    async def run():
        await client.fetch_klines("BTCUSDT", limit=5)
        await client.fetch_klines("BTCUSDT", "markPriceKlines", limit=1500)
        await client.fetch_premium_index()
        await client.fetch_all_funding_rates()
        await client.fetch_open_interest_hist("BTCUSDT")

    asyncio.run(run())
    assert throttle.charged == [
        (WEIGHT, 1),
        (WEIGHT, 10),
        (WEIGHT, 10),
        (FUNDING, 1),
        (FUTURES_DATA, 1),
    ]
    assert session.urls[1].endswith("/fapi/v1/markPriceKlines")


def test_unknown_kline_data_type_is_rejected(monkeypatch):
    client, _, _ = _client(monkeypatch, [])
    with pytest.raises(ValueError):
        asyncio.run(client.fetch_klines("BTCUSDT", "trades"))


def test_429_blocks_the_throttle_and_is_retried(monkeypatch):
    responses = [_Resp(429, headers={"Retry-After": "7"}), _Resp(body={"ok": True})]
    client, session, throttle = _client(monkeypatch, responses)
    assert asyncio.run(client.fetch_exchange_info()) == {"ok": True}
    assert throttle.blocks == [7]
    assert len(throttle.charged) == 2  # the retry paid for its budget again


def test_5xx_is_retried_and_4xx_is_not(monkeypatch):
    client, session, _ = _client(monkeypatch, [_Resp(503, "busy"), _Resp(body={"ok": True})])
    assert asyncio.run(client.fetch_exchange_info()) == {"ok": True}

    client, session, _ = _client(monkeypatch, [_Resp(400, "bad symbol"), _Resp(body={})])
    with pytest.raises(BinanceAPIError, match="HTTP 400"):
        asyncio.run(client.fetch_exchange_info())
    assert len(session.urls) == 1


def test_ip_ban_opens_the_circuit(monkeypatch):
    # One 418, then nothing else may reach the network.
    client, session, _ = _client(monkeypatch, [_Resp(418, headers={"Retry-After": "120"})])

    async def run():
        return await asyncio.gather(
            *(client.fetch_klines(f"S{i}USDT") for i in range(20)), return_exceptions=True
        )

    results = asyncio.run(run())
    assert all(isinstance(r, IPBanError) for r in results)
    assert len(session.urls) == 1

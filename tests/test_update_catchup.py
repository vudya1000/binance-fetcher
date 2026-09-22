"""Hourly update: fetch window and partial-candle replacement.

The run at :00:30 fetches from the newest stored candle, inclusive (Binance
``startTime`` is inclusive on open_time). That candle was in progress when it
was stored; the closed version fetched now is the newer REST row, and ``build``
keeps the newest. However many runs were missed, the window still starts at
that candle, so the partial snapshot never survives.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import polars as pl
import pytest

from binance_fetcher.client.rest import MAX_FUTURES_DATA_LIMIT, MAX_KLINE_LIMIT
from binance_fetcher.config import Config
from binance_fetcher.pipeline import update as upd
from binance_fetcher.pipeline.build import run_build
from binance_fetcher.pipeline.update import (
    HOUR_MS,
    NEW_SYMBOL_LIMIT,
    _kline_window,
    _latest_limit,
)
from binance_fetcher.storage.parquet import ParquetStore

NOW = 1_700_000_000_000 - (1_700_000_000_000 % HOUR_MS) + 30_000  # some hour + 30s
CUR_HOUR = NOW - NOW % HOUR_MS


# -- unit: window arithmetic --------------------------------------------------


def test_nothing_stored_fetches_the_latest_candles():
    assert _kline_window(None, now_ms=NOW) == (None, NEW_SYMBOL_LIMIT)


@pytest.mark.parametrize("hours_behind", [0, 1, 4, 100])
def test_window_starts_at_the_stored_candle_and_reaches_now(hours_behind):
    last = CUR_HOUR - hours_behind * HOUR_MS
    start, limit = _kline_window(last, now_ms=NOW)
    # Inclusive start: the stored candle (possibly a partial snapshot) is re-fetched.
    assert start == last
    # Room for it and every newer candle, including the one in progress.
    assert hours_behind + 1 <= limit <= MAX_KLINE_LIMIT


def test_window_is_capped_at_binance_max():
    last = CUR_HOUR - 5_000 * HOUR_MS
    assert _kline_window(last, now_ms=NOW) == (last, MAX_KLINE_LIMIT)


def test_latest_limit_spans_the_gap_since_the_newest_stored_point():
    assert _latest_limit(None, NOW) == MAX_FUTURES_DATA_LIMIT  # first run: all on offer
    assert _latest_limit(NOW - HOUR_MS, NOW) == 3  # routine: the new hour plus overlap
    assert _latest_limit(NOW - 50 * HOUR_MS, NOW) == 52  # after an outage
    assert _latest_limit(NOW - 9999 * HOUR_MS, NOW) == MAX_FUTURES_DATA_LIMIT  # capped


# -- e2e: partial candle is replaced after a missed run ------------------------


class _Exchange:
    """Minimal /fapi/v1/klines stand-in with a movable clock.

    Every candle up to the current hour is closed (volume=100); the candle
    for the current hour is in progress (volume=1). ``startTime`` is
    inclusive on open_time, like Binance.
    """

    def __init__(self, now_ms: int):
        self.now_ms = now_ms
        self.calls: list[tuple[int | None, int]] = []

    @property
    def cur_hour(self) -> int:
        return self.now_ms - self.now_ms % HOUR_MS

    def _candle(self, open_time: int) -> list:
        vol = 1.0 if open_time == self.cur_hour else 100.0
        return [open_time, 1, 2, 0.5, 1.5, vol, open_time + HOUR_MS - 1, 10, 5, 4, 3, 0]

    def klines(self, start_time: int | None, limit: int) -> list[list]:
        self.calls.append((start_time, limit))
        newest = self.cur_hour
        if start_time is None:
            first = newest - (limit - 1) * HOUR_MS
        else:
            first = start_time
        times = range(first, newest + 1, HOUR_MS)
        return [self._candle(t) for t in list(times)[:limit]]


def _fake_client(exchange: _Exchange):
    class _FakeClient:
        def __init__(self, config):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def fetch_klines(self, symbol, data_type, start_time=None, limit=5):
            assert data_type == "klines"
            return exchange.klines(start_time, limit)

    return _FakeClient


def _run(cfg: Config, exchange: _Exchange):
    report = asyncio.run(upd.run_update(cfg, symbols=["BTCUSDT"], families=["ohlcv"]))
    today = datetime.fromtimestamp(exchange.now_ms / 1000, tz=UTC).date()
    assert not report.errors and not run_build(cfg, ["ohlcv"], today=today).errors
    return ParquetStore(cfg).read_all("ohlcv").filter(pl.col("symbol") == "BTCUSDT")


def test_partial_candle_is_replaced_after_missed_runs(tmp_path, monkeypatch):
    cfg = Config(data_dir=tmp_path / "data")
    cfg.meta_dir.mkdir(parents=True)

    exchange = _Exchange(NOW)
    monkeypatch.setattr(upd, "BinanceClient", _fake_client(exchange))
    monkeypatch.setattr(upd.time, "time", lambda: exchange.now_ms / 1000)

    # Run 1 at HH:00:30 stores the in-progress candle; it is the newest built row.
    stored = _run(cfg, exchange)
    partial_open = exchange.cur_hour
    assert stored["open_time"].max() == partial_open
    assert stored.filter(pl.col("open_time") == partial_open)["volume"][0] == 1.0

    # Six hours pass with no cron runs.
    exchange.now_ms += 6 * HOUR_MS
    stored = _run(cfg, exchange)

    start_time, _ = exchange.calls[-1]
    assert start_time == partial_open, "the fetch must start at the stored candle"

    row = stored.filter(pl.col("open_time") == partial_open)
    assert row.height == 1
    assert row["volume"][0] == 100.0, "partial snapshot must be replaced by the closed candle"

    # No holes between the first candle and the new current hour.
    diffs = stored["open_time"].diff().drop_nulls().unique().to_list()
    assert diffs == [HOUR_MS]
    assert stored["open_time"].max() == exchange.cur_hour

"""Unified update: every family in one run, stored raw; independent failures."""

from __future__ import annotations

import asyncio
from datetime import date

import polars as pl

from binance_fetcher.config import Config
from binance_fetcher.pipeline import update as upd
from binance_fetcher.pipeline.build import run_build
from binance_fetcher.pipeline.funding import SNAPSHOT_FILE
from binance_fetcher.storage.parquet import ParquetStore
from binance_fetcher.storage.raw import RawStore
from binance_fetcher.storage.state import load_state, save_state

HOUR_MS = 3_600_000
T0 = 1_700_000_000_000 - 1_700_000_000_000 % HOUR_MS


class _Exchange:
    """Scripted REST API. Records calls; selected calls can be made to fail."""

    def __init__(self):
        self.calls: list[tuple] = []
        self.fail: set[tuple[str, str]] = set()  # (endpoint, symbol)
        self.funding_pages: list[list[dict]] = [[]]
        self.listed: list[str] | None = ["BTCUSDT", "ETHUSDT"]  # None: exchangeInfo is down

    def client(self):
        exchange = self

        class FakeClient:
            def __init__(self, config):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def fetch_exchange_info(self):
                if exchange.listed is None:
                    raise RuntimeError("exchangeInfo down")
                return {
                    "symbols": [
                        {"symbol": s, "status": "TRADING", "contractType": "PERPETUAL"}
                        for s in exchange.listed
                    ]
                }

            async def fetch_klines(self, symbol, data_type, start_time=None, limit=5):
                exchange.calls.append((data_type, symbol, start_time, limit))
                if (data_type, symbol) in exchange.fail:
                    raise RuntimeError("boom")
                return [[T0, 1, 2, 0.5, 1.5, 100, T0 + HOUR_MS - 1, 10, 5, 4, 3, 0]]

            async def fetch_open_interest_hist(self, symbol, period, limit):
                exchange.calls.append(("openInterestHist", symbol, None, limit))
                if ("openInterestHist", symbol) in exchange.fail:
                    raise RuntimeError("boom")
                return [{"timestamp": T0, "sumOpenInterest": "1", "sumOpenInterestValue": "2"}]

            async def fetch_all_funding_rates(self, limit=1000, start_time=None):
                exchange.calls.append(("fundingRate", None, start_time, limit))
                if ("fundingRate", "*") in exchange.fail:
                    raise RuntimeError("funding down")
                return exchange.funding_pages.pop(0) if exchange.funding_pages else []

            async def fetch_funding_info(self):
                return [{"symbol": "ETHUSDT", "fundingIntervalHours": 4}]

            async def fetch_premium_index(self):
                return [
                    {"symbol": s, "nextFundingTime": T0 + 8 * HOUR_MS, "lastFundingRate": "0.0001"}
                    for s in ("BTCUSDT", "ETHUSDT")
                ]

        return FakeClient


def _funding(symbol, t, rate="0.0001"):
    return {"symbol": symbol, "fundingTime": t, "fundingRate": rate, "markPrice": "100"}


def _setup(tmp_path, monkeypatch, exchange):
    cfg = Config(data_dir=tmp_path / "data")
    cfg.meta_dir.mkdir(parents=True, exist_ok=True)
    save_state(cfg.meta_dir, {"BTCUSDT": {"status": "active"}, "ETHUSDT": {"status": "active"}})
    monkeypatch.setattr(upd, "BinanceClient", exchange.client())
    monkeypatch.setattr(upd.time, "time", lambda: (T0 + 30_000) / 1000)
    return cfg


def _responses(cfg, cat) -> dict:
    """What the one run stored for a family."""
    raw = RawStore(cfg)
    [path] = raw.rest_files(cat)
    return raw.read_rest(path)["responses"]


def test_one_run_stores_every_family_raw_and_build_makes_the_rows(tmp_path, monkeypatch):
    exchange = _Exchange()
    exchange.funding_pages = [[_funding("BTCUSDT", T0), _funding("ETHUSDT", T0)]]
    cfg = _setup(tmp_path, monkeypatch, exchange)

    report = asyncio.run(upd.run_update(cfg))

    assert not report.errors
    assert list(report.per_family) == [
        "ohlcv",
        "mark_price",
        "premium_index_klines",
        "funding",
        "open_interest",
    ]
    for stats in report.per_family.values():
        assert (stats.symbols_ok, stats.symbols_failed, stats.rows_fetched) == (2, 0, 2)
    # Stored as received, nothing parsed.
    assert _responses(cfg, "ohlcv")["BTCUSDT"][0][5] == 100
    assert not ParquetStore(cfg).files("ohlcv")

    # The kline requests were issued before any other family's.
    kinds = [c[0] for c in exchange.calls]
    assert (
        kinds.index("openInterestHist") > kinds.index("premiumIndexKlines") > kinds.index("klines")
    )

    assert not run_build(cfg, today=date(2023, 11, 15)).errors
    store = ParquetStore(cfg)
    for cat in report.per_family:
        assert store.read_all(cat).height == 2
        assert store.last_times(cat) == {"BTCUSDT": T0, "ETHUSDT": T0}  # where the next run resumes
    snapshot = pl.read_parquet(cfg.parquet_dir / SNAPSHOT_FILE).sort("symbol")
    assert snapshot["interval_hours"].to_list() == [8, 4]  # BTC default, ETH from fundingInfo


def test_next_run_resumes_from_the_newest_built_row(tmp_path, monkeypatch):
    exchange = _Exchange()
    cfg = _setup(tmp_path, monkeypatch, exchange)
    asyncio.run(upd.run_update(cfg, families=["ohlcv", "open_interest"]))
    run_build(cfg, today=date(2023, 11, 15))

    exchange.calls.clear()
    monkeypatch.setattr(upd.time, "time", lambda: (T0 + 2 * HOUR_MS + 30_000) / 1000)
    asyncio.run(upd.run_update(cfg, symbols=["BTCUSDT"], families=["ohlcv", "open_interest"]))

    # From the stored candle itself, which may have been in progress, up to now.
    assert ("klines", "BTCUSDT", T0, 4) in exchange.calls
    assert ("openInterestHist", "BTCUSDT", None, 4) in exchange.calls


def test_families_fail_independently_and_a_failed_symbol_stores_nothing(tmp_path, monkeypatch):
    exchange = _Exchange()
    exchange.fail = {("openInterestHist", "ETHUSDT"), ("fundingRate", "*")}
    cfg = _setup(tmp_path, monkeypatch, exchange)

    report = asyncio.run(upd.run_update(cfg, families=["ohlcv", "funding", "open_interest"]))

    assert report.per_family["ohlcv"].symbols_ok == 2  # untouched by the other failures
    oi = report.per_family["open_interest"]
    assert (oi.symbols_ok, oi.symbols_failed) == (1, 1)
    assert report.per_family["funding"].symbols_failed == 1
    assert sorted(report.errors) == [
        "funding fundingRate: funding down",
        "open_interest ETHUSDT: boom",
    ]
    # Nothing stored for ETH, so the next run asks for the whole gap again.
    assert set(_responses(cfg, "open_interest")) == {"BTCUSDT"}
    assert set(_responses(cfg, "funding")) == {"fundingInfo", "premiumIndex"}  # that half still ran


def test_funding_pages_forward_from_the_market_wide_watermark(tmp_path, monkeypatch):
    exchange = _Exchange()
    full_page = [_funding("BTCUSDT", T0 + i) for i in range(1000)]
    exchange.funding_pages = [full_page, [_funding("ETHUSDT", T0 + 5000)]]
    cfg = _setup(tmp_path, monkeypatch, exchange)
    built = pl.DataFrame({"symbol": ["BTCUSDT", "ETHUSDT"], "funding_time": [T0 - 1, T0 - 9]})
    ParquetStore(cfg).write_month("funding", "2023-11", built, inputs="")

    report = asyncio.run(upd.run_update(cfg, families=["funding"]))

    starts = [c[2] for c in exchange.calls if c[0] == "fundingRate"]
    assert starts == [T0, T0 + 1000]  # newest stored + 1, then past the last event of the page
    assert report.per_family["funding"].rows_fetched == 1001


def test_explicit_symbols_narrow_the_market_wide_family(tmp_path, monkeypatch):
    exchange = _Exchange()
    exchange.funding_pages = [[_funding("BTCUSDT", T0), _funding("ETHUSDT", T0)]]
    cfg = _setup(tmp_path, monkeypatch, exchange)

    asyncio.run(upd.run_update(cfg, symbols=["BTCUSDT"], families=["funding"]))
    assert {r["symbol"] for r in _responses(cfg, "funding")["fundingRate"]} == {"BTCUSDT"}


def test_update_discovers_listings_and_delistings_before_fetching(tmp_path, monkeypatch):
    exchange = _Exchange()
    exchange.listed = ["BTCUSDT", "NEWUSDT"]
    cfg = _setup(tmp_path, monkeypatch, exchange)

    report = asyncio.run(upd.run_update(cfg, families=["ohlcv"]))

    assert not report.errors
    assert set(_responses(cfg, "ohlcv")) == {"BTCUSDT", "NEWUSDT"}  # ETH is no longer polled
    assert load_state(cfg.meta_dir)["ETHUSDT"]["status"] == "delisted"


def test_failed_discovery_falls_back_to_the_stored_symbols(tmp_path, monkeypatch):
    exchange = _Exchange()
    exchange.listed = None
    cfg = _setup(tmp_path, monkeypatch, exchange)

    report = asyncio.run(upd.run_update(cfg, families=["ohlcv"]))

    assert report.errors == ["discover: exchangeInfo down"]  # the run still exits non-zero
    assert report.per_family["ohlcv"].symbols_ok == 2

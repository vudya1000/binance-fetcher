"""Symbol discovery: status follows exchangeInfo, listing dates are recorded."""

from __future__ import annotations

import asyncio

from binance_fetcher.config import Config
from binance_fetcher.pipeline import discover as dc
from binance_fetcher.storage.state import load_state, save_state


def _sym(name, status="TRADING", contract="PERPETUAL", onboard=1_600_000_000_000):
    return {"symbol": name, "status": status, "contractType": contract, "onboardDate": onboard}


def _discover(tmp_path, symbols):
    class FakeClient:
        async def fetch_exchange_info(self):
            return {"symbols": symbols}

    cfg = Config(data_dir=tmp_path / "data")
    cfg.meta_dir.mkdir(parents=True, exist_ok=True)
    return cfg, asyncio.run(dc.run_discover(cfg, FakeClient()))


def test_discover_tracks_trading_perpetuals_and_their_listing_date(tmp_path):
    info = [_sym("BTCUSDT", onboard=111), _sym("ETHUSDT_260925", contract="CURRENT_QUARTER")]
    cfg, (new, removed, symbols) = _discover(tmp_path, info)
    assert new == ["BTCUSDT"] and removed == [] and symbols == ["BTCUSDT"]
    assert load_state(cfg.meta_dir) == {"BTCUSDT": {"status": "active", "onboard_date": 111}}


def test_delisting_and_relisting_flip_status(tmp_path):
    cfg = Config(data_dir=tmp_path / "data")
    cfg.meta_dir.mkdir(parents=True, exist_ok=True)
    save_state(cfg.meta_dir, {"OLDUSDT": {"status": "active"}})

    _, (_, removed, symbols) = _discover(tmp_path, [_sym("OLDUSDT", "SETTLING")])
    assert removed == ["OLDUSDT"] and symbols == ["OLDUSDT"]  # still tracked, for its history
    assert load_state(cfg.meta_dir)["OLDUSDT"]["status"] == "delisted"

    _, (new, _, _) = _discover(tmp_path, [_sym("OLDUSDT")])
    assert new == ["OLDUSDT"]
    assert load_state(cfg.meta_dir)["OLDUSDT"] == {
        "status": "active",
        "onboard_date": 1_600_000_000_000,
    }


def test_already_delisted_perpetuals_are_tracked_without_activating(tmp_path):
    info = [_sym("BTCUSDT"), _sym("GONEUSDT", "CLOSE", onboard=222)]
    cfg, (new, _, symbols) = _discover(tmp_path, info)
    assert new == ["BTCUSDT"]
    assert symbols == ["BTCUSDT", "GONEUSDT"]
    assert load_state(cfg.meta_dir)["GONEUSDT"] == {"status": "delisted", "onboard_date": 222}


def test_delivery_date_is_recorded_once_a_delisting_is_scheduled(tmp_path):
    never = 4_133_404_800_000  # what a perpetual reports while none is scheduled
    info = [
        _sym("BTCUSDT") | {"deliveryDate": never},
        _sym("GONEUSDT", "SETTLING") | {"deliveryDate": 999},
    ]
    cfg, _ = _discover(tmp_path, info)
    state = load_state(cfg.meta_dir)
    assert "delivery_date" not in state["BTCUSDT"] and state["GONEUSDT"]["delivery_date"] == 999

    _discover(tmp_path, [_sym("GONEUSDT") | {"deliveryDate": never}])  # relisted
    assert "delivery_date" not in load_state(cfg.meta_dir)["GONEUSDT"]

"""Build: partitions are a function of the raw files; archives win, REST fills in."""

from __future__ import annotations

from datetime import date

import polars as pl

from binance_fetcher.config import Config
from binance_fetcher.families import FAMILIES
from binance_fetcher.pipeline import build as build_module
from binance_fetcher.pipeline.build import run_build
from binance_fetcher.pipeline.funding import SNAPSHOT_FILE
from binance_fetcher.storage.parquet import ParquetStore
from binance_fetcher.storage.raw import RawStore
from tests.rawdata import HOUR_MS, kline, klines_csv, metrics_csv, ms, zipped

SYM = "BTCUSDT"
OCT1, OCT20, OCT31, NOV1 = (
    date(2023, 10, 1),
    date(2023, 10, 20),
    date(2023, 10, 31),
    date(2023, 11, 1),
)
MID_OCT = date(2023, 10, 21)  # "today" while October is the month in progress


def _cfg(tmp_path) -> Config:
    return Config(data_dir=tmp_path / "data")


def _archive(cfg, month: str, csv: bytes, family: str = "ohlcv", symbol: str = SYM) -> None:
    raw, fam = RawStore(cfg), FAMILIES[family]
    raw.write_archive(raw.archive_path(fam, symbol, month), zipped(f"{month}.csv", csv))


def _candles(cfg) -> pl.DataFrame:
    return ParquetStore(cfg).read_all("ohlcv")


def _volume(cfg, open_time: int) -> float:
    return _candles(cfg).filter(pl.col("open_time") == open_time)["volume"].item()


def test_archive_rows_win_and_rest_fills_what_no_archive_has(tmp_path):
    cfg = _cfg(tmp_path)
    _archive(cfg, "2023-10", klines_csv(OCT1, OCT20))
    last_archived, first_live = ms(OCT20, 23), ms(MID_OCT)
    RawStore(cfg).write_rest(
        "ohlcv", first_live + 30_000, {SYM: [kline(last_archived, 7.0), kline(first_live, 1.0)]}
    )

    report = run_build(cfg, ["ohlcv"], today=MID_OCT)

    assert not report.errors and report.per_family["ohlcv"].months_built == 1
    assert _candles(cfg).height == 20 * 24 + 1
    assert _volume(cfg, last_archived) == 100.0  # the archive's row, not the REST one
    assert _volume(cfg, first_live) == 1.0
    assert _candles(cfg).columns[0] == "symbol"


def test_newest_rest_run_wins_so_a_partial_candle_is_replaced(tmp_path):
    cfg, raw, t = _cfg(tmp_path), RawStore(_cfg(tmp_path)), ms(MID_OCT)
    raw.write_rest("ohlcv", t + 30_000, {SYM: [kline(t, 1.0)]})  # in progress
    raw.write_rest("ohlcv", t + HOUR_MS + 30_000, {SYM: [kline(t), kline(t + HOUR_MS, 1.0)]})

    run_build(cfg, ["ohlcv"], today=MID_OCT)

    assert _candles(cfg).height == 2
    assert _volume(cfg, t) == 100.0 and _volume(cfg, t + HOUR_MS) == 1.0


def test_unchanged_month_is_skipped_and_a_new_archive_reconciles_it(tmp_path):
    cfg, t = _cfg(tmp_path), ms(OCT1, 5)
    RawStore(cfg).write_rest("ohlcv", t + 30_000, {SYM: [kline(t, 7.0)]})
    run_build(cfg, ["ohlcv"], today=MID_OCT)
    assert _volume(cfg, t) == 7.0

    stats = run_build(cfg, ["ohlcv"], today=MID_OCT).per_family["ohlcv"]
    assert (stats.months_built, stats.months_unchanged) == (0, 1)

    _archive(cfg, "2023-10", klines_csv(OCT1, OCT31))  # Vision publishes the month
    stats = run_build(cfg, ["ohlcv"], today=MID_OCT).per_family["ohlcv"]
    assert stats.months_built == 1
    assert _volume(cfg, t) == 100.0 and _candles(cfg).height == 31 * 24

    stats = run_build(cfg, ["ohlcv"], force=True, today=MID_OCT).per_family["ohlcv"]
    assert stats.months_built == 1  # --force rebuilds regardless


def test_settled_month_folds_its_run_files_into_the_uncovered_rows(tmp_path):
    cfg = _cfg(tmp_path)
    raw = RawStore(cfg)
    hole = ms(date(2023, 10, 10), 5)  # an hour the archive lacks and REST has
    _archive(cfg, "2023-10", klines_csv(OCT1, OCT31, skip={hole}))
    raw.write_rest("ohlcv", hole + 30_000, {SYM: [kline(hole - HOUR_MS), kline(hole, 1.0)]})
    raw.write_rest("ohlcv", hole + HOUR_MS + 30_000, {SYM: [kline(hole), kline(hole + HOUR_MS)]})
    boundary = raw.write_rest(  # holds rows of both months
        "ohlcv", ms(NOV1) + 30_000, {SYM: [kline(ms(OCT31, 23)), kline(ms(NOV1), 1.0)]}
    )

    def names() -> set[str]:
        return {p.name for p in raw.rest_files("ohlcv")}

    # October has been over for more than SETTLE_DAYS; November is in progress.
    stats = run_build(cfg, ["ohlcv"], today=date(2023, 11, 15)).per_family["ohlcv"]
    assert stats.rest_files_folded == 2
    assert names() == {"residual-2023-10.json.gz", boundary.name}
    residual = raw.read_rest(raw.rest_dir("ohlcv") / "residual-2023-10.json.gz")
    assert residual["responses"] == {SYM: [kline(hole)]}  # only the uncovered row, closed version
    october = _candles(cfg).filter(pl.col("open_time") < ms(NOV1))
    assert october.height == 31 * 24 and _volume(cfg, hole) == 100.0

    stats = run_build(cfg, ["ohlcv"], today=date(2023, 11, 15)).per_family["ohlcv"]
    assert (stats.months_built, stats.rest_files_folded) == (0, 0)  # folding is stable

    # November settles: the boundary file goes too, and nothing is lost.
    run_build(cfg, ["ohlcv"], today=date(2023, 12, 15))
    assert names() == {"residual-2023-10.json.gz", "residual-2023-11.json.gz"}
    assert _candles(cfg).height == 31 * 24 + 1


def test_packed_daily_archives_and_rest_build_open_interest(tmp_path):
    cfg = _cfg(tmp_path)
    raw, fam = RawStore(cfg), FAMILIES["open_interest"]
    path = raw.archive_path(fam, SYM, "2023-10")
    raw.add_members(path, {f"d{d}.csv": metrics_csv(SYM, date(2023, 10, d)) for d in (1, 2)})
    live = ms(date(2023, 10, 3))
    row = {"timestamp": live, "sumOpenInterest": "11", "sumOpenInterestValue": "110"}
    raw.write_rest("open_interest", live + 30_000, {SYM: [row]})

    run_build(cfg, ["open_interest"], today=date(2023, 10, 3))
    rows = ParquetStore(cfg).read_all("open_interest")
    assert rows.height == 49 and rows["sum_open_interest"].to_list()[-1] == 11.0

    # The next day is packed into the month: the archive now covers the REST row.
    raw.add_members(path, {"d3.csv": metrics_csv(SYM, date(2023, 10, 3))})
    run_build(cfg, ["open_interest"], today=date(2023, 10, 4))
    rows = ParquetStore(cfg).read_all("open_interest")
    assert rows.height == 72 and set(rows["sum_open_interest"]) == {10.0}


def test_funding_joins_per_symbol_archives_with_the_market_wide_feed(tmp_path):
    cfg, t = _cfg(tmp_path), ms(OCT1)
    _archive(cfg, "2023-10", f"{t},8,0.0001,100".encode(), family="funding")
    responses = {
        "fundingRate": [
            {"symbol": SYM, "fundingTime": t, "fundingRate": "0.9", "markPrice": "1"},
            {"symbol": "ETHUSDT", "fundingTime": t, "fundingRate": "0.0002", "markPrice": "2"},
        ],
        "fundingInfo": [{"symbol": "ETHUSDT", "fundingIntervalHours": 4}],
        "premiumIndex": [
            {"symbol": s, "nextFundingTime": t + 8 * HOUR_MS, "lastFundingRate": "0.0001"}
            for s in (SYM, "ETHUSDT")
        ],
    }
    RawStore(cfg).write_rest("funding", t + 30_000, responses)

    assert not run_build(cfg, ["funding"], today=MID_OCT).errors

    rows = ParquetStore(cfg).read_all("funding")
    assert rows["symbol"].to_list() == [SYM, "ETHUSDT"]
    assert rows["funding_rate"].to_list() == [0.0001, 0.0002]  # BTC from the archive
    snapshot = pl.read_parquet(cfg.parquet_dir / SNAPSHOT_FILE).sort("symbol")
    assert snapshot["interval_hours"].to_list() == [8, 4]  # BTC default, ETH from fundingInfo


def test_large_month_is_parsed_in_worker_processes_with_the_same_result(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    for i in range(6):
        _archive(cfg, "2023-10", klines_csv(OCT1, OCT20), symbol=f"S{i}USDT")
    run_build(cfg, ["ohlcv"], today=MID_OCT, workers=1)
    inline = _candles(cfg)

    monkeypatch.setattr(build_module, "PARALLEL_MIN", 2)
    stats = run_build(cfg, ["ohlcv"], today=MID_OCT, force=True, workers=2).per_family["ohlcv"]
    assert stats.months_built == 1 and _candles(cfg).equals(inline)


def test_periods_recorded_as_absent_build_to_nothing(tmp_path):
    cfg = _cfg(tmp_path)
    raw = RawStore(cfg)
    raw.write_archive(raw.archive_path(FAMILIES["ohlcv"], SYM, "2023-09"))  # never published
    oi = raw.archive_path(FAMILIES["open_interest"], SYM, "2023-10")
    raw.add_members(oi, {"d1.csv": metrics_csv(SYM, OCT1), "d2.csv": b""})

    assert not run_build(cfg, ["ohlcv", "open_interest"], today=MID_OCT).errors
    assert _candles(cfg) is None
    assert ParquetStore(cfg).read_all("open_interest").height == 24

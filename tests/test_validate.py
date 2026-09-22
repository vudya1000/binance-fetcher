"""validate: OHLC rules, the hourly sequence and, on request, 24 candles a day."""

from __future__ import annotations

from datetime import date

import polars as pl

from binance_fetcher.transform import klines
from binance_fetcher.validate import validate_symbol
from tests.rawdata import HOUR_MS, kline, ms

DAY = date(2023, 10, 2)


def _frame(rows: list[list]) -> pl.DataFrame:
    return klines.parse_rest(rows, klines.OHLCV_SCHEMA)


def test_clean_day_passes_every_check():
    report = validate_symbol(_frame([kline(ms(DAY, h)) for h in range(24)]), check_daily=True)
    assert report.ok and report.total_rows == 24


def test_broken_candles_gaps_and_short_days_are_reported():
    rows = [kline(ms(DAY, h)) for h in range(24) if h != 5]  # one hour missing
    rows[0][2] = 0.1  # high below open
    rows[1][6] = rows[1][0] + 10  # wrong duration
    report = validate_symbol(_frame(rows), check_daily=True)
    assert report.invalid_rows == 2
    assert report.gaps == [(ms(DAY, 4), ms(DAY, 6))]
    assert report.incomplete_days == ["2023-10-02: 23 candles"]
    assert not report.ok

    without_daily = validate_symbol(_frame(rows[:-1] + [kline(ms(DAY, 23) + HOUR_MS)]))
    assert without_daily.incomplete_days == [] and len(without_daily.gaps) == 2

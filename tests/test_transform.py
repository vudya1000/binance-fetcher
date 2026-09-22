"""Parsers: Vision CSV and REST JSON produce the same frame under one schema per family."""

from __future__ import annotations

import polars as pl

from binance_fetcher.transform import klines
from binance_fetcher.transform.funding import parse_api_funding, parse_vision_funding_csv
from tests.rawdata import HOUR_MS, kline

T0 = 1_700_000_000_000 - 1_700_000_000_000 % HOUR_MS
HEADER = (
    b"open_time,open,high,low,close,volume,close_time,quote_volume,count,"
    b"taker_buy_volume,taker_buy_quote_volume,ignore\n"
)


def _csv(rows: list[list]) -> bytes:
    return "\n".join(",".join(map(str, r)) for r in rows).encode()


def test_klines_parse_the_same_with_and_without_a_header_row():
    rows = [kline(T0), kline(T0 + HOUR_MS, 7.5)]
    rest = klines.parse_rest(rows, klines.OHLCV_SCHEMA)
    assert rest.schema == pl.Schema(klines.OHLCV_SCHEMA)
    assert rest.equals(klines.parse_vision(_csv(rows), klines.OHLCV_SCHEMA))
    assert rest.equals(klines.parse_vision(HEADER + _csv(rows), klines.OHLCV_SCHEMA))
    assert rest["volume"].to_list() == [100.0, 7.5]


def test_mark_price_schema_keeps_the_count_as_update_count():
    frame = klines.parse_rest([kline(T0)], klines.MARK_PRICE_SCHEMA)
    assert frame.columns == list(klines.MARK_PRICE_SCHEMA)
    assert frame["update_count"].to_list() == [5]


def test_empty_input_gives_an_empty_frame_with_the_schema():
    for parser, arg in (
        (klines.parse_vision, b""),
        (klines.parse_rest, []),
    ):
        frame = parser(arg, klines.OHLCV_SCHEMA)
        assert frame.height == 0 and frame.schema == pl.Schema(klines.OHLCV_SCHEMA)


def test_funding_mark_price_is_null_where_the_source_lacks_it():
    old_layout = parse_vision_funding_csv(f"{T0},8,0.0001\n".encode())
    new_layout = parse_vision_funding_csv(f"calc_time,h,rate,mark\n{T0},8,0.0001,100\n".encode())
    assert old_layout["mark_price"].to_list() == [None]
    assert new_layout["mark_price"].to_list() == [100.0]
    rest = parse_api_funding([{"symbol": "X", "fundingTime": T0, "fundingRate": "0.0001"}])
    assert rest.row(0) == ("X", T0, 0.0001, None)

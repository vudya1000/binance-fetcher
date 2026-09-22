"""Kline parsers: OHLCV, mark-price and premium-index candles.

A Vision CSV row and a REST JSON row carry the same twelve fields in the same
order, so one parser pair serves every kline family; the family's schema says
which fields it keeps and under what name.
"""

from __future__ import annotations

import io

import polars as pl

# The twelve fields of one candle, in the order both sources serve them.
_FIELDS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
]

OHLCV_SCHEMA = {
    "open_time": pl.Int64,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "close_time": pl.Int64,
    "quote_volume": pl.Float64,
    "trade_count": pl.Int32,
    "taker_buy_volume": pl.Float64,
    "taker_buy_quote_volume": pl.Float64,
}

# Mark-price and premium-index klines report no volume (every volume field is
# 0); ``count`` is the number of price updates inside the interval.
MARK_PRICE_SCHEMA = {
    "open_time": pl.Int64,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "close_time": pl.Int64,
    "update_count": pl.Int32,
}

_SOURCE = {"trade_count": "count", "update_count": "count"}  # renamed on the way in


def _select(df: pl.DataFrame, schema: dict) -> pl.DataFrame:
    return df.select(
        pl.col(_SOURCE.get(name, name)).cast(dtype).alias(name) for name, dtype in schema.items()
    )


def parse_vision(csv_bytes: bytes, schema: dict) -> pl.DataFrame:
    """One Vision klines CSV. Newer archives carry a header row, older ones do not."""
    if not csv_bytes.strip():
        return pl.DataFrame(schema=schema)
    df = pl.read_csv(
        io.BytesIO(csv_bytes),
        has_header=csv_bytes.startswith(b"open_time"),
        new_columns=_FIELDS,
        infer_schema_length=0,  # everything as text, cast by the schema below
    )
    return _select(df, schema)


def parse_rest(rows: list[list], schema: dict) -> pl.DataFrame:
    """REST kline rows (``/fapi/v1/{klines,markPriceKlines,premiumIndexKlines}``), order kept."""
    return _select(pl.DataFrame(rows, orient="row", schema=_FIELDS), schema)

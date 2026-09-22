"""Open interest: hourly snapshots, one row per symbol and hour.

Live rows come from REST ``/futures/data/openInterestHist``, which Binance
retains for ~30 days; history comes from the Vision daily ``metrics`` archive.
Both report the open interest standing at an instant, so an hour's row means
the same thing whichever source wrote it.
"""

from __future__ import annotations

import io

import polars as pl

OPEN_INTEREST_SCHEMA = {
    "timestamp": pl.Int64,  # hour boundary, ms
    "sum_open_interest": pl.Float64,  # in contracts (base asset)
    "sum_open_interest_value": pl.Float64,  # notional, USDT
}


def _cast(df: pl.DataFrame) -> pl.DataFrame:
    # Binance leaves a value empty now and then: that is a null, not an error.
    return df.select(
        pl.col(name).cast(dtype, strict=False) for name, dtype in OPEN_INTEREST_SCHEMA.items()
    )


def parse_api_open_interest(rows: list[dict]) -> pl.DataFrame:
    """REST openInterestHist rows (period=1h), order kept."""
    df = pl.DataFrame(
        [[r["timestamp"], r.get("sumOpenInterest"), r.get("sumOpenInterestValue")] for r in rows],
        orient="row",
        schema=list(OPEN_INTEREST_SCHEMA),
    )
    return _cast(df)


def parse_vision_metrics_csv(csv_bytes: bytes) -> pl.DataFrame:
    """One Vision daily metrics CSV, reduced to hourly open-interest rows.

    The file holds 5-minute snapshots: create_time ("YYYY-MM-DD HH:MM:SS",
    UTC), symbol, sum_open_interest, sum_open_interest_value, then long/short
    and taker ratios that are not collected. Only the HH:00:00 snapshot of each
    hour is kept, which is the instant the REST endpoint reports for period=1h.
    """
    if not csv_bytes.strip():
        return pl.DataFrame(schema=OPEN_INTEREST_SCHEMA)
    df = pl.read_csv(
        io.BytesIO(csv_bytes),
        has_header=csv_bytes.startswith(b"create_time"),
        columns=[0, 2, 3],
        new_columns=["create_time", "sum_open_interest", "sum_open_interest_value"],
        infer_schema_length=0,
    )
    hourly = df.filter(pl.col("create_time").str.slice(14) == "00:00").with_columns(
        pl.col("create_time")
        .str.strptime(pl.Datetime("ms"), "%Y-%m-%d %H:%M:%S")
        .dt.epoch("ms")
        .alias("timestamp")
    )
    return _cast(hourly).unique("timestamp", keep="last").sort("timestamp")

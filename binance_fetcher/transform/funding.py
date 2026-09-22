"""Funding settlements: one row per symbol and settlement time."""

from __future__ import annotations

import io

import polars as pl

FUNDING_SCHEMA = {
    "symbol": pl.String,
    "funding_time": pl.Int64,
    "funding_rate": pl.Float64,
    "mark_price": pl.Float64,  # null where the source did not report it
}

# Vision fundingRate CSV columns; the last one exists only in newer files.
_VISION_FIELDS = ["calc_time", "funding_interval_hours", "last_funding_rate", "mark_price"]


def parse_vision_funding_csv(csv_bytes: bytes) -> pl.DataFrame:
    """One Vision fundingRate CSV. The file carries no symbol; ``build`` adds it."""
    schema = {name: dtype for name, dtype in FUNDING_SCHEMA.items() if name != "symbol"}
    if not csv_bytes.strip():
        return pl.DataFrame(schema=schema)
    width = csv_bytes.split(b"\n", 1)[0].count(b",") + 1
    df = pl.read_csv(
        io.BytesIO(csv_bytes),
        has_header=csv_bytes.startswith(b"calc_time"),
        new_columns=_VISION_FIELDS[:width],
        infer_schema_length=0,
    )
    if "mark_price" not in df.columns:
        df = df.with_columns(pl.lit(None).alias("mark_price"))
    return df.select(
        pl.col("calc_time").cast(pl.Int64).alias("funding_time"),
        pl.col("last_funding_rate").cast(pl.Float64).alias("funding_rate"),
        pl.col("mark_price").cast(pl.Float64, strict=False),
    )


def parse_api_funding(rows: list[dict]) -> pl.DataFrame:
    """REST ``/fapi/v1/fundingRate`` rows, order kept."""
    df = pl.DataFrame(
        [[r["symbol"], r["fundingTime"], r["fundingRate"], r.get("markPrice")] for r in rows],
        orient="row",
        schema=list(FUNDING_SCHEMA),
    )
    return df.select(
        pl.col("symbol").cast(pl.String),
        pl.col("funding_time").cast(pl.Int64),
        pl.col("funding_rate").cast(pl.Float64),
        pl.col("mark_price").cast(pl.Float64, strict=False),
    )

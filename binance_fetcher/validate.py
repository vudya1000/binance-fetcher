"""Structural checks on built OHLCV candles: OHLC consistency, candle duration,
the hourly sequence and, on request, 24 candles per day."""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

HOUR_MS = 3_600_000
EXPECTED_DURATION_MS = 3_599_999


@dataclass
class ValidationReport:
    total_rows: int = 0
    invalid_rows: int = 0
    gaps: list[tuple[int, int]] = field(default_factory=list)  # (open_time before, after)
    incomplete_days: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.invalid_rows == 0 and not self.gaps and not self.incomplete_days


def invalid_candles(df: pl.DataFrame) -> pl.DataFrame:
    """Rows that break the candle rules: OHLC ordering, non-negative volume, 1h duration."""
    return df.filter(
        (pl.col("high") < pl.max_horizontal("open", "close"))
        | (pl.col("low") > pl.min_horizontal("open", "close"))
        | (pl.col("high") < pl.col("low"))
        | (pl.col("volume") < 0)
        | ((pl.col("close_time") - pl.col("open_time")) != EXPECTED_DURATION_MS)
    )


def gaps(df: pl.DataFrame) -> list[tuple[int, int]]:
    """Consecutive open_times that are not exactly one hour apart."""
    times = df["open_time"].sort()
    before = times.shift(1)
    broken = (times - before) != HOUR_MS  # null on the first row, which filter drops
    return list(zip(before.filter(broken), times.filter(broken), strict=True))


def incomplete_days(df: pl.DataFrame) -> list[str]:
    """Days without exactly 24 candles, as ``YYYY-MM-DD: n candles``."""
    per_day = (
        df.group_by(pl.from_epoch("open_time", time_unit="ms").dt.date().alias("date"))
        .len()
        .filter(pl.col("len") != 24)
        .sort("date")
    )
    return [f"{day}: {n} candles" for day, n in per_day.iter_rows()]


def validate_symbol(df: pl.DataFrame, check_daily: bool = False) -> ValidationReport:
    """Run the structural checks on one symbol's OHLCV rows."""
    return ValidationReport(
        total_rows=df.height,
        invalid_rows=invalid_candles(df).height,
        gaps=gaps(df),
        incomplete_days=incomplete_days(df) if check_daily else [],
    )

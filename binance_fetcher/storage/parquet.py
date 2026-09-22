"""The derived layer: ``data/parquet/<category>/<YYYY-MM>.parquet``.

One partition per category and month, all symbols, unique on
(symbol, time key) and sorted by it. Only ``build`` writes here, and each
partition records a fingerprint of the raw files it was built from, so an
unchanged month is never rebuilt. Consumers read the files as they are.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from binance_fetcher.config import Config
from binance_fetcher.families import FAMILIES
from binance_fetcher.storage.atomic import atomic_path

ZSTD_LEVEL = 3
_INPUTS_KEY = "binance_fetcher.inputs"


def write_parquet_atomic(path: Path, df: pl.DataFrame, inputs: str | None = None) -> None:
    metadata = {_INPUTS_KEY: inputs} if inputs is not None else None
    with atomic_path(path) as tmp:
        df.write_parquet(tmp, compression="zstd", compression_level=ZSTD_LEVEL, metadata=metadata)


class ParquetStore:
    def __init__(self, config: Config):
        self.root = config.parquet_dir

    def path(self, cat: str, month: str) -> Path:
        return self.root / cat / f"{month}.parquet"

    def files(self, cat: str) -> list[Path]:
        return sorted((self.root / cat).glob("*.parquet"))

    def inputs(self, cat: str, month: str) -> str | None:
        """Fingerprint of the raw files the partition was built from."""
        path = self.path(cat, month)
        return pl.read_parquet_metadata(path).get(_INPUTS_KEY) if path.exists() else None

    def write_month(self, cat: str, month: str, df: pl.DataFrame, inputs: str) -> None:
        write_parquet_atomic(self.path(cat, month), df, inputs)

    def read_all(self, cat: str) -> pl.DataFrame | None:
        files = self.files(cat)
        return pl.concat([pl.read_parquet(f) for f in files]) if files else None

    def last_times(self, cat: str) -> dict[str, int]:
        """``{symbol: newest stored time key}``; where an update resumes from."""
        files = self.files(cat)
        if not files:
            return {}
        col = FAMILIES[cat].time_col
        rows = pl.scan_parquet(files).group_by("symbol").agg(pl.col(col).max()).collect()
        return dict(rows.iter_rows())

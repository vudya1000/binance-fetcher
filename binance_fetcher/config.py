from __future__ import annotations

import dataclasses
import os
import tomllib
import typing
from pathlib import Path


@dataclasses.dataclass(frozen=True)
class Config:
    data_dir: Path = Path("./data")
    log_level: str = "INFO"

    # Binance endpoints
    base_url: str = "https://fapi.binance.com"
    vision_base_url: str = "https://data.binance.vision"

    # Backfill
    start_month: str = "2023-01"
    download_workers: int = 6
    include_mark_price: bool = True
    include_premium_index: bool = True

    # Rate limiting: one budget per Binance limit pool, each set below the
    # server's per-IP limit (2400 / 1000 / 500) to leave headroom for clock
    # skew and other processes on the same IP.
    max_concurrent: int = 15
    weight_per_minute: int = 2000  # /fapi/v1 request weight
    futures_data_per_5min: int = 900  # /futures/data/* requests
    funding_per_5min: int = 450  # fundingRate + fundingInfo requests

    @property
    def meta_dir(self) -> Path:
        return self.data_dir / "meta"

    @property
    def raw_dir(self) -> Path:
        """What the sources served, as received: Vision archives and REST responses."""
        return self.data_dir / "raw"

    @property
    def parquet_dir(self) -> Path:
        """Derived from raw by ``build``: data/parquet/<category>/<YYYY-MM>.parquet."""
        return self.data_dir / "parquet"


ENV_PREFIX = "FETCHER_"


def _coerce(typ: type, value: object) -> object:
    if typ is bool and isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return typ(value)


def load_config(path: Path | None = None) -> Config:
    """Every ``Config`` field can be set in any section of the TOML file, and
    overridden by ``FETCHER_<FIELD>`` in the environment. An unknown key is an error."""
    fields = typing.get_type_hints(Config)
    values: dict[str, object] = {}

    path = path or Path("config.toml")
    if path.exists():
        with open(path, "rb") as f:
            sections = tomllib.load(f)
        for section, entries in sections.items():
            for key, value in entries.items():
                if key not in fields:
                    raise ValueError(f"{path}: unknown key {key!r} in [{section}]")
                values[key] = _coerce(fields[key], value)

    for key, typ in fields.items():
        env = os.environ.get(f"{ENV_PREFIX}{key.upper()}")
        if env is not None:
            values[key] = _coerce(typ, env)

    cfg = Config(**values)  # type: ignore[arg-type]
    # Storage creates its own paths lazily; only the state directory is needed up front.
    cfg.meta_dir.mkdir(parents=True, exist_ok=True)
    return cfg

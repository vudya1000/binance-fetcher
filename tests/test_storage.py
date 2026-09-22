"""The two stores: raw files as received, Parquet partitions with a fingerprint."""

from __future__ import annotations

import hashlib
from datetime import date

import polars as pl

from binance_fetcher.config import Config
from binance_fetcher.families import FAMILIES
from binance_fetcher.storage.parquet import ParquetStore
from binance_fetcher.storage.raw import RawStore
from tests.rawdata import metrics_csv, zipped

OI = FAMILIES["open_interest"]
OHLCV = FAMILIES["ohlcv"]


def _raw(tmp_path) -> RawStore:
    return RawStore(Config(data_dir=tmp_path / "data"))


def test_archive_is_stored_byte_for_byte_with_its_checksum(tmp_path):
    raw = _raw(tmp_path)
    zip_bytes = zipped("BTCUSDT-1h-2023-10.csv", b"1,2,3")
    path = raw.archive_path(OHLCV, "BTCUSDT", "2023-10")
    raw.write_archive(path, zip_bytes)

    assert path.name == "BTCUSDT-1h-2023-10.zip" and path.read_bytes() == zip_bytes
    sidecar = path.with_name(path.name + ".CHECKSUM").read_text()
    assert sidecar == f"{hashlib.sha256(zip_bytes).hexdigest()}  {path.name}\n"
    assert raw.months(OHLCV, "BTCUSDT") == {"2023-10"}
    assert raw.read_archive(path) == [b"1,2,3"]


def test_daily_files_are_packed_into_the_month(tmp_path):
    raw = _raw(tmp_path)
    path = raw.archive_path(OI, "BTCUSDT", "2023-10")
    d1, d2 = metrics_csv("BTCUSDT", date(2023, 10, 1)), metrics_csv("BTCUSDT", date(2023, 10, 2))

    raw.add_members(path, {"BTCUSDT-metrics-2023-10-02.csv": d2})
    first_stamp = raw.archives(OI)["2023-10"][0].stamp
    raw.add_members(path, {"BTCUSDT-metrics-2023-10-01.csv": d1})

    assert raw.members(path) == ["BTCUSDT-metrics-2023-10-01.csv", "BTCUSDT-metrics-2023-10-02.csv"]
    assert raw.read_archive(path) == [d1, d2]  # the CSV bytes are untouched
    [archive] = raw.archives(OI)["2023-10"]
    assert archive.symbol == "BTCUSDT" and archive.stamp != first_stamp  # a new day changes it


def test_packing_is_deterministic(tmp_path):
    a, b = _raw(tmp_path / "a"), _raw(tmp_path / "b")
    members = {f"X-metrics-2023-10-0{d}.csv": metrics_csv("X", date(2023, 10, d)) for d in (1, 2)}
    pa_, pb = a.archive_path(OI, "X", "2023-10"), b.archive_path(OI, "X", "2023-10")
    a.add_members(pa_, members)
    for name, content in reversed(members.items()):  # other order, one at a time
        b.add_members(pb, {name: content})
    assert pa_.read_bytes() == pb.read_bytes()


def test_rest_run_round_trips(tmp_path):
    raw = _raw(tmp_path)
    path = raw.write_rest("ohlcv", 1_700_000_000_000, {"BTCUSDT": [[1, "2"]]})
    assert path.name == "1700000000000.json.gz" and raw.rest_files("ohlcv") == [path]
    assert raw.read_rest(path) == {
        "fetched_at": 1_700_000_000_000,
        "responses": {"BTCUSDT": [[1, "2"]]},
    }


def test_partition_remembers_its_inputs_and_reports_last_times(tmp_path):
    store = ParquetStore(Config(data_dir=tmp_path / "data"))
    assert store.inputs("ohlcv", "2023-10") is None and store.last_times("ohlcv") == {}

    rows = pl.DataFrame(
        {"symbol": ["A", "A", "B"], "open_time": [1, 2, 5], "close": [1.0, 2.0, 3.0]}
    )
    store.write_month("ohlcv", "2023-10", rows, inputs="abc")

    assert store.inputs("ohlcv", "2023-10") == "abc"
    assert store.last_times("ohlcv") == {"A": 2, "B": 5}
    assert store.read_all("ohlcv").equals(rows)

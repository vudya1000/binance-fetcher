"""Vision backfill: the plan is what the raw store lacks; reruns fetch nothing stored."""

from __future__ import annotations

import asyncio
import hashlib
from datetime import date, timedelta

import pytest

from binance_fetcher.client.vision import ArchiveError
from binance_fetcher.config import Config
from binance_fetcher.families import FAMILIES, select_families
from binance_fetcher.pipeline import backfill as bf
from binance_fetcher.pipeline.backfill import months_between, parse_day, run_backfill
from binance_fetcher.storage.raw import RawStore
from binance_fetcher.storage.state import load_state, save_state
from tests.rawdata import klines_csv, metrics_csv, ms, zipped

TODAY = date(2023, 11, 21)
SYM = "BTCUSDT"
VISION = "https://data.binance.vision"
OI = FAMILIES["open_interest"]


def _klines_url(month: str) -> str:
    return f"{VISION}/data/futures/um/monthly/klines/{SYM}/1h/{SYM}-1h-{month}.zip"


def _metrics_url(day: date) -> str:
    return f"{VISION}/data/futures/um/daily/metrics/{SYM}/{SYM}-metrics-{day.isoformat()}.zip"


def _cfg(tmp_path, **entry) -> Config:
    cfg = Config(data_dir=tmp_path / "data", start_month="2023-09")
    cfg.meta_dir.mkdir(parents=True)
    entry = {"status": "active", "onboard_date": ms(date(2023, 10, 1))} | entry
    save_state(cfg.meta_dir, {SYM: entry})
    return cfg


def _fake_downloader(monkeypatch, files: dict[str, bytes | Exception]) -> list[str]:
    """Serve ``files`` as Vision; returns the list the downloaded URLs are recorded in."""
    calls: list[str] = []

    async def fake(session, zip_url, semaphore, **kwargs):
        calls.append(zip_url)
        result = files.get(zip_url)
        if isinstance(result, Exception):
            raise result
        return result

    async def fake_checksum(session, zip_url, semaphore):
        content = files.get(zip_url)
        return hashlib.sha256(content).hexdigest() if isinstance(content, bytes) else None

    monkeypatch.setattr(bf, "download_archive", fake)
    monkeypatch.setattr(bf, "fetch_checksum", fake_checksum)
    return calls


def _run(cfg, today: date = TODAY, **kw):
    return asyncio.run(run_backfill(cfg, [SYM], today=today, **kw))


# -- planning helpers ----------------------------------------------------------


def test_parse_day_accepts_month_or_day():
    assert parse_day("2024-02") == date(2024, 2, 1)
    assert parse_day("2024-02", end=True) == date(2024, 2, 29)
    assert parse_day("2024-02-10", end=True) == date(2024, 2, 10)


def test_months_between_covers_every_month_touched():
    assert months_between(date(2023, 11, 30), date(2024, 1, 1)) == ["2023-11", "2023-12", "2024-01"]
    assert months_between(date(2024, 1, 2), date(2024, 1, 1)) == ["2024-01"]


# -- end to end ----------------------------------------------------------------


def test_backfill_stores_what_is_missing_and_reruns_fetch_nothing_stored(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    october = zipped("oct.csv", klines_csv(date(2023, 10, 1), date(2023, 10, 31)))
    day5 = date(2023, 10, 5)
    files = {
        _klines_url("2023-10"): october,
        _metrics_url(day5): zipped("whatever.csv", metrics_csv(SYM, day5)),
    }
    calls = _fake_downloader(monkeypatch, files)

    report = _run(cfg, families=["ohlcv", "open_interest"])

    # OHLCV: September predates the listing, November is unfinished.
    assert [c for c in calls if "/klines/" in c] == [_klines_url("2023-10")]
    raw = RawStore(cfg)
    assert raw.archive_path(FAMILIES["ohlcv"], SYM, "2023-10").read_bytes() == october
    # Metrics: planned from the listing date; an active symbol is still trading,
    # so the plan runs to yesterday even though the OHLCV archives end in October.
    metric_calls = [c for c in calls if "/metrics/" in c]
    assert metric_calls[0] == _metrics_url(date(2023, 10, 1))
    assert metric_calls[-1] == _metrics_url(TODAY - timedelta(days=1))
    stats = report.per_family["open_interest"]
    assert stats.periods_downloaded == 1 and stats.periods_missing == len(metric_calls) - 1
    packed = raw.archive_path(OI, SYM, "2023-10")
    assert raw.members(packed, absent=False) == [f"{SYM}-metrics-2023-10-05.csv"]
    assert metrics_csv(SYM, day5) in raw.read_archive(packed)
    assert not report.errors and load_state(cfg.meta_dir)[SYM]["status"] == "active"

    # Rerun: what is stored is skipped. Days overdue by more than ABSENT_AFTER_DAYS
    # were recorded as absent; only the recent ones may still appear, so only
    # they are asked for again.
    calls.clear()
    _run(cfg, families=["ohlcv", "open_interest"])
    recent = [TODAY - timedelta(days=n) for n in range(bf.ABSENT_AFTER_DAYS, 0, -1)]
    assert calls == [_metrics_url(d) for d in recent]


def test_recheck_replaces_a_reissued_archive_and_asks_again_for_absent_days(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    url, day5 = _klines_url("2023-10"), date(2023, 10, 5)
    first = zipped("oct.csv", klines_csv(date(2023, 10, 1), date(2023, 10, 30)))
    _fake_downloader(monkeypatch, {url: first})
    _run(cfg, families=["ohlcv", "open_interest"], end="2023-10")  # every metrics day absent

    calls = _fake_downloader(monkeypatch, {url: first})
    stats = _run(cfg, families=["ohlcv"], recheck=True).per_family["ohlcv"]
    assert calls == [] and stats.periods_unchanged == 1  # same checksum: nothing downloaded

    reissued = zipped("oct.csv", klines_csv(date(2023, 10, 1), date(2023, 10, 31)))
    late = zipped("d.csv", metrics_csv(SYM, day5))
    calls = _fake_downloader(monkeypatch, {url: reissued, _metrics_url(day5): late})
    report = _run(cfg, families=["ohlcv", "open_interest"], end="2023-10", recheck=True)

    raw = RawStore(cfg)
    assert raw.archive_path(FAMILIES["ohlcv"], SYM, "2023-10").read_bytes() == reissued
    assert report.per_family["ohlcv"].periods_downloaded == 1
    assert len([c for c in calls if "/metrics/" in c]) == 31  # the absent days, asked again
    packed = raw.archive_path(OI, SYM, "2023-10")
    assert raw.members(packed, absent=False) == [f"{SYM}-metrics-2023-10-05.csv"]


def test_month_vision_never_published_is_asked_for_once(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, onboard_date=ms(date(2023, 9, 1)))
    october = zipped("oct.csv", klines_csv(date(2023, 10, 1), date(2023, 10, 31)))
    calls = _fake_downloader(monkeypatch, {_klines_url("2023-10"): october})

    _run(cfg, families=["ohlcv"])
    assert calls == [_klines_url("2023-09"), _klines_url("2023-10")]
    raw = RawStore(cfg)
    assert raw.read_archive(raw.archive_path(FAMILIES["ohlcv"], SYM, "2023-09")) == []

    calls.clear()
    _run(cfg, families=["ohlcv"])
    assert calls == []


def test_failed_download_is_reported_and_retried_next_run(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    url = _klines_url("2023-10")
    _fake_downloader(monkeypatch, {url: ArchiveError("checksum mismatch")})
    report = _run(cfg, families=["ohlcv"])
    assert report.symbols_failed == 1 and "checksum mismatch" in report.errors[0]
    assert RawStore(cfg).months(FAMILIES["ohlcv"], SYM) == set()  # nothing unverified is stored

    calls = _fake_downloader(monkeypatch, {url: zipped("oct.csv", b"")})
    assert _run(cfg, families=["ohlcv"]).symbols_failed == 0 and calls == [url]


def test_nothing_is_asked_for_past_the_delivery_date(tmp_path, monkeypatch):
    last = date(2023, 10, 20)
    cfg = _cfg(tmp_path, status="delisted", delivery_date=ms(last, 9))
    files = {_klines_url("2023-10"): zipped("oct.csv", klines_csv(date(2023, 10, 1), last))}
    calls = _fake_downloader(monkeypatch, files)

    _run(cfg, families=["ohlcv", "open_interest"], today=date(2024, 3, 1))
    assert [c for c in calls if "/klines/" in c] == [_klines_url("2023-10")]  # not Nov..Feb
    assert [c for c in calls if "/metrics/" in c][-1] == _metrics_url(last)


def test_symbol_without_ohlcv_is_skipped_by_other_families(tmp_path, monkeypatch):
    calls = _fake_downloader(monkeypatch, {})
    report = _run(_cfg(tmp_path), families=["funding", "open_interest"])
    assert calls == []
    assert report.per_family["funding"].symbols == 0


def test_start_and_end_narrow_the_window(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    calls = _fake_downloader(monkeypatch, {})
    _run(cfg, families=["ohlcv"], start="2023-06", end="2023-09")
    assert calls == []  # everything before the listing date
    _run(cfg, families=["ohlcv"], end="2023-10-15")
    assert calls == [_klines_url("2023-10")]  # a month the window touches is fetched whole


def test_unknown_family_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="unknown families"):
        _run(_cfg(tmp_path), families=["ohlcv", "trades"])


def test_default_families_follow_config(tmp_path):
    cfg = Config(data_dir=tmp_path / "data", include_mark_price=False)
    names = [f.cat for f in select_families(cfg)]
    assert "mark_price" not in names and names[0] == "ohlcv"
    # an explicit list comes back in registry order, whatever order it was given in
    assert [f.cat for f in select_families(cfg, ["funding", "ohlcv"])] == ["ohlcv", "funding"]

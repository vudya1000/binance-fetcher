"""Vision downloads: only verified bytes come back; "not published" is not an error."""

from __future__ import annotations

import asyncio
import hashlib

import aiohttp
import pytest

from binance_fetcher.client.vision import ArchiveError, archive_url, download_archive
from tests.rawdata import zipped

URL = "https://data.binance.vision/x.zip"
GOOD = zipped("x.csv", b"1,2,3")
CHECKSUM = f"{hashlib.sha256(GOOD).hexdigest()}  x.zip\n".encode()


class _Resp:
    def __init__(self, status=200, body=b""):
        self.status, self._body = status, body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status >= 400:
            raise aiohttp.ClientResponseError(None, (), status=self.status)

    async def read(self):
        return self._body


class _Session:
    """Replays scripted responses in order."""

    def __init__(self, responses):
        self._responses = list(responses)

    def get(self, url):
        return self._responses.pop(0)


def _download(*responses, **kw):
    return asyncio.run(download_archive(_Session(responses), URL, asyncio.Semaphore(2), **kw))


def test_verified_archive_is_returned_as_downloaded():
    assert _download(_Resp(body=GOOD), _Resp(body=CHECKSUM)) == GOOD


def test_unpublished_archive_is_none():
    assert _download(_Resp(status=404)) is None


def test_corrupt_download_is_retried_then_raises():
    bad = [_Resp(body=GOOD[:-5]), _Resp(body=CHECKSUM)]
    assert _download(*bad, _Resp(body=GOOD), _Resp(body=CHECKSUM)) == GOOD

    with pytest.raises(ArchiveError, match="checksum mismatch"):
        _download(*bad, *bad)


def test_without_checksum_the_member_crc_still_guards():
    with pytest.raises(ArchiveError, match="bad ZIP"):
        _download(_Resp(body=b"not a zip"), _Resp(body=b"not a zip"), verify=False)


def test_server_error_is_not_mistaken_for_unpublished():
    with pytest.raises(aiohttp.ClientResponseError):
        _download(_Resp(status=503))


@pytest.mark.parametrize(
    "args, expected",
    [
        (
            ("klines", "BTCUSDT", "2024-05", "1h"),
            "monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2024-05.zip",
        ),
        (
            ("fundingRate", "BTCUSDT", "2024-05"),
            "monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2024-05.zip",
        ),
        (
            ("metrics", "BTCUSDT", "2024-05-17"),
            "daily/metrics/BTCUSDT/BTCUSDT-metrics-2024-05-17.zip",
        ),
    ],
)
def test_archive_url_layouts(args, expected):
    assert archive_url("https://v", *args) == f"https://v/data/futures/um/{expected}"

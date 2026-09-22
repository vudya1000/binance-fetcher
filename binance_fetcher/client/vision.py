"""Binance Vision bulk archives: URL layout and verified downloads."""

from __future__ import annotations

import asyncio
import hashlib
import io
import logging
import zipfile

import aiohttp

logger = logging.getLogger(__name__)


class ArchiveError(Exception):
    """Vision serves the archive, but it could not be fetched intact."""


async def download_bytes(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
) -> bytes | None:
    """The body at ``url``; None on 404. Any other failure raises, so a network
    error is never mistaken for "not published"."""
    async with semaphore:
        async with session.get(url) as resp:
            if resp.status == 404:
                return None
            resp.raise_for_status()
            return await resp.read()


def archive_url(
    base_url: str, data_type: str, symbol: str, period: str, interval: str | None = None
) -> str:
    """URL of one USD-M futures archive ZIP.

    ``period`` selects the layout: ``YYYY-MM`` is a monthly archive,
    ``YYYY-MM-DD`` a daily one. Kline-like data types (klines, markPriceKlines,
    premiumIndexKlines) are additionally keyed by ``interval``; fundingRate and
    metrics are not.
    """
    freq = "monthly" if len(period) == 7 else "daily"
    root = f"{base_url}/data/futures/um/{freq}/{data_type}/{symbol}"
    if interval:
        return f"{root}/{interval}/{symbol}-{interval}-{period}.zip"
    return f"{root}/{symbol}-{data_type}-{period}.zip"


async def fetch_checksum(
    session: aiohttp.ClientSession, zip_url: str, semaphore: asyncio.Semaphore
) -> str | None:
    """The SHA-256 Vision publishes next to an archive; None if there is none."""
    body = await download_bytes(session, zip_url + ".CHECKSUM", semaphore)
    # Checksum file format: "<hash>  <filename>\n"
    return body.decode().split()[0].lower() if body else None


def _zip_ok(zip_bytes: bytes) -> bool:
    """Readable, non-empty, and every member passes its CRC-32."""
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            return bool(zf.namelist()) and zf.testzip() is None
    except zipfile.BadZipFile:
        return False


async def download_archive(
    session: aiohttp.ClientSession,
    zip_url: str,
    semaphore: asyncio.Semaphore,
    verify: bool = True,
    retries: int = 1,
) -> bytes | None:
    """Download a ZIP and return its bytes once verified; None if Vision has
    not published it.

    ``verify`` checks the ZIP against its published ``.CHECKSUM`` (SHA-256). It
    costs a second request per archive, so callers fetching very many tiny
    files may turn it off and rely on the CRC-32 of the members, which is
    always checked. A corrupt download is retried, then raises ArchiveError:
    only bytes that passed verification ever reach the raw store.
    """
    for attempt in range(1 + retries):
        zip_bytes = await download_bytes(session, zip_url, semaphore)
        if zip_bytes is None:
            return None

        problem = None
        if verify:
            checksum = await fetch_checksum(session, zip_url, semaphore)
            if checksum is not None and hashlib.sha256(zip_bytes).hexdigest() != checksum:
                problem = "checksum mismatch"
        if problem is None and not _zip_ok(zip_bytes):
            problem = "bad ZIP"
        if problem is None:
            return zip_bytes
        logger.warning("%s for %s (attempt %d/%d)", problem, zip_url, attempt + 1, 1 + retries)

    raise ArchiveError(f"{zip_url}: {problem} after {1 + retries} attempts")

"""Builders for raw-store fixtures shared by the tests."""

from __future__ import annotations

import io
import zipfile
from datetime import UTC, date, datetime

HOUR_MS = 3_600_000


def ms(day: date, hour: int = 0) -> int:
    return int(datetime(day.year, day.month, day.day, hour, tzinfo=UTC).timestamp() * 1000)


def kline(open_time: int, volume: float = 100.0) -> list:
    """One REST kline row; Vision CSV rows carry the same fields."""
    return [open_time, 1, 2, 0.5, 1.5, volume, open_time + HOUR_MS - 1, 10, 5, 4, 3, 0]


def klines_csv(first: date, last: date, skip: set[int] = frozenset()) -> bytes:
    times = range(ms(first), ms(last, 23) + 1, HOUR_MS)
    return "\n".join(",".join(map(str, kline(t))) for t in times if t not in skip).encode()


def metrics_csv(symbol: str, day: date) -> bytes:
    lines = ["create_time,symbol,oi,oi_value,top_acct,top_pos,global_acct,taker"]
    for h in range(24):
        for minute in (0, 5):  # only the :00 snapshot is kept
            lines.append(
                f"{day.isoformat()} {h:02d}:{minute:02d}:00,{symbol},10,100,1.5,2.0,1.0,0.9"
            )
    return "\n".join(lines).encode()


def zipped(name: str, content: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(name, content)
    return buf.getvalue()

"""Client-side rate limiting for the Binance REST API.

Binance meters an IP through several independent pools (see the pool names in ``rest``):
request *weight* per minute for most endpoints, and plain request counts per
five minutes for two endpoint groups that carry no weight at all. Each pool is
a sliding-window log, which guarantees the budget holds inside *any* window of
that length. A token bucket does not: it admits a full burst and then refills,
so up to twice the limit can land inside one of the server's windows.

On top of the local accounting the throttle obeys the server: a reported
used-weight at or above budget, or a 429, blocks every request until the
server's window has moved on.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable

logger = logging.getLogger(__name__)


class SlidingWindow:
    """At most ``limit`` cost units inside any ``window_sec`` seconds."""

    def __init__(
        self, limit: int, window_sec: float, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self.limit = limit
        self.window_sec = window_sec
        self._clock = clock
        self._log: deque[tuple[float, int]] = deque()  # (time, cost), oldest first
        self._used = 0

    def reserve(self, cost: int) -> float:
        """Take ``cost`` units if they fit now and return 0.0; otherwise take
        nothing and return the seconds until they would fit."""
        if cost > self.limit:
            raise ValueError(f"cost {cost} exceeds the pool limit {self.limit}")
        now = self._clock()
        while self._log and self._log[0][0] <= now - self.window_sec:
            self._used -= self._log.popleft()[1]

        if self._used + cost <= self.limit:
            self._log.append((now, cost))
            self._used += cost
            return 0.0

        # Wait for the oldest entries to expire until enough budget is free.
        excess = self._used + cost - self.limit
        for at, spent in self._log:
            excess -= spent
            if excess <= 0:
                return at + self.window_sec - now
        raise AssertionError("unreachable: cost <= limit")


class Throttle:
    """Per-pool budgets, a cap on in-flight requests, and server-driven blocks."""

    def __init__(self, pools: dict[str, SlidingWindow], max_concurrent: int) -> None:
        self._pools = pools
        self._in_flight = asyncio.Semaphore(max_concurrent)
        self._blocked_until = 0.0  # monotonic

    def block_for(self, seconds: float, reason: str) -> None:
        """Hold every request for ``seconds``; limits are per IP, not per endpoint."""
        until = time.monotonic() + seconds
        if until > self._blocked_until:
            self._blocked_until = until
            logger.warning("Throttle blocked for %.1fs: %s", seconds, reason)

    def observe_used_weight(self, used: int) -> None:
        """Feed the server's ``X-MBX-USED-WEIGHT-1m`` value.

        It counts everything sent from this IP, including other processes. The
        server's weight window resets on the minute, so once the budget is
        spent the only useful move is to wait for the next minute.
        """
        if used >= self._pools["weight"].limit:
            to_next_minute = 60 - time.time() % 60
            self.block_for(to_next_minute + 1, f"server reports used weight {used}")

    async def acquire(self, pool: str, cost: int) -> None:
        """Wait for budget, then for an in-flight slot. Pair with ``release``.

        The slot is taken last so that requests waiting for budget do not
        occupy the connections that running requests need.
        """
        while True:
            wait = self._blocked_until - time.monotonic()
            if wait <= 0:
                wait = self._pools[pool].reserve(cost)
                if wait == 0:
                    break
            await asyncio.sleep(wait)
        await self._in_flight.acquire()

    def release(self) -> None:
        self._in_flight.release()

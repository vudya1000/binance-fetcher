from __future__ import annotations

import logging

from binance_fetcher.client.rest import BinanceClient
from binance_fetcher.config import Config
from binance_fetcher.storage.state import load_state, save_state

logger = logging.getLogger(__name__)


_DELISTED = ("SETTLING", "CLOSE", "DELISTED")
_NO_DELIVERY = 4_000_000_000_000  # a perpetual with no delisting scheduled reports the year 2100


async def run_discover(
    config: Config, client: BinanceClient
) -> tuple[list[str], list[str], list[str]]:
    """Discover perpetual futures symbols. Not a job of its own: ``update`` and
    ``backfill`` run it first, on their own client, to learn what to fetch.

    Returns (new, removed, tracked): the last is every symbol with a status,
    active or delisted, which is what a backfill covers.
    """
    info = await client.fetch_exchange_info()
    state = load_state(config.meta_dir)

    # Status follows exchangeInfo, not presence in the state file: a delisted
    # symbol can be listed again.
    perpetuals = [s for s in info.get("symbols", []) if s.get("contractType") == "PERPETUAL"]
    active = {s["symbol"] for s in perpetuals if s.get("status") == "TRADING"}
    new_symbols = sorted(s for s in active if state.get(s, {}).get("status") != "active")
    removed = sorted(s for s, v in state.items() if v.get("status") == "active" and s not in active)

    for sym in new_symbols:
        state.setdefault(sym, {})["status"] = "active"
        logger.info("New symbol discovered: %s", sym)

    # Delisted symbols keep their data; they just stop being polled.
    for sym in removed:
        state[sym]["status"] = "delisted"
        logger.info("Symbol delisted: %s", sym)

    # Perpetuals delisted before the first discover are tracked for their history.
    for s in perpetuals:
        if s.get("status") in _DELISTED:
            state.setdefault(s["symbol"], {}).setdefault("status", "delisted")

    # Listing and delivery times bound the backfill plan: nothing exists before
    # the one or after the other. A relisted symbol loses its delivery date.
    for s in info.get("symbols", []):
        entry = state.get(s["symbol"])
        if entry is None:
            continue
        if s.get("onboardDate"):
            entry["onboard_date"] = int(s["onboardDate"])
        if 0 < int(s.get("deliveryDate") or 0) < _NO_DELIVERY:
            entry["delivery_date"] = int(s["deliveryDate"])
        else:
            entry.pop("delivery_date", None)

    save_state(config.meta_dir, state)
    tracked = sorted(s for s, v in state.items() if "status" in v)

    logger.info(
        "Discovery: %d active, %d new, %d removed",
        len(active),
        len(new_symbols),
        len(removed),
    )
    return new_symbols, removed, tracked

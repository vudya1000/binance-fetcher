"""symbol_state.json: what discovery knows about each symbol.

``{symbol: {"status": "active" | "delisted", "onboard_date": ms, "delivery_date": ms}}``
(the last only once a delisting is scheduled). Discovery
is the only writer and rewrites the file from exchangeInfo, so two jobs saving
at once lose nothing that the next run does not restore. Where each family's
data ends is read from the stored data, not kept here.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from binance_fetcher.storage.atomic import atomic_path


def _state_path(meta_dir: Path) -> Path:
    return meta_dir / "symbol_state.json"


def load_state(meta_dir: Path) -> dict[str, dict[str, Any]]:
    path = _state_path(meta_dir)
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def save_state(meta_dir: Path, state: dict[str, dict[str, Any]]) -> None:
    with atomic_path(_state_path(meta_dir)) as tmp:
        tmp.write_text(json.dumps(state, indent=2))


def get_active_symbols(meta_dir: Path) -> list[str]:
    return [s for s, v in load_state(meta_dir).items() if v.get("status") == "active"]

"""The raw layer: what the sources served, stored as received.

  raw/vision/<data_type>/<SYMBOL>/<name>-<YYYY-MM>.zip (+ .CHECKSUM)
      One archive per family, symbol and month. Monthly Vision archives are
      stored byte for byte. A family Vision publishes by day is packed: each
      day's CSV becomes one member of the month's ZIP, bytes untouched, so the
      member list says which days are there. The sidecar holds the file's
      SHA-256 in Vision's own format; for a Vision monthly archive it is the
      published checksum, which the download was verified against. A period
      Vision never published is recorded as an archive, or a member, with
      nothing in it, so that it is not asked for again.
  raw/rest/<category>/<fetched_at ms>.json.gz
      One file per family per update run: the REST responses as received.
      ``build`` folds the files of a finished month into
      ``residual-<YYYY-MM>.json.gz``, the rows no archive covers.

Fetch jobs only ever add files here and never parse them; every file is written
to a temp name and renamed into place. ``build`` derives the Parquet partitions
from this directory alone.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import zipfile
from dataclasses import dataclass
from pathlib import Path

from binance_fetcher.config import Config
from binance_fetcher.families import Family
from binance_fetcher.storage.atomic import atomic_path

_ZIP_EPOCH = (1980, 1, 1, 0, 0, 0)  # fixed member timestamps: same members, same bytes
_EMPTY_ZIP = b"PK\x05\x06" + bytes(18)  # a ZIP with no members


@dataclass(frozen=True)
class Archive:
    path: Path
    symbol: str
    month: str
    stamp: str  # size and mtime: changes whenever the file is replaced


class RawStore:
    def __init__(self, config: Config):
        self.root = config.raw_dir

    # -- Vision archives -------------------------------------------------------

    def archive_path(self, fam: Family, symbol: str, month: str) -> Path:
        kind = fam.interval or fam.data_type
        return self.root / "vision" / fam.data_type / symbol / f"{symbol}-{kind}-{month}.zip"

    def archives(self, fam: Family) -> dict[str, list[Archive]]:
        """Every stored archive of a family, by month, in symbol order."""
        out: dict[str, list[Archive]] = {}
        root = self.root / "vision" / fam.data_type
        if not root.exists():
            return out
        for sym_dir in sorted(root.iterdir()):
            with os.scandir(sym_dir) as entries:
                for e in entries:
                    if e.name.endswith(".zip"):
                        st = e.stat()
                        month = e.name[-11:-4]
                        out.setdefault(month, []).append(
                            Archive(
                                Path(e.path), sym_dir.name, month, f"{st.st_size}:{st.st_mtime_ns}"
                            )
                        )
        return out

    def months(self, fam: Family, symbol: str) -> set[str]:
        """Months a symbol has an archive for."""
        sym_dir = self.root / "vision" / fam.data_type / symbol
        return {p.name[-11:-4] for p in sym_dir.glob("*.zip")} if sym_dir.exists() else set()

    @staticmethod
    def members(path: Path, absent: bool = True) -> list[str]:
        """Member names; ``absent=False`` leaves out the ones recorded as never published."""
        if not path.exists():
            return []
        with zipfile.ZipFile(path) as zf:
            return [i.filename for i in zf.infolist() if absent or i.file_size]

    @staticmethod
    def read_archive(path: Path) -> list[bytes]:
        """The CSV members: one for a Vision monthly archive, one per day for a packed one."""
        with zipfile.ZipFile(path) as zf:
            return [zf.read(name) for name in sorted(zf.namelist())]

    def write_archive(self, path: Path, zip_bytes: bytes = _EMPTY_ZIP) -> None:
        """Store a verified archive; with no bytes, record that Vision has none."""
        with atomic_path(path) as tmp:
            tmp.write_bytes(zip_bytes)
        self._write_checksum(path, hashlib.sha256(zip_bytes).hexdigest())

    @staticmethod
    def checksum(path: Path) -> str | None:
        sidecar = path.with_name(path.name + ".CHECKSUM")
        return sidecar.read_text().split()[0] if sidecar.exists() else None

    def add_members(self, path: Path, members: dict[str, bytes]) -> None:
        """Pack CSVs into the month's ZIP, keeping the members already there.
        An empty CSV records a day Vision never published."""
        if path.exists():
            with zipfile.ZipFile(path) as zf:
                members = {name: zf.read(name) for name in zf.namelist()} | members
        with atomic_path(path) as tmp:
            with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zf:
                for name in sorted(members):
                    info = zipfile.ZipInfo(name, date_time=_ZIP_EPOCH)
                    info.compress_type = zipfile.ZIP_DEFLATED
                    zf.writestr(info, members[name])
        self._write_checksum(path, hashlib.sha256(path.read_bytes()).hexdigest())

    @staticmethod
    def _write_checksum(path: Path, sha256: str) -> None:
        with atomic_path(path.with_name(path.name + ".CHECKSUM")) as tmp:
            tmp.write_text(f"{sha256}  {path.name}\n")

    # -- REST responses --------------------------------------------------------

    def rest_dir(self, cat: str) -> Path:
        return self.root / "rest" / cat

    def rest_files(self, cat: str) -> list[Path]:
        return sorted(self.rest_dir(cat).glob("*.json.gz"))

    def write_rest(
        self, cat: str, fetched_at: int, responses: dict, name: str | None = None
    ) -> Path:
        """Store one run's responses; ``name`` defaults to the fetch time."""
        path = self.rest_dir(cat) / f"{name or fetched_at}.json.gz"
        doc = {"fetched_at": fetched_at, "responses": responses}
        with atomic_path(path) as tmp:
            tmp.write_bytes(gzip.compress(json.dumps(doc, separators=(",", ":")).encode()))
        return path

    @staticmethod
    def read_rest(path: Path) -> dict:
        return json.loads(gzip.decompress(path.read_bytes()))

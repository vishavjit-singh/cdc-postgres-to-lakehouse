"""Lake layout and the commit protocol.

::

    lake/
      bronze/changelog/table=<t>/date=<yyyy-mm-dd>/part-<batch>.parquet   every record, as delivered
      bronze/dead_letter/part-<batch>.parquet                             records that could not be parsed
      silver/<table>/current.parquet                                     one row per key (soft deletes kept)
      silver/<table>_history/current.parquet                             SCD2: every version with validity
      silver/_schema_changes.json
      _checkpoint.json                                                   offsets + consumer state, atomically

Per micro-batch: (1) bronze files are written under a name derived from the batch's offset ranges;
(2) ``_checkpoint.json`` is replaced atomically with the next offsets *and* the merged state, and lists
the committed batch ids; (3) silver files are re-published from that state. After a crash before (2),
the next run deletes the bronze files of the uncommitted batch and replays it from the previous
checkpoint; after a crash in (3) it simply publishes again. Parquet is used when pyarrow is installed, CSV otherwise.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .state import TableState


def default_format() -> str:
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        return "csv"
    return "parquet"


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def _write_frame(df: pd.DataFrame, path: Path, fmt: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    if fmt == "parquet":
        df.to_parquet(tmp, index=False)
    else:
        df.to_csv(tmp, index=False)
    os.replace(tmp, path)


def read_frame(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, keep_default_na=False, na_values=[""], low_memory=False)


def batch_id(offset_ranges: dict[str, tuple[int, int]]) -> str:
    text = ";".join(f"{k}={a}-{b}" for k, (a, b) in sorted(offset_ranges.items()))
    return hashlib.sha1(text.encode()).hexdigest()[:12]


class Lake:
    def __init__(self, root: str | Path, fmt: str | None = None):
        self.root = Path(root)
        self.fmt = fmt or default_format()

    @property
    def checkpoint_path(self) -> Path:
        return self.root / "_checkpoint.json"

    def load_checkpoint(self) -> dict:
        if not self.checkpoint_path.exists():
            return {"offsets": {}, "batches": [], "state": {}, "stats": {}, "dead_letters": 0}
        return json.loads(self.checkpoint_path.read_text())

    def commit(self, checkpoint: dict) -> None:
        _atomic_write_text(self.checkpoint_path, json.dumps(checkpoint))

    # ------------------------------------------------------------------------------------------ bronze
    def write_bronze(self, batch: str, rows: list[dict], dead: list[dict]) -> int:
        written = 0
        if rows:
            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["source_ts_ms"], unit="ms", utc=True).dt.strftime("%Y-%m-%d")
            for (table, date), part in df.groupby(["table", "date"]):
                folder = self.root / "bronze" / "changelog" / f"table={table}" / f"date={date}"
                path = folder / f"part-{batch}.{self.fmt}"
                _write_frame(part.drop(columns=["date"]), path, self.fmt)
                written += len(part)
        if dead:
            path = self.root / "bronze" / "dead_letter" / f"part-{batch}.{self.fmt}"
            _write_frame(pd.DataFrame(dead), path, self.fmt)
        return written

    def remove_uncommitted(self, committed: set[str]) -> int:
        """Delete bronze files from batches that never reached a checkpoint (a crash between steps 1 and 2)."""
        removed = 0
        for f in (self.root / "bronze").glob("**/part-*.*"):
            if f.name.split("-", 1)[1].split(".", 1)[0] not in committed:
                f.unlink()
                removed += 1
        return removed

    def read_bronze(self) -> pd.DataFrame:
        files = sorted((self.root / "bronze" / "changelog").glob("table=*/date=*/part-*.*"))
        frames = [read_frame(f) for f in files if not f.name.endswith(".tmp")]
        if not frames:
            return pd.DataFrame()
        df = pd.concat(frames, ignore_index=True)
        # bronze is at-least-once by design (redeliveries are kept); one row per delivered Kafka record
        return df.drop_duplicates(subset=["topic", "partition", "offset"]).sort_values("seq").reset_index(drop=True)

    # ------------------------------------------------------------------------------------------ silver
    def publish_silver(self, states: dict[str, TableState]) -> dict[str, int]:
        counts = {}
        changes = []
        for name, st in states.items():
            cols = st.columns + ["_op", "_lsn", "_tx_id", "_source_ts_ms", "_deleted"]
            cur = pd.DataFrame(st.rows(), columns=cols)
            _write_frame(cur, self.root / "silver" / name / f"current.{self.fmt}", self.fmt)
            hist_cols = st.columns + ["_op", "_lsn", "_tx_id", "valid_from_ms", "valid_to_ms", "is_current",
                                      "is_deleted"]
            hist = pd.DataFrame(st.history(), columns=hist_cols)
            _write_frame(hist, self.root / "silver" / f"{name}_history" / f"current.{self.fmt}", self.fmt)
            counts[name] = int((~cur["_deleted"].astype(bool)).sum()) if len(cur) else 0
            changes += st.schema_changes
        _atomic_write_text(self.root / "silver" / "_schema_changes.json", json.dumps(changes, indent=2))
        _atomic_write_text(self.root / "silver" / "_published.json", json.dumps(
            {"published_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "rows": counts}, indent=2))
        return counts

    def read_silver(self, table: str, history: bool = False) -> pd.DataFrame:
        folder = self.root / "silver" / (f"{table}_history" if history else table)
        return read_frame(next(folder.glob("current.*")))

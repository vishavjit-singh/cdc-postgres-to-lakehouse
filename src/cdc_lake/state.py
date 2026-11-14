"""Apply change events to per-table state: current rows (silver) and every version (SCD2 history).

Rules, per primary key:

* An event's position is ``(source.lsn, 0 for snapshot reads else 1)``. Positions only move forward.
* **Duplicate:** an event whose position was already applied for that key (at-least-once redelivery).
  It changes nothing.
* **Late:** an event older than the key's current position (delivered out of order). The current row
  stays as it is, but the version is slotted into the history so point-in-time queries stay correct.
* **Delete:** soft delete. The row is kept with ``_deleted = true`` so downstream merges can propagate
  it, and a later insert of the same key (delete + reinsert, or a primary-key change) revives it.
* **Schema evolution:** a column seen for the first time is added to the table. Older rows read it
  as null, which is exactly what PostgreSQL returns after ``ALTER TABLE … ADD COLUMN``.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from dataclasses import dataclass, field

from .envelope import ChangeEvent, pk_tuple

META_COLUMNS = ["_op", "_lsn", "_tx_id", "_source_ts_ms", "_deleted"]


@dataclass
class TableState:
    name: str
    primary_key: tuple[str, ...]
    columns: list[str] = field(default_factory=list)
    current: dict[tuple, dict] = field(default_factory=dict)       # pk -> {"row", "pos", "op", "deleted", ...}
    versions: dict[tuple, list[dict]] = field(default_factory=dict)
    schema_changes: list[dict] = field(default_factory=list)

    # ------------------------------------------------------------------------------------------ apply
    def _learn_columns(self, row: dict | None, event: ChangeEvent) -> None:
        if not row:
            return
        known = bool(self.columns)
        for col in row:
            if col not in self.columns:
                if known:            # not the very first event: this is a schema change
                    self.schema_changes.append({"table": self.name, "column": col, "first_lsn": event.lsn,
                                                "first_seen_ts_ms": event.source_ts_ms})
                self.columns.append(col)

    def apply(self, event: ChangeEvent) -> str:
        """Return the outcome: ``applied``, ``duplicate`` or ``late``."""
        if event.op == "t":
            return self._truncate(event)
        pk = pk_tuple(event, self.primary_key)
        pos = list(event.position)
        versions = self.versions.setdefault(pk, [])
        if any(v["pos"] == pos for v in versions):
            return "duplicate"
        self._learn_columns(event.after or event.before, event)
        cur = self.current.get(pk)
        if event.op == "d":
            row = event.before or (cur["row"] if cur else None) or dict(event.key)
        else:
            row = event.after
        versions.append({"pos": pos, "op": event.op, "ts_ms": event.source_ts_ms, "tx_id": event.tx_id,
                         "row": row})
        versions.sort(key=lambda v: v["pos"])
        if cur is None or pos > cur["pos"]:
            self.current[pk] = {"row": row, "pos": pos, "op": event.op, "deleted": event.op == "d",
                                "tx_id": event.tx_id, "ts_ms": event.source_ts_ms}
            return "applied"
        return "late"

    def _truncate(self, event: ChangeEvent) -> str:
        pos = list(event.position)
        for pk, cur in self.current.items():
            if not cur["deleted"] and pos > cur["pos"]:
                self.versions[pk].append({"pos": pos, "op": "d", "ts_ms": event.source_ts_ms,
                                          "tx_id": event.tx_id, "row": cur["row"]})
                self.current[pk] = {**cur, "pos": pos, "op": "t", "deleted": True, "ts_ms": event.source_ts_ms}
        return "applied"

    # ------------------------------------------------------------------------------------------ views
    def rows(self, include_deleted: bool = True) -> list[dict]:
        out = []
        for pk in sorted(self.current):
            c = self.current[pk]
            if c["deleted"] and not include_deleted:
                continue
            row = {col: (c["row"] or {}).get(col) for col in self.columns}
            row.update(_op=c["op"], _lsn=c["pos"][0], _tx_id=c["tx_id"], _source_ts_ms=c["ts_ms"],
                       _deleted=c["deleted"])
            out.append(row)
        return out

    def history(self) -> list[dict]:
        out = []
        for pk in sorted(self.versions):
            vs = self.versions[pk]
            for i, v in enumerate(vs):
                nxt = vs[i + 1] if i + 1 < len(vs) else None
                row = {col: (v["row"] or {}).get(col) for col in self.columns}
                row.update(_op=v["op"], _lsn=v["pos"][0], _tx_id=v["tx_id"], valid_from_ms=v["ts_ms"],
                           valid_to_ms=nxt["ts_ms"] if nxt else None, is_current=nxt is None,
                           is_deleted=v["op"] in ("d", "t"))
                out.append(row)
        return out

    def as_of(self, ts_ms: int) -> list[dict]:
        """Point-in-time view: each key's last version at or before ``ts_ms`` that is not a delete."""
        out = []
        for pk in sorted(self.versions):
            past = [v for v in self.versions[pk] if v["ts_ms"] <= ts_ms]
            if past and past[-1]["op"] not in ("d", "t"):
                out.append({col: past[-1]["row"].get(col) for col in self.columns})
        return out

    # ------------------------------------------------------------------------------------------ persistence
    def to_json(self) -> dict:
        return {"name": self.name, "primary_key": list(self.primary_key), "columns": self.columns,
                "current": [[list(k), v] for k, v in self.current.items()],
                "versions": [[list(k), v] for k, v in self.versions.items()],
                "schema_changes": self.schema_changes}

    @classmethod
    def from_json(cls, d: dict) -> TableState:
        return cls(name=d["name"], primary_key=tuple(d["primary_key"]), columns=list(d["columns"]),
                   current={tuple(k): v for k, v in d["current"]},
                   versions={tuple(k): v for k, v in d["versions"]}, schema_changes=list(d["schema_changes"]))


# ---------------------------------------------------------------------------------------------- checksum
_TS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$")


def canonical(value):
    """Normalise representation differences that are not data differences: 5.0 vs 5 after a CSV round trip,
    and ``…:00Z`` vs ``…:00.000000Z`` between Debezium's ZonedTimestamp and a SQL ``to_char``."""
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and _TS.match(value):
        dt = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return value


def table_checksum(rows: list[dict], columns: list[str], primary_key: tuple[str, ...]) -> str:
    """Order-independent SHA-256 over the business columns (missing columns count as null)."""
    lines = sorted(json.dumps([canonical(r.get(c)) for c in columns], default=str) for r in rows)
    h = hashlib.sha256()
    for line in lines:
        h.update(line.encode())
        h.update(b"\n")
    return h.hexdigest()

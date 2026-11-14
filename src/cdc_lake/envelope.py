"""Parse Debezium change events (JSON converter, with or without embedded schemas).

A Kafka record from the Debezium PostgreSQL connector looks like::

    key   = {"id": 42}                                   (or {"schema": ..., "payload": {"id": 42}})
    value = {"before": {...} | null, "after": {...} | null, "op": "c|u|d|r|t",
             "ts_ms": 1730451600123,
             "source": {"connector": "postgresql", "name": "shop", "db": "shop", "schema": "public",
                        "table": "orders", "txId": 771, "lsn": 24023128, "ts_ms": 1730451600100,
                        "snapshot": "false", ...}}
    value = null                                          (tombstone that follows every delete)

Heartbeat records go to their own topic and only carry the source position.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

OPS = {"c": "create", "u": "update", "d": "delete", "r": "read", "t": "truncate"}


class EnvelopeError(ValueError):
    pass


@dataclass(frozen=True)
class Record:
    """A raw Kafka record, as any source (Kafka, replay file, test) hands it to the consumer."""
    topic: str
    partition: int
    offset: int
    key: str | bytes | None
    value: str | bytes | None
    timestamp_ms: int | None = None
    headers: dict = field(default_factory=dict)


@dataclass(frozen=True)
class ChangeEvent:
    table: str
    op: str                       # c, u, d, r (t = truncate)
    key: dict
    before: dict | None
    after: dict | None
    lsn: int
    tx_id: int | None
    source_ts_ms: int
    ts_ms: int
    snapshot: str
    record: Record

    @property
    def position(self) -> tuple[int, int]:
        """Ordering position for one primary key: snapshot reads sort before streamed changes at the same LSN."""
        return (self.lsn, 0 if self.op == "r" else 1)


def _decode(raw: str | bytes | None):
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        data = json.loads(raw)
    except ValueError as exc:
        raise EnvelopeError(f"not JSON: {exc}") from None
    if isinstance(data, dict) and set(data) == {"schema", "payload"}:   # schemas.enable=true
        data = data["payload"]
    return data


def parse(record: Record, table: str, primary_key: tuple[str, ...]) -> ChangeEvent | None:
    """Return a ChangeEvent, or None for a tombstone. Raises EnvelopeError for malformed records."""
    value = _decode(record.value)
    if value is None:
        return None
    if not isinstance(value, dict) or "op" not in value or "source" not in value:
        raise EnvelopeError("missing op/source: not a Debezium change event")
    op = value["op"]
    if op not in OPS:
        raise EnvelopeError(f"unknown op {op!r}")
    src = value["source"]
    before, after = value.get("before"), value.get("after")
    if op in ("c", "r", "u") and after is None:
        raise EnvelopeError(f"op {op} without 'after'")
    if op == "d" and before is None and record.key is None:
        raise EnvelopeError("delete without 'before' or key")
    key = _decode(record.key)
    if not isinstance(key, dict):
        row = after if op != "d" else before
        key = {k: row[k] for k in primary_key} if row else {}
    missing = [k for k in primary_key if k not in key] if op != "t" else []
    if missing:
        raise EnvelopeError(f"key is missing {missing}")
    try:
        lsn = int(src["lsn"])
    except (KeyError, TypeError, ValueError):
        raise EnvelopeError("source.lsn missing") from None
    return ChangeEvent(table=table, op=op, key={k: key[k] for k in primary_key} if op != "t" else {},
                       before=before, after=after, lsn=lsn, tx_id=src.get("txId"),
                       source_ts_ms=int(src.get("ts_ms") or value.get("ts_ms") or 0),
                       ts_ms=int(value.get("ts_ms") or 0), snapshot=str(src.get("snapshot", "false")), record=record)


def pk_tuple(event_or_key, primary_key: tuple[str, ...]) -> tuple:
    key = event_or_key.key if isinstance(event_or_key, ChangeEvent) else event_or_key
    return tuple(key[k] for k in primary_key)

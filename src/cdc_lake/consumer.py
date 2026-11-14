"""The micro-batch consumer: records → bronze change log → merged state → checkpoint → silver."""
from __future__ import annotations

import json
import logging
import time
from collections import Counter

from .config import Settings
from .envelope import EnvelopeError, Record, parse
from .lake import Lake, batch_id
from .sources import Source
from .state import TableState

log = logging.getLogger("cdc_lake")


class SimulatedCrash(RuntimeError):
    """Raised by ``crash_after_bronze`` to test recovery: bronze written, checkpoint not committed."""


class Consumer:
    def __init__(self, cfg: Settings, lake: Lake):
        self.cfg, self.lake = cfg, lake
        cp = lake.load_checkpoint()
        self.offsets: dict[str, int] = dict(cp["offsets"])
        self.batches: list[dict] = list(cp["batches"])
        self.stats: Counter = Counter(cp["stats"])
        self.states = {name: TableState.from_json(cp["state"][name]) if name in cp["state"]
                       else TableState(name, pk) for name, pk in cfg.tables.items()}
        self.seq = int(self.stats.get("records", 0))
        self.removed_uncommitted = lake.remove_uncommitted({b["id"] for b in self.batches})
        if self.removed_uncommitted:
            log.warning("removed %d bronze file(s) from an uncommitted batch", self.removed_uncommitted)
        if cp["state"]:
            lake.publish_silver(self.states)        # finish a publish that a crash may have interrupted

    def _process(self, records: list[Record]) -> tuple[list[dict], list[dict], Counter, dict]:
        rows, dead, counts = [], [], Counter()
        ranges: dict[str, tuple[int, int]] = {}
        max_lsn = 0
        for rec in records:
            tp = f"{rec.topic}:{rec.partition}"
            lo, hi = ranges.get(tp, (rec.offset, rec.offset))
            ranges[tp] = (min(lo, rec.offset), max(hi, rec.offset))
            self.seq += 1
            counts["records"] += 1
            if rec.topic == self.cfg.heartbeat_topic:
                counts["heartbeats"] += 1
                continue
            table = self.cfg.table_for_topic(rec.topic)
            if table is None:
                counts["unknown_topic"] += 1
                continue
            try:
                ev = parse(rec, table, self.cfg.tables[table])
            except EnvelopeError as exc:
                counts["dead_letter"] += 1
                dead.append({"topic": rec.topic, "partition": rec.partition, "offset": rec.offset,
                             "error": str(exc), "value": rec.value if isinstance(rec.value, str) else repr(rec.value)})
                continue
            if ev is None:
                counts["tombstones"] += 1
                continue
            outcome = self.states[table].apply(ev)
            counts[outcome] += 1
            counts[f"op_{ev.op}"] += 1
            max_lsn = max(max_lsn, ev.lsn)
            rows.append({"seq": self.seq, "topic": rec.topic, "partition": rec.partition, "offset": rec.offset,
                         "table": table, "op": ev.op, "outcome": outcome, "lsn": ev.lsn, "tx_id": ev.tx_id,
                         "source_ts_ms": ev.source_ts_ms, "ts_ms": ev.ts_ms, "snapshot": ev.snapshot,
                         "key": json.dumps(ev.key, sort_keys=True),
                         "before": json.dumps(ev.before, sort_keys=True) if ev.before else None,
                         "after": json.dumps(ev.after, sort_keys=True) if ev.after else None})
        return rows, dead, counts, {"ranges": ranges, "max_lsn": max_lsn}

    def run(self, source: Source, max_batches: int | None = None, crash_after_bronze: int | None = None,
            idle_polls: int = 1) -> dict:
        done, idle = 0, 0
        while max_batches is None or done < max_batches:
            records = source.poll(self.cfg.batch_size)
            if not records:
                idle += 1
                if idle >= idle_polls:
                    break
                continue
            seq_before = self.seq
            started = time.perf_counter()
            rows, dead, counts, meta = self._process(records)
            bid = batch_id(meta["ranges"])
            self.lake.write_bronze(bid, rows, dead)
            if crash_after_bronze is not None and len(self.batches) + 1 == crash_after_bronze:
                self.seq = seq_before
                raise SimulatedCrash(f"crashed after writing bronze for batch {len(self.batches) + 1}")
            for tp, (_, hi) in meta["ranges"].items():
                self.offsets[tp] = max(self.offsets.get(tp, 0), hi + 1)
            self.stats.update(counts)
            self.batches.append({"batch": len(self.batches) + 1, "id": bid, **{k: counts.get(k, 0) for k in (
                "records", "applied", "duplicate", "late", "tombstones", "heartbeats", "dead_letter")},
                "max_lsn": meta["max_lsn"], "seconds": round(time.perf_counter() - started, 4)})
            self.lake.commit({"offsets": self.offsets, "batches": self.batches, "stats": dict(self.stats),
                              "state": {n: s.to_json() for n, s in self.states.items()}})
            if hasattr(source, "commit"):
                source.commit(self.offsets)
            self.lake.publish_silver(self.states)
            done += 1
            log.info("batch %d: %s", len(self.batches), dict(counts))
            # Debezium keeps sending heartbeats while the database is quiet, so a poll is rarely empty.
            # A batch of nothing but heartbeats is committed (its offsets advance) but counts as idle.
            idle = idle + 1 if counts["records"] == counts["heartbeats"] else 0
            if idle >= idle_polls:
                break
        return {"batches": len(self.batches), "stats": dict(self.stats), "offsets": dict(self.offsets)}

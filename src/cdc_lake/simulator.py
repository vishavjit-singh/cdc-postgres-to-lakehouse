"""A SYNTHETIC OLTP workload and the exact Kafka records Debezium would produce for it.

The local demo cannot run PostgreSQL + Kafka + Debezium, so this module plays all three:

* an in-memory "database" with ``customers``, ``orders`` and ``order_items`` (the same schema as
  ``docker/postgres/init.sql``) that executes transactions and keeps the true final state;
* a "connector" that turns every committed change into a Debezium envelope (``before``/``after``/``op``/
  ``source.lsn``/``txId``/``ts_ms``, snapshot reads first, a tombstone after each delete, a
  delete + create pair for a primary-key change, heartbeats, ``schemas.enable`` on one topic);
* a "broker" that writes them to keyed topic partitions, then misbehaves the way real systems do:
  a connector restart re-sends a range of records (at-least-once), and a few records arrive after
  newer records for the same key (producer retries without idempotence).

Everything is seeded, so every run produces the same stream and the same numbers.
"""
from __future__ import annotations

import json
import random
import zlib
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path

from .config import Settings

FIRST = ["Aarav", "Diya", "Kabir", "Meera", "Rohan", "Isha", "Arjun", "Sana", "Vihaan", "Anaya", "Neha", "Rahul",
         "Priya", "Aman", "Simran", "Dev", "Tara", "Nikhil", "Riya", "Harpreet"]
LAST = ["Sharma", "Verma", "Gill", "Sandhu", "Iyer", "Nair", "Khan", "Das", "Mehta", "Kapoor", "Reddy", "Joshi"]
CITIES = ["Mohali", "Chandigarh", "Ludhiana", "Patiala", "Jaipur", "Delhi", "Pune", "Bengaluru"]
SKUS = [f"SKU-{i:03d}" for i in range(1, 41)]
NEXT_STATUS = {"pending": "paid", "paid": "shipped", "shipped": "delivered"}
LSN_START = 0x1A2B3C00


@dataclass
class Change:
    table: str
    op: str
    before: dict | None
    after: dict | None
    lsn: int
    tx_id: int | None
    ts_ms: int
    snapshot: str = "false"


@dataclass
class Simulation:
    truth: dict[str, dict[tuple, dict]]
    changes: list[Change]
    records: dict[tuple[str, int], list[dict]]          # (topic, partition) -> records
    stats: dict = field(default_factory=dict)


def _iso(ms: int) -> str:
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _pk(table: str, row: dict, cfg: Settings) -> tuple:
    return tuple(row[k] for k in cfg.tables[table])


class Database:
    """The in-memory source database. Every mutation is recorded as a Change with a WAL position."""

    def __init__(self, cfg: Settings, rng: random.Random):
        self.cfg, self.rng = cfg, rng
        self.tables: dict[str, dict[tuple, dict]] = {t: {} for t in cfg.tables}
        self.changes: list[Change] = []
        self.lsn = LSN_START
        self.tx_id = 700
        self.clock_ms = int(cfg.simulator.start.timestamp() * 1000)
        self.loyalty_column = False
        self._tx: list[Change] | None = None

    # transactions ------------------------------------------------------------------------------
    def begin(self):
        self.tx_id += 1
        self.clock_ms += self.rng.randint(800, 6000)
        self._tx = []

    def commit(self):
        self.changes.extend(self._tx)
        self.lsn += self.rng.randint(40, 400)
        self._tx = None

    def _log(self, table, op, before, after):
        self.lsn += self.rng.randint(24, 160)
        self._tx.append(Change(table, op, before and dict(before), after and dict(after), self.lsn, self.tx_id,
                               self.clock_ms + len(self._tx)))

    def insert(self, table, row):
        if table == "customers" and self.loyalty_column:
            row.setdefault("loyalty_points", None)
        self.tables[table][_pk(table, row, self.cfg)] = row
        self._log(table, "c", None, row)

    def update(self, table, pk, **changes):
        before = self.tables[table][pk]
        after = {**before, **changes}
        if "updated_at" in after:
            after["updated_at"] = _iso(self.clock_ms)
        self.tables[table][pk] = after
        self._log(table, "u", before, after)

    def delete(self, table, pk):
        before = self.tables[table].pop(pk)
        self._log(table, "d", before, None)

    def add_loyalty_column(self):
        """ALTER TABLE customers ADD COLUMN loyalty_points integer (no event: Debezium emits no DDL here)."""
        self.loyalty_column = True
        for row in self.tables["customers"].values():
            row["loyalty_points"] = None


def _new_customer(i: int, db: Database) -> dict:
    r = db.rng
    first, last = r.choice(FIRST), r.choice(LAST)
    ts = _iso(db.clock_ms)
    return {"id": i, "name": f"{first} {last}", "email": f"{first.lower()}.{last.lower()}{i}@example.com",
            "city": r.choice(CITIES), "tier": r.choices(["basic", "silver", "gold"], [6, 3, 1])[0],
            "created_at": ts, "updated_at": ts}


def _new_order(i: int, customer_id: int, db: Database) -> tuple[dict, list[dict]]:
    r = db.rng
    items = [{"order_id": i, "line_no": n, "sku": r.choice(SKUS), "qty": r.randint(1, 4),
              "unit_price": f"{r.randrange(9900, 899900) / 100:.2f}"} for n in range(1, r.randint(1, 4) + 1)]
    total = sum(int(it["qty"]) * float(it["unit_price"]) for it in items)
    ts = _iso(db.clock_ms)
    order = {"id": i, "customer_id": customer_id, "status": "pending", "total": f"{total:.2f}", "created_at": ts,
             "updated_at": ts}
    return order, items


def run_workload(cfg: Settings) -> tuple[Database, int]:
    """Seed the database, then run the transaction mix. Returns the db and the snapshot LSN."""
    s = cfg.simulator
    rng = random.Random(s.seed)
    db = Database(cfg, rng)
    # seed data (exists before the connector starts: it reaches Kafka through the snapshot)
    db.begin()
    for i in range(1, s.customers + 1):
        db.insert("customers", _new_customer(i, db))
    for i in range(1, s.orders + 1):
        order, items = _new_order(i, rng.randint(1, s.customers), db)
        order["status"] = rng.choice(["pending", "paid", "shipped", "delivered"])
        db.insert("orders", order)
        for it in items:
            db.insert("order_items", it)
    db.commit()
    seeded_changes = len(db.changes)
    db.changes.clear()
    snapshot_lsn = db.lsn
    next_customer, next_order = s.customers + 1, s.orders + 1

    for n in range(1, s.transactions + 1):
        if n == s.schema_change_at:
            db.add_loyalty_column()
        db.begin()
        kind = rng.choices(["new_customer", "update_customer", "new_order", "advance_order", "cancel_order",
                            "delete_customer", "renumber_line", "loyalty"],
                           [8, 14, 30, 26, 6, 3, 4, 9 if db.loyalty_column else 0])[0]
        customers = db.tables["customers"]
        orders = db.tables["orders"]
        if kind == "new_customer" or not customers:
            db.insert("customers", _new_customer(next_customer, db))
            next_customer += 1
        elif kind == "update_customer":
            pk = rng.choice(sorted(customers))
            field_ = rng.choice(["city", "tier", "email"])
            value = {"city": rng.choice(CITIES), "tier": rng.choice(["basic", "silver", "gold"]),
                     "email": f"user{pk[0]}.{n}@example.com"}[field_]
            db.update("customers", pk, **{field_: value})
        elif kind == "new_order":
            order, items = _new_order(next_order, rng.choice(sorted(customers))[0], db)
            next_order += 1
            db.insert("orders", order)
            for it in items:
                db.insert("order_items", it)
        elif kind == "advance_order":
            open_ = [k for k, o in orders.items() if o["status"] in NEXT_STATUS]
            if open_:
                pk = rng.choice(sorted(open_))
                db.update("orders", pk, status=NEXT_STATUS[orders[pk]["status"]])
        elif kind == "cancel_order":
            pending = [k for k, o in orders.items() if o["status"] in ("pending", "paid")]
            if pending:
                pk = rng.choice(sorted(pending))
                for ik in sorted(k for k in db.tables["order_items"] if k[0] == pk[0]):
                    db.delete("order_items", ik)
                db.update("orders", pk, status="cancelled", total="0.00")
        elif kind == "delete_customer":
            with_orders = {o["customer_id"] for o in orders.values()}
            free = [k for k in customers if k[0] not in with_orders]
            if free:
                db.delete("customers", rng.choice(sorted(free)))
        elif kind == "renumber_line":
            # UPDATE order_items SET line_no = <max + 1>: a primary-key change
            items = db.tables["order_items"]
            if items:
                pk = rng.choice(sorted(items))
                row = dict(items[pk])
                row["line_no"] = max(k[1] for k in items if k[0] == pk[0]) + 1
                db.delete("order_items", pk)
                db.insert("order_items", row)
        elif kind == "loyalty":
            pk = rng.choice(sorted(customers))
            db.update("customers", pk, loyalty_points=rng.randint(10, 900))
        if not db._tx:          # nothing to do for this kind right now: make it a no-op commit
            db._tx = []
        db.commit()
    db.stats = {"seeded_rows": seeded_changes, "streamed_changes": len(db.changes)}
    return db, snapshot_lsn


# ------------------------------------------------------------------------------------------- connector
def _envelope(cfg: Settings, ch: Change, with_schema: bool) -> tuple[str, str]:
    pk_cols = cfg.tables[ch.table]
    row = ch.after if ch.after is not None else ch.before
    key = {k: row[k] for k in pk_cols}
    value = {"before": ch.before, "after": ch.after, "op": ch.op, "ts_ms": ch.ts_ms + 35,
             "source": {"version": "2.7.3.Final", "connector": "postgresql", "name": cfg.topic_prefix,
                        "ts_ms": ch.ts_ms, "snapshot": ch.snapshot, "db": "shop", "schema": "public",
                        "table": ch.table, "txId": ch.tx_id, "lsn": ch.lsn, "xmin": None},
             "transaction": None}
    if with_schema:
        key = {"schema": {"type": "struct", "optional": False, "name": f"{cfg.topic(ch.table)}.Key",
                          "fields": [{"field": k, "type": "int32", "optional": False} for k in pk_cols]},
               "payload": key}
        value = {"schema": {"type": "struct", "optional": False, "name": f"{cfg.topic(ch.table)}.Envelope",
                            "fields": [{"field": f, "type": "struct", "optional": True}
                                       for f in ("before", "after", "source")]}, "payload": value}
    return json.dumps(key, sort_keys=True), json.dumps(value)


def simulate(cfg: Settings) -> Simulation:
    db, snapshot_lsn = run_workload(cfg)
    s = cfg.simulator
    rng = random.Random(s.seed + 1)
    # snapshot: one read event per seeded row, ordered by table, all at the snapshot LSN
    snap_ts = int(s.start.timestamp() * 1000) - 60_000
    changes: list[Change] = []
    final_streamed = db.changes
    # rebuild the seeded state by replaying the workload's inverse would be complex: re-run the seed instead
    seed_db, _ = _seed_only(cfg)
    rows = [(t, r) for t in cfg.tables for r in seed_db.tables[t].values()]
    for i, (t, r) in enumerate(rows):
        changes.append(Change(t, "r", None, dict(r), snapshot_lsn, None, snap_ts,
                              "first" if i == 0 else "last" if i == len(rows) - 1 else "true"))
    changes.extend(final_streamed)

    # broker: keyed partitions per topic
    logs: dict[tuple[str, int], list[dict]] = {}
    sent: list[tuple[str, int, dict]] = []

    def append(topic, partition, key, value, ts):
        log = logs.setdefault((topic, partition), [])
        rec = {"topic": topic, "partition": partition, "offset": len(log), "key": key, "value": value,
               "timestamp_ms": ts}
        log.append(rec)
        sent.append((topic, partition, rec))

    heartbeat_every = 120
    restart_at = int(len(changes) * s.connector_restart_at)
    redelivered = 0
    for i, ch in enumerate(changes):
        topic = cfg.topic(ch.table)
        key, value = _envelope(cfg, ch, with_schema=ch.table == "customers")
        part = zlib.crc32(key.encode()) % s.partitions
        append(topic, part, key, value, ch.ts_ms + 40)
        if ch.op == "d":
            append(topic, part, key, None, ch.ts_ms + 40)                      # tombstone for compaction
        if i % heartbeat_every == 0:
            append(cfg.heartbeat_topic, 0, json.dumps({"serverName": cfg.topic_prefix}),
                   json.dumps({"ts_ms": ch.ts_ms, "source": {"lsn": ch.lsn}}), ch.ts_ms)
        if i == restart_at:
            # connector restart: it resumes from its last flushed LSN and re-sends what came after it
            for topic_, part_, rec in [x for x in sent if not x[0].startswith("__")][-s.redelivered_records:]:
                append(topic_, part_, rec["key"], rec["value"], rec["timestamp_ms"] + 5)
                redelivered += 1

    # late delivery: a few updates land after a newer update for the same key (producer retry, no idempotence)
    late = 0
    for (topic, part), log in sorted(logs.items()):
        if topic.startswith("__"):
            continue
        by_key: dict[str, list[int]] = {}
        for idx, rec in enumerate(log):
            if rec["value"] is not None and '"op": "u"' in rec["value"]:
                by_key.setdefault(rec["key"], []).append(idx)
        candidates = sorted(k for k, idxs in by_key.items() if len(idxs) >= 2)
        rng.shuffle(candidates)
        per_partition = -(-s.late_events // sum(1 for t, _ in logs if not t.startswith("__")))
        for k in candidates[:per_partition]:
            if late >= s.late_events:
                break
            first, second = by_key[k][0], by_key[k][1]
            rec = log.pop(first)
            log.insert(second, rec)            # second shifted left by one: rec now follows the newer update
            late += 1
            by_key = {kk: [x - 1 if first < x <= second else x for x in v] for kk, v in by_key.items()}
        for off, rec in enumerate(log):
            rec["offset"] = off

    truth = {t: {k: dict(v) for k, v in rows_.items()} for t, rows_ in db.tables.items()}
    stats = {"snapshot_reads": len(rows), "streamed_changes": len(final_streamed), "transactions": s.transactions,
             "redelivered_records": redelivered, "late_events": late,
             "records": sum(len(v) for v in logs.values()),
             "tombstones": sum(1 for v in logs.values() for r in v if r["value"] is None),
             "heartbeats": len(logs.get((cfg.heartbeat_topic, 0), [])),
             "ops": {op: sum(1 for c in changes if c.op == op) for op in ("r", "c", "u", "d")},
             "schema_change_at_tx": s.schema_change_at}
    return Simulation(truth=truth, changes=changes, records=logs, stats=stats)


def _seed_only(cfg: Settings) -> tuple[Database, int]:
    """The database exactly as it was when the connector took its snapshot."""
    s = cfg.simulator
    zero = type(s)(**{**s.__dict__, "transactions": 0})
    cfg0 = type(cfg)(**{**cfg.__dict__, "simulator": zero})
    return run_workload(cfg0)


def write_stream(sim: Simulation, out: Path) -> None:
    """One JSONL file per topic-partition, like a Kafka log directory."""
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("*.jsonl"):
        old.unlink()
    for (topic, part), log in sim.records.items():
        with (out / f"{topic}-{part}.jsonl").open("w") as f:
            for rec in log:
                f.write(json.dumps(rec) + "\n")
    (out / "_truth.json").write_text(json.dumps(
        {t: [r for _, r in sorted(rows.items())] for t, rows in sim.truth.items()}, indent=0))
    (out / "_stats.json").write_text(json.dumps(sim.stats, indent=2))


def to_ms(start, minutes: float) -> int:
    return int((start + timedelta(minutes=minutes)).timestamp() * 1000)

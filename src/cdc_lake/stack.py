"""Helpers for the docker compose stack: register the Debezium connector and drive a workload on PostgreSQL.

Needs ``requirements-stack.txt`` (psycopg, confluent-kafka). Not used by the local demo or the unit tests.
"""
from __future__ import annotations

import json
import logging
import random
import time
from pathlib import Path

import requests

log = logging.getLogger("cdc_lake.stack")


def register_connector(connect_url: str, config_path: str | Path, timeout_s: float = 180) -> dict:
    """PUT the connector config (idempotent) and wait until the connector and its task are RUNNING."""
    spec = json.loads(Path(config_path).read_text())
    name = spec["name"]
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            r = requests.put(f"{connect_url}/connectors/{name}/config", json=spec["config"], timeout=10)
            if r.status_code in (200, 201):
                break
            log.info("connect not ready yet: %s %s", r.status_code, r.text[:200])
        except requests.ConnectionError:
            log.info("waiting for Kafka Connect at %s", connect_url)
        if time.monotonic() > deadline:
            raise TimeoutError("Kafka Connect did not accept the connector")
        time.sleep(3)
    while time.monotonic() < deadline:
        status = requests.get(f"{connect_url}/connectors/{name}/status", timeout=10).json()
        tasks = status.get("tasks", [])
        if status.get("connector", {}).get("state") == "RUNNING" and tasks and all(
                t["state"] == "RUNNING" for t in tasks):
            return status
        if any(t["state"] == "FAILED" for t in tasks):
            raise RuntimeError(f"connector task failed: {tasks}")
        time.sleep(2)
    raise TimeoutError("connector did not reach RUNNING")


def run_workload(dsn: str, transactions: int = 400, seed: int = 23, schema_change_at: int = 200) -> dict:
    """The same transaction mix as the simulator, executed on PostgreSQL (so Debezium captures real WAL)."""
    import psycopg

    rng = random.Random(seed)
    counts = {"transactions": 0, "alter_table": 0}
    with psycopg.connect(dsn, autocommit=False) as conn:
        cur = conn.cursor()
        cur.execute("SELECT coalesce(max(id), 0) FROM customers")
        next_c = cur.fetchone()[0] + 1
        cur.execute("SELECT coalesce(max(id), 0) FROM orders")
        next_o = cur.fetchone()[0] + 1
        loyalty = False
        for n in range(1, transactions + 1):
            if n == schema_change_at:
                cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS loyalty_points integer")
                conn.commit()
                loyalty, counts["alter_table"] = True, 1
            kind = rng.choices(["new_customer", "update_customer", "new_order", "advance_order", "cancel_order",
                                "renumber_line", "loyalty"], [8, 14, 30, 26, 6, 4, 9 if loyalty else 0])[0]
            if kind == "new_customer":
                cur.execute("INSERT INTO customers (id, name, email, city) VALUES (%s, %s, %s, 'Delhi')",
                            (next_c, f"Customer {next_c}", f"customer{next_c}@example.com"))
                next_c += 1
            elif kind == "update_customer":
                cur.execute("UPDATE customers SET city = %s, updated_at = now() WHERE id = "
                            "(SELECT id FROM customers ORDER BY random() LIMIT 1)", (rng.choice(["Pune", "Jaipur"]),))
            elif kind == "new_order":
                cur.execute("INSERT INTO orders (id, customer_id, status, total) "
                            "SELECT %s, id, 'pending', 0 FROM customers ORDER BY random() LIMIT 1", (next_o,))
                for line in range(1, rng.randint(1, 4) + 1):
                    cur.execute("INSERT INTO order_items VALUES (%s, %s, %s, %s, %s)",
                                (next_o, line, f"SKU-{rng.randint(1, 40):03d}", rng.randint(1, 4),
                                 rng.randrange(9900, 899900) / 100))
                cur.execute("UPDATE orders SET total = (SELECT sum(qty * unit_price) FROM order_items "
                            "WHERE order_id = %s) WHERE id = %s", (next_o, next_o))
                next_o += 1
            elif kind == "advance_order":
                cur.execute("UPDATE orders SET status = CASE status WHEN 'pending' THEN 'paid' WHEN 'paid' "
                            "THEN 'shipped' ELSE 'delivered' END, updated_at = now() WHERE id = (SELECT id FROM "
                            "orders WHERE status IN ('pending','paid','shipped') ORDER BY random() LIMIT 1)")
            elif kind == "cancel_order":
                cur.execute("SELECT id FROM orders WHERE status IN ('pending','paid') ORDER BY random() LIMIT 1")
                row = cur.fetchone()
                if row:
                    cur.execute("DELETE FROM order_items WHERE order_id = %s", row)
                    cur.execute("UPDATE orders SET status = 'cancelled', total = 0, updated_at = now() "
                                "WHERE id = %s", row)
            elif kind == "renumber_line":     # primary-key change → Debezium emits delete + create
                cur.execute("UPDATE order_items oi SET line_no = (SELECT max(x.line_no) + 1 FROM order_items x "
                            "WHERE x.order_id = oi.order_id) WHERE (order_id, line_no) = "
                            "(SELECT order_id, line_no FROM order_items ORDER BY random() LIMIT 1)")
            elif kind == "loyalty":
                cur.execute("UPDATE customers SET loyalty_points = %s WHERE id = "
                            "(SELECT id FROM customers ORDER BY random() LIMIT 1)", (rng.randint(10, 900),))
            conn.commit()
            counts["transactions"] += 1
            counts[kind] = counts.get(kind, 0) + 1
    return counts

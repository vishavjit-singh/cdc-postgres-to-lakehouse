"""README images and numbers, computed from the bronze log, the checkpoint and a reconciliation run."""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402

from .config import Settings  # noqa: E402
from .consumer import Consumer  # noqa: E402
from .lake import Lake  # noqa: E402
from .reconcile import reconcile  # noqa: E402

INK = "#0e1a2b"
MUTED = "#5b6676"
GRID = "#e3e7ee"
ACCENT = "#2a78d6"
SIGNAL = "#eda100"
BAD = "#e34948"
GOOD = "#1baf7a"
PURPLE = "#6250d6"
OP_COLORS = {"c": GOOD, "u": ACCENT, "d": BAD, "r": "#9aa5b4"}
OP_NAMES = {"c": "insert (c)", "u": "update (u)", "d": "delete (d)", "r": "snapshot read (r)"}

plt.rcParams.update({
    "font.family": "DejaVu Sans", "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "xtick.color": MUTED,
    "ytick.color": MUTED, "axes.titleweight": "bold", "axes.titlesize": 13, "axes.titlecolor": INK,
    "axes.titlelocation": "left", "axes.spines.top": False, "axes.spines.right": False, "figure.dpi": 150,
})


def _save(fig, out: Path, name: str) -> Path:
    fig.tight_layout()
    path = out / name
    fig.savefig(path, facecolor="white")
    plt.close(fig)
    return path


def _reconcile(cfg: Settings) -> list[dict]:
    states = Consumer(cfg, Lake(cfg.path("lake"))).states
    live = {t: s.rows(include_deleted=False) for t, s in states.items()}
    truth = json.loads((cfg.path("stream") / "_truth.json").read_text())
    return reconcile(truth, live, cfg.tables)


def summary(cfg: Settings) -> dict:
    cp = Lake(cfg.path("lake")).load_checkpoint()
    sim = json.loads((cfg.path("stream") / "_stats.json").read_text())
    states = Consumer(cfg, Lake(cfg.path("lake"))).states
    rec = _reconcile(cfg)
    st = cp["stats"]
    return {
        "simulated": {k: sim[k] for k in ("snapshot_reads", "streamed_changes", "transactions", "records",
                                          "redelivered_records", "late_events", "tombstones", "heartbeats")},
        "consumer": {"batches": len(cp["batches"]), "records": st.get("records", 0), "applied": st.get("applied", 0),
                     "duplicates_ignored": st.get("duplicate", 0), "late_into_history_only": st.get("late", 0),
                     "tombstones": st.get("tombstones", 0), "heartbeats": st.get("heartbeats", 0),
                     "dead_letter": st.get("dead_letter", 0)},
        "silver": {t: {"live_rows": sum(1 for r in s.rows() if not r["_deleted"]),
                       "soft_deleted": sum(1 for r in s.rows() if r["_deleted"]),
                       "history_versions": len(s.history())} for t, s in states.items()},
        "schema_changes": [f"{c['table']}.{c['column']}" for s in states.values() for c in s.schema_changes],
        "reconciliation": {r["table"]: {"source": r["source_rows"], "lake": r["lake_rows"], "match": r["match"]}
                           for r in rec},
    }


# ------------------------------------------------------------------------------------------------ charts
def architecture(out: Path) -> Path:
    fig, ax = plt.subplots(figsize=(13, 5.8))
    ax.set_xlim(0, 13)
    ax.set_ylim(0, 6.2)
    ax.axis("off")

    def box(x, y, w, h, title, sub, color=INK):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.12", fc="white",
                                    ec=color, lw=2))
        ax.text(x + w / 2, y + h * 0.7, title, ha="center", va="center", fontsize=11, weight="bold", color=color)
        ax.text(x + w / 2, y + h * 0.32, sub, ha="center", va="center", fontsize=8.2, color=MUTED, linespacing=1.4)

    def arrow(x1, y1, x2, y2, color=MUTED):
        ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=14, lw=1.6, color=color))

    ax.text(0.15, 5.9, "cdc-postgres-to-lakehouse: WAL → Debezium → Kafka → bronze change log → silver",
            fontsize=13.5, weight="bold", color=INK)
    box(0.2, 3.2, 2.2, 1.5, "PostgreSQL", "customers · orders · items\nwal_level=logical\nREPLICA IDENTITY FULL")
    box(2.9, 3.2, 2.2, 1.5, "Debezium", "pgoutput slot\nsnapshot + streaming\nheartbeats · tombstones", PURPLE)
    box(5.6, 3.2, 2.0, 1.5, "Kafka", "shop.public.<table>\n3 partitions, keyed\nby primary key", ACCENT)
    box(8.1, 3.0, 2.3, 1.9, "Consumer",
        "micro-batch\nposition = (LSN, op)\nduplicate · late · delete\nschema evolution", GOOD)
    box(10.9, 4.15, 1.95, 1.2, "bronze", "change log, every\nrecord as delivered", "#b0702b")
    box(10.9, 2.55, 1.95, 1.3, "silver", "current state\n(soft deletes)\n+ SCD2 history", "#7d8794")
    box(8.1, 0.75, 2.3, 1.35, "_checkpoint.json", "offsets + state\nreplaced atomically", SIGNAL)
    box(10.9, 0.75, 1.95, 1.35, "Reconcile", "counts · key diffs\nSHA-256 vs source", BAD)
    arrow(2.4, 3.95, 2.9, 3.95)
    arrow(5.1, 3.95, 5.6, 3.95)
    arrow(7.6, 3.95, 8.1, 3.95)
    arrow(10.4, 4.3, 10.9, 4.7)
    arrow(10.4, 3.6, 10.9, 3.25)
    arrow(9.25, 3.0, 9.25, 2.1, SIGNAL)
    arrow(11.87, 2.55, 11.87, 2.1, BAD)
    ax.text(0.2, 2.55, "Local demo: a seeded simulator writes the exact Kafka records\nDebezium would produce "
            "(snapshot, c/u/d, tombstones,\nPK change, ALTER TABLE, redelivery, late events)\nand the same consumer "
            "replays them. The docker compose\nstack runs real PostgreSQL + Kafka + Debezium.", fontsize=8.4,
            color=MUTED, va="top", linespacing=1.45)
    return _save(fig, out, "architecture.png")


def timeline(bronze: pd.DataFrame, schema_changes: list[dict], out: Path) -> Path:
    b = bronze[(bronze["op"] != "r") & (bronze["outcome"] != "duplicate")].copy()
    b["minute"] = pd.to_datetime(b["source_ts_ms"], unit="ms", utc=True).dt.floor("5min")
    piv = b.pivot_table(index="minute", columns="op", values="seq", aggfunc="count").fillna(0)
    fig, ax = plt.subplots(figsize=(11, 4.0))
    bottom = np.zeros(len(piv))
    x = np.arange(len(piv))
    for op in ("c", "u", "d"):
        if op in piv:
            ax.bar(x, piv[op].values, bottom=bottom, color=OP_COLORS[op], label=OP_NAMES[op], width=0.8)
            bottom += piv[op].values
    ticks = list(range(0, len(piv), max(1, len(piv) // 10)))
    ax.set_xticks(ticks, [piv.index[i].strftime("%H:%M") for i in ticks])
    for sc in schema_changes:
        t = pd.to_datetime(sc["first_seen_ts_ms"], unit="ms", utc=True).floor("5min")
        if t in piv.index:
            i = list(piv.index).index(t)
            ax.axvline(i, color=SIGNAL, lw=1.5, ls="--")
            ax.text(i + 0.15, bottom.max() * 1.3, f"first event with new column {sc['table']}.{sc['column']}",
                    fontsize=8, color="#9a6a00", va="top")
    ax.grid(axis="y", color=GRID)
    ax.set_ylabel("change events / 5 min")
    ax.set_xlabel("source commit time (UTC, synthetic workload)")
    ax.legend(frameon=False, fontsize=8.5, loc="upper left", ncol=3)
    ax.set_ylim(0, bottom.max() * 1.38)
    ax.set_title(f"Streamed change events after the snapshot ({len(b):,} unique events)")
    return _save(fig, out, "events_timeline.png")


def ordering(bronze: pd.DataFrame, stats: dict, out: Path) -> Path:
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(13, 4.6), gridspec_kw={"width_ratios": [2.2, 1]})
    b = bronze[bronze["op"] != "r"]
    base = b["lsn"].min()
    for outcome, color, size, z in (("applied", "#b9c2ce", 4, 1), ("duplicate", SIGNAL, 14, 2), ("late", BAD, 30, 3)):
        part = b[b["outcome"] == outcome]
        label = {"applied": "applied", "duplicate": "duplicate redelivery (ignored)",
                 "late": "late: older than the key's current version (history only)"}[outcome]
        ax.scatter(part["seq"], (part["lsn"] - base) / 1e3, s=size, color=color, label=f"{label} ({len(part)})",
                   zorder=z, edgecolors="none")
    ax.set_xlabel("delivery order (consumer sequence)")
    ax.set_ylabel("source LSN (thousands, relative)")
    ax.grid(color=GRID)
    ax.legend(frameon=False, fontsize=8.3, loc="upper left")
    ax.set_title("Delivery order vs WAL position")
    labels = ["applied", "duplicate", "late", "tombstones", "heartbeats"]
    vals = [stats.get(k, 0) for k in labels]
    colors = [GOOD, SIGNAL, BAD, "#9aa5b4", PURPLE]
    ax2.barh(labels[::-1], vals[::-1], color=colors[::-1], height=0.6)
    for i, v in enumerate(vals[::-1]):
        ax2.text(v, i, f" {v:,}", va="center", fontsize=9, color=INK)
    ax2.set_xscale("log")
    ax2.set_xlim(1, max(vals) * 6)
    ax2.grid(axis="x", color=GRID)
    ax2.set_title("Records by outcome")
    return _save(fig, out, "ordering.png")


def reconciliation(rec: list[dict], states, out: Path) -> Path:
    fig, ax = plt.subplots(figsize=(10, 3.8))
    tables = [r["table"] for r in rec]
    x = np.arange(len(tables))
    src = [r["source_rows"] for r in rec]
    lake = [r["lake_rows"] for r in rec]
    deleted = [sum(1 for row in states[t].rows() if row["_deleted"]) for t in tables]
    ax.bar(x - 0.27, src, width=0.27, color=INK, label="source (simulator's final state)")
    ax.bar(x, lake, width=0.27, color=GOOD, label="silver live rows")
    ax.bar(x + 0.27, deleted, width=0.27, color="#c9cfd8", label="silver soft-deleted keys")
    for i, r in enumerate(rec):
        ax.text(x[i], max(src[i], lake[i]) * 1.04, ("checksum MATCH" if r["match"] else "DIFF") +
                f"\n{r['missing']} missing · {r['extra']} extra · {r['mismatched']} differ", ha="center",
                fontsize=8.5, color=GOOD if r["match"] else BAD, weight="bold")
    ax.set_xticks(x, tables)
    ax.set_ylim(0, max(src + lake) * 1.3)
    ax.grid(axis="y", color=GRID)
    ax.legend(frameon=False, fontsize=8.5, loc="upper left")
    ax.set_title("Reconciliation: silver vs source, row by row and by SHA-256 checksum")
    return _save(fig, out, "reconciliation.png")


def scd2_example(state, out: Path) -> Path:
    hist = state.history()
    by_key: dict = {}
    for h in hist:
        by_key.setdefault(h["id"], []).append(h)
    late_keys = {k for k, vs in by_key.items() if any(v["loyalty_points"] is not None for v in vs)}
    cid = max(late_keys or by_key, key=lambda k: (len(by_key[k]), -k))
    rows = by_key[cid]

    def ts(ms):
        return "—" if ms is None else pd.to_datetime(ms, unit="ms", utc=True).strftime("%m-%d %H:%M:%S")

    cells = [[r["_op"], f"{r['_lsn']:,}", r["city"], r["tier"], r["email"][:28],
              "null" if r["loyalty_points"] is None else str(r["loyalty_points"]),
              ts(r["valid_from_ms"]), ts(r["valid_to_ms"]), "yes" if r["is_current"] else ""] for r in rows]
    cols = ["op", "LSN", "city", "tier", "email", "loyalty_points", "valid_from", "valid_to", "current"]
    fig, ax = plt.subplots(figsize=(13, 0.36 * len(cells) + 0.9))
    ax.axis("off")
    t = ax.table(cellText=cells, colLabels=cols, loc="center", cellLoc="left", colLoc="left",
                 colWidths=[0.05, 0.11, 0.1, 0.07, 0.24, 0.11, 0.12, 0.12, 0.07])
    t.auto_set_font_size(False)
    t.set_fontsize(9)
    t.scale(1, 1.45)
    for (r, c), cell in t.get_celld().items():
        cell.set_edgecolor(GRID)
        if r == 0:
            cell.set_text_props(weight="bold", color=INK)
            cell.set_facecolor("#f3f5f8")
        elif cells[r - 1][-1]:
            cell.set_facecolor("#e8f6ef")
    ax.set_title(f"silver.customers_history for customer {cid}: every version with its validity window "
                 "(synthetic data)", loc="left", fontsize=12, weight="bold", color=INK)
    return _save(fig, out, "scd2_history.png")


def render_all(cfg: Settings, out: str | Path | None = None) -> list[Path]:
    out = Path(out) if out else cfg.path("reports")
    out.mkdir(parents=True, exist_ok=True)
    lake = Lake(cfg.path("lake"))
    bronze = lake.read_bronze()
    cp = lake.load_checkpoint()
    states = Consumer(cfg, lake).states
    changes = [c for s in states.values() for c in s.schema_changes]
    return [architecture(out), timeline(bronze, changes, out), ordering(bronze, cp["stats"], out),
            reconciliation(_reconcile(cfg), states, out), scd2_example(states["customers"], out)]

"""Typed view of ``config/pipeline.yaml``."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = ROOT / "config" / "pipeline.yaml"


@dataclass(frozen=True)
class SimulatorSettings:
    seed: int
    start: datetime
    customers: int
    orders: int
    transactions: int
    partitions: int
    schema_change_at: int
    connector_restart_at: float
    redelivered_records: int
    late_events: int


@dataclass
class Settings:
    root: Path
    topic_prefix: str
    heartbeat_topic: str
    tables: dict[str, tuple[str, ...]]          # table -> primary key columns
    paths: dict[str, str]
    batch_size: int
    simulator: SimulatorSettings

    def path(self, key: str) -> Path:
        p = Path(self.paths[key])
        return p if p.is_absolute() else self.root / p

    def topic(self, table: str) -> str:
        return f"{self.topic_prefix}.public.{table}"

    def table_for_topic(self, topic: str) -> str | None:
        prefix = f"{self.topic_prefix}.public."
        if topic.startswith(prefix) and topic[len(prefix):] in self.tables:
            return topic[len(prefix):]
        return None


def load_settings(path: str | Path | None = None, root: Path | None = None) -> Settings:
    raw = yaml.safe_load(Path(path or DEFAULT_CONFIG).read_text())
    s = raw["simulator"]
    sim = SimulatorSettings(
        seed=int(s["seed"]), start=datetime.fromisoformat(str(s["start"]).replace("Z", "+00:00")),
        customers=int(s["customers"]), orders=int(s["orders"]), transactions=int(s["transactions"]),
        partitions=int(s["partitions"]), schema_change_at=int(s["schema_change_at"]),
        connector_restart_at=float(s["connector_restart_at"]), redelivered_records=int(s["redelivered_records"]),
        late_events=int(s["late_events"]))
    tables = {name: tuple(t["primary_key"]) for name, t in raw["tables"].items()}
    if any(not pk for pk in tables.values()):
        raise ValueError("every table needs a primary_key")
    return Settings(root=root or ROOT, topic_prefix=raw["topic_prefix"], heartbeat_topic=raw["heartbeat_topic"],
                    tables=tables, paths=raw["paths"], batch_size=int(raw["consumer"]["batch_size"]), simulator=sim)

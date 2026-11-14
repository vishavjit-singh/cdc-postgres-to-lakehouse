from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from cdc_lake.config import DEFAULT_CONFIG, load_settings
from cdc_lake.simulator import simulate, write_stream


def small_config(tmp: Path, **sim) -> Path:
    raw = yaml.safe_load(DEFAULT_CONFIG.read_text())
    raw["simulator"].update({"customers": 60, "orders": 80, "transactions": 260, "schema_change_at": 120,
                             "redelivered_records": 40, "late_events": 9, **sim})
    raw["consumer"]["batch_size"] = 80
    raw["paths"] = {"stream": str(tmp / "stream"), "lake": str(tmp / "lake"), "reports": str(tmp / "img")}
    path = tmp / "pipeline.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return path


@pytest.fixture
def cfg(tmp_path):
    return load_settings(small_config(tmp_path), root=tmp_path)


@pytest.fixture
def stream(cfg):
    sim = simulate(cfg)
    write_stream(sim, cfg.path("stream"))
    return sim

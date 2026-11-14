"""End to end on the synthetic stream: consume, restart, crash, replay, reconcile."""
import json

import pytest

from cdc_lake.consumer import Consumer, SimulatedCrash
from cdc_lake.envelope import Record
from cdc_lake.lake import Lake
from cdc_lake.reconcile import reconcile
from cdc_lake.sources import ReplaySource


def consume(cfg, **kw):
    lake = Lake(cfg.path("lake"))
    c = Consumer(cfg, lake)
    c.run(ReplaySource(cfg.path("stream"), c.offsets), **kw)
    return c


def truth(cfg):
    return json.loads((cfg.path("stream") / "_truth.json").read_text())


def live(c):
    return {t: s.rows(include_deleted=False) for t, s in c.states.items()}


def test_lake_matches_source(cfg, stream):
    c = consume(cfg)
    report = reconcile(truth(cfg), live(c), cfg.tables)
    assert all(r["match"] for r in report), report
    st = c.stats
    assert st["records"] == stream.stats["records"]
    assert st["duplicate"] > 0 and st["late"] > 0 and st["tombstones"] == stream.stats["tombstones"]
    assert st["heartbeats"] == stream.stats["heartbeats"]


def test_schema_change_is_detected(cfg, stream):
    c = consume(cfg)
    assert [s["column"] for s in c.states["customers"].schema_changes] == ["loyalty_points"]
    assert "loyalty_points" in c.states["customers"].columns


def test_restart_gives_the_same_result(cfg, stream, tmp_path):
    consume(cfg, max_batches=3)
    c = consume(cfg)                                   # new process: state + offsets from the checkpoint
    assert all(r["match"] for r in reconcile(truth(cfg), live(c), cfg.tables))
    assert c.stats["records"] == stream.stats["records"]


def test_crash_between_bronze_and_checkpoint(cfg, stream):
    consume(cfg, max_batches=2)
    with pytest.raises(SimulatedCrash):
        consume(cfg, crash_after_bronze=4)
    c = consume(cfg)
    assert c.removed_uncommitted > 0
    assert all(r["match"] for r in reconcile(truth(cfg), live(c), cfg.tables))
    bronze = Lake(cfg.path("lake")).read_bronze()
    events = c.stats["applied"] + c.stats["duplicate"] + c.stats["late"]
    assert len(bronze) == events                       # nothing lost, nothing written twice
    assert c.stats["records"] == stream.stats["records"]


def test_rereading_the_whole_stream_changes_nothing(cfg, stream):
    c = consume(cfg)
    before = {t: s.rows() for t, s in c.states.items()}
    c.offsets = {}                                     # e.g. consumer group reset to earliest
    c.run(ReplaySource(cfg.path("stream"), {}, seed=99))
    assert {t: s.rows() for t, s in c.states.items()} == before


def test_pk_change_and_deletes(cfg, stream):
    c = consume(cfg)
    items = c.states["order_items"]
    deleted = [r for r in items.rows() if r["_deleted"]]
    assert deleted                                     # cancelled orders and renumbered lines
    assert c.states["customers"].history()


def test_malformed_records_go_to_dead_letter(cfg, stream):
    lake = Lake(cfg.path("lake"))
    c = Consumer(cfg, lake)

    class Bad:
        sent = False

        def poll(self, n):
            if self.sent:
                return []
            self.sent = True
            return [Record(cfg.topic("orders"), 0, 0, None, "{not json"),
                    Record("some.other.topic", 0, 0, None, "{}")]

        def close(self):
            pass

    c.run(Bad())
    assert c.stats["dead_letter"] == 1 and c.stats["unknown_topic"] == 1
    assert list((cfg.path("lake") / "bronze" / "dead_letter").glob("part-*"))


def test_silver_files_are_published(cfg, stream):
    consume(cfg)
    lake = Lake(cfg.path("lake"))
    cur = lake.read_silver("orders")
    hist = lake.read_silver("orders", history=True)
    assert {"_lsn", "_deleted", "_op"} <= set(cur.columns)
    assert {"valid_from_ms", "valid_to_ms", "is_current"} <= set(hist.columns)
    assert len(hist) >= len(cur)

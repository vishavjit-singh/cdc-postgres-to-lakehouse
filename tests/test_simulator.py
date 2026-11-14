import json

from cdc_lake.simulator import simulate


def test_deterministic(cfg):
    a, b = simulate(cfg), simulate(cfg)
    assert a.records == b.records and a.truth == b.truth


def test_stream_shape(cfg):
    sim = simulate(cfg)
    s = sim.stats
    assert s["snapshot_reads"] > 0 and s["ops"]["d"] > 0 and s["late_events"] > 0
    assert s["redelivered_records"] == cfg.simulator.redelivered_records
    first = sim.records[(cfg.topic("orders"), 0)][0]
    value = json.loads(first["value"])
    assert value["op"] == "r" and value["source"]["snapshot"] in ("first", "true", "last")
    cust = json.loads(sim.records[(cfg.topic("customers"), 0)][0]["value"])
    assert set(cust) == {"schema", "payload"}            # one topic uses schemas.enable=true
    for (_, _), log in sim.records.items():
        assert [r["offset"] for r in log] == list(range(len(log)))


def test_every_delete_is_followed_by_a_tombstone(cfg):
    sim = simulate(cfg)
    for (topic, _), log in sim.records.items():
        for i, r in enumerate(log):
            if r["value"] and '"op": "d"' in r["value"]:
                assert any(x["value"] is None and x["key"] == r["key"] for x in log[i + 1:i + 3])

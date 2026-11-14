import json

import pytest

from cdc_lake.envelope import EnvelopeError, Record, parse

SRC = {"connector": "postgresql", "name": "shop", "db": "shop", "schema": "public", "table": "orders",
       "txId": 771, "lsn": 24023128, "ts_ms": 1730451600100, "snapshot": "false"}


def rec(value, key='{"id": 7}'):
    return Record("shop.public.orders", 0, 5, key, None if value is None else json.dumps(value))


def test_plain_update():
    ev = parse(rec({"before": {"id": 7, "status": "pending"}, "after": {"id": 7, "status": "paid"}, "op": "u",
                    "ts_ms": 1730451600200, "source": SRC}), "orders", ("id",))
    assert ev.op == "u" and ev.key == {"id": 7} and ev.after["status"] == "paid"
    assert ev.lsn == 24023128 and ev.tx_id == 771 and ev.source_ts_ms == 1730451600100
    assert ev.position == (24023128, 1)


def test_schema_wrapped_key_and_value():
    value = {"schema": {"type": "struct"}, "payload": {"before": None, "after": {"id": 7}, "op": "c", "source": SRC}}
    key = json.dumps({"schema": {"type": "struct"}, "payload": {"id": 7}})
    ev = parse(Record("t", 0, 0, key.encode(), json.dumps(value).encode()), "orders", ("id",))
    assert ev.op == "c" and ev.key == {"id": 7}


def test_tombstone_is_none():
    assert parse(rec(None), "orders", ("id",)) is None


def test_snapshot_reads_sort_before_streamed_changes_at_the_same_lsn():
    r = parse(rec({"after": {"id": 7}, "op": "r", "source": {**SRC, "snapshot": "true"}}), "orders", ("id",))
    c = parse(rec({"after": {"id": 7}, "op": "c", "source": SRC}), "orders", ("id",))
    assert r.position < c.position and r.snapshot == "true"


def test_key_falls_back_to_the_row():
    ev = parse(rec({"before": {"order_id": 1, "line_no": 2, "qty": 3}, "after": None, "op": "d", "source": SRC},
                   key=None), "order_items", ("order_id", "line_no"))
    assert ev.key == {"order_id": 1, "line_no": 2}


@pytest.mark.parametrize("value, match", [
    ({"after": {"id": 7}, "op": "c"}, "missing op/source"),
    ({"after": {"id": 7}, "op": "x", "source": SRC}, "unknown op"),
    ({"after": None, "op": "u", "source": SRC}, "without 'after'"),
    ({"after": {"id": 7}, "op": "c", "source": {**SRC, "lsn": None}}, "lsn"),
])
def test_malformed(value, match):
    with pytest.raises(EnvelopeError, match=match):
        parse(rec(value, key=None), "orders", ("id",))


def test_not_json():
    with pytest.raises(EnvelopeError, match="not JSON"):
        parse(Record("t", 0, 0, None, "{oops"), "orders", ("id",))


def test_missing_key_column():
    with pytest.raises(EnvelopeError, match="missing"):
        parse(rec({"after": {"order_id": 1}, "op": "c", "source": SRC}, key='{"order_id": 1}'), "order_items",
              ("order_id", "line_no"))

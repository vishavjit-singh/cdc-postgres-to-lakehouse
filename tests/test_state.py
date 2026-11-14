from cdc_lake.envelope import ChangeEvent, Record
from cdc_lake.state import TableState, canonical, table_checksum

R = Record("t", 0, 0, None, None)


def ev(op, lsn, row=None, before=None, ts=None, key=None):
    k = key or {"id": (row or before)["id"]}
    return ChangeEvent("customers", op, k, before, row, lsn, 1, ts or lsn, ts or lsn, "false", R)


def st():
    return TableState("customers", ("id",))


def test_insert_update_and_duplicate():
    s = st()
    assert s.apply(ev("c", 10, {"id": 1, "city": "Pune"})) == "applied"
    assert s.apply(ev("u", 20, {"id": 1, "city": "Delhi"})) == "applied"
    assert s.apply(ev("u", 20, {"id": 1, "city": "Delhi"})) == "duplicate"
    assert s.rows()[0]["city"] == "Delhi" and s.rows()[0]["_lsn"] == 20


def test_late_event_goes_to_history_only():
    s = st()
    s.apply(ev("c", 10, {"id": 1, "city": "Pune"}))
    s.apply(ev("u", 30, {"id": 1, "city": "Jaipur"}))
    assert s.apply(ev("u", 20, {"id": 1, "city": "Delhi"})) == "late"
    assert s.rows()[0]["city"] == "Jaipur"
    hist = s.history()
    assert [h["city"] for h in hist] == ["Pune", "Delhi", "Jaipur"]
    assert hist[0]["valid_to_ms"] == 20 and hist[1]["valid_to_ms"] == 30 and hist[2]["is_current"]


def test_soft_delete_and_reinsert():
    s = st()
    s.apply(ev("c", 10, {"id": 1, "city": "Pune"}))
    s.apply(ev("d", 20, None, before={"id": 1, "city": "Pune"}))
    row = s.rows()[0]
    assert row["_deleted"] and row["_op"] == "d" and row["city"] == "Pune"
    assert s.rows(include_deleted=False) == []
    s.apply(ev("c", 30, {"id": 1, "city": "Delhi"}))
    assert not s.rows()[0]["_deleted"]
    assert [h["is_deleted"] for h in s.history()] == [False, True, False]


def test_stale_insert_cannot_resurrect_a_deleted_row():
    s = st()
    s.apply(ev("c", 10, {"id": 1}))
    s.apply(ev("d", 30, None, before={"id": 1}))
    assert s.apply(ev("u", 20, {"id": 1})) == "late"
    assert s.rows()[0]["_deleted"]


def test_schema_evolution():
    s = st()
    s.apply(ev("r", 5, {"id": 1, "city": "Pune"}))
    s.apply(ev("u", 10, {"id": 1, "city": "Pune", "loyalty_points": 40}))
    s.apply(ev("c", 11, {"id": 2, "city": "Delhi"}))
    assert s.columns == ["id", "city", "loyalty_points"]
    assert s.schema_changes == [{"table": "customers", "column": "loyalty_points", "first_lsn": 10,
                                 "first_seen_ts_ms": 10}]
    assert {r["id"]: r["loyalty_points"] for r in s.rows()} == {1: 40, 2: None}


def test_as_of():
    s = st()
    s.apply(ev("c", 10, {"id": 1, "city": "Pune"}, ts=100))
    s.apply(ev("u", 20, {"id": 1, "city": "Delhi"}, ts=200))
    s.apply(ev("d", 30, None, before={"id": 1, "city": "Delhi"}, ts=300))
    assert s.as_of(50) == [] and s.as_of(150)[0]["city"] == "Pune" and s.as_of(250)[0]["city"] == "Delhi"
    assert s.as_of(350) == []


def test_truncate_soft_deletes_everything():
    s = st()
    s.apply(ev("c", 10, {"id": 1}))
    s.apply(ev("c", 11, {"id": 2}))
    s.apply(ChangeEvent("customers", "t", {}, None, None, 20, 9, 20, 20, "false", R))
    assert all(r["_deleted"] for r in s.rows())


def test_json_round_trip():
    s = st()
    s.apply(ev("c", 10, {"id": 1, "city": "Pune"}))
    s.apply(ev("u", 20, {"id": 1, "city": "Delhi"}))
    back = TableState.from_json(s.to_json())
    assert back.rows() == s.rows() and back.history() == s.history()
    assert back.apply(ev("u", 20, {"id": 1, "city": "Delhi"})) == "duplicate"


def test_checksum_ignores_order_and_representation():
    a = [{"id": 1, "n": 5, "ts": "2026-11-01T09:00:00Z"}, {"id": 2, "n": None, "ts": None}]
    b = [{"id": 2.0, "n": None, "ts": None}, {"id": 1, "n": 5.0, "ts": "2026-11-01T09:00:00.000000Z"}]
    assert table_checksum(a, ["id", "n", "ts"], ("id",)) == table_checksum(b, ["id", "n", "ts"], ("id",))
    assert canonical("2026-11-01T14:30:00+05:30") == "2026-11-01T09:00:00.000000Z"
    assert table_checksum(a, ["id", "n"], ("id",)) != table_checksum([a[0]], ["id", "n"], ("id",))

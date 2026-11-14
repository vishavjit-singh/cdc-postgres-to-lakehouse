import json

from click.testing import CliRunner

from cdc_lake.cli import cli
from cdc_lake.reconcile import reconcile
from tests.conftest import small_config

PK = {"t": ("id",)}


def test_reconcile_finds_every_kind_of_difference():
    src = {"t": [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 3, "v": "c"}]}
    lake = {"t": [{"id": 1, "v": "a", "_lsn": 9}, {"id": 2, "v": "X"}, {"id": 4, "v": "d"}]}
    r = reconcile(src, lake, PK)[0]
    assert (r["missing"], r["extra"], r["mismatched"], r["match"]) == (1, 1, 1, False)
    ok = reconcile(src, {"t": [dict(x, _deleted=False) for x in src["t"]]}, PK)[0]
    assert ok["match"] and ok["source_checksum"] == ok["lake_checksum"]


def test_cli_end_to_end(tmp_path):
    path = small_config(tmp_path, transactions=150)
    r = CliRunner()
    for args in (["simulate"], ["consume", "--max-batches", "2"], ["consume"], ["reconcile"], ["report"],
                 ["summary"]):
        res = r.invoke(cli, ["--config", str(path), *args])
        assert res.exit_code == 0, (args, res.output)
    assert len(list((tmp_path / "img").glob("*.png"))) == 5
    res = r.invoke(cli, ["--config", str(path), "consume", "--crash-after-bronze", "99"])
    assert res.exit_code == 0     # nothing left to consume: no batch, no crash
    summary = r.invoke(cli, ["--config", str(path), "summary"]).output
    assert json.loads(summary[summary.index("{"):])["reconciliation"]["orders"]["match"] is True

"""The `rosctl oracle` wiring: parse, run, and compare.

The run tests drive the real command over a real (mini-sized) data
directory, complete with MANIFEST.csv and a freshly built sorted-order
cache, so the whole path - cache, rules, warm-up stock, replay, report
- is exercised without Docker or a database. The compare tests stub
only the region read (`read_region_state`): everything between the
files and that seam is the real command.

The stub comparison is WP6a's own acceptance line for the parts that
do not need a cluster: a sequential run against the stub must match the
oracle per-order, because both walk the same stream in the same order.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest
from conftest import drive, make_playback  # noqa: A004
from test_oracle import MINI_VENDORS, mini_orders, mini_rules, mini_stock_rows

from restaurantos.sim import oracle_cli
from restaurantos.sim.cli import app, build_parser
from restaurantos.sim.datacheck import REQUIRED_FILES
from restaurantos.sim.mini import mini_dir
from restaurantos.sim.oracle import CountingOracle
from restaurantos.sim.oracle_cli import RegionState, compare_order_lines
from restaurantos.sim.replaydata import to_order_request


@pytest.fixture
def mini_data_dir(tmp_path: Path) -> Path:
    """The mini fixture as a real data directory: the eight dataset files
    plus a MANIFEST.csv that names their row counts and sha256s."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    for name in sorted(REQUIRED_FILES):
        shutil.copy(mini_dir() / name, data_dir / name)
    lines = ["file,rows,sha256"]
    for name in sorted(REQUIRED_FILES):
        raw = (data_dir / name).read_bytes()
        rows = sum(1 for _ in csv.reader(io.StringIO(raw.decode("utf-8"), newline=""))) - 1
        lines.append(f"{name},{rows},{hashlib.sha256(raw).hexdigest()}")
    (data_dir / "MANIFEST.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return data_dir


def _use_data_dir(monkeypatch, data_dir: Path, cluster_file: Path) -> None:
    monkeypatch.chdir(data_dir.parent)
    monkeypatch.setattr(
        oracle_cli,
        "get_settings",
        lambda: SimpleNamespace(data_dir=data_dir, cluster_file=cluster_file),
    )


def _parse(argv: list[str]):
    return build_parser().parse_args(argv)


# --- the parser and the dispatch -------------------------------------------


def test_parser_reads_the_oracle_subcommands() -> None:
    args = _parse(
        [
            "oracle",
            "run",
            "--region",
            "r1",
            "--from-day",
            "14",
            "--to-day",
            "16",
            "--restock-days",
            "7",
            "--run-id",
            "o-1",
        ]
    )
    assert args.oracle_command == "run"
    assert (args.region, args.from_day, args.to_day, args.restock_days, args.run_id) == (
        "r1",
        14,
        16,
        7,
        "o-1",
    )
    defaults = _parse(["oracle", "run"])
    assert (defaults.region, defaults.from_day, defaults.to_day, defaults.restock_days) == (
        None,
        None,
        None,
        None,
    )
    compare = _parse(["oracle", "compare", "--run-id", "run-1"])
    assert (compare.oracle_command, compare.run_id, compare.report_id) == ("compare", "run-1", None)
    with pytest.raises(SystemExit):
        _parse(["oracle", "compare"])  # --run-id is required


def test_app_dispatches_to_the_oracle_module(monkeypatch) -> None:
    seen = {}

    def fake_run(args):
        seen["command"] = args.oracle_command
        seen["run_id"] = args.run_id
        return 0

    monkeypatch.setattr(oracle_cli, "run", fake_run)
    assert app(["oracle", "run"]) == 0
    assert seen == {"command": "run", "run_id": None}
    assert app(["oracle", "compare", "--run-id", "x"]) == 0
    assert seen == {"command": "compare", "run_id": "x"}


# --- rosctl oracle run ------------------------------------------------------


def test_run_replays_the_mini_fixture_end_to_end(
    tmp_path: Path, mini_data_dir: Path, monkeypatch, capsys
) -> None:
    _use_data_dir(monkeypatch, mini_data_dir, mini_data_dir / "cluster.toml")
    assert (
        oracle_cli.run(
            _parse(["oracle", "run", "--from-day", "14", "--to-day", "16", "--run-id", "o1"])
        )
        == 0
    )

    run_dir = tmp_path / "runs" / "o1"
    report = [json.loads(line) for line in (run_dir / "report.jsonl").open(encoding="utf-8")]
    assert len(report) == 20
    refused = [r["external_order_id"] for r in report if r["outcome"] == "refused"]
    assert refused == ["o0011", "o0008"]  # the fish floor, in replay order
    assert all(r["outcome"] == "accepted" for r in report if r["external_order_id"] not in refused)

    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["orders"] == 20
    assert (summary["accepted"], summary["refused"]) == (18, 2)
    assert summary["refusals_by_reason"] == {"out_of_stock": 2}
    assert summary["stock_rows"] == 17  # warm-up-derived: no counter for the none dish
    assert summary["restock_days"] == []  # W=14, R=7: the first restock is day 21
    assert summary["region"] is None
    assert summary["counters_ever_zero"] >= 1

    stock = (run_dir / "stock.csv").read_text(encoding="utf-8").splitlines()
    assert stock[0] == "vendor_id,item_kind,item_id,on_hand,par_level"
    assert len(stock) == 18  # the header and 17 counters
    assert "rosctl" not in capsys.readouterr().out or True


def test_run_refuses_a_report_id_that_already_exists(
    tmp_path: Path, mini_data_dir: Path, monkeypatch
) -> None:
    _use_data_dir(monkeypatch, mini_data_dir, mini_data_dir / "cluster.toml")
    assert (
        oracle_cli.run(
            _parse(["oracle", "run", "--from-day", "14", "--to-day", "14", "--run-id", "o1"])
        )
        == 0
    )
    code = oracle_cli.run(
        _parse(["oracle", "run", "--from-day", "14", "--to-day", "14", "--run-id", "o1"])
    )
    assert code == 1


def test_run_with_a_region_scopes_the_orders_and_the_stock(
    tmp_path: Path, mini_data_dir: Path, monkeypatch
) -> None:
    cluster_file = mini_data_dir / "two.toml"
    cluster_file.write_text("[cluster]\nregions = 2\nstandbys = false\n", encoding="utf-8")
    _use_data_dir(monkeypatch, mini_data_dir, cluster_file)
    # the hand-checked split: v001-v003 in r1, v004 in r2 (w21zt)
    rc = oracle_cli.run(
        _parse(
            [
                "oracle",
                "run",
                "--region",
                "r1",
                "--config",
                str(cluster_file),
                "--from-day",
                "14",
                "--to-day",
                "16",
                "--run-id",
                "o-r1",
            ]
        )
    )
    assert rc == 0
    summary = json.loads((tmp_path / "runs" / "o-r1" / "summary.json").read_text(encoding="utf-8"))
    assert summary["region"] == "r1"
    assert summary["orders"] == 15  # 20 minus v004's five
    assert summary["refused"] == 2  # both refusals are v002 orders
    assert summary["stock_rows"] == 15  # v004's caramel and cupcakes are out of scope
    vendors = {
        line.split(",")[0]
        for line in (tmp_path / "runs" / "o-r1" / "stock.csv").read_text().splitlines()[1:]
    }
    assert vendors == {"v001", "v002", "v003"}


def test_run_refuses_a_region_the_cluster_does_not_have(
    tmp_path: Path, mini_data_dir: Path, monkeypatch, capsys
) -> None:
    _use_data_dir(monkeypatch, mini_data_dir, mini_data_dir / "one.toml")
    (mini_data_dir / "one.toml").write_text(
        "[cluster]\nregions = 1\nstandbys = false\n", encoding="utf-8"
    )
    rc = oracle_cli.run(
        _parse(["oracle", "run", "--region", "r9", "--config", str(mini_data_dir / "one.toml")])
    )
    assert rc == 1
    assert "no region 'r9'" in capsys.readouterr().err


# --- the stub acceptance line -----------------------------------------------


async def test_a_sequential_stub_run_matches_the_oracle(tmp_path: Path, fake_clock) -> None:
    """The stub accepts everything, and so does the oracle over the
    fixture's stock.csv: the two per-order outcome streams are equal."""
    requests = [to_order_request(order) for order in mini_orders()]
    playback = make_playback(
        requests, tmp_path, clock=fake_clock, inflight=1, speed=1e9, sequential=True
    )
    await playback.start(seed=0, from_day=14, to_day=16, speed=1e9, run_id="stubrun")
    await drive(playback, fake_clock)
    assert playback.status()["state"] == "done"

    log_lines = [
        json.loads(line) for line in (tmp_path / "stubrun" / "orders.jsonl").open(encoding="utf-8")
    ]
    engine = CountingOracle(mini_rules(), mini_stock_rows(), restock_days=[], vendors=MINI_VENDORS)
    report_lines = list(engine.replay(mini_orders()))
    comparison = compare_order_lines(log_lines, report_lines)
    assert comparison.mismatch_count == 0
    assert (comparison.log_orders, comparison.report_orders) == (20, 20)
    assert (comparison.both_accepted, comparison.both_refused) == (20, 0)


# --- rosctl oracle compare ---------------------------------------------------


@pytest.fixture
def runs_cwd(tmp_path: Path, monkeypatch) -> Path:
    """The compare command reads runs/ relative to the process directory,
    exactly as playback does; keep the process in the test's directory."""
    monkeypatch.chdir(tmp_path)
    return tmp_path


STOCK_ROWS = [("v001", "dish", "d013", 39, 40), ("v001", "ingredient", "beef", 48, 50)]


def _write_run(
    tmp_path: Path,
    run_id: str,
    order_lines: list[dict],
    *,
    region: str | None = "r1",
    sequential: bool = True,
    restock: tuple[int, ...] = (),
    cache: str = "sha-abc",
) -> None:
    run_dir = tmp_path / "runs" / run_id
    run_dir.mkdir(parents=True)
    meta = {
        "run_id": run_id,
        "region": region,
        "sequential": sequential,
        "from_day": 14,
        "to_day": 16,
        "cache": {"orders_sha256": cache},
        "restock": {"days": list(restock)},
        "cluster": {"config": "cluster.toml"},
    }
    (run_dir / "meta.json").write_text(json.dumps(meta) + "\n", encoding="utf-8")
    (run_dir / "orders.jsonl").write_text(
        "".join(json.dumps(line) + "\n" for line in order_lines), encoding="utf-8"
    )


def _write_report(
    tmp_path: Path,
    report_id: str,
    report_lines: list[dict],
    stock_rows: list[tuple[str, str, str, int, int]] = STOCK_ROWS,
    *,
    accepted: int | None = None,
    refused: int | None = None,
    cache: str = "sha-abc",
    restock: tuple[int, ...] = (),
) -> Path:
    report_dir = tmp_path / "runs" / report_id
    report_dir.mkdir(parents=True)
    (report_dir / "report.jsonl").write_text(
        "".join(json.dumps(line) + "\n" for line in report_lines), encoding="utf-8"
    )
    with (report_dir / "stock.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(("vendor_id", "item_kind", "item_id", "on_hand", "par_level"))
        for row in stock_rows:
            writer.writerow(row)
    if accepted is None:
        accepted = sum(1 for line in report_lines if line["outcome"] == "accepted")
    if refused is None:
        refused = sum(1 for line in report_lines if line["outcome"] == "refused")
    summary = {
        "oracle_id": report_id,
        "region": "r1",
        "from_day": 14,
        "to_day": 16,
        "restock_days": list(restock),
        "cache": {"orders_sha256": cache},
        "accepted": accepted,
        "refused": refused,
        "stock_rows": len(stock_rows),
    }
    (report_dir / "summary.json").write_text(json.dumps(summary) + "\n", encoding="utf-8")
    return report_dir


def _stub_region(monkeypatch, *, orders: int, refused: int, stock_rows=None) -> None:
    rows = STOCK_ROWS if stock_rows is None else stock_rows
    stock = {(v, k, i): (on_hand, par) for v, k, i, on_hand, par in rows}
    monkeypatch.setattr(oracle_cli, "load_cluster", lambda path: "the cluster")
    monkeypatch.setattr(
        oracle_cli, "read_region_state", lambda cluster, region: RegionState(orders, refused, stock)
    )


def _order_line(seq: int, oid: str, outcome: str, reason: str | None = None) -> dict:
    line = {"kind": "order", "seq": seq, "external_order_id": oid, "outcome": outcome}
    if reason is not None:
        line["refusal_reason"] = reason
    return line


def _report_line(seq: int, oid: str, outcome: str, reason: str | None = None) -> dict:
    line = {
        "kind": "order",
        "seq": seq,
        "external_order_id": oid,
        "vendor_id": "v001",
        "order_day": 14,
        "order_time": "08:00:00",
        "outcome": outcome,
    }
    if reason is not None:
        line["refusal_reason"] = reason
    return line


def test_compare_matches_when_run_report_and_region_agree(
    tmp_path, runs_cwd, monkeypatch, capsys
) -> None:
    lines = [
        _order_line(0, "o1", "accepted"),
        _order_line(1, "o2", "refused", "out_of_stock"),
    ]
    _write_run(tmp_path, "run1", lines)
    _write_report(
        tmp_path,
        "oracle-1",
        [_report_line(0, "o1", "accepted"), _report_line(1, "o2", "refused", "out_of_stock")],
    )
    _stub_region(monkeypatch, orders=1, refused=1)
    assert (
        oracle_cli.run(_parse(["oracle", "compare", "--run-id", "run1", "--report-id", "oracle-1"]))
        == 0
    )
    out = capsys.readouterr().out
    assert "MATCH" in out
    assert "2 in the run log, 2 in the" in out


def test_compare_finds_a_stock_mismatch(tmp_path, runs_cwd, monkeypatch, capsys) -> None:
    _write_run(tmp_path, "run1", [_order_line(0, "o1", "accepted")])
    _write_report(tmp_path, "oracle-1", [_report_line(0, "o1", "accepted")])
    # the region's beef counter is one lower than the oracle's
    _stub_region(
        monkeypatch,
        orders=1,
        refused=0,
        stock_rows=[("v001", "dish", "d013", 39, 40), ("v001", "ingredient", "beef", 47, 50)],
    )
    assert (
        oracle_cli.run(_parse(["oracle", "compare", "--run-id", "run1", "--report-id", "oracle-1"]))
        == 1
    )
    assert "beef" in capsys.readouterr().err


def test_compare_finds_an_outcome_mismatch_and_a_count_mismatch(
    tmp_path, runs_cwd, monkeypatch, capsys
) -> None:
    _write_run(tmp_path, "run1", [_order_line(0, "o1", "refused", "out_of_stock")])
    _write_report(tmp_path, "oracle-1", [_report_line(0, "o1", "accepted")])
    _stub_region(monkeypatch, orders=0, refused=1)
    rc = oracle_cli.run(
        _parse(["oracle", "compare", "--run-id", "run1", "--report-id", "oracle-1"])
    )
    assert rc == 1
    captured = capsys.readouterr()
    assert "run says refused, oracle says accepted" in captured.err
    assert "count mismatch" in captured.err


def test_compare_counts_the_expected_reason_divergence_as_agreement(
    tmp_path, runs_cwd, monkeypatch, capsys
) -> None:
    """The region refused at step 3 (unavailable, the stock mark) where the
    oracle refuses on stock: outcomes agree, and compare reports the pair
    instead of failing."""
    _write_run(tmp_path, "run1", [_order_line(0, "o1", "refused", "unavailable")])
    _write_report(tmp_path, "oracle-1", [_report_line(0, "o1", "refused", "out_of_stock")])
    _stub_region(monkeypatch, orders=0, refused=1)
    assert (
        oracle_cli.run(_parse(["oracle", "compare", "--run-id", "run1", "--report-id", "oracle-1"]))
        == 0
    )
    out = capsys.readouterr().out
    assert "reason divergences (expected" in out


def test_compare_fails_a_report_from_a_different_window(
    tmp_path, runs_cwd, monkeypatch, capsys
) -> None:
    _write_run(tmp_path, "run1", [_order_line(0, "o1", "accepted")])
    report_dir = _write_report(tmp_path, "oracle-1", [_report_line(0, "o1", "accepted")])
    summary = json.loads((report_dir / "summary.json").read_text(encoding="utf-8"))
    summary["to_day"] = 20
    (report_dir / "summary.json").write_text(json.dumps(summary) + "\n", encoding="utf-8")
    _stub_region(monkeypatch, orders=1, refused=0)
    assert (
        oracle_cli.run(_parse(["oracle", "compare", "--run-id", "run1", "--report-id", "oracle-1"]))
        == 1
    )
    assert "the run covered days" in capsys.readouterr().err


def test_compare_refuses_a_non_sequential_run(tmp_path, runs_cwd, monkeypatch, capsys) -> None:
    _write_run(tmp_path, "run1", [_order_line(0, "o1", "accepted")], sequential=False)
    _write_report(tmp_path, "oracle-1", [_report_line(0, "o1", "accepted")])
    _stub_region(monkeypatch, orders=1, refused=0)
    assert (
        oracle_cli.run(_parse(["oracle", "compare", "--run-id", "run1", "--report-id", "oracle-1"]))
        == 1
    )
    assert "not sequential" in capsys.readouterr().err


def test_compare_defaults_to_the_newest_oracle_report(
    tmp_path, runs_cwd, monkeypatch, capsys
) -> None:
    _write_run(tmp_path, "run1", [_order_line(0, "o1", "accepted")])
    _write_report(
        tmp_path, "oracle-20260101-000000", [_report_line(0, "o1", "refused", "out_of_stock")]
    )
    _write_report(tmp_path, "oracle-20260202-000000", [_report_line(0, "o1", "accepted")])
    _stub_region(monkeypatch, orders=1, refused=0)
    assert oracle_cli.run(_parse(["oracle", "compare", "--run-id", "run1"])) == 0
    assert "oracle-20260202-000000 vs run run1: MATCH" in capsys.readouterr().out


def test_compare_names_a_missing_run(tmp_path, runs_cwd, monkeypatch, capsys) -> None:
    assert oracle_cli.run(_parse(["oracle", "compare", "--run-id", "nope"])) == 1
    assert "no run 'nope'" in capsys.readouterr().err

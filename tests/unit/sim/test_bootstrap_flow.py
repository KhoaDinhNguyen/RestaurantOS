"""The rebuild order, and everything that must fail before teardown.

Two properties:

- Teardown runs against the OLD generated Compose file — the one the
  previous bootstrap wrote — because switching from more regions to
  fewer must remove the services and volumes the new file will not
  name; volumes the files could not name are removed by name.
- Nothing that only reads input (the render, the stock, the reference
  rows) runs after the teardown: a bad input leaves the running cluster
  untouched, and no `docker` call is ever made.

Docker and the file writes are faked; nothing here touches a container.
"""

import hashlib
import subprocess
from pathlib import Path

import pytest

import restaurantos.sim.bootstrap as bootstrap
from restaurantos.common.cluster import Cluster
from restaurantos.sim.config import SimSettings
from restaurantos.sim.loading import reference_rows, slice_reference_rows

SIM = SimSettings()


# ---- fakes -------------------------------------------------------------------


def _fake_docker(events: list[str], *, volume_rm_fails: bool = False):
    """Record every docker invocation; never run anything."""

    def run(argv, **kwargs):  # noqa: ANN001, ANN003 - subprocess signature
        assert argv[0] == "docker"
        if "down" in argv:
            events.append(f"down:{argv[argv.index('-f') + 1]}")
        elif "up" in argv:
            events.append("up")
        elif "rm" in argv:
            events.append(f"rm:{argv[-1]}")
            if volume_rm_fails:
                return subprocess.CompletedProcess(argv, 1, stdout="", stderr="volume is in use")
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    return run


def _fake_docker_with_volumes(
    events: list[str], volumes: list[str], *, volume_rm_fails: bool = False
):
    """Like _fake_docker, but `volume ls` answers real names, so the
    teardown's remove-by-name sweep can be exercised."""

    plain = _fake_docker(events, volume_rm_fails=volume_rm_fails)

    def run(argv, **kwargs):  # noqa: ANN001, ANN003
        assert argv[0] == "docker"
        if "ls" in argv:
            events.append("ls")
            return subprocess.CompletedProcess(
                argv, 0, stdout="".join(f"{name}\n" for name in volumes), stderr=""
            )
        return plain(argv, **kwargs)

    return run


def _fake_write(events: list[str]):
    def write(out_dir, compose, proxy_list):
        events.append("write")

    return write


# ---- the rebuild order ---------------------------------------------------------


def test_refusal_tears_down_nothing(monkeypatch):
    events: list[str] = []
    monkeypatch.setattr(bootstrap, "_existing_volumes", lambda: ["restaurantos_x-data"])
    monkeypatch.setattr(bootstrap, "_confirm_volume_deletion", lambda volumes: False)
    monkeypatch.setattr(bootstrap.subprocess, "run", _fake_docker(events))
    monkeypatch.setattr(bootstrap, "write_generated", _fake_write(events))

    assert bootstrap._rebuild(yes=False, compose="x", proxy_list="y") is False
    assert events == []


def test_a_failed_teardown_stops_bootstrap_before_writing(monkeypatch, tmp_path):
    events: list[str] = []
    old_file = tmp_path / "compose.yaml"
    old_file.write_text("OLD", encoding="utf-8")
    monkeypatch.setattr(bootstrap, "COMPOSE_FILE", old_file)
    monkeypatch.setattr(bootstrap, "_existing_volumes", lambda: ["restaurantos_x-data"])
    monkeypatch.setattr(bootstrap.subprocess, "run", _fake_docker(events))
    monkeypatch.setattr(bootstrap, "write_generated", _fake_write(events))

    # the down of the old file fails; the committed baseline is never
    # reached, nothing is written or started
    def failing_down(*args, compose_file=old_file):  # noqa: ANN001, ANN002
        events.append(f"down:{compose_file}")
        raise RuntimeError("docker compose down failed (1):\ndocker is grumpy")

    monkeypatch.setattr(bootstrap, "_compose", failing_down)
    with pytest.raises(RuntimeError, match="docker is grumpy"):
        bootstrap._rebuild(yes=True, compose="x", proxy_list="y")
    assert events == [f"down:{old_file}"]


def test_teardown_uses_the_old_file_then_write_then_up(monkeypatch, tmp_path):
    old_file = tmp_path / "compose.yaml"
    old_file.write_text("OLD", encoding="utf-8")
    events: list[str] = []
    monkeypatch.setattr(bootstrap, "COMPOSE_FILE", old_file)
    monkeypatch.setattr(bootstrap, "_existing_volumes", lambda: ["restaurantos_x-data"])

    def fake_run(argv, **kwargs):  # noqa: ANN001, ANN003
        assert argv[0] == "docker"
        if "down" in argv:
            # teardown still sees the old generated file, not the new one
            assert old_file.read_text(encoding="utf-8") == "OLD"
            assert "--remove-orphans" in argv
            assert "-v" in argv
            events.append(f"down:{argv[argv.index('-f') + 1]}")
        elif "up" in argv:
            # and the cluster is started from the file that was written
            assert old_file.read_text(encoding="utf-8") == "NEW"
            events.append("up")
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    def fake_write(out_dir, compose, proxy_list):
        events.append("write")
        old_file.write_text("NEW", encoding="utf-8")

    monkeypatch.setattr(bootstrap.subprocess, "run", fake_run)
    monkeypatch.setattr(bootstrap, "write_generated", fake_write)

    assert bootstrap._rebuild(yes=True, compose="NEW", proxy_list="{}") is True
    # the old generated file first, then the committed baseline, then the
    # write, then the start — never write before teardown
    assert events[0] == f"down:{old_file}"
    assert events[1] == f"down:{bootstrap.COMMITTED_COMPOSE}"
    assert events[2] == "write"
    assert events[3] == "up"


def test_no_volumes_no_prompt(monkeypatch):
    events: list[str] = []
    asked: list[bool] = []
    monkeypatch.setattr(bootstrap, "_existing_volumes", lambda: [])
    monkeypatch.setattr(
        bootstrap, "_confirm_volume_deletion", lambda volumes: asked.append(True) or False
    )
    monkeypatch.setattr(bootstrap.subprocess, "run", _fake_docker(events))
    monkeypatch.setattr(bootstrap, "write_generated", _fake_write(events))

    assert bootstrap._rebuild(yes=False, compose="x", proxy_list="y") is True
    assert asked == []  # nothing to ask about
    assert events[-1] == "up"


# ---- volumes the teardown files cannot name (round 2, item 2) --------------------


def test_leftover_volumes_are_removed_by_name(monkeypatch, tmp_path):
    # no generated file on disk: the committed baseline alone cannot name
    # the baseline and r2/r3 volumes a previous shape left behind
    events: list[str] = []
    monkeypatch.setattr(bootstrap, "COMPOSE_FILE", tmp_path / "compose.yaml")
    monkeypatch.setattr(
        bootstrap.subprocess,
        "run",
        _fake_docker_with_volumes(
            events, ["restaurantos_baseline-data", "restaurantos_r2-primary-data"]
        ),
    )
    monkeypatch.setattr(bootstrap, "write_generated", _fake_write(events))

    assert bootstrap._rebuild(yes=True, compose="x", proxy_list="y") is True
    # after the downs, the leftovers were removed by name, before the write
    assert events.index("rm:restaurantos_baseline-data") < events.index("write")
    assert "rm:restaurantos_r2-primary-data" in events
    assert events[-1] == "up"


def test_a_failed_volume_removal_stops_bootstrap(monkeypatch, tmp_path):
    events: list[str] = []
    monkeypatch.setattr(bootstrap, "COMPOSE_FILE", tmp_path / "compose.yaml")
    monkeypatch.setattr(
        bootstrap.subprocess,
        "run",
        _fake_docker_with_volumes(events, ["restaurantos_baseline-data"], volume_rm_fails=True),
    )
    monkeypatch.setattr(bootstrap, "write_generated", _fake_write(events))

    with pytest.raises(RuntimeError, match="volume is in use"):
        bootstrap._rebuild(yes=True, compose="x", proxy_list="y")
    assert "write" not in events
    assert "up" not in events


# ---- everything that can fail on the input fails before teardown ------------------

DATASET = {
    "ingredients.csv": "ingredient_id,source\nbeef,recipenlg\n",
    "dishes.csv": (
        "dish_id,dish_name,scope,stock_mode,n_products,n_vendors,example_name\n"
        "d1,noodles,independent,per_ingredient,1,1,x\n"
        "d2,drink,independent,per_item,1,1,x\n"
        "d3,sauce,independent,none,1,1,x\n"
    ),
    "dish_ingredients.csv": "dish_id,ingredient_id,amount,unit\nd1,beef,2,kg\nd3,beef,1,kg\n",
    "vendors.csv": "vendor_id,chain_id,geohash,primary_cuisine\nv1,,w21z7,x\n",
    "products.csv": "product_id,vendor_id,name,unit_price,price_units\np1,v1,noodles,0.01,10\n",
    "product_dish.csv": (
        "product_id,vendor_id,dish_id,multiplier,multiplier_ambiguous\np1,v1,d1,2,false\n"
    ),
    "orders.csv": (
        "order_id,customer_id,vendor_id,geohash,day_of_week,order_day,order_time\n"
        "o1,c1,v1,w21z7,0,0,10:00:00\n"
    ),
    "order_items.csv": "order_id,product_id,quantity\no1,p1,3\n",
}


def _write_dataset(tmp_path, **override: str) -> None:
    """The eight required files with a valid MANIFEST; one file's content
    can be overridden per test."""
    files = dict(DATASET)
    files.update(override)
    manifest = ["file,rows,sha256"]
    for name, content in sorted(files.items()):
        (tmp_path / name).write_text(content, encoding="utf-8")
        rows = content.count("\n") - 1
        manifest.append(f"{name},{rows},{hashlib.sha256(content.encode()).hexdigest()}")
    (tmp_path / "MANIFEST.csv").write_text("\n".join(manifest) + "\n", encoding="utf-8")


def _no_docker(monkeypatch) -> list[str]:
    events: list[str] = []
    monkeypatch.setattr(bootstrap.subprocess, "run", _fake_docker(events))
    monkeypatch.setattr(bootstrap, "write_generated", _fake_write(events))
    return events


def test_a_synchronous_cluster_fails_before_teardown(monkeypatch, tmp_path):
    _write_dataset(tmp_path)
    events = _no_docker(monkeypatch)
    with pytest.raises(NotImplementedError, match="synchronous"):
        bootstrap._prepare(Cluster(regions=1, synchronous=True), tmp_path, SIM)
    assert events == []  # the running cluster was never touched


def test_a_unit_conflict_fails_before_teardown(monkeypatch, tmp_path):
    _write_dataset(
        tmp_path,
        **{
            "dish_ingredients.csv": "dish_id,ingredient_id,amount,unit\nd1,beef,2,kg\nd3,beef,1,l\n"
        },
    )
    events = _no_docker(monkeypatch)
    with pytest.raises(ValueError, match="two different units"):
        bootstrap._prepare(Cluster(regions=1), tmp_path, SIM)
    assert events == []


def test_an_unexpected_empty_field_fails_before_teardown(monkeypatch, tmp_path):
    # ingredients.source is not nullable; an empty value is a data error
    _write_dataset(tmp_path, **{"ingredients.csv": "ingredient_id,source\nbeef,\n"})
    events = _no_docker(monkeypatch)
    with pytest.raises(ValueError, match="ingredients.source: empty value"):
        bootstrap._prepare(Cluster(regions=1), tmp_path, SIM)
    assert events == []


def test_prepare_reads_the_reference_files_once(monkeypatch, tmp_path):
    calls: list[Path] = []
    _write_dataset(
        tmp_path,
        **{
            "vendors.csv": "vendor_id,chain_id,geohash,primary_cuisine\nv1,,w21z7,x\nv2,,w21z6,x\n",
            "products.csv": (
                "product_id,vendor_id,name,unit_price,price_units\n"
                "p1,v1,noodles,0.01,10\np2,v2,drink,0.01,10\n"
            ),
            "product_dish.csv": (
                "product_id,vendor_id,dish_id,multiplier,multiplier_ambiguous\n"
                "p1,v1,d1,2,false\np2,v2,d2,1,false\n"
            ),
        },
    )

    real_read = bootstrap.reference_rows

    def counting_read(data_dir, *, vendors=None):
        calls.append(data_dir)
        return real_read(data_dir, vendors=vendors)

    monkeypatch.setattr(bootstrap, "reference_rows", counting_read)
    prepared = bootstrap._prepare(Cluster(regions=2), tmp_path, SIM)
    assert calls == [tmp_path]  # one read, however many regions
    # the slices together hold exactly the whole vendor table
    assert (
        sum(len(s["vendors.csv"]) for s in prepared.slices.values())
        == len(prepared.rows["vendors.csv"])
        == 2
    )


def test_a_manifest_mismatch_fails_before_teardown(monkeypatch, tmp_path):
    # the manifest verifies hash and count per file, so a divergence on
    # disk is caught by the data check; what this guards against is a
    # row count that no longer matches what was verified (a read that
    # dropped or duplicated rows)
    _write_dataset(tmp_path)
    real_read = bootstrap.reference_rows

    def tampered(data_dir, *, vendors=None):
        rows = real_read(data_dir)
        rows["vendors.csv"].append(["v2", "", "w21z7", "x"])
        return rows

    monkeypatch.setattr(bootstrap, "reference_rows", tampered)
    events = _no_docker(monkeypatch)
    with pytest.raises(ValueError, match="vendors.csv: 2 rows to load"):
        bootstrap._prepare(Cluster(regions=1), tmp_path, SIM)
    assert events == []


def test_an_in_memory_slice_matches_a_reread(tmp_path):
    # the regions' tables are cut from one read; the in-memory slice must
    # be exactly what a fresh read of the sliced tables would give
    _write_dataset(
        tmp_path,
        **{
            "vendors.csv": "vendor_id,chain_id,geohash,primary_cuisine\nv1,,w21z7,x\nv2,,w21z6,x\n",
            "products.csv": (
                "product_id,vendor_id,name,unit_price,price_units\n"
                "p1,v1,noodles,0.01,10\np2,v2,drink,0.01,10\n"
            ),
            "product_dish.csv": (
                "product_id,vendor_id,dish_id,multiplier,multiplier_ambiguous\n"
                "p1,v1,d1,2,false\np2,v2,d2,1,false\n"
            ),
        },
    )
    once = reference_rows(tmp_path)
    in_memory = slice_reference_rows(once, frozenset({"v1"}))
    reread = reference_rows(tmp_path, vendors=frozenset({"v1"}))
    assert in_memory == reread
    assert in_memory["products.csv"] == [["p1", "v1", "noodles", "0.01", "10"]]
    assert in_memory["product_dish.csv"] == [["p1", "v1", "d1", "2", "false"]]
    # the small tables are whole either way
    assert in_memory["dishes.csv"] == once["dishes.csv"]

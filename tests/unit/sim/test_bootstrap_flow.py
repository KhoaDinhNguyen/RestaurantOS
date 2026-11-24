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


# ---- the rebuild order ---------------------------------------------------------


def test_refusal_tears_down_nothing(monkeypatch, tmp_path):
    events: list[str] = []
    monkeypatch.setattr(bootstrap, "COMPOSE_FILE", tmp_path / "compose.yaml")
    monkeypatch.setattr(bootstrap, "_existing_volumes", lambda: ["restaurantos_x-data"])
    monkeypatch.setattr(bootstrap, "_confirm_volume_deletion", lambda volumes: False)
    monkeypatch.setattr(bootstrap.subprocess, "run", _fake_docker(events))

    assert bootstrap._rebuild(yes=False, compose="x", proxy_list="y", cluster_toml="z") is False
    assert events == []
    assert not list(tmp_path.glob("*.tmp"))  # not even staged


def test_a_failed_teardown_stops_bootstrap_and_cleans_the_staged_files(monkeypatch, tmp_path):
    events: list[str] = []
    old_file = tmp_path / "compose.yaml"
    old_file.write_text("OLD", encoding="utf-8")
    monkeypatch.setattr(bootstrap, "COMPOSE_FILE", old_file)
    monkeypatch.setattr(bootstrap, "_existing_volumes", lambda: ["restaurantos_x-data"])
    monkeypatch.setattr(bootstrap.subprocess, "run", _fake_docker(events))

    # the down of the old file fails; the committed baseline is never
    # reached, nothing is started, the staged files are removed
    def failing_down(*args, compose_file=old_file):  # noqa: ANN001, ANN002
        events.append(f"down:{compose_file}")
        raise bootstrap.BootstrapError("docker compose down failed (1):\ndocker is grumpy")

    monkeypatch.setattr(bootstrap, "_compose", failing_down)
    with pytest.raises(bootstrap.BootstrapError, match="docker is grumpy"):
        bootstrap._rebuild(yes=True, compose="x", proxy_list="y", cluster_toml="z")
    assert events == [f"down:{old_file}"]
    # the old generated file is as it was, and no .tmp survives
    assert old_file.read_text(encoding="utf-8") == "OLD"
    assert not list(tmp_path.glob("*.tmp"))


def test_teardown_uses_the_old_file_then_replace_then_up(monkeypatch, tmp_path):
    old_file = tmp_path / "compose.yaml"
    old_file.write_text("OLD", encoding="utf-8")
    events: list[str] = []
    monkeypatch.setattr(bootstrap, "COMPOSE_FILE", old_file)
    monkeypatch.setattr(bootstrap, "_existing_volumes", lambda: ["restaurantos_x-data"])

    def fake_run(argv, **kwargs):  # noqa: ANN001, ANN003
        assert argv[0] == "docker"
        if "down" in argv:
            # teardown still sees the old generated file, not the staged
            # one (the staged names differ, so teardown reads this file)
            assert old_file.read_text(encoding="utf-8") == "OLD"
            assert "--remove-orphans" in argv
            assert "-v" in argv
            # the service entries are profile-gated, and the teardown
            # must sweep them away too, not orphan them
            assert argv[argv.index("--profile") + 1] == "services"
            events.append(f"down:{argv[argv.index('-f') + 1]}")
        elif "rm" in argv:
            events.append(f"rm:{argv[-1]}")
        elif "up" in argv:
            # the cluster starts WITHOUT the services profile: a data
            # load must never start the service containers
            assert "--profile" not in argv
            # and the cluster is started from the file that was staged
            # and moved into place
            assert old_file.read_text(encoding="utf-8") == "NEW"
            assert not list(tmp_path.glob("*.tmp"))
            events.append("up")
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr(bootstrap.subprocess, "run", fake_run)

    assert bootstrap._rebuild(yes=True, compose="NEW", proxy_list="{}", cluster_toml="z") is True
    # the old generated file first, then the committed baseline, then the
    # leftover sweep, then the start — never up before teardown
    assert events[0] == f"down:{old_file}"
    assert events[1] == f"down:{bootstrap.COMMITTED_COMPOSE}"
    assert events[2] == "rm:restaurantos_x-data"
    assert events[3] == "up"


def test_no_volumes_no_prompt(monkeypatch, tmp_path):
    events: list[str] = []
    asked: list[bool] = []
    monkeypatch.setattr(bootstrap, "COMPOSE_FILE", tmp_path / "compose.yaml")
    monkeypatch.setattr(bootstrap, "_existing_volumes", lambda: [])
    monkeypatch.setattr(
        bootstrap, "_confirm_volume_deletion", lambda volumes: asked.append(True) or False
    )
    monkeypatch.setattr(bootstrap.subprocess, "run", _fake_docker(events))

    assert bootstrap._rebuild(yes=False, compose="x", proxy_list="y", cluster_toml="z") is True
    assert asked == []  # nothing to ask about
    assert events[-1] == "up"
    assert not list(tmp_path.glob("*.tmp"))  # staged then replaced


def test_the_staged_files_include_the_cluster_config(monkeypatch, tmp_path):
    # the service entries mount ./cluster.toml, so bootstrap must stage
    # the copy of the config it ran with beside the Compose file, with
    # the same staging (a failure leaves no half-copied config)
    monkeypatch.setattr(bootstrap, "COMPOSE_FILE", tmp_path / "compose.yaml")
    monkeypatch.setattr(bootstrap, "_existing_volumes", lambda: [])
    events: list[str] = []
    monkeypatch.setattr(bootstrap.subprocess, "run", _fake_docker(events))

    assert bootstrap._rebuild(yes=True, compose="C", proxy_list="P", cluster_toml="T") is True
    assert (tmp_path / "compose.yaml").read_text(encoding="utf-8") == "C"
    assert (tmp_path / "proxies.json").read_text(encoding="utf-8") == "P"
    assert (tmp_path / "cluster.toml").read_text(encoding="utf-8") == "T"
    assert not list(tmp_path.glob("*.tmp"))


def test_an_inherited_compose_profiles_never_starts_the_services(monkeypatch, tmp_path):
    # COMPOSE_PROFILES=services in the shell would activate the profile on
    # bootstrap's plain up and start the service containers during a data
    # load; _compose strips it from every call, everything else inherited
    monkeypatch.setenv("COMPOSE_PROFILES", "services")
    monkeypatch.setattr(bootstrap, "COMPOSE_FILE", tmp_path / "compose.yaml")
    monkeypatch.setattr(bootstrap, "_existing_volumes", lambda: [])
    calls: list[dict] = []

    def fake_run(argv, **kwargs):
        calls.append({"argv": list(argv), **kwargs})
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr(bootstrap.subprocess, "run", fake_run)

    assert bootstrap._rebuild(yes=True, compose="x", proxy_list="y", cluster_toml="z") is True
    ups = [call for call in calls if "up" in call["argv"]]
    assert ups, "bootstrap never called compose up"
    # the empty value in the process environment outranks both a shell
    # export and the .env that --env-file hands to Compose
    assert ups[0]["env"]["COMPOSE_PROFILES"] == ""
    assert "PATH" in ups[0]["env"]


def test_an_env_file_compose_profiles_never_starts_the_services(monkeypatch, tmp_path):
    # COMPOSE_PROFILES=services in the root .env reaches Compose through
    # --env-file; the empty value _compose exports blocks that source
    # too. The up names no profile, the teardown still passes its
    # explicit --profile services.
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("COMPOSE_PROFILES=services\n", encoding="utf-8")
    monkeypatch.delenv("COMPOSE_PROFILES", raising=False)
    # the old generated file exists, so the teardown runs its down on it
    (tmp_path / "compose.yaml").write_text("OLD", encoding="utf-8")
    monkeypatch.setattr(bootstrap, "COMPOSE_FILE", tmp_path / "compose.yaml")
    monkeypatch.setattr(bootstrap, "_existing_volumes", lambda: [])
    calls: list[dict] = []

    def fake_run(argv, **kwargs):
        calls.append({"argv": list(argv), **kwargs})
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr(bootstrap.subprocess, "run", fake_run)

    assert bootstrap._rebuild(yes=True, compose="x", proxy_list="y", cluster_toml="z") is True
    ups = [call for call in calls if "up" in call["argv"]]
    assert ups, "bootstrap never called compose up"
    assert "--env-file" in ups[0]["argv"]
    assert "--profile" not in ups[0]["argv"]
    assert ups[0]["env"]["COMPOSE_PROFILES"] == ""
    downs = [call for call in calls if "down" in call["argv"]]
    assert downs, "bootstrap never called compose down"
    for down in downs:
        assert down["argv"][down["argv"].index("--profile") + 1] == "services"


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

    assert bootstrap._rebuild(yes=True, compose="x", proxy_list="y", cluster_toml="z") is True
    # after the downs, the leftovers were removed by name, before the up
    assert events.index("rm:restaurantos_baseline-data") < events.index("up")
    assert "rm:restaurantos_r2-primary-data" in events
    assert events[-1] == "up"
    assert not list(tmp_path.glob("*.tmp"))  # the staged files were moved into place


def test_a_failed_volume_removal_stops_bootstrap(monkeypatch, tmp_path):
    events: list[str] = []
    monkeypatch.setattr(bootstrap, "COMPOSE_FILE", tmp_path / "compose.yaml")
    monkeypatch.setattr(
        bootstrap.subprocess,
        "run",
        _fake_docker_with_volumes(events, ["restaurantos_baseline-data"], volume_rm_fails=True),
    )

    with pytest.raises(bootstrap.BootstrapError, match="volume is in use"):
        bootstrap._rebuild(yes=True, compose="x", proxy_list="y", cluster_toml="z")
    assert "up" not in events
    # the staged files were removed with the failed teardown
    assert not list(tmp_path.glob("*.tmp"))


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
    return events


def test_a_synchronous_cluster_prepares_reloadable_settings_before_teardown(monkeypatch, tmp_path):
    _write_dataset(tmp_path)
    events = _no_docker(monkeypatch)
    prepared = bootstrap._prepare(Cluster(regions=1, synchronous=True), tmp_path, SIM)
    assert "ROS_SYNC_STANDBY: r1_standby" in prepared.compose
    assert "ROS_SYNC_STANDBY: hq_standby" in prepared.compose
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


# ---- staging (round 3, item 3) -------------------------------------------------


def test_a_staging_failure_tears_down_nothing(monkeypatch, tmp_path):
    events: list[str] = []
    monkeypatch.setattr(bootstrap, "COMPOSE_FILE", tmp_path / "compose.yaml")
    monkeypatch.setattr(bootstrap, "_existing_volumes", lambda: ["restaurantos_x-data"])

    def failing_stage(compose, proxy_list, cluster_toml):  # noqa: ANN001
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(bootstrap, "_stage_generated", failing_stage)
    monkeypatch.setattr(bootstrap.subprocess, "run", _fake_docker(events))
    with pytest.raises(OSError, match="No space left"):
        bootstrap._rebuild(yes=True, compose="x", proxy_list="y", cluster_toml="z")
    assert events == []  # no down, no up: the old cluster is untouched


def test_a_partial_staging_failure_removes_what_was_staged(monkeypatch, tmp_path):
    # the first file stages, the second write fails: the first .tmp
    # must not be left behind
    monkeypatch.setattr(bootstrap, "COMPOSE_FILE", tmp_path / "compose.yaml")
    real_write = Path.write_text

    def failing_write(self, data, encoding=None):
        if self.name == "proxies.json.tmp":
            raise OSError(28, "No space left on device")
        return real_write(self, data, encoding=encoding)

    monkeypatch.setattr(Path, "write_text", failing_write)
    with pytest.raises(OSError, match="No space left"):
        bootstrap._stage_generated("x", "y", "z")
    # the file that did stage was removed again
    assert not (tmp_path / "compose.yaml.tmp").exists()


# ---- column value rules (round 3, item 2) ---------------------------------------


def _typed_dataset(tmp_path, table: str, bad_row: str) -> None:
    _write_dataset(tmp_path, **{table: bad_row})


def test_a_negative_whole_number_fails_before_teardown(monkeypatch, tmp_path):
    _typed_dataset(
        tmp_path,
        "dishes.csv",
        "dish_id,dish_name,scope,stock_mode,n_products,n_vendors,example_name\n"
        "d1,noodles,independent,per_ingredient,-1,1,x\n",
    )
    events = _no_docker(monkeypatch)
    with pytest.raises(ValueError, match=r"dishes.n_products: d1: '-1'"):
        bootstrap._prepare(Cluster(regions=1), tmp_path, SIM)
    assert events == []


def test_a_zero_amount_fails_before_teardown(monkeypatch, tmp_path):
    _typed_dataset(
        tmp_path, "dish_ingredients.csv", "dish_id,ingredient_id,amount,unit\nd1,beef,0,kg\n"
    )
    events = _no_docker(monkeypatch)
    with pytest.raises(ValueError, match=r"dish_ingredients.amount: d1: '0'"):
        bootstrap._prepare(Cluster(regions=1), tmp_path, SIM)
    assert events == []


def test_a_non_numeric_price_fails_before_teardown(monkeypatch, tmp_path):
    _typed_dataset(
        tmp_path,
        "products.csv",
        "product_id,vendor_id,name,unit_price,price_units\np1,v1,noodles,cheap,10\n",
    )
    events = _no_docker(monkeypatch)
    with pytest.raises(ValueError, match=r"products.unit_price: p1: 'cheap'"):
        bootstrap._prepare(Cluster(regions=1), tmp_path, SIM)
    assert events == []


def test_a_non_boolean_flag_fails_before_teardown(monkeypatch, tmp_path):
    _typed_dataset(
        tmp_path,
        "product_dish.csv",
        "product_id,vendor_id,dish_id,multiplier,multiplier_ambiguous\np1,v1,d1,2,maybe\n",
    )
    events = _no_docker(monkeypatch)
    with pytest.raises(ValueError, match=r"product_dish.multiplier_ambiguous: p1: 'maybe'"):
        bootstrap._prepare(Cluster(regions=1), tmp_path, SIM)
    assert events == []


def test_an_unknown_stock_mode_fails_before_teardown(monkeypatch, tmp_path):
    _typed_dataset(
        tmp_path,
        "dishes.csv",
        "dish_id,dish_name,scope,stock_mode,n_products,n_vendors,example_name\n"
        "d1,noodles,independent,per_recipe,1,1,x\n",
    )
    events = _no_docker(monkeypatch)
    # the stock computation runs before the rows pass, so its explicit
    # stock_mode dispatch rejects the dish first (the column rule would
    # catch it too, one pass later)
    with pytest.raises(ValueError, match="dish d1: unknown stock_mode 'per_recipe'"):
        bootstrap._prepare(Cluster(regions=1), tmp_path, SIM)
    assert events == []


def test_a_short_geohash_fails_before_teardown(monkeypatch, tmp_path):
    _typed_dataset(
        tmp_path, "vendors.csv", "vendor_id,chain_id,geohash,primary_cuisine\nv1,,w21z,x\n"
    )
    events = _no_docker(monkeypatch)
    with pytest.raises(ValueError, match=r"vendors.geohash: v1: 'w21z'"):
        bootstrap._prepare(Cluster(regions=1), tmp_path, SIM)
    assert events == []


# ---- the product key is (vendor_id, product_id) (round 3, item 1) ---------------


def test_a_shared_product_id_slices_by_vendor(tmp_path):
    # two vendors share the product_id p1 with different dishes; a
    # region's slice must hold only its own link rows, or the load hits
    # a foreign-key violation on the other vendor's (vendor, product)
    _write_dataset(
        tmp_path,
        **{
            "vendors.csv": "vendor_id,chain_id,geohash,primary_cuisine\nv1,,w21z7,x\nv2,,w21z6,x\n",
            "products.csv": (
                "product_id,vendor_id,name,unit_price,price_units\n"
                "p1,v1,noodles,0.01,10\np1,v2,drink,0.02,10\n"
            ),
            "product_dish.csv": (
                "product_id,vendor_id,dish_id,multiplier,multiplier_ambiguous\n"
                "p1,v1,d1,2,false\np1,v2,d2,1,false\n"
            ),
        },
    )
    rows = reference_rows(tmp_path)
    v1_slice = slice_reference_rows(rows, frozenset({"v1"}))
    v2_slice = slice_reference_rows(rows, frozenset({"v2"}))
    assert v1_slice["products.csv"] == [["p1", "v1", "noodles", "0.01", "10"]]
    assert v1_slice["product_dish.csv"] == [["p1", "v1", "d1", "2", "false"]]
    assert v2_slice["products.csv"] == [["p1", "v2", "drink", "0.02", "10"]]
    assert v2_slice["product_dish.csv"] == [["p1", "v2", "d2", "1", "false"]]
    # together the slices are the whole, and nothing is duplicated
    assert len(v1_slice["product_dish.csv"]) + len(v2_slice["product_dish.csv"]) == len(
        rows["product_dish.csv"]
    )

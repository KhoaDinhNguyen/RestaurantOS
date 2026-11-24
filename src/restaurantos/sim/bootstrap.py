"""rosctl bootstrap: turn an empty machine into a loaded cluster.

Everything that can fail on bad input fails before anything is
destroyed: the manifest check, the shard assignment, the Compose
rendering, the stock computation and the reference reads (with the
unit check, the empty-field rule and the column value rules) all run
first. Only then does bootstrap ask about the existing volumes, stage
the rendered Compose files under temporary names, tear the old cluster
down — with the Compose file that is on disk, which may describe a
shape with more regions than the one about to be built — remove any
volumes the teardown files could not name, move the staged files into
place, start the cluster, wait for every node to be healthy, sync
Toxiproxy, write HQ's control rows, and load the data.

Reference data goes to every node: HQ and baseline hold all of it, each
region holds its own vendors and products plus full copies of the small
tables, cut from one read of the files. Stock comes from the warm-up
window only, and each region keeps counters only for its own vendors;
the baseline keeps them all. After loading, the rows in the database
are compared with the manifest, so a load that silently dropped rows
fails bootstrap.

Bootstrap is a batch job, not a request path: plain psycopg.AsyncConnection
straight to the node from the host (common.cluster.host_conninfo), no
Toxiproxy, no statement timeout, never common.db.make_pool.

Bootstrap always rebuilds from nothing (keeping state is what snapshots
are for): it deletes the project's volumes, after asking, unless --yes
was given. The prompt needs a terminal; without one, --yes is the only
way forward. A failed teardown stops bootstrap with the captured
stderr, before any file is written or started, so the Compose file on
disk never stops describing what is running.
"""

from __future__ import annotations

import asyncio
import csv
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import psycopg

from restaurantos.common.cluster import (
    BASELINE,
    HQ_PRIMARY,
    Cluster,
    host_conninfo,
    load_cluster,
    primary_of,
    proxies,
    standby_of,
)
from restaurantos.common.config import get_settings
from restaurantos.sim.config import SimSettings, get_sim_settings
from restaurantos.sim.datacheck import check_manifest
from restaurantos.sim.generator import compose_text, toxiproxy_config
from restaurantos.sim.loading import (
    REFERENCE_COLUMNS,
    REFERENCE_ORDER,
    apply_null_rule,
    check_data_rules,
    copy_async,
    reference_rows,
    slice_reference_rows,
    unit_conflicts,
)
from restaurantos.sim.shard import assign_shards, vendor_counts
from restaurantos.sim.stock import warm_up_stock
from restaurantos.sim.toxiproxy import Toxiproxy, ToxiproxyError

COMPOSE_FILE = Path("infra/generated/compose.yaml")
COMMITTED_COMPOSE = Path("infra/compose.yaml")
PROJECT = "restaurantos"


class BootstrapError(RuntimeError):
    """A bootstrap failure the user can act on: bad input, a failed
    teardown, a load that did not verify. The CLI prints these as a
    message. Genuine bugs (a broken invariant, for instance) keep their
    own exception and their traceback."""


def _compose(*args: str, compose_file: Path = COMPOSE_FILE) -> subprocess.CompletedProcess:
    command = ["docker", "compose", "-p", PROJECT]
    # Compose does not read the root .env by itself (docs/setup.md); pass
    # it so a ROS_POSTGRES_IMAGE override applies to bootstrap's `up` too.
    if Path(".env").is_file():
        command += ["--env-file", ".env"]
    command += ["-f", str(compose_file), *args]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise BootstrapError(
            f"{' '.join(command)} failed ({result.returncode}):\n{result.stdout}{result.stderr}"
        )
    return result


def _existing_volumes() -> list[str]:
    result = subprocess.run(
        ["docker", "volume", "ls", "--format", "{{.Name}}"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [name for name in result.stdout.splitlines() if name.startswith(f"{PROJECT}_")]


def _remove_volumes(names: list[str]) -> None:
    """Remove volumes by name. `down -v` only deletes the volumes the
    given file names, so volumes from a shape whose generated file is
    gone survive it; every name here was listed in the prompt, so
    removing them is what the user approved."""
    for name in names:
        result = subprocess.run(["docker", "volume", "rm", name], capture_output=True, text=True)
        if result.returncode != 0:
            raise BootstrapError(
                f"docker volume rm {name} failed ({result.returncode}):\n"
                f"{result.stdout}{result.stderr}"
            )


def _teardown() -> None:
    """Remove the running cluster and every project volume, with the
    Compose files that are on disk right now.

    Both `down` calls pass --profile services: the generated file's
    service entries are gated behind that profile, and without the flag
    a leftover service container survives the teardown as an orphan.
    On the committed baseline file the flag is inert (it has no service
    entries), but it keeps both calls consistent.

    The previously generated file goes first: switching from more
    regions to fewer must remove the services and volumes that the new
    file will not name, and only the old file still names them. The
    committed dev baseline goes second: it shares the project name, so
    without this an `up` after it would silently reuse its containers
    and their stale volumes. Whatever volumes survive both files — a
    shape whose generated file was deleted — is removed by name. A
    failure here stops bootstrap with the captured stderr, before
    anything is written or started.
    """
    for path in (COMPOSE_FILE, COMMITTED_COMPOSE):
        if path.is_file():
            _compose("--profile", "services", "down", "-v", "--remove-orphans", compose_file=path)
    _remove_volumes(_existing_volumes())


def _confirm_volume_deletion(volumes: list[str]) -> bool:
    """Ask on a terminal. Without one there is no way to ask, so refuse:
    --yes is the only way forward for scripts and agents."""
    print("These Docker volumes exist and would be deleted:")
    for name in sorted(volumes):
        print(f"  {name}")
    if not sys.stdin.isatty():
        print(
            "Refusing to delete them without a terminal to ask on; "
            "re-run with --yes to rebuild from nothing.",
            file=sys.stderr,
        )
        return False
    answer = input("Delete them and rebuild the cluster from nothing? [y/N] ")
    return answer.strip().lower() in ("y", "yes")


def _stage_generated(compose: str, proxy_list: str, cluster_toml: str) -> list[tuple[Path, Path]]:
    """Write the rendered files to temporary names, so a write failure
    (full disk, permissions) happens before anything is torn down. The
    temporary names differ from the real ones, so the teardown keeps
    reading the old compose.yaml. The third file is the copy of the
    config bootstrap ran with: the generated Compose file mounts it as
    cluster.toml, so the service containers agree with the shape that
    was built (and a stale file from a previous shape cannot be
    mounted)."""
    directory = COMPOSE_FILE.parent
    directory.mkdir(parents=True, exist_ok=True)
    staged: list[tuple[Path, Path]] = []
    try:
        for name, text in (
            ("compose.yaml", compose),
            ("proxies.json", proxy_list),
            ("cluster.toml", cluster_toml),
        ):
            tmp = directory / f"{name}.tmp"
            tmp.write_text(text, encoding="utf-8")
            staged.append((tmp, directory / name))
    except BaseException:
        for tmp, _ in staged:
            tmp.unlink(missing_ok=True)
        raise
    return staged


def _rebuild(*, yes: bool, compose: str, proxy_list: str, cluster_toml: str) -> bool:
    """Prompt about the volumes, stage the rendered Compose files, tear
    the old cluster down, move the staged files into place and start the
    cluster.

    The plain `up -d --wait` deliberately starts WITHOUT --profile
    services: the service entries are profile-gated, so a data load
    never starts the containers (they need the image, and the data is
    not in the databases yet); starting the services is the documented
    `docker compose ... --profile services up` (docs/setup.md).

    Returns False when the user refused. A staging failure happens
    before the teardown, so nothing has been torn down; a teardown
    failure removes the staged files and leaves the old generated files
    as they are.
    """
    volumes = _existing_volumes()
    if volumes and not yes and not _confirm_volume_deletion(volumes):
        return False
    staged = _stage_generated(compose, proxy_list, cluster_toml)
    try:
        _teardown()
    except BaseException:
        for tmp, _ in staged:
            tmp.unlink(missing_ok=True)
        raise
    for tmp, final in staged:
        os.replace(tmp, final)
    _compose("up", "-d", "--wait")
    return True


@dataclass
class Prepared:
    """Everything bootstrap computed from the input, before it destroyed
    anything: the render, the stock rows, the reference rows read once,
    the per-region slices, and the manifest row counts to verify the
    loaded database against."""

    shards: list[tuple[str, str]]
    compose: str
    proxy_list: str
    stock: list[tuple[str, str, str, int, int]]
    rows: dict[str, list[list[str]]]
    slices: dict[str, dict[str, list[list[str]]]]
    manifest: dict[str, int]


def _prepare(cluster: Cluster, data_dir: Path, sim: SimSettings) -> Prepared:
    """Every bit of work that only reads files, so a bad input leaves the
    running cluster untouched."""
    checked = check_manifest(data_dir)
    manifest = dict(checked)
    print(f"manifest verified: {', '.join(f'{n} ({r} rows)' for n, r in checked)}")

    counts = vendor_counts(data_dir)
    shards = assign_shards(counts, cluster.region_ids)
    print(
        f"shard directory: {len(shards)} cells across {len(cluster.region_ids)} "
        f"region(s), {sum(counts.values())} vendors"
    )

    compose = compose_text(cluster)
    proxy_list = toxiproxy_config(cluster)

    stock = warm_up_stock(
        data_dir,
        warm_days=sim.stock_warm_up_days,
        cover_days=sim.stock_cover_days,
        floor=sim.stock_floor,
    )

    rows = reference_rows(data_dir)
    conflicts = unit_conflicts(rows["dish_ingredients.csv"])
    if conflicts:
        raise ValueError(
            "an ingredient appears with two different units: " + ", ".join(sorted(conflicts))
        )
    for name in REFERENCE_ORDER:
        # the empty-field rule and the column value rules, applied up
        # front: a bad value fails here, before anything is destroyed
        apply_null_rule(name.removesuffix(".csv"), REFERENCE_COLUMNS[name], rows[name])
        check_data_rules(name.removesuffix(".csv"), REFERENCE_COLUMNS[name], rows[name])

    for name in REFERENCE_ORDER:
        expected = manifest.get(name)
        if expected != len(rows[name]):
            raise ValueError(
                f"{name}: {len(rows[name])} rows to load, the manifest verified {expected}"
            )

    vendors_by_cell = _vendors_by_cell(data_dir)
    region_of_vendor: dict[str, str] = {}
    for cell, rid in shards:
        for vendor in vendors_by_cell.get(cell, ()):
            region_of_vendor[vendor] = rid
    every_vendor = {row[0] for row in rows["vendors.csv"]}
    missing = sorted(every_vendor - region_of_vendor.keys())
    if missing:
        raise ValueError(
            f"{len(missing)} vendors are not covered by the shard directory: "
            + ", ".join(missing[:5])
            + ("..." if len(missing) > 5 else "")
        )

    slices: dict[str, dict[str, list[list[str]]]] = {}
    for rid in cluster.region_ids:
        vendors = frozenset(v for v, r in region_of_vendor.items() if r == rid)
        slices[rid] = slice_reference_rows(rows, vendors)
    if sum(len(s["vendors.csv"]) for s in slices.values()) != len(rows["vendors.csv"]):
        raise ValueError("the region vendor slices do not add up to the whole")
    return Prepared(shards, compose, proxy_list, stock, rows, slices, manifest)


async def _load_reference(conn: psycopg.AsyncConnection, rows: dict[str, list[list[str]]]) -> None:
    for name in REFERENCE_ORDER:
        await copy_async(conn, name.removesuffix(".csv"), REFERENCE_COLUMNS[name], rows[name])


async def _verify_loaded(
    conn: psycopg.AsyncConnection, manifest: dict[str, int], *, label: str
) -> None:
    """Compare the rows in the loaded database with the manifest counts."""
    for name in REFERENCE_ORDER:
        async with conn.cursor() as cur:
            await cur.execute(f"SELECT count(*) FROM {name.removesuffix('.csv')}")
            count = (await cur.fetchone())[0]
        if count != manifest[name]:
            raise ValueError(
                f"{label}: {name.removesuffix('.csv')} holds {count} rows, "
                f"the manifest verified {manifest[name]}"
            )


async def _connect(cluster: Cluster, node: str) -> psycopg.AsyncConnection:
    return await psycopg.AsyncConnection.connect(host_conninfo(cluster, node), autocommit=True)


async def bootstrap(*, yes: bool = False, config: Path = Path("infra/cluster.toml")) -> int:
    """Build and load the cluster described by `config`.

    Returns 0 when the cluster is up and loaded, 1 when the user refused
    to delete the existing volumes.
    """
    cluster = load_cluster(config)
    # The service entries in the generated Compose file mount this copy
    # of the config bootstrap actually ran with, so container mode
    # always agrees with the shape that was built (--config included).
    cluster_toml = config.read_text(encoding="utf-8")
    try:
        prepared = _prepare(cluster, get_settings().data_dir, get_sim_settings())
    except (ValueError, NotImplementedError) as error:
        # bad input or an unsupported cluster description: user-facing,
        # and nothing has been destroyed yet
        raise BootstrapError(str(error)) from error

    if not _rebuild(
        yes=yes, compose=prepared.compose, proxy_list=prepared.proxy_list, cluster_toml=cluster_toml
    ):
        return 1
    print(f"wrote {COMPOSE_FILE}, infra/generated/proxies.json and infra/generated/cluster.toml")
    print("every node is healthy")

    try:
        return await _load_cluster(cluster, prepared)
    except (ToxiproxyError, ValueError, psycopg.Error) as error:
        # the cluster is up by now, so the user must know that re-running
        # bootstrap is the way forward; genuine bugs (a broken invariant)
        # keep their own exception and traceback
        raise BootstrapError(
            f"{error}\nThe cluster is running but not fully loaded; run bootstrap again."
        ) from error


async def _load_cluster(cluster: Cluster, prepared: Prepared) -> int:
    """Everything after `up`: sync, HQ control rows, and the loads."""
    async with Toxiproxy() as tox:
        for attempt in range(30):
            try:
                await tox.sync(cluster)
                break
            except ToxiproxyError:
                # the API needs a moment after `up`; anything else is a
                # bug in sync and fails at once, not after 30 seconds
                if attempt == 29:
                    raise
                await asyncio.sleep(1)
    print(f"toxiproxy: {len(proxies(cluster))} proxies, all enabled")

    rows = prepared.rows
    stock = prepared.stock
    loaded: list[str] = []

    # HQ: the control rows, then the full reference data.
    region_ids = cluster.region_ids
    hq = await _connect(cluster, HQ_PRIMARY)
    async with hq:
        for rid in region_ids:
            await hq.execute(
                "INSERT INTO region_state (region_id, primary_node, standby_node) "
                "VALUES (%s, %s, %s)",
                (rid, primary_of(rid), standby_of(rid)),
            )
        async with hq.cursor() as cur:
            await cur.executemany(
                "INSERT INTO shard_directory (geohash, region_id) VALUES (%s, %s)",
                prepared.shards,
            )
        await _load_reference(hq, rows)
        await _verify_loaded(hq, prepared.manifest, label="hq-primary")
    loaded.append(
        f"hq-primary: region_state {len(region_ids)}, "
        f"shard_directory {len(prepared.shards)}, full reference"
    )

    # Baseline: full reference data, and every region's stock.
    baseline = await _connect(cluster, BASELINE)
    async with baseline:
        await _load_reference(baseline, rows)
        await _verify_loaded(baseline, prepared.manifest, label=BASELINE)
        await copy_async(
            baseline, "stock", ("vendor_id", "item_kind", "item_id", "on_hand", "par_level"), stock
        )
    loaded.append(f"baseline: reference + {len(stock)} stock rows")

    # Regions: their slice of the reference data, and their own stock.
    region_vendor_counts: list[int] = []
    for rid in region_ids:
        slice_ = prepared.slices[rid]
        vendors = {row[0] for row in slice_["vendors.csv"]}
        node = primary_of(rid)
        region = await _connect(cluster, node)
        async with region:
            await _load_reference(region, slice_)
            async with region.cursor() as cur:
                await cur.execute("SELECT count(*) FROM vendors")
                region_vendor_counts.append((await cur.fetchone())[0])
            stock_rows = [row for row in stock if row[0] in vendors]
            await copy_async(
                region,
                "stock",
                ("vendor_id", "item_kind", "item_id", "on_hand", "par_level"),
                stock_rows,
            )
        loaded.append(f"{node}: {len(vendors)} vendors, {len(stock_rows)} stock rows")

    if sum(region_vendor_counts) != prepared.manifest["vendors.csv"]:
        raise ValueError(
            f"the regions hold {sum(region_vendor_counts)} vendors together, "
            f"HQ holds {prepared.manifest['vendors.csv']}"
        )
    for line in loaded:
        print(line)
    return 0


def _vendors_by_cell(data_dir: Path) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    with (data_dir / "vendors.csv").open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            out.setdefault(row["geohash"], []).append(row["vendor_id"])
    return out

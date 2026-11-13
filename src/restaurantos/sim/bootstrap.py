"""rosctl bootstrap: turn an empty machine into a loaded cluster.

The steps are the WP6a brief's, in its order: verify the data directory
against MANIFEST.csv, generate infra/generated/, rebuild the Compose
project from empty volumes, wait for every node to be healthy, sync
Toxiproxy, write HQ's control rows, and load the data with
`restaurantos.sim.loading`'s rules. Reference data goes to every node:
HQ and baseline hold all of it, each region holds its own vendors and
products plus full copies of the small tables. Stock comes from the
warm-up window only, and each region keeps counters only for its own
vendors; the baseline keeps them all.

Bootstrap is a batch job, not a request path: plain psycopg.AsyncConnection
straight to the node from the host (common.cluster.host_conninfo), no
Toxiproxy, no statement timeout, never common.db.make_pool.

Bootstrap always rebuilds from nothing (keeping state is what snapshots
are for): it deletes the project's volumes, after asking, unless --yes
was given. The prompt needs a terminal; without one, --yes is the only
way forward.
"""

from __future__ import annotations

import asyncio
import csv
import subprocess
import sys
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
from restaurantos.sim.config import get_sim_settings
from restaurantos.sim.datacheck import check_manifest
from restaurantos.sim.generator import generate
from restaurantos.sim.loading import (
    REFERENCE_COLUMNS,
    REFERENCE_ORDER,
    copy_async,
    reference_rows,
    unit_conflicts,
)
from restaurantos.sim.shard import assign_shards, vendor_counts
from restaurantos.sim.stock import warm_up_stock
from restaurantos.sim.toxiproxy import Toxiproxy

COMPOSE_FILE = Path("infra/generated/compose.yaml")
PROJECT = "restaurantos"


def _docker(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    command = ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE_FILE), *args]
    result = subprocess.run(command, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(
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


async def _load_reference(
    conn: psycopg.AsyncConnection, data_dir: Path, *, vendors: frozenset[str] | None
) -> None:
    rows = reference_rows(data_dir, vendors=vendors)
    conflicts = unit_conflicts(rows["dish_ingredients.csv"])
    if conflicts:
        raise ValueError(
            "an ingredient appears with two different units: " + ", ".join(sorted(conflicts))
        )
    for name in REFERENCE_ORDER:
        await copy_async(conn, name.removesuffix(".csv"), REFERENCE_COLUMNS[name], rows[name])


async def _connect(cluster: Cluster, node: str) -> psycopg.AsyncConnection:
    return await psycopg.AsyncConnection.connect(host_conninfo(cluster, node), autocommit=True)


async def bootstrap(*, yes: bool = False, config: Path = Path("infra/cluster.toml")) -> int:
    """Build and load the cluster described by `config`.

    Returns 0 when the cluster is up and loaded, 1 when the user refused
    to delete the existing volumes.
    """
    cluster = load_cluster(config)
    data_dir = get_settings().data_dir
    sim = get_sim_settings()

    checked = check_manifest(data_dir)
    print(f"manifest verified: {', '.join(f'{n} ({r} rows)' for n, r in checked)}")

    counts = vendor_counts(data_dir)
    shards = assign_shards(counts, cluster.region_ids)
    print(
        f"shard directory: {len(shards)} cells across {len(cluster.region_ids)} "
        f"region(s), {sum(counts.values())} vendors"
    )

    generate(cluster, COMPOSE_FILE.parent)
    print(f"wrote {COMPOSE_FILE} and infra/generated/proxies.json")

    volumes = _existing_volumes()
    if volumes and not yes and not _confirm_volume_deletion(volumes):
        return 1
    _docker("down", "-v", check=False)
    _docker("up", "-d", "--wait")
    print("every node is healthy")

    tox = Toxiproxy()
    for attempt in range(30):
        try:
            await tox.sync(cluster)
            break
        except Exception:  # noqa: BLE001 - the API needs a moment after start
            if attempt == 29:
                raise
            await asyncio.sleep(1)
    await tox.aclose()
    print(f"toxiproxy: {len(proxies(cluster))} proxies, all enabled")

    stock = warm_up_stock(
        data_dir,
        warm_days=sim.stock_warm_up_days,
        cover_days=sim.stock_cover_days,
        floor=sim.stock_floor,
    )
    loaded: list[str] = []

    # HQ: the control rows, then the full reference data.
    region_ids = cluster.region_ids
    region_of_vendor: dict[str, str] = {}
    vendors_by_cell = _vendors_by_cell(data_dir)
    for cell, rid in shards:
        for vendor in vendors_by_cell.get(cell, ()):
            region_of_vendor[vendor] = rid

    hq = await _connect(cluster, HQ_PRIMARY)
    async with hq:
        for rid in region_ids:
            await hq.execute(
                "INSERT INTO region_state (region_id, primary_node, standby_node) "
                "VALUES (%s, %s, %s)",
                (rid, primary_of(rid), standby_of(rid)),
            )
        await hq.executemany(
            "INSERT INTO shard_directory (geohash, region_id) VALUES (%s, %s)",
            shards,
        )
        await _load_reference(hq, data_dir, vendors=None)
    loaded.append(
        f"hq-primary: region_state {len(region_ids)}, shard_directory {len(shards)}, full reference"
    )

    # Baseline: full reference data, and every region's stock.
    baseline = await _connect(cluster, BASELINE)
    async with baseline:
        await _load_reference(baseline, data_dir, vendors=None)
        await copy_async(
            baseline, "stock", ("vendor_id", "item_kind", "item_id", "on_hand", "par_level"), stock
        )
    loaded.append(f"baseline: reference + {len(stock)} stock rows")

    # Regions: their slice of the reference data, and their own stock.
    for rid in region_ids:
        vendors = frozenset(v for v, r in region_of_vendor.items() if r == rid)
        node = primary_of(rid)
        region = await _connect(cluster, node)
        async with region:
            await _load_reference(region, data_dir, vendors=vendors)
            rows = [row for row in stock if row[0] in vendors]
            await copy_async(
                region, "stock", ("vendor_id", "item_kind", "item_id", "on_hand", "par_level"), rows
            )
        loaded.append(f"{node}: {len(vendors)} vendors, {len(rows)} stock rows")

    for line in loaded:
        print(line)
    return 0


def _vendors_by_cell(data_dir: Path) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    with (data_dir / "vendors.csv").open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            out.setdefault(row["geohash"], []).append(row["vendor_id"])
    return out

"""Which region a vendor belongs to, and where that region's edge API is.

Shared by every playback feature that has to answer "which region is
this order's vendor in?" without a database: the single-region mode
filters the replay stream with it here, and the playback extras route an
order's status events to its own region's API through the same map, so
both derive the split from one reviewed place instead of duplicating it.

Both answers are derived, never typed by hand. The region split is
recomputed from the data files with the exact functions bootstrap used
to write ``shard_directory`` (``shard.assign_shards`` over the
vendors-per-cell counts): reading the directory back from HQ would make
playback depend on a database and on the cluster being healthy, and the
recomputation cannot disagree with the cluster because bootstrap wrote
its rows from the same functions over the same files. The edge URL
comes from ``common.cluster`` plus the ``/api/v1`` prefix the gateway
serves (architecture Section 6.1); the edge app registers the same
routes on that same prefix in edge mode (architecture Section 6).
``rosctl`` always runs on the Docker host (Section 4), so the address is
the host one.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from restaurantos.common.cluster import HOST_ADDRESS, Cluster, service_port
from restaurantos.sim.replaydata import iter_orders
from restaurantos.sim.shard import assign_shards

API_PREFIX = "/api/v1"


@dataclass(frozen=True)
class RegionShare:
    """One pass over the cache: the region's share, and who is missing.

    `unknown_orders` counts cached orders whose vendor is not in the
    map at all: an order like that belongs to no region and would be
    silently dropped from every region's run, so the caller refuses to
    run rather than replay a hole.
    """

    region_orders: int
    window_orders: int
    unknown_orders: int
    unknown_example: str | None


def vendor_region_map(
    pairs: list[tuple[str, str]], counts: dict[str, int], cluster: Cluster
) -> dict[str, str]:
    """vendor -> its region, built exactly the way bootstrap shards it.

    The same ``assign_shards`` walk over ``cluster.region_ids``, then each
    vendor's geohash looked up in the result. ``pairs`` are the
    vendor/geohash rows (``playback_cli.restock_vendors`` reads
    vendors.csv and checks it against the manifest, and stays the only
    reader of that file), and ``counts`` is the vendors-per-cell count of
    those same pairs (``shard.counts_by_cell``), so the map cannot drift
    from bootstrap's sharding. Deterministic, and no HQ call at run
    start.
    """

    cell_region = dict(assign_shards(counts, cluster.region_ids))
    return {vendor: cell_region[geohash] for vendor, geohash in pairs}


def region_order_counts(
    cache_file: Path,
    vendor_region: dict[str, str],
    region: str,
    *,
    from_day: int,
    to_day: int,
) -> RegionShare:
    """The region's share of the cache in one streaming pass.

    `region_orders` is the whole-cache count (what the share of the
    split is); `window_orders` is the same restricted to the run's day
    window (what the run will actually send). Both are printed by the
    CLI so a wrong split shows before anything is sent, and both go
    into the run's meta so a run log can be reconciled against them.
    """
    region_orders = 0
    window_orders = 0
    unknown_orders = 0
    unknown_example: str | None = None
    for order in iter_orders(cache_file):
        owner = vendor_region.get(order["vendor_id"])
        if owner is None:
            unknown_orders += 1
            if unknown_example is None:
                unknown_example = order["vendor_id"]
        elif owner == region:
            region_orders += 1
            if from_day <= order["order_day"] <= to_day:
                window_orders += 1
    return RegionShare(
        region_orders=region_orders,
        window_orders=window_orders,
        unknown_orders=unknown_orders,
        unknown_example=unknown_example,
    )


def edge_target_url(cluster: Cluster, region_id: str) -> str:
    """The region's edge API base, as seen from the Docker host.

    The port comes from ``common.cluster.service_port`` (8010 + n, the
    same port ``rosctl up`` starts the service on), never typed here.
    """
    if region_id not in cluster.region_ids:
        raise KeyError(f"no region {region_id!r} in this cluster")
    port = service_port(cluster, f"edge-{region_id}")
    return f"http://{HOST_ADDRESS}:{port}{API_PREFIX}"

"""Which region a vendor belongs to, and where that region's edge API is.

Shared by every playback feature that has to answer "which region is
this order's vendor in?" without a database: the single-region mode
filters the replay stream with it here, and the playback extras route an
order's status events to its own region's API through the same map, so
both derive the split from one reviewed place instead of duplicating it.

Both answers are derived, never typed by hand. The region split is
recomputed from the data files with the exact functions bootstrap used
to write ``shard_directory`` (``shard.vendor_counts`` +
``shard.assign_shards``): reading the directory back from HQ would make
playback depend on a database and on the cluster being healthy, and the
recomputation cannot disagree with the cluster because bootstrap wrote
its rows from the same two functions over the same files. The edge URL
comes from ``common.cluster`` plus the fixed ``/api/v1`` prefix the
gateway serves (architecture Section 6.1); the edge API is the same code
in edge mode (Section 4), so it serves the same prefix, and ``rosctl``
always runs on the Docker host (Section 4), so the address is the host
one.
"""

from __future__ import annotations

import csv
from collections.abc import Sequence
from pathlib import Path

from restaurantos.common.cluster import HOST_ADDRESS, Cluster, service_port
from restaurantos.sim.shard import assign_shards, vendor_counts

API_PREFIX = "/api/v1"


def vendor_region_map(data_dir: Path, regions: Sequence[str]) -> dict[str, str]:
    """vendor_id -> region_id, from the split bootstrap itself writes.

    Raises ValueError when a vendor's cell has no region: the split and
    vendors.csv are read from the same directory in one call, so that
    only happens if the files changed underneath the run, and it must
    be loud rather than quietly dropping or double-counting a vendor.
    """
    cell_region = dict(assign_shards(vendor_counts(data_dir), regions))
    out: dict[str, str] = {}
    with (data_dir / "vendors.csv").open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cell = row["geohash"]
            if cell not in cell_region:
                raise ValueError(
                    f"vendor {row['vendor_id']} is in cell {cell}, which the shard split "
                    "did not assign: vendors.csv and the split disagree"
                )
            out[row["vendor_id"]] = cell_region[cell]
    return out


def edge_target_url(cluster: Cluster, region_id: str) -> str:
    """The region's edge API base, as seen from the Docker host.

    The port comes from ``common.cluster.service_port`` (8010 + n, the
    same port ``rosctl up`` starts the service on), never typed here.
    """
    if region_id not in cluster.region_ids:
        raise KeyError(f"no region {region_id!r} in this cluster")
    port = service_port(cluster, f"edge-{region_id}")
    return f"http://{HOST_ADDRESS}:{port}{API_PREFIX}"

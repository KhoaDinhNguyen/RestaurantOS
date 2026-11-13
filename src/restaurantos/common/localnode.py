"""Which of a region's two nodes is its primary, seen from inside the region.

The edge API and the relay sit next to their region's nodes and connect to
them directly. They cannot rely on HQ to tell them about a failover (the
region may be cut off from HQ), so they look at both local nodes themselves:
at start, on a timer, and after any database error.

This module is the rule only. It does no I/O, so every process applies the
same rule; restaurantos.common.db.find_local_primary does the looking.
Standard library only.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from restaurantos.common.cluster import Cluster, primary_of, standby_of


@dataclass(frozen=True)
class NodeView:
    """What one look at a node showed. The defaults describe a node that did not answer."""

    node: str
    reachable: bool = False
    in_recovery: bool = True
    epoch: int = 0

    @property
    def writable(self) -> bool:
        return self.reachable and not self.in_recovery


def local_nodes(cluster: Cluster, region_id: str) -> list[str]:
    """The nodes a region-local process has to look at."""
    if region_id not in cluster.region_ids:
        raise KeyError(f"no region {region_id!r} in this cluster")
    nodes = [primary_of(region_id)]
    if cluster.standbys:
        nodes.append(standby_of(region_id))
    return nodes


def pick_local_primary(current: str | None, views: Iterable[NodeView]) -> str | None:
    """The node to use from now on, or None if there is none to use yet.

    - A node that does not answer, or is in recovery (a standby), is never used.
    - One writable node: use it.
    - Two writable nodes: one of them is an old primary that was cut off or
      restarted without being rejoined. The coordinator writes the new epoch
      to the node it promotes, so the higher epoch is the real primary.
    - Two writable nodes with the same epoch: a promotion is half done (the
      standby is promoted, its epoch not yet written). Stay on `current` if it
      is one of them; never move on a tie. With no `current`, wait.

    None means "answer 503" for the edge API and "sleep and look again" for
    the relay. It does not mean "keep using the old node".
    """
    writable = [view for view in views if view.writable]
    if not writable:
        return None
    best = max(view.epoch for view in writable)
    leaders = [view.node for view in writable if view.epoch == best]
    if len(leaders) == 1:
        return leaders[0]
    return current if current in leaders else None

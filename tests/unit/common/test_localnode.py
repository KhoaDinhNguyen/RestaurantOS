import pytest

from restaurantos.common.cluster import Cluster
from restaurantos.common.localnode import NodeView, local_nodes, pick_local_primary

P, S = "r1-primary", "r1-standby"


def primary(node, epoch):
    return NodeView(node, reachable=True, in_recovery=False, epoch=epoch)


def standby(node, epoch):
    return NodeView(node, reachable=True, in_recovery=True, epoch=epoch)


def down(node):
    return NodeView(node)


def test_local_nodes():
    assert local_nodes(Cluster(regions=2), "r2") == ["r2-primary", "r2-standby"]
    assert local_nodes(Cluster(regions=1, standbys=False), "r1") == ["r1-primary"]
    with pytest.raises(KeyError):
        local_nodes(Cluster(regions=1), "r2")


@pytest.mark.parametrize("current", [None, P, S])
def test_normal_running_uses_the_primary(current):
    assert pick_local_primary(current, [primary(P, 1), standby(S, 1)]) == P


def test_single_region_mode_has_no_standby():
    assert pick_local_primary(None, [primary(P, 1)]) == P


def test_killed_primary_before_the_promotion_leaves_nothing_to_use():
    # T1b: the standby is still in recovery, so writes must wait for the coordinator.
    assert pick_local_primary(P, [down(P), standby(S, 1)]) is None


def test_killed_primary_after_the_promotion_moves_to_the_standby():
    assert pick_local_primary(P, [down(P), primary(S, 2)]) == S


def test_cut_off_primary_loses_to_the_promoted_standby():
    # T1: the old primary is alive and still says it is a primary, at the old epoch.
    assert pick_local_primary(P, [primary(P, 1), primary(S, 2)]) == S


def test_old_primary_restarted_without_a_rejoin_is_ignored():
    assert pick_local_primary(S, [primary(P, 1), primary(S, 2)]) == S


def test_old_primary_rejoined_as_a_standby():
    assert pick_local_primary(S, [standby(P, 2), primary(S, 2)]) == S


def test_half_done_promotion_never_moves_anyone():
    # Promoted, but the coordinator has not written the new epoch yet.
    views = [primary(P, 1), primary(S, 1)]
    assert pick_local_primary(P, views) == P
    assert pick_local_primary(S, views) == S
    assert pick_local_primary(None, views) is None


def test_nothing_answers():
    assert pick_local_primary(P, [down(P), down(S)]) is None
    assert pick_local_primary(None, []) is None


def test_view_order_does_not_matter():
    assert pick_local_primary(P, [primary(S, 2), primary(P, 1)]) == S

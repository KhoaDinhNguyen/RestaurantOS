"""The fault layer against a fake Toxiproxy: ids, overlap, rollback."""

from __future__ import annotations

import pytest
from fake_toxiproxy import FakeToxiproxyServer

from restaurantos.common.cluster import Cluster, link_groups, proxies, proxy_name
from restaurantos.sim.faults import (
    FaultExistsError,
    FaultLayer,
    FaultParamsError,
    UnknownFaultError,
    UnknownGroupError,
    normalize_params,
)
from restaurantos.sim.toxiproxy import ToxiproxyError

CLUSTER = Cluster(regions=1)
SHARED_REPL = proxy_name("r1-standby", "r1-primary")  # in node:r1-primary and repl:r1
SHARED_GATEWAY = proxy_name("gateway", "r1-primary")  # in node:r1-primary and link:r1<->hq


@pytest.fixture
def server() -> FakeToxiproxyServer:
    return FakeToxiproxyServer(proxy.name for proxy in proxies(CLUSTER))


@pytest.fixture
async def layer(server: FakeToxiproxyServer):
    client = server.client()
    yield FaultLayer(client, link_groups(CLUSTER))
    await client.aclose()


def test_the_overlap_this_layer_handles_exists() -> None:
    groups = link_groups(CLUSTER)
    assert SHARED_REPL in groups["node:r1-primary"] and SHARED_REPL in groups["repl:r1"]
    assert SHARED_GATEWAY in groups["node:r1-primary"]
    assert SHARED_GATEWAY in groups["link:r1<->hq"]


@pytest.mark.parametrize(
    ("fault", "params", "expected"),
    [
        ("cut", None, {}),
        ("silence", {}, {}),
        ("latency", {"latency_ms": 4000}, {"latency_ms": 4000, "jitter_ms": 0}),
        ("latency", {"latency_ms": 10, "jitter_ms": 3}, {"latency_ms": 10, "jitter_ms": 3}),
        ("reset", None, {"timeout_ms": 0}),
        ("reset", {"timeout_ms": 250}, {"timeout_ms": 250}),
    ],
)
def test_params_follow_section_6_3(fault, params, expected) -> None:
    assert normalize_params(fault, params) == expected


@pytest.mark.parametrize(
    ("fault", "params", "message"),
    [
        ("latency", {}, "needs latency_ms"),
        ("latency", {"latency_ms": -1}, "0 or more"),
        ("latency", {"latency_ms": 1.5}, "whole number"),
        ("latency", {"latency_ms": True}, "whole number"),
        ("cut", {"latency_ms": 5}, "does not take"),
        ("silence", {"timeout_ms": 0}, "does not take"),
        ("flood", {}, "unknown fault"),
    ],
)
def test_bad_params_are_refused(fault, params, message) -> None:
    with pytest.raises(FaultParamsError, match=message):
        normalize_params(fault, params)


async def test_latency_adds_named_toxics_in_both_directions_and_heals(layer, server) -> None:
    fault = await layer.create("link:r1<->hq", "latency", {"latency_ms": 4000})
    assert fault.fault_id == "f1"
    assert fault.as_dict()["params"] == {"latency_ms": 4000, "jitter_ms": 0}
    for proxy in fault.proxies:
        assert server.toxic_names(proxy) == ["f1-latency-down", "f1-latency-up"]
        toxic = server.proxies[proxy]["toxics"][0]
        assert toxic["type"] == "latency"
        assert toxic["attributes"] == {"latency": 4000, "jitter": 0}
    await layer.heal("f1")
    assert all(server.toxic_names(proxy) == [] for proxy in fault.proxies)
    assert layer.faults() == []


@pytest.mark.parametrize(
    ("fault", "toxic_type", "attributes"),
    [
        ("silence", "timeout", {"timeout": 0}),
        ("reset", "reset_peer", {"timeout": 0}),
    ],
)
async def test_silence_and_reset_use_their_toxics(layer, server, fault, toxic_type, attributes):
    created = await layer.create("node:r1-primary", fault)
    toxics = server.proxies[created.proxies[0]]["toxics"]
    assert {toxic["stream"] for toxic in toxics} == {"upstream", "downstream"}
    assert {toxic["type"] for toxic in toxics} == {toxic_type}
    assert all(toxic["attributes"] == attributes for toxic in toxics)


async def test_cut_disables_the_group_and_heal_enables_it(layer, server) -> None:
    fault = await layer.create("link:r1<->hq", "cut")
    assert fault.toxics == ()
    assert all(not server.proxies[proxy]["enabled"] for proxy in fault.proxies)
    await layer.heal(fault.fault_id)
    assert all(server.proxies[proxy]["enabled"] for proxy in fault.proxies)


async def test_overlapping_cuts_keep_the_shared_proxy_cut_until_the_last_heal(
    layer, server
) -> None:
    node = await layer.create("node:r1-primary", "cut")
    repl = await layer.create("repl:r1", "cut")
    assert layer.cut_count(SHARED_REPL) == 2
    await layer.heal(node.fault_id)
    assert not server.proxies[SHARED_REPL]["enabled"]  # repl:r1 still holds it
    assert layer.cut_count(SHARED_REPL) == 1
    assert server.proxies[SHARED_GATEWAY]["enabled"]  # only node:r1-primary held this one
    await layer.heal(repl.fault_id)
    assert server.proxies[SHARED_REPL]["enabled"]
    assert layer.cut_count(SHARED_REPL) == 0


async def test_a_failed_enable_leaves_no_stale_count_and_is_reported(layer, server) -> None:
    """A heal whose enable fails partway: every count is already zero,
    the proxies that could be enabled are, and the stuck one shows up
    as unexplained in links()."""
    members = link_groups(CLUSTER)["repl:r1"]
    await layer.create("repl:r1", "cut")
    # the enable for the first member fails
    server.fail.add(("POST", f"/proxies/{members[0]}"))
    with pytest.raises(ToxiproxyError, match="only partly healed"):
        await layer.heal("f1")
    assert all(layer.cut_count(proxy) == 0 for proxy in members)
    assert server.proxies[members[0]]["enabled"] is False  # the failed enable
    assert server.proxies[members[1]]["enabled"] is True  # still enabled
    stuck = members[0]
    links = {entry["group"]: entry for entry in await layer.links()}
    for group, entry in links.items():
        expected = stuck if stuck in link_groups(CLUSTER)[group] else None
        assert entry["unexplained"]["disabled"] == ([expected] if expected else [])


async def test_overlapping_toxics_do_not_collide_and_heal_separately(layer, server) -> None:
    first = await layer.create("node:r1-primary", "latency", {"latency_ms": 100})
    second = await layer.create("link:r1<->hq", "latency", {"latency_ms": 200})
    assert server.toxic_names(SHARED_GATEWAY) == [
        "f1-latency-down",
        "f1-latency-up",
        "f2-latency-down",
        "f2-latency-up",
    ]
    await layer.heal(first.fault_id)
    assert server.toxic_names(SHARED_GATEWAY) == ["f2-latency-down", "f2-latency-up"]
    await layer.heal(second.fault_id)
    assert server.toxic_names(SHARED_GATEWAY) == []


async def test_a_second_fault_of_one_kind_on_one_group_is_refused(layer) -> None:
    await layer.create("repl:r1", "silence")
    with pytest.raises(FaultExistsError) as caught:
        await layer.create("repl:r1", "silence")
    assert caught.value.fault_id == "f1"
    # a different kind on the same group is fine
    await layer.create("repl:r1", "cut")


async def test_unknown_group_and_fault_are_refused(layer) -> None:
    with pytest.raises(UnknownGroupError):
        await layer.create("node:r9-primary", "cut")
    with pytest.raises(UnknownFaultError):
        await layer.heal("f42")


async def test_heal_all_resets_toxiproxy_and_forgets_every_fault(layer, server) -> None:
    await layer.create("node:r1-primary", "cut")
    await layer.create("repl:r1", "latency", {"latency_ms": 50})
    assert await layer.heal_all() == 2
    assert layer.faults() == []
    assert layer.cut_count(SHARED_REPL) == 0
    assert all(proxy["enabled"] and proxy["toxics"] == [] for proxy in server.proxies.values())


async def test_links_list_known_faults_and_what_no_fault_explains(layer, server) -> None:
    known = await layer.create("repl:r1", "latency", {"latency_ms": 50})
    # what a previous control process left behind: a disabled proxy and a toxic
    server.proxies[SHARED_GATEWAY]["enabled"] = False
    server.proxies[SHARED_GATEWAY]["toxics"].append(
        {"name": "f7-silence-up", "type": "timeout", "stream": "upstream", "attributes": {}}
    )
    links = {entry["group"]: entry for entry in await layer.links()}
    assert set(links) == set(link_groups(CLUSTER))
    repl = links["repl:r1"]
    assert [fault["fault_id"] for fault in repl["faults"]] == [known.fault_id]
    assert repl["unexplained"] == {"disabled": [], "toxics": [], "missing": []}
    link = links["link:r1<->hq"]
    assert link["faults"] == []
    assert link["unexplained"]["disabled"] == [SHARED_GATEWAY]
    assert link["unexplained"]["toxics"] == [
        {"proxy": SHARED_GATEWAY, "name": "f7-silence-up", "type": "timeout", "stream": "upstream"}
    ]


async def test_a_cut_held_by_a_known_fault_is_not_unexplained(layer) -> None:
    await layer.create("node:r1-primary", "cut")
    links = {entry["group"]: entry for entry in await layer.links()}
    assert links["link:r1<->hq"]["unexplained"]["disabled"] == []


async def test_a_failed_toxic_rolls_back_the_ones_already_added(layer, server) -> None:
    groups = link_groups(CLUSTER)
    members = groups["link:r1<->hq"]
    server.fail.add(("POST", f"/proxies/{members[-1]}/toxics"))
    with pytest.raises(ToxiproxyError):
        await layer.create("link:r1<->hq", "silence")
    assert all(server.toxic_names(proxy) == [] for proxy in members)
    assert layer.faults() == []


async def test_a_failed_cut_rolls_back_counts_and_proxies(layer, server) -> None:
    members = link_groups(CLUSTER)["link:r1<->hq"]
    server.fail.add(("POST", f"/proxies/{members[-1]}"))
    with pytest.raises(ToxiproxyError):
        await layer.create("link:r1<->hq", "cut")
    assert all(server.proxies[proxy]["enabled"] for proxy in members)
    assert all(layer.cut_count(proxy) == 0 for proxy in members)
    assert layer.faults() == []


async def test_a_new_layer_has_no_ids_but_sees_the_leftovers(server) -> None:
    """A control-process restart: the ids are gone, the toxics stay."""
    first = server.client()
    await FaultLayer(first, link_groups(CLUSTER)).create("repl:r1", "silence")
    await first.aclose()
    second = server.client()
    fresh = FaultLayer(second, link_groups(CLUSTER))
    with pytest.raises(UnknownFaultError):
        await fresh.heal("f1")
    links = {entry["group"]: entry for entry in await fresh.links()}
    assert len(links["repl:r1"]["unexplained"]["toxics"]) == 4  # 2 proxies x 2 directions
    assert await fresh.heal_all() == 0
    assert all(proxy["toxics"] == [] for proxy in server.proxies.values())
    await second.aclose()

"""Regional drain loops stay independent while one destination is down."""

import asyncio
from contextlib import asynccontextmanager

import pytest

from restaurantos.common.cluster import Cluster
from restaurantos.common.config import Settings
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.relay import holding
from restaurantos.relay.config import RelaySettings


async def test_unreachable_region_does_not_stop_another_region(monkeypatch):
    healthy_region_ran = asyncio.Event()
    failed_region_finished = asyncio.Event()
    pools = []
    real_sleep = asyncio.sleep

    @asynccontextmanager
    async def pool(*args):
        instance = object()
        pools.append(instance)
        yield instance

    class StopWorker(Exception):
        pass

    async def drain(hq_pool, cluster, region_id, settings, relay, place_order):
        if region_id == "r1":
            # This region cannot proceed until the healthy task gets CPU time.
            await healthy_region_ran.wait()
            failed_region_finished.set()
            raise RegionUnreachableError(region_id=region_id)
        healthy_region_ran.set()
        await failed_region_finished.wait()
        return 0

    async def stop_after_poll(seconds):
        await real_sleep(0)
        raise StopWorker

    monkeypatch.setattr(holding, "make_pool", pool)
    monkeypatch.setattr(holding, "drain_batch", drain)
    monkeypatch.setattr(holding.asyncio, "sleep", stop_after_poll)
    settings = Settings(_env_file=None, network="compose", client="hq")
    relay = RelaySettings(_env_file=None, role="hq")

    with pytest.raises(StopWorker):
        await holding.run_hq_worker(Cluster(regions=2), settings, relay, None)

    assert healthy_region_ran.is_set()
    assert failed_region_finished.is_set()
    assert len(pools) == 2
    assert pools[0] is not pools[1]

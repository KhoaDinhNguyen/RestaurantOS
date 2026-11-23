"""HQ selection must fail closed on zero/two writable nodes."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from restaurantos.common import hq
from restaurantos.common.cluster import HQ_PRIMARY, HQ_STANDBY, Cluster
from restaurantos.common.config import Settings
from restaurantos.common.errors import RegionUnreachableError


@pytest.mark.parametrize(
    "states,expected",
    [
        ({HQ_PRIMARY: False, HQ_STANDBY: True}, HQ_PRIMARY),
        ({HQ_PRIMARY: None, HQ_STANDBY: False}, HQ_STANDBY),
        ({HQ_PRIMARY: True, HQ_STANDBY: False}, HQ_STANDBY),
        ({HQ_PRIMARY: False, HQ_STANDBY: False}, None),
        ({HQ_PRIMARY: True, HQ_STANDBY: True}, None),
        ({HQ_PRIMARY: None, HQ_STANDBY: None}, None),
    ],
)
async def test_selects_only_one_writable_node(monkeypatch, states, expected):
    async def run(pool, work, **kwargs):
        if states[pool] is None:
            raise RegionUnreachableError(region_id="hq")
        cursor = SimpleNamespace(fetchone=AsyncMock(return_value=(states[pool],)))
        return await work(SimpleNamespace(execute=AsyncMock(return_value=cursor)))

    monkeypatch.setattr(hq, "run_on", run)
    pool = hq.HQPool(Cluster(), Settings(_env_file=None), pools={n: n for n in states})
    if expected:
        assert await pool.select() == expected
    else:
        with pytest.raises(RegionUnreachableError):
            await pool.select()


async def test_checkout_rechecks_role_and_does_not_replay_work(monkeypatch):
    cursor = SimpleNamespace(fetchone=AsyncMock(return_value=(True,)))
    conn = SimpleNamespace(execute=AsyncMock(return_value=cursor))

    @asynccontextmanager
    async def connection():
        yield conn

    pool = hq.HQPool(
        Cluster(standbys=False),
        Settings(_env_file=None),
        pools={HQ_PRIMARY: SimpleNamespace(connection=connection)},
    )
    monkeypatch.setattr(pool, "select", AsyncMock(return_value=HQ_PRIMARY))
    work = AsyncMock()
    with pytest.raises(RegionUnreachableError):
        async with pool.connection():
            await work()
    work.assert_not_awaited()


async def test_borrowed_pools_are_not_opened_or_closed_twice():
    inner = AsyncMock()
    pool = hq.HQPool(Cluster(standbys=False), Settings(_env_file=None), pools={HQ_PRIMARY: inner})
    async with pool:
        pass
    inner.open.assert_not_awaited()
    inner.close.assert_not_awaited()


async def test_gateway_hq_probe_does_not_require_regional_epoch_table(monkeypatch):
    from restaurantos.common import db

    cursor = SimpleNamespace(fetchone=AsyncMock(return_value=(False,)))
    conn = SimpleNamespace(execute=AsyncMock(return_value=cursor))

    async def run(acquire, work, seconds):
        return await work(conn)

    monkeypatch.setattr(db, "run_with_deadline", run)
    view = await db.probe_node(HQ_PRIMARY, "host=unused", Settings(_env_file=None))
    assert view.reachable and view.writable
    assert "node_state" not in conn.execute.await_args.args[0]

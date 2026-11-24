"""The acceptance harness must retain uncertain IDs and always clean up."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from tests.cluster.wp5_support import cleanup_acceptance, produce_orders


async def test_shutdown_retries_the_same_uncertain_identity_until_acknowledged():
    stop, acknowledgements, sent = asyncio.Event(), {}, []

    async def post(path, *, json):
        sent.append(dict(json))
        stop.set()
        return httpx.Response(503 if len(sent) == 1 else 200, json={"outcome": "accepted"})

    retries = await produce_orders(
        SimpleNamespace(post=post), "edge", stop, acknowledgements, retry_interval_s=0
    )
    assert retries == 1
    assert sent[0] == sent[1]
    assert acknowledgements == {"edge-0": 200}
    assert len(sent) == 2


async def test_uncertain_shutdown_is_bounded_and_never_invented_as_acknowledged():
    stop, acknowledgements = asyncio.Event(), {}

    async def post(*args, **kwargs):
        stop.set()
        return httpx.Response(503)

    client = SimpleNamespace(post=AsyncMock(side_effect=post))
    with pytest.raises(AssertionError, match="uncertain at shutdown"):
        await produce_orders(
            client, "edge", stop, acknowledgements, retry_interval_s=0, shutdown_attempts=3
        )
    assert client.post.await_count == 3
    assert acknowledgements == {}
    assert len({c.kwargs["json"]["external_order_id"] for c in client.post.await_args_list}) == 1


async def test_already_stopped_producer_does_not_create_a_new_identity():
    stop, client = asyncio.Event(), SimpleNamespace(post=AsyncMock())
    stop.set()
    assert await produce_orders(client, "edge", stop, {}) == 0
    client.post.assert_not_awaited()


async def test_acknowledged_inflight_order_is_recorded_before_stopping():
    stop, acknowledgements = asyncio.Event(), {}

    async def post(*args, **kwargs):
        stop.set()
        return httpx.Response(201, json={"outcome": "accepted"})

    assert await produce_orders(SimpleNamespace(post=post), "gateway", stop, acknowledgements) == 0
    assert acknowledgements == {"gateway-0": 201}


@pytest.mark.parametrize("http_failure", [False, True])
async def test_reset_failure_still_stops_tasks_and_closes_every_pool(http_failure):
    producers_stop, workers_stop = asyncio.Event(), asyncio.Event()
    producers = [asyncio.create_task(producers_stop.wait())]
    workers = [asyncio.create_task(workers_stop.wait())]
    runtime = SimpleNamespace(close=AsyncMock(side_effect=RuntimeError("close also failed")))
    pools = {"one": SimpleNamespace(close=AsyncMock()), "two": SimpleNamespace(close=AsyncMock())}
    if http_failure:
        response = httpx.Response(503, request=httpx.Request("POST", "http://tox/reset"))
        tox = SimpleNamespace(post=AsyncMock(return_value=response))
        error_type = httpx.HTTPStatusError
    else:
        error = httpx.ConnectError("reset disconnected")
        tox = SimpleNamespace(post=AsyncMock(side_effect=error))
        error_type = httpx.ConnectError
    with pytest.raises(error_type):
        await cleanup_acceptance(
            tox, producers_stop, workers_stop, producers, workers, runtime, pools
        )
    assert producers[0].done() and workers[0].done()
    runtime.close.assert_awaited_once()
    for pool in pools.values():
        pool.close.assert_awaited_once()


async def test_close_failure_is_propagated_after_other_resources_are_closed():
    tox = SimpleNamespace(
        post=AsyncMock(
            return_value=httpx.Response(200, request=httpx.Request("POST", "http://tox/reset"))
        )
    )
    runtime = SimpleNamespace(close=AsyncMock(side_effect=RuntimeError("close failed")))
    pool = SimpleNamespace(close=AsyncMock())
    with pytest.raises(RuntimeError, match="close failed"):
        await cleanup_acceptance(
            tox, asyncio.Event(), asyncio.Event(), [], [], runtime, {"observer": pool}
        )
    pool.close.assert_awaited_once()

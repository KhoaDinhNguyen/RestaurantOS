"""Load and cleanup helpers; unit-tested without running a Docker cluster."""

import asyncio


async def produce_orders(
    client, label, stop, acknowledgements, *, retry_interval_s=0.15, shutdown_attempts=10
) -> int:
    number, retries, final_attempts = 0, 0, 0
    pending = None
    while not stop.is_set() or pending is not None:
        if pending is None:
            pending = {
                "channel": "wp5-load",
                "external_order_id": f"{label}-{number}",
                "vendor_id": "v001",
                "order_day": 14,
                "order_time": "12:00:00",
                "items": [{"product_id": "p003", "quantity": 1}],
            }
        response = await client.post("/api/v1/orders", json=pending)
        if response.status_code in (200, 201, 202):
            assert response.json().get("outcome") != "refused", response.text
            acknowledgements[pending["external_order_id"]] = response.status_code
            pending = None
            number += 1
        else:
            assert response.status_code == 503, response.text
            retries += 1
            if stop.is_set():
                final_attempts += 1
                assert final_attempts < shutdown_attempts, (
                    "pending order stayed uncertain at shutdown"
                )
        # A 503 may still have committed. Finish this exact identity after
        # shutdown, with a bounded retry budget, before ending the producer.
        if stop.is_set() and pending is None:
            break
        await asyncio.sleep(retry_interval_s)
    return retries


async def cleanup_acceptance(tox, producers_stop, workers_stop, producers, workers, runtime, pools):
    reset_error = None
    try:
        (await tox.post("/reset")).raise_for_status()
    except Exception as error:
        reset_error = error
    finally:
        producers_stop.set()
        workers_stop.set()
        try:
            await asyncio.gather(*producers, *workers, return_exceptions=True)
        finally:
            # Attempt every close even when reset or another close fails.
            results = await asyncio.gather(
                runtime.close(), *(pool.close() for pool in pools.values()), return_exceptions=True
            )
    if reset_error is not None:
        raise reset_error
    for result in results:
        if isinstance(result, BaseException):
            raise result

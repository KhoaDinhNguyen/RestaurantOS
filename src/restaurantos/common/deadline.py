"""A deadline on the caller's side of a database call.

statement_timeout runs on the server and its error comes back over the same
link. When that link is silent (a Toxiproxy `timeout` toxic) or slow (a
`latency` toxic), nothing comes back in time and a plain `await` waits for as
long as the fault lasts. run_with_deadline bounds the wait with the caller's
own clock, which needs nothing from the far side.

It does not try to stop the stuck call cleanly. Cancelling a psycopg call
makes psycopg ask the server to cancel the query and then wait for it, over
the same dead link, so waiting for the cancellation would hang as well. The
call is left to finish in the background: it ends when the fault is removed,
and its connection goes back to the pool or is thrown away then. Until that
moment the connection stays checked out, so a pool empties during a long
fault and later callers fail fast on the pool's own timeout. That is the
behaviour we want; the pool refills after the fault.

After a deadline the caller does not know whether a write was committed. Only
retry through a path that is safe to repeat (place_order and the HQ apply are).

No psycopg import here, so the rule can be unit-tested without a database.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager

from restaurantos.common.errors import RegionUnreachableError

_abandoned: set[asyncio.Task[object]] = set()


def abandoned_calls() -> int:
    """How many calls were given up on and are still running in the background."""
    return len(_abandoned)


def _forget(task: asyncio.Task[object]) -> None:
    _abandoned.discard(task)
    if not task.cancelled():
        task.exception()  # mark it as seen, so asyncio does not log it at exit


def _abandon(task: asyncio.Task[object]) -> None:
    _abandoned.add(task)
    task.add_done_callback(_forget)
    task.cancel()


async def run_with_deadline[C, T](
    acquire: Callable[[], AbstractAsyncContextManager[C]],
    work: Callable[[C], Awaitable[T]],
    seconds: float,
    *,
    region_id: str | None = None,
) -> T:
    """Run `work(conn)` inside `async with acquire() as conn`, for at most `seconds`.

    `acquire` is normally `pool.connection`, so getting a connection counts
    towards the deadline and leaving the block commits or rolls back as usual.

    Returns what `work` returns and raises what it raises. If the deadline
    passes first, raises RegionUnreachableError at once and leaves the call
    running in the background (see the module docstring).
    """

    async def call() -> T:
        async with acquire() as conn:
            return await work(conn)

    task: asyncio.Task[T] = asyncio.create_task(call())
    try:
        done, _ = await asyncio.wait({task}, timeout=seconds)
    except BaseException:
        # The caller itself was cancelled (the client hung up, shutdown).
        _abandon(task)
        raise
    if done:
        return task.result()
    _abandon(task)
    where = f"region {region_id}" if region_id else "database"
    raise RegionUnreachableError(f"{where}: no answer within {seconds:g} s", region_id=region_id)

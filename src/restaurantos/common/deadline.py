"""A deadline on the caller's side of a database call.

statement_timeout runs on the server and its error comes back over the same
link. When that link is silent (a Toxiproxy `timeout` toxic) or slow (a
`latency` toxic), nothing comes back in time and a plain `await` waits for as
long as the fault lasts. run_with_deadline bounds the wait with the caller's
own clock, which needs nothing from the far side.

A call that misses its deadline is **abandoned, never cancelled**. Cancelling
the task would deliver CancelledError into psycopg, and psycopg answers that
by asking the server to cancel the query: a second connection, through the
same dead link. With a libpq older than 17 that request is the blocking
PQcancel, which holds the interpreter lock until the kernel gives up (about a
minute on a silent link), so one cancelled call would freeze the whole
process, every region and every request included. From libpq 17 on the
request does not block (docs/setup.md requires that), but it still cannot
get through a dead link. So nothing here ever calls
`task.cancel()`, not at the deadline and not when the caller itself is
cancelled.

What happens to an abandoned call instead:

- It keeps waiting in the background and ends when the fault is removed or
  the connection breaks. Until then its connection stays checked out, so a
  pool empties during a long fault and later callers fail fast on the pool's
  own timeout. The pool refills after the fault.
- When its work finally finishes, the transaction is rolled back, not
  committed: the caller was told the region could not be reached, and a write
  that appears a minute later would contradict that.

That rollback is best effort. If the deadline passes while the commit itself
is on the wire, or if `work` commits on its own, the write may still exist.
So after a deadline the caller does not know whether a write was committed,
and may only retry through a path that is safe to repeat (place_order and the
HQ apply are).

No psycopg import here, so the rule can be unit-tested without a database.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager

from restaurantos.common.errors import RegionUnreachableError

_abandoned: set[asyncio.Task[object]] = set()


class _FinishedTooLate(Exception):
    """Raised inside an abandoned call once its work is done, to roll it back."""


def abandoned_calls() -> int:
    """How many calls were given up on and are still running in the background."""
    return len(_abandoned)


def _forget(task: asyncio.Task[object]) -> None:
    _abandoned.discard(task)
    if not task.cancelled():
        task.exception()  # mark it as seen, so asyncio does not log it at exit


def _abandon(task: asyncio.Task[object]) -> None:
    # Keep a reference so the task is not garbage-collected while it waits,
    # and drop it when it ends. Deliberately no task.cancel(): see above.
    _abandoned.add(task)
    task.add_done_callback(_forget)


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
    passes first, raises RegionUnreachableError at once; the call is abandoned
    and rolled back when it ends (see the module docstring). The same happens
    to the call if the caller is cancelled while waiting.
    """
    given_up = False

    async def call() -> T:
        async with acquire() as conn:
            result = await work(conn)
            if given_up:
                raise _FinishedTooLate
            return result

    task: asyncio.Task[T] = asyncio.create_task(call())
    try:
        done, _ = await asyncio.wait({task}, timeout=seconds)
    except BaseException:
        # The caller itself was cancelled (the client hung up, shutdown).
        given_up = True
        _abandon(task)
        raise
    if done:
        return task.result()
    given_up = True
    _abandon(task)
    where = f"region {region_id}" if region_id else "database"
    raise RegionUnreachableError(f"{where}: no answer within {seconds:g} s", region_id=region_id)

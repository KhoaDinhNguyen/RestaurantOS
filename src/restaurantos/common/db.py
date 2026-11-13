"""psycopg helpers: connection strings, pools with timeouts, deadlines, schema, epoch errors."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg_pool import AsyncConnectionPool

from restaurantos.common.cluster import Cluster, conninfo
from restaurantos.common.config import Settings, get_settings
from restaurantos.common.deadline import run_with_deadline
from restaurantos.common.errors import (
    STALE_EPOCH_SQLSTATE,
    RegionUnreachableError,
    StaleEpochError,
)
from restaurantos.common.localnode import NodeView, local_nodes, pick_local_primary
from restaurantos.common.sqlfiles import NodeKind, load_query, schema_files


def node_conninfo(
    cluster: Cluster,
    node: str,
    *,
    direct: bool = False,
    settings: Settings | None = None,
) -> str:
    """Connection string from this process to `node`, built from its settings.

    Fills in who is calling (`client`), where it runs (`network`) and the
    database credentials, so that a service works unchanged in a container and
    on the host. Use this in services instead of calling `conninfo` yourself.
    """
    s = settings or get_settings()
    return conninfo(
        cluster,
        s.client,
        node,
        dbname=s.db_name,
        user=s.db_user,
        password=s.db_password,
        direct=direct,
        network=s.network,
    )


def _with_session_limits(conninfo: str, s: Settings) -> str:
    """Add the connect timeout and the two server-side limits to a connection string.

    Options already in the string are kept, ours come after them.
    """
    limits = (
        f"-c statement_timeout={s.statement_timeout_ms}"
        f" -c idle_in_transaction_session_timeout={s.idle_in_transaction_timeout_ms}"
    )
    params = conninfo_to_dict(conninfo)
    params["options"] = f"{params['options']} {limits}" if params.get("options") else limits
    params["connect_timeout"] = max(1, round(s.connect_timeout_s))
    return make_conninfo(**params)


def make_pool(conninfo: str, settings: Settings | None = None) -> AsyncConnectionPool:
    """A pool for request paths: its connections give up quickly.

    Three limits are set here, and one is not:

    - connect_timeout bounds opening a connection, so a silenced Toxiproxy
      link (it accepts connections and then never answers) cannot hang a new
      one.
    - statement_timeout bounds one statement, on the server.
    - idle_in_transaction_session_timeout makes the server end a session that
      sits inside an open transaction. Without it, a caller that was cut off
      half-way through a transaction keeps its row locks for as long as the
      fault lasts, and blocks everyone else who needs those rows.
    - A link that goes silent or slow while a statement is in flight is not
      covered: the server's error cannot come back in time either. Call the
      database through `run_on`, which adds the caller's own deadline.

    Not for batch work such as bulk COPY, the checker or snapshots: the
    statement timeout would cut it short. Open a plain connection for those
    (restaurantos.common.cluster.host_conninfo).

    Open it with `await pool.open()` and close it with `await pool.close()`.
    """
    s = settings or get_settings()
    return AsyncConnectionPool(
        _with_session_limits(conninfo, s),
        min_size=s.pool_min_size,
        max_size=s.pool_max_size,
        timeout=s.connect_timeout_s,
        open=False,
    )


async def run_on[T](
    pool: AsyncConnectionPool,
    work: Callable[[psycopg.AsyncConnection], Awaitable[T]],
    *,
    region_id: str | None = None,
    settings: Settings | None = None,
) -> T:
    """Run `work(conn)` on a pooled connection, giving up after the request deadline.

        result = await run_on(pool, lambda conn: place_order(conn, req, epoch), region_id="r1")

    This is how a request path calls a database. The block commits when `work`
    returns and rolls back when it raises, as `pool.connection()` does.

    Raises RegionUnreachableError when the node cannot be reached or does not
    answer in time: no connection, pool exhausted, statement timeout, or the
    deadline (`request_deadline_s`, kept by this process's own clock, so it
    holds on a silent link too). Raises StaleEpochError for a stale epoch.
    Anything else `work` raises comes through unchanged.

    After RegionUnreachableError the write may or may not have been committed.
    Retry only through something that is safe to repeat.
    """
    s = settings or get_settings()
    try:
        async with translate_db_errors():
            return await run_with_deadline(
                pool.connection, work, s.request_deadline_s, region_id=region_id
            )
    except psycopg.OperationalError as exc:
        raise RegionUnreachableError(str(exc), region_id=region_id) from exc


async def probe_node(node: str, conninfo: str, settings: Settings | None = None) -> NodeView:
    """One look at a region node: does it answer, is it a standby, which epoch?

    Uses its own short-lived connection, not a pool, so a broken pool cannot
    hide a healthy node. A node that does not answer in time, or has no
    node_state row yet, comes back as not reachable.
    """
    s = settings or get_settings()
    target = _with_session_limits(conninfo, s)

    @asynccontextmanager
    async def connect() -> AsyncIterator[psycopg.AsyncConnection]:
        async with await psycopg.AsyncConnection.connect(target, autocommit=True) as conn:
            yield conn

    async def look(conn: psycopg.AsyncConnection) -> NodeView:
        cur = await conn.execute(load_query("common/local_node_probe"))
        row = await cur.fetchone()
        if row is None:
            return NodeView(node)
        # Column order is (epoch, in_recovery); see common/local_node_probe.sql.
        return NodeView(node, reachable=True, in_recovery=row[1], epoch=row[0])

    try:
        return await run_with_deadline(connect, look, s.request_deadline_s)
    except psycopg.Error, OSError, RegionUnreachableError:
        return NodeView(node)


async def find_local_primary(
    cluster: Cluster,
    region_id: str,
    current: str | None = None,
    settings: Settings | None = None,
) -> str | None:
    """The node a region-local process (edge API, relay) should use now.

    Looks at the region's nodes directly, not through Toxiproxy, and applies
    restaurantos.common.localnode.pick_local_primary. Call it at start, on a
    timer and after any database error; pass the node in use as `current`.
    None means there is no usable primary at the moment.
    """
    s = settings or get_settings()
    views = await asyncio.gather(
        *(
            probe_node(node, node_conninfo(cluster, node, direct=True, settings=s), s)
            for node in local_nodes(cluster, region_id)
        )
    )
    return pick_local_primary(current, views)


@asynccontextmanager
async def translate_db_errors() -> AsyncIterator[None]:
    """Turn the SQLSTATE raised by assert_epoch() into StaleEpochError."""
    try:
        yield
    except psycopg.Error as exc:
        if exc.sqlstate == STALE_EPOCH_SQLSTATE:
            raise StaleEpochError(str(exc)) from exc
        raise


async def apply_schema(conn: psycopg.AsyncConnection, kind: NodeKind) -> None:
    """Run every schema file for this node kind, in order, in one transaction."""
    async with conn.transaction():
        for path in schema_files(kind):
            await conn.execute(path.read_text(encoding="utf-8"))

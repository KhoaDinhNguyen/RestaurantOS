"""psycopg helpers: pools with timeouts, schema application, epoch errors."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import psycopg
from psycopg_pool import AsyncConnectionPool

from restaurantos.common.config import Settings, get_settings
from restaurantos.common.errors import STALE_EPOCH_SQLSTATE, StaleEpochError
from restaurantos.common.sqlfiles import NodeKind, schema_files


def make_pool(conninfo: str, settings: Settings | None = None) -> AsyncConnectionPool:
    """A pool whose connections give up quickly.

    The timeouts matter: a silenced Toxiproxy link accepts connections and then
    never answers, so without them a request would hang forever.
    Open it with `await pool.open()` and close it with `await pool.close()`.
    """
    s = settings or get_settings()
    return AsyncConnectionPool(
        conninfo,
        min_size=s.pool_min_size,
        max_size=s.pool_max_size,
        timeout=s.connect_timeout_s,
        open=False,
        kwargs={
            "connect_timeout": max(1, round(s.connect_timeout_s)),
            "options": f"-c statement_timeout={s.statement_timeout_ms}",
        },
    )


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

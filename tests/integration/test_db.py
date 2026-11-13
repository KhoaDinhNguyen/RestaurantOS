"""common.db against a real PostgreSQL. Needs ROS_TEST_DSN."""

import asyncio
import os
import time

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

from restaurantos.common.config import Settings
from restaurantos.common.db import make_pool, probe_node, run_on
from restaurantos.common.deadline import abandoned_calls
from restaurantos.common.errors import RegionUnreachableError, StaleEpochError
from restaurantos.common.sqlfiles import schema_files

pytestmark = pytest.mark.integration

SCHEMA = "ros_test_db"


def settings(**values):
    return Settings(_env_file=None, **values)


@pytest.fixture
def dsn():
    value = os.environ.get("ROS_TEST_DSN")
    if not value:
        pytest.skip("ROS_TEST_DSN is not set")
    return value


@pytest.fixture
def region_dsn(dsn):
    """A region schema with its node_state row, and a DSN whose sessions see it."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        conn.execute(f"CREATE SCHEMA {SCHEMA}")
        conn.execute(f"SET search_path TO {SCHEMA}")
        for path in schema_files("region"):
            conn.execute(path.read_text(encoding="utf-8"))
        conn.execute("INSERT INTO node_state (region_id, epoch) VALUES ('r1', 3)")
    return make_conninfo(dsn, options=f"-c search_path={SCHEMA}")


async def show(conn, name):
    cur = await conn.execute(f"SHOW {name}")
    return (await cur.fetchone())[0]


async def one(conn):
    cur = await conn.execute("SELECT 1")
    return (await cur.fetchone())[0]


async def test_pool_sessions_carry_the_limits_and_keep_the_callers_options(region_dsn):
    pool = make_pool(region_dsn, settings())
    await pool.open(wait=True, timeout=10)
    try:
        async with pool.connection() as conn:
            assert await show(conn, "statement_timeout") == "2s"
            assert await show(conn, "idle_in_transaction_session_timeout") == "5s"
            assert await show(conn, "search_path") == SCHEMA
    finally:
        await pool.close()


async def test_the_server_ends_a_session_left_inside_a_transaction(dsn):
    pool = make_pool(dsn, settings(idle_in_transaction_timeout_ms=200))
    await pool.open(wait=True, timeout=10)
    try:
        with pytest.raises(psycopg.Error):
            async with pool.connection() as conn:
                await conn.execute("SELECT 1")  # opens a transaction
                await asyncio.sleep(1)  # a caller that went away
                await conn.execute("SELECT 1")
    finally:
        await pool.close()


async def test_run_on_returns_the_value_and_commits(region_dsn):
    pool = make_pool(region_dsn, settings())
    await pool.open(wait=True, timeout=10)

    async def bump(conn):
        cur = await conn.execute("UPDATE node_state SET menu_version = 9 RETURNING menu_version")
        return (await cur.fetchone())[0]

    async def read(conn):
        cur = await conn.execute("SELECT menu_version FROM node_state")
        return (await cur.fetchone())[0]

    try:
        assert await run_on(pool, bump) == 9
        assert await run_on(pool, read) == 9
    finally:
        await pool.close()


async def no_abandoned_calls(within=5.0):
    until = time.monotonic() + within
    while abandoned_calls() and time.monotonic() < until:
        await asyncio.sleep(0.05)
    return abandoned_calls() == 0


async def test_run_on_gives_up_at_the_deadline_and_the_pool_recovers(dsn):
    s = settings(request_deadline_s=0.3, statement_timeout_ms=10_000)
    pool = make_pool(dsn, s)
    await pool.open(wait=True, timeout=10)
    try:
        started = time.monotonic()
        with pytest.raises(RegionUnreachableError) as caught:
            await run_on(
                pool, lambda conn: conn.execute("SELECT pg_sleep(1)"), region_id="r1", settings=s
            )
        assert time.monotonic() - started < 0.8
        assert caught.value.region_id == "r1"
        assert abandoned_calls() == 1

        # The abandoned call is not cancelled: it runs to its end in the
        # background, and then its connection comes back.
        assert await no_abandoned_calls()
        assert await run_on(pool, one, settings=s) == 1
    finally:
        await pool.close()


async def test_work_that_finishes_after_the_deadline_is_rolled_back(region_dsn):
    s = settings(request_deadline_s=0.3)
    pool = make_pool(region_dsn, s)
    await pool.open(wait=True, timeout=10)

    async def slow_write(conn):
        await conn.execute("UPDATE node_state SET menu_version = 9")
        await conn.execute("SELECT pg_sleep(1)")

    async def read(conn):
        cur = await conn.execute("SELECT menu_version FROM node_state")
        return (await cur.fetchone())[0]

    try:
        before = await run_on(pool, read, settings=s)
        with pytest.raises(RegionUnreachableError):
            await run_on(pool, slow_write, settings=s)
        assert await no_abandoned_calls()
        # The caller was told "unreachable", so the late write must not appear.
        assert await run_on(pool, read, settings=s) == before != 9
    finally:
        await pool.close()


async def test_a_statement_timeout_counts_as_unreachable(dsn):
    s = settings(statement_timeout_ms=200)
    pool = make_pool(dsn, s)
    await pool.open(wait=True, timeout=10)
    try:
        with pytest.raises(RegionUnreachableError):
            await run_on(pool, lambda conn: conn.execute("SELECT pg_sleep(5)"), settings=s)
        assert await run_on(pool, one, settings=s) == 1
    finally:
        await pool.close()


async def test_run_on_reports_a_stale_epoch(region_dsn):
    pool = make_pool(region_dsn, settings())
    await pool.open(wait=True, timeout=10)
    try:
        await run_on(pool, lambda conn: conn.execute("SELECT assert_epoch(3)"))
        with pytest.raises(StaleEpochError):
            await run_on(pool, lambda conn: conn.execute("SELECT assert_epoch(2)"))
    finally:
        await pool.close()


async def test_run_on_reports_a_node_that_is_not_there(dsn):
    nowhere = make_conninfo(dsn, host="127.0.0.1", port=1)
    pool = make_pool(nowhere, settings())
    await pool.open()
    try:
        with pytest.raises(RegionUnreachableError):
            await run_on(pool, one)
    finally:
        await pool.close()


async def test_probe_node_reads_role_and_epoch(dsn, region_dsn):
    view = await probe_node("r1-primary", region_dsn, settings())
    assert (view.reachable, view.in_recovery, view.epoch) == (True, False, 3)
    assert view.writable

    nowhere = make_conninfo(dsn, host="127.0.0.1", port=1)
    view = await probe_node("r1-standby", nowhere, settings())
    assert (view.node, view.reachable, view.writable) == ("r1-standby", False, False)

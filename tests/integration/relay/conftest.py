"""Isolated PostgreSQL schemas shared by relay integration tests."""

import os
from contextlib import asynccontextmanager
from uuid import uuid4

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

from restaurantos.common.config import Settings
from restaurantos.common.db import apply_schema, make_pool


@asynccontextmanager
async def relay_schema(kind):
    dsn = os.environ.get("ROS_TEST_DSN")
    if not dsn:
        pytest.skip("ROS_TEST_DSN is not set")
    schema = f"relay_test_{uuid4().hex}"
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        await conn.execute(f"CREATE SCHEMA {schema}")
        try:
            await conn.execute(f"SET search_path TO {schema}")
            await apply_schema(conn, kind)
            settings = Settings(_env_file=None, pool_min_size=1, pool_max_size=1)
            pool_dsn = make_conninfo(dsn, options=f"-c search_path={schema}")
            async with make_pool(pool_dsn, settings) as pool:
                yield conn, pool, settings
        finally:
            await conn.execute(f"DROP SCHEMA {schema} CASCADE")


@pytest.fixture
async def outbox():
    async with relay_schema("region") as db:
        yield db


@pytest.fixture
async def hq():
    async with relay_schema("hq") as db:
        yield db

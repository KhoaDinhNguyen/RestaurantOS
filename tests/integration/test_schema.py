"""Applies the real schema files to PostgreSQL. Needs ROS_TEST_DSN."""

import os

import psycopg
import pytest

from restaurantos.common.errors import STALE_EPOCH_SQLSTATE
from restaurantos.common.sqlfiles import schema_files

pytestmark = pytest.mark.integration

REGION_TABLES = {
    "ingredients", "dishes", "dish_ingredients", "vendors", "products", "product_dish",
    "node_state", "stock", "product_availability", "orders", "order_items",
    "inbox", "processed_events", "outbox", "exception_log",
}  # fmt: skip
HQ_TABLES = {
    "ingredients", "dishes", "dish_ingredients", "vendors", "products", "product_dish",
    "region_state", "shard_directory", "menu_versions", "holding_queue", "cluster_events",
    "hq_inbox", "analytics_orders", "analytics_order_items", "analytics_exceptions",
}  # fmt: skip


@pytest.fixture
def conn():
    dsn = os.environ.get("ROS_TEST_DSN")
    if not dsn:
        pytest.skip("ROS_TEST_DSN is not set")
    with psycopg.connect(dsn, autocommit=True) as connection:
        yield connection


def apply(conn, kind):
    """Apply one node kind's schema inside its own throwaway schema."""
    schema = f"ros_test_{kind}"
    conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    conn.execute(f"CREATE SCHEMA {schema}")
    conn.execute(f"SET search_path TO {schema}")
    for path in schema_files(kind):
        conn.execute(path.read_text(encoding="utf-8"))
    rows = conn.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = %s", (schema,)
    ).fetchall()
    return {r[0] for r in rows}


@pytest.mark.parametrize(("kind", "expected"), [("region", REGION_TABLES), ("hq", HQ_TABLES)])
def test_schema_applies_and_creates_the_contract_tables(conn, kind, expected):
    tables = apply(conn, kind)
    assert expected <= tables
    assert "orders_d000" in tables or "analytics_orders_d000" in tables


def test_stale_epoch_raises_the_agreed_sqlstate(conn):
    apply(conn, "region")
    conn.execute("INSERT INTO node_state (region_id) VALUES ('r1')")
    conn.execute("SELECT assert_epoch(1)")
    with pytest.raises(psycopg.Error) as err:
        conn.execute("SELECT assert_epoch(2)")
    assert err.value.sqlstate == STALE_EPOCH_SQLSTATE


def test_orders_land_in_their_day_partition(conn):
    apply(conn, "region")
    conn.execute("INSERT INTO vendors (vendor_id, geohash) VALUES ('v1', 'w21z7')")
    conn.execute(
        "INSERT INTO orders (order_day, order_id, vendor_id, channel, external_order_id,"
        " order_time, menu_version, epoch)"
        " VALUES (14, gen_random_uuid(), 'v1', 'dhrd', '1', '12:00', 1, 1)"
    )
    part = conn.execute("SELECT tableoid::regclass::text FROM orders").fetchone()[0]
    assert part.endswith("orders_d014")

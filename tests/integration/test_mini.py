"""load_mini against a real schema: counts, traps, slices, determinism.

Needs ROS_TEST_DSN, like tests/integration/test_schema.py.
"""

import csv
import os

import psycopg
import pytest

from restaurantos.common.sqlfiles import schema_files
from restaurantos.sim.mini import MiniCounts, load_mini, mini_dir

pytestmark = pytest.mark.integration


@pytest.fixture
def conn():
    dsn = os.environ.get("ROS_TEST_DSN")
    if not dsn:
        pytest.skip("ROS_TEST_DSN is not set")
    with psycopg.connect(dsn, autocommit=True) as connection:
        yield connection


def apply_region(conn, name: str) -> None:
    """The region schema inside its own throwaway schema (test_schema's pattern)."""
    conn.execute(f"DROP SCHEMA IF EXISTS ros_test_{name} CASCADE")
    conn.execute(f"CREATE SCHEMA ros_test_{name}")
    conn.execute(f"SET search_path TO ros_test_{name}")
    for path in schema_files("region"):
        conn.execute(path.read_text(encoding="utf-8"))


def fixture_rows(name: str) -> int:
    with (mini_dir() / name).open(newline="", encoding="utf-8") as f:
        return len(list(csv.DictReader(f)))


def test_full_load_counts_match_the_files(conn):
    apply_region(conn, "mini_full")
    counts = load_mini(conn)
    assert isinstance(counts, MiniCounts)
    for table, name in [
        ("ingredients", "ingredients.csv"),
        ("dishes", "dishes.csv"),
        ("dish_ingredients", "dish_ingredients.csv"),
        ("vendors", "vendors.csv"),
        ("products", "products.csv"),
        ("product_dish", "product_dish.csv"),
        ("orders", "orders.csv"),
        ("order_items", "order_items.csv"),
        ("stock", "stock.csv"),
    ]:
        assert getattr(counts, table) == fixture_rows(name)


def test_traps_survive_the_load(conn):
    apply_region(conn, "mini_traps")
    load_mini(conn)
    rows = conn.execute(
        "SELECT ingredient_id, amount, unit FROM dish_ingredients "
        "WHERE unit <> 'portion' OR amount <> 1 ORDER BY ingredient_id"
    ).fetchall()
    assert {(r[0], float(r[1]), r[2]) for r in rows} == {
        ("beef", 2.0, "kg"),
        ("caramel", 3.0, "piece"),
    }
    low = conn.execute("SELECT on_hand, par_level FROM stock WHERE on_hand < par_level").fetchall()
    assert low == [(2, 10)]


def test_live_only_columns_are_invented_deterministically(conn):
    apply_region(conn, "mini_det")
    load_mini(conn)
    first = conn.execute(
        "SELECT external_order_id, order_id, channel, status, epoch, menu_version "
        "FROM orders ORDER BY external_order_id LIMIT 1"
    ).fetchone()
    assert first[2:] == ("mini", "accepted", 1, 1)
    conn.execute(
        "TRUNCATE ingredients, dishes, dish_ingredients, vendors, products, "
        "product_dish, orders, order_items, stock"
    )
    load_mini(conn)
    again = conn.execute(
        "SELECT external_order_id, order_id FROM orders WHERE external_order_id = %s", (first[0],)
    ).fetchone()
    assert again is not None and again[1] == first[1]


def test_slices_add_up_to_the_whole(conn):
    apply_region(conn, "mini_v12")
    counts_a = load_mini(conn, vendors=["v001", "v002"])
    apply_region(conn, "mini_v34")
    counts_b = load_mini(conn, vendors=["v003", "v004"])
    apply_region(conn, "mini_all")
    whole = load_mini(conn, vendors=["v001", "v002", "v003", "v004"])
    assert counts_a.orders + counts_b.orders == whole.orders == fixture_rows("orders.csv")
    assert counts_a.products + counts_b.products == whole.products
    assert counts_a.order_items + counts_b.order_items == whole.order_items
    # the shared reference rows are sliced too: a slice holds only its own dishes
    assert counts_a.vendors == 2
    assert counts_a.dishes < whole.dishes


def test_order_ids_match_across_a_region_and_a_baseline(conn):
    apply_region(conn, "mini_region")
    load_mini(conn, vendors=["v001"])
    region = conn.execute(
        "SELECT external_order_id, order_id FROM orders ORDER BY external_order_id"
    ).fetchall()
    apply_region(conn, "mini_base")
    load_mini(conn)
    everything = dict(conn.execute("SELECT external_order_id, order_id FROM orders").fetchall())
    assert len(region) >= 1
    for external, order_id in region:
        assert everything[external] == order_id


def test_reference_only_load_for_hq_schemas(conn):
    schema = "ros_test_mini_hq"
    conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    conn.execute(f"CREATE SCHEMA {schema}")
    conn.execute(f"SET search_path TO {schema}")
    for path in schema_files("hq"):
        conn.execute(path.read_text(encoding="utf-8"))
    counts = load_mini(conn, with_region_data=False)
    assert counts.orders == counts.order_items == counts.stock == 0
    assert counts.vendors > 0
    conn.execute("SELECT count(*) FROM products")

"""rosctl baseline sync against the real generated cluster. Run locally,
never in CI.

Needs the 1-region cluster from `rosctl bootstrap`: r1-primary on 5510
and baseline on 5509, schema and reference data loaded. The test
inserts a handful of identifiable rows on r1, syncs, and checks the
baseline holds the same counts; it also checks idempotency and the
stopped-region error path. The inserted rows are removed from r1 and
the baseline at the end.

    uv run pytest -m cluster tests/cluster/test_baseline_sync.py
"""

from __future__ import annotations

import dataclasses
import uuid

import psycopg
import pytest

from restaurantos.common.cluster import BASELINE, Cluster, host_conninfo, load_cluster, primary_of
from restaurantos.sim import baseline_sync
from restaurantos.sim.baseline_sync import SYNC_TABLES, BaselineSyncError, sync
from restaurantos.sim.config import SimSettings

pytestmark = pytest.mark.cluster

CHANNEL = "baseline-sync-test"
STOCK_ITEM = "baseline-sync-test-item"
TEST_PRODUCT = "baseline-sync-test-product"


@pytest.fixture(scope="module")
def cluster() -> Cluster:
    return load_cluster()


def _connect(cluster: Cluster, node: str) -> psycopg.Connection:
    try:
        return psycopg.connect(host_conninfo(cluster, node), autocommit=True)
    except psycopg.OperationalError:
        pytest.skip(
            f"the generated cluster is not running ({node} unreachable); "
            "run: uv run rosctl bootstrap --yes"
        )


def _count(conn: psycopg.Connection, table: str) -> int:
    return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def _insert_test_rows(conn: psycopg.Connection) -> None:
    """A handful of identifiable rows in each of the four synced tables."""
    vendor = conn.execute("SELECT vendor_id FROM vendors ORDER BY vendor_id LIMIT 1").fetchone()[0]
    conn.execute(
        "INSERT INTO orders (order_day, order_id, vendor_id, channel, external_order_id, "
        "customer_id, customer_geohash, order_time, status, menu_version, epoch) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'accepted', 1, 1) "
        "ON CONFLICT DO NOTHING",
        (
            90,
            uuid.uuid4(),
            vendor,
            CHANNEL,
            "baseline-sync-test-1",
            "c000000001",
            "w21z7",
            "12:00:00",
        ),
    )
    conn.execute(
        "INSERT INTO order_items (order_day, order_id, line_no, vendor_id, product_id, "
        "quantity, price_units) "
        "SELECT order_day, order_id, 1, vendor_id, %s, 1, 100 FROM orders "
        "WHERE channel = %s AND order_id NOT IN "
        "(SELECT order_id FROM order_items) LIMIT 1",
        (TEST_PRODUCT, CHANNEL),
    )
    conn.execute(
        "INSERT INTO stock (vendor_id, item_kind, item_id, on_hand, par_level) "
        "VALUES (%s, 'ingredient', %s, 5, 10) ON CONFLICT DO NOTHING",
        (vendor, STOCK_ITEM),
    )
    conn.execute(
        "INSERT INTO product_availability (vendor_id, product_id, available, reason) "
        "VALUES (%s, %s, false, 'manual') ON CONFLICT DO NOTHING",
        (vendor, TEST_PRODUCT),
    )


def _remove_test_rows(conn: psycopg.Connection) -> None:
    """Remove the test's rows wherever they are (region or baseline)."""
    conn.execute("DELETE FROM orders WHERE channel = %s", (CHANNEL,))
    conn.execute("DELETE FROM stock WHERE item_id = %s", (STOCK_ITEM,))
    conn.execute("DELETE FROM product_availability WHERE product_id = %s", (TEST_PRODUCT,))


@pytest.fixture
def seeded(cluster):
    """r1 with test rows inserted, plus the baseline connection.

    Teardown removes the test rows from BOTH sides (the sync copies
    them to the baseline, so they land there too) and closes both
    connections even when an assertion or a cleanup step fails."""
    region = _connect(cluster, "r1-primary")
    baseline = _connect(cluster, BASELINE)
    _insert_test_rows(region)
    yield region, baseline
    try:
        _remove_test_rows(region)
        _remove_test_rows(baseline)
    finally:
        region.close()
        baseline.close()


def _settings() -> SimSettings:
    # Defaults point the quiet gate at 127.0.0.1:8002; no playback
    # process means connection refused, which is quiet. _env_file=None so
    # a developer's .env cannot bend the test.
    return SimSettings(_env_file=None)


def test_sync_copies_the_regions_counts_to_the_baseline(cluster, seeded):
    region, baseline = seeded
    counts, warning = sync(cluster, _settings())
    assert warning is None
    assert set(counts) == {"r1"}
    for table in SYNC_TABLES:
        # what the command copied is what the region holds...
        assert counts["r1"][table] == _count(region, table)
        # ...and what the baseline now holds
        assert counts["r1"][table] == _count(baseline, table)
    # the test's rows really made the trip
    assert (
        region.execute("SELECT count(*) FROM orders WHERE channel = %s", (CHANNEL,)).fetchone()[0]
        == baseline.execute(
            "SELECT count(*) FROM orders WHERE channel = %s", (CHANNEL,)
        ).fetchone()[0]
    )


def test_a_second_sync_is_idempotent(cluster, seeded):
    region, baseline = seeded
    first, _ = sync(cluster, _settings())
    before = {table: _count(baseline, table) for table in SYNC_TABLES}
    second, _ = sync(cluster, _settings())
    assert second == first
    # the baseline holds the same rows after the second sync, not just
    # the same reported counts
    for table in SYNC_TABLES:
        assert _count(baseline, table) == before[table]
        assert _count(baseline, table) == _count(region, table)


def test_a_region_that_cannot_be_reached_is_an_error_that_leaves_the_baseline_alone(
    cluster, seeded
):
    """r2 does not exist in the 1-region cluster, so its primary refuses
    the connection: the sync must fail loudly, name it, and leave the
    baseline exactly as the previous sync left it (the region preflight
    runs before the baseline is touched, and the one transaction covers
    everything after)."""
    region, baseline = seeded
    sync(cluster, _settings())  # a good sync first, so there is something to lose
    before = {table: _count(baseline, table) for table in SYNC_TABLES}
    missing_rid = f"r{cluster.regions + 1}"
    missing = dataclasses.replace(cluster, regions=cluster.regions + 1)
    with pytest.raises(BaselineSyncError) as caught:
        sync(missing, _settings())

    assert primary_of(missing_rid) in str(caught.value)
    for table in SYNC_TABLES:
        assert _count(baseline, table) == before[table]


def test_a_copy_failure_after_the_delete_rolls_the_baseline_back(cluster, seeded, monkeypatch):
    """A region that dies mid-copy is still "a region that cannot be
    reached": the error names it and the table, and the one transaction
    puts the baseline back exactly as it was - including the rows the
    DELETE had already removed."""
    region, baseline = seeded
    sync(cluster, _settings())  # a good sync first: there is something to lose
    before = {table: _count(baseline, table) for table in SYNC_TABLES}

    real_copy = baseline_sync._copy_table

    def fail_on_order_items(source, bs, table):
        if table == "order_items":
            raise psycopg.OperationalError("connection reset by peer")
        return real_copy(source, bs, table)

    monkeypatch.setattr(baseline_sync, "_copy_table", fail_on_order_items)

    with pytest.raises(BaselineSyncError) as caught:
        sync(cluster, _settings())
    message = str(caught.value)
    assert "order_items" in message
    assert "r1-primary" in message
    assert "left untouched" in message
    for table in SYNC_TABLES:
        assert _count(baseline, table) == before[table]

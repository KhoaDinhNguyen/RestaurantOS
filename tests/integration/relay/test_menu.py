"""Publisher tests use real schemas and a test-only atomic WP1 stand-in."""

from decimal import Decimal

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict

from restaurantos.common.cluster import Cluster, proxy_for
from restaurantos.common.errors import RegionUnreachableError, StaleEpochError
from restaurantos.relay import menu

pytestmark = pytest.mark.integration


@pytest.fixture
async def menus(hq, outbox, monkeypatch):
    headquarters, hq_pool, settings = hq
    region, region_pool, _ = outbox
    for conn in (headquarters, region):
        await conn.execute(
            "INSERT INTO vendors (vendor_id, geohash) VALUES ('v001', 'w21z7'), ('v002', 'w21z6')"
        )
        await conn.execute(
            "INSERT INTO dishes (dish_id, stock_mode, scope) VALUES ('d001', 'per_item', 'global')"
        )
        for product_id in ("p001", "p002"):
            await conn.execute(
                "INSERT INTO products (vendor_id, product_id, name, unit_price, price_units)"
                " VALUES ('v001', %s, 'old', 0.1234567890123456789, 10)",
                (product_id,),
            )
            await conn.execute(
                "INSERT INTO product_dish (vendor_id, product_id, dish_id, multiplier,"
                " multiplier_ambiguous) VALUES ('v001', %s, 'd001', 3, true)",
                (product_id,),
            )
    await headquarters.execute(
        "INSERT INTO products (vendor_id, product_id, name, unit_price, price_units)"
        " VALUES ('v002', 'other', 'unaffected', 0.2, 20)"
    )
    await headquarters.execute(
        "INSERT INTO product_dish (vendor_id, product_id, dish_id) VALUES ('v002', 'other', 'd001')"
    )
    await region.execute("INSERT INTO node_state (region_id, epoch) VALUES ('r1', 7)")
    await region.execute(
        "INSERT INTO products (vendor_id, product_id, name, unit_price, price_units, origin)"
        " VALUES ('v001', 'local', 'local menu', 0.2, 20, 'region')"
    )
    await headquarters.execute(
        "INSERT INTO region_state (region_id, primary_node, epoch) "
        "VALUES ('r1', 'r1-standby', 7), ('r2', 'r2-primary', 1)"
    )
    await headquarters.execute(
        "INSERT INTO shard_directory VALUES ('w21z7', 'r1'), ('w21z6', 'r2')"
    )
    await headquarters.execute("INSERT INTO menu_versions (version) VALUES (2), (3)")
    await headquarters.execute(
        "UPDATE products SET name = 'latest', menu_version = 3 WHERE product_id = 'p001'"
    )
    # p002 remains at version 1 and must still be included in the full slice.
    destinations = []
    real_conninfo = menu.node_conninfo

    def destination(cluster, node, **kwargs):
        destinations.append((node, real_conninfo(cluster, node, **kwargs)))
        return region_pool.conninfo

    monkeypatch.setattr(menu, "node_conninfo", destination)
    return headquarters, hq_pool, region, settings.model_copy(update={"client": "hq"}), destinations


class AtomicMenu:
    """Only models the transaction boundary and version guard, not WP1 stock logic."""

    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    async def __call__(self, conn, version, rows, epoch):
        self.calls.append((version, rows, epoch))
        await conn.execute("SELECT assert_epoch(%s)", (epoch,))
        current = await conn.execute("SELECT menu_version FROM node_state")
        if version <= (await current.fetchone())[0]:
            return
        for row in rows:
            await conn.execute(
                "UPDATE products SET name = %s, menu_version = %s, price_units = %s"
                " WHERE vendor_id = %s AND product_id = %s",
                (
                    row["name"],
                    row["menu_version"],
                    row["price_units"],
                    row["vendor_id"],
                    row["product_id"],
                ),
            )
            if self.fail:
                raise psycopg.OperationalError("simulated reset during regional apply")
        await conn.execute("UPDATE node_state SET menu_version = %s", (version,))


async def local_state(region):
    version = await region.execute("SELECT menu_version FROM node_state")
    products = await region.execute(
        "SELECT product_id, name, menu_version FROM products ORDER BY product_id"
    )
    return (await version.fetchone())[0], await products.fetchall()


async def test_newest_complete_slice_and_promoted_node(menus):
    hq, pool, region, settings, destinations = menus
    apply = AtomicMenu()
    await menu.publish_menu(pool, Cluster(regions=2), "r1", settings, apply)
    assert len(apply.calls) == 1
    version, rows, epoch = apply.calls[0]
    assert (version, epoch) == (3, 7)
    assert [row["product_id"] for row in rows] == ["p001", "p002"]
    assert [row["menu_version"] for row in rows] == [3, 1]
    assert rows[0]["unit_price"] == Decimal("0.1234567890123456789")
    assert rows[0]["dish_id"] == "d001"
    assert rows[0]["multiplier"] == 3
    assert rows[0]["multiplier_ambiguous"] is True
    assert destinations[0][0] == "r1-standby"
    assert conninfo_to_dict(destinations[0][1])["port"] == str(
        proxy_for(Cluster(regions=2), "hq", "r1-standby").port
    )
    recorded = await hq.execute("SELECT menu_version FROM region_state WHERE region_id = 'r1'")
    assert await recorded.fetchone() == (3,)
    assert await local_state(region) == (
        3,
        [("local", "local menu", 1), ("p001", "latest", 3), ("p002", "old", 1)],
    )
    await menu.publish_menu(pool, Cluster(regions=2), "r1", settings, apply)
    assert len(apply.calls) == 1


async def test_unaffected_region_does_not_receive_a_slice(menus):
    _, pool, region, settings, _ = menus
    apply = AtomicMenu()
    await menu.publish_menu(pool, Cluster(regions=2), "r2", settings, apply)
    assert apply.calls == []
    assert (await local_state(region))[0] == 1


async def test_timer_resends_to_behind_region_even_when_hq_records_delivery(menus):
    hq, pool, region, settings, _ = menus
    await hq.execute("UPDATE region_state SET menu_version = 3 WHERE region_id = 'r1'")
    apply = AtomicMenu()
    await menu.publish_menu(pool, Cluster(regions=2), "r1", settings, apply)
    assert len(apply.calls) == 1
    assert (await local_state(region))[0] == 3


async def test_reset_rolls_back_whole_regional_menu_then_retry_succeeds(menus):
    hq, pool, region, settings, _ = menus
    before = await local_state(region)
    apply = AtomicMenu(fail=True)
    with pytest.raises(RegionUnreachableError):
        await menu.publish_menu(pool, Cluster(regions=2), "r1", settings, apply)
    assert await local_state(region) == before
    recorded = await hq.execute("SELECT menu_version FROM region_state WHERE region_id = 'r1'")
    assert await recorded.fetchone() == (1,)
    apply.fail = False
    await menu.publish_menu(pool, Cluster(regions=2), "r1", settings, apply)
    assert (await local_state(region))[0] == 3


async def test_lost_hq_ack_is_repaired_without_reapplying(menus, monkeypatch):
    hq, pool, _, settings, _ = menus
    apply = AtomicMenu()
    real_run_on = menu.run_on

    async def lose_ack(pool, work, **kwargs):
        if work.__name__ == "acknowledge":
            raise RegionUnreachableError("HQ acknowledgement lost")
        return await real_run_on(pool, work, **kwargs)

    monkeypatch.setattr(menu, "run_on", lose_ack)
    with pytest.raises(RegionUnreachableError):
        await menu.publish_menu(pool, Cluster(regions=2), "r1", settings, apply)
    monkeypatch.setattr(menu, "run_on", real_run_on)
    await menu.publish_menu(pool, Cluster(regions=2), "r1", settings, apply)
    assert len(apply.calls) == 1
    recorded = await hq.execute("SELECT menu_version FROM region_state WHERE region_id = 'r1'")
    assert await recorded.fetchone() == (3,)


async def test_slice_and_label_stay_coherent_while_hq_changes(menus, monkeypatch):
    hq, pool, _, settings, _ = menus
    apply = AtomicMenu()
    real_run_on = menu.run_on

    async def concurrent_publish(pool, work, **kwargs):
        result = await real_run_on(pool, work, **kwargs)
        if work.__name__ == "snapshot":
            await hq.execute("INSERT INTO menu_versions (version) VALUES (4)")
            await hq.execute(
                "UPDATE products SET name = 'next', menu_version = 4 WHERE product_id = 'p001'"
            )
        return result

    monkeypatch.setattr(menu, "run_on", concurrent_publish)
    await menu.publish_menu(pool, Cluster(regions=2), "r1", settings, apply)
    assert apply.calls[0][0] == 3
    assert apply.calls[0][1][0]["name"] == "latest"
    monkeypatch.setattr(menu, "run_on", real_run_on)
    await menu.publish_menu(pool, Cluster(regions=2), "r1", settings, apply)
    assert apply.calls[1][0] == 4
    assert apply.calls[1][1][0]["name"] == "next"


async def test_stale_epoch_refreshes_target_and_retries(menus):
    hq, pool, _, settings, destinations = menus
    apply = AtomicMenu()
    attempts = []

    async def promote(conn, version, rows, epoch):
        attempts.append(epoch)
        if epoch == 7:
            await hq.execute(
                "UPDATE region_state SET primary_node = 'r1-primary', epoch = 8"
                " WHERE region_id = 'r1'"
            )
            raise StaleEpochError()
        # The stand-in checks the actual test node's epoch.
        await conn.execute("UPDATE node_state SET epoch = 8")
        await apply(conn, version, rows, epoch)

    await menu.publish_menu(pool, Cluster(regions=2), "r1", settings, promote)
    assert attempts == [7, 8]
    assert [node for node, _ in destinations] == ["r1-standby", "r1-primary"]

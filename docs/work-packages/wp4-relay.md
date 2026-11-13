# WP4: Relay

**Goal:** move data between HQ and the regions, safely, in the background. Three small workers that all survive being killed at any moment.

**You own:** `src/restaurantos/relay/`, the HQ apply functions in `db/hq/`, `db/queries/relay/`, `tests/unit/relay/`, `tests/integration/relay/`.

**Read first:** `docs/architecture.md` Sections 4 (proxy names), 5.1 and 5.3; Phase 2 Sections 2.3 and 2.4 (recovery logs).

**Needs:** WP0. The outbox is filled by WP1; until then, insert outbox rows by hand in tests.

## Shared code you must use

| For | Use |
| --- | --- |
| which local node is the region's primary | `common.db.find_local_primary` |
| connection strings and pools | `common.db.node_conninfo`, `make_pool` |
| every database call | `common.db.run_on`; treat `RegionUnreachableError` as "try again later" |
| the events you move | `common.models`: `OutboxEvent` and the three payload models |
| delivering a parked order, applying a menu version | `restaurantos.region.place_order` and `apply_menu_version` (WP1) |
| loading your SQL | `common.sqlfiles.load_query("relay/<name>")`, files in `db/queries/relay/` |
| settings | your own in `relay/config.py`: role, batch size, poll interval, local check interval |

## What to build

### 1. Outbox relay (region to HQ), one process per region

Loop:

0. Find your region's primary: `common.db.find_local_primary(cluster, region_id, current)`. Do it at start, before every batch after an error, and at least every `ROS_RELAY_LOCAL_CHECK_S` (default 1). If it returns nothing, sleep and look again. If it returns a different node, finish nothing on the old one: drop that pool and read the new node's outbox from now on. The rule is in architecture Section 4.
1. Read `outbox` rows where `relayed_at IS NULL`, in `seq` order, up to a batch size (`ROS_RELAY_BATCH_SIZE`, default 500).
2. In **one transaction at HQ** (through `relay-r<n>__hq-primary`): for each event, insert into `hq_inbox` with `ON CONFLICT DO NOTHING`; if it was new, apply it and stamp `hq_applied_at`.
   - `order.created`: insert into `analytics_orders` and `analytics_order_items`;
   - `order.status_changed`: update the status;
   - `exception.logged`: insert into `analytics_exceptions`.
3. After HQ commits, set `relayed_at` on those rows in the region.
4. Sleep for the poll interval (`ROS_RELAY_POLL_INTERVAL_S`, default 0.5; it sets the floor for the HQ staleness metric). Skip the sleep when the batch was full: there is more waiting.

These settings live in your own `relay/config.py`, not in `common/config.py` (see `AGENTS.md`).

After a failover the new primary's outbox can hold rows the old relay already sent: the `relayed_at` update had not been replicated. Sending them again is harmless, `hq_inbox` skips them. The reverse also happens: rows the old primary held and you had already forwarded may not exist on the new primary at all. That is the "lost at failover" amount in architecture Section 7.5, not something to repair here.

If the process dies between steps 2 and 3, it resends the batch and HQ skips every event it already has.

### The HQ worker: parts 2 and 3 in one process

The next two workers run together in one process on the HQ side, called `hq-worker`. It is the same package started in its other role (`ROS_RELAY_ROLE=hq`, `ROS_CLIENT=hq`), with one task per region so that a region that is down does not hold up the others. It is its own process on purpose, not part of the coordinator or the gateway: a large drain after a partition must not share an event loop with heartbeats or with live requests, and test T3 must be able to kill it alone. It reaches the regions through the `hq__<node>` proxies and HQ's own database directly.

### 2. Holding-queue drainer (HQ to region)

For each region, take undelivered `holding_queue` rows in order, call `region.place_order` on that region's primary (with the current epoch, and `received_at` from the queue row so the time spent parked is measured), then set `delivered_at` and `delivered_outcome`. Safe to repeat: the region's inbox drops duplicates. A refused order is still "delivered".

Reach the primary through `hq__<node>`, where `<node>` is `region_state.primary_node`. After a failover that is `hq__r<n>-standby`; do not build the proxy name from the region ID.

### 3. Menu publisher (HQ to region)

When a new row appears in `menu_versions`, send each region its slice with `region.apply_menu_version` (through `hq__<node>` as above, with the current epoch) in one transaction, then record the version in `region_state`. If the connection resets halfway, the region's transaction rolls back and it keeps serving the old version; retry.

## Run it on its own

Needs the dev cluster (see "Running things" in `README.md` in this folder). The package runs in one of two roles, chosen by `ROS_RELAY_ROLE` in your own `relay/config.py`:

```bash
# part 1, once per region, region side
ROS_RELAY_ROLE=region ROS_CLIENT=relay-r1 ROS_REGION_ID=r1 uv run python -m restaurantos.relay
# parts 2 and 3 together, once, HQ side: the "hq-worker"
ROS_RELAY_ROLE=hq ROS_CLIENT=hq uv run python -m restaurantos.relay
```

Watch the two ends while it runs (password `ros`):

```bash
psql -h 127.0.0.1 -p 5510 -U ros -d restaurantos -c "SELECT count(*) FROM outbox WHERE relayed_at IS NULL"
psql -h 127.0.0.1 -p 5500 -U ros -d restaurantos -c "SELECT count(*) FROM hq_inbox"
```

Until WP1 fills the outbox, insert rows yourself: build the payload with `OrderCreatedPayload(...).model_dump_json()` so it has the real shape. To see a partition by hand, switch the link off, let a backlog build, and switch it on again:

```bash
curl -s -X POST 127.0.0.1:8474/proxies/relay-r1__hq-primary -d '{"enabled": false}'
curl -s -X POST 127.0.0.1:8474/reset
```

## Done when

What each line needs is marked in front of it (the marks are explained in `README.md` in this folder).

- `data (needs WP1 and WP6a)` After a replay and a drain, the HQ analytics copy has exactly the orders the regions have: same count, no duplicates.
- `cluster` Kill the relay at random points during a drain (test T3), and the HQ worker likewise: the final counts are still right.
- `cluster (needs WP5, or promote by hand)` After a promotion in its region (T1 and T1b), the relay moves to the new primary without a restart and HQ receives the orders placed there.
- `cluster` After a region-to-HQ partition heals, the outbox empties upward and the holding queue empties downward.
- `cluster` A connection reset during menu publishing (test T6) leaves the region on a complete old or complete new version, never a mix.
- `db` Outbox backlog (count of rows with `relayed_at IS NULL`) is readable, for the admin page.

## Watch out for

- Do not replace the `relayed_at` flag with a "last seq sent" cursor: concurrent commits can land out of `seq` order and a cursor would skip rows.
- Apply events in `seq` order. A status change must not reach HQ before its order.
- Make pools with `common.db.make_pool(common.db.node_conninfo(cluster, node))` and run every database call through `common.db.run_on`. A silenced or slowed link must make a worker back off and retry, not hang, and only the deadline in `run_on` gives you that (architecture Section 5.2). Treat `RegionUnreachableError` as "try again later"; every write here is safe to repeat.
- Only switch local nodes when `find_local_primary` says so. Never guess from a failed query that the other node must be the primary.
- Keep batches modest so one bad event does not block a huge transaction.

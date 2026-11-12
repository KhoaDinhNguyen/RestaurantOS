# WP4: Relay

**Goal:** move data between HQ and the regions, safely, in the background. Three small workers that all survive being killed at any moment.

**You own:** `src/restaurantos/relay/`, the HQ apply functions in `db/hq/`, `tests/unit/relay/`, `tests/integration/relay/`.

**Read first:** `docs/architecture.md` Sections 4 (proxy names), 5.1 and 5.3; Phase 2 Sections 2.3 and 2.4 (recovery logs).

**Needs:** WP0. The outbox is filled by WP1; until then, insert outbox rows by hand in tests.

## What to build

### 1. Outbox relay (region to HQ), one process per region

Loop:

1. Read `outbox` rows with `seq > outbox_cursor.last_acked_seq`, in `seq` order, up to a batch size.
2. In **one transaction at HQ** (through `relay-r<n>__hq-primary`): for each event, insert into `hq_inbox` with `ON CONFLICT DO NOTHING`; if it was new, apply it and stamp `hq_applied_at`.
   - `order.created`: insert into `analytics_orders` and `analytics_order_items`;
   - `order.status_changed`: update the status;
   - `exception.logged`: insert into `analytics_exceptions`.
3. After HQ commits, update `outbox_cursor` in the region.
4. Sleep for the poll interval (config; it sets the floor for the HQ staleness metric).

If the process dies between steps 2 and 3, it resends the batch and HQ skips every event it already has.

### 2. Holding-queue drainer (HQ to region)

For each region, take undelivered `holding_queue` rows in order, call `region.place_order` on that region's primary (through `hq__r<n>-primary`, with the current epoch), then set `delivered_at`. Safe to repeat: the region's inbox drops duplicates. A refused order is still "delivered".

### 3. Menu publisher (HQ to region)

When a new row appears in `menu_versions`, send each region its slice with `region.apply_menu_version` in one transaction, then record the version in `region_state`. If the connection resets halfway, the region's transaction rolls back and it keeps serving the old version; retry.

## Done when

- After a replay and a drain, the HQ analytics copy has exactly the orders the regions have: same count, no duplicates.
- Kill the relay at random points during a drain (test T3): the final counts are still right.
- After a region-to-HQ partition heals, the outbox empties upward and the holding queue empties downward.
- A connection reset during menu publishing (test T6) leaves the region on a complete old or complete new version, never a mix.
- Outbox backlog (`max(seq) - last_acked_seq`) is readable, for the admin page.

## Watch out for

- Apply events in `seq` order. A status change must not reach HQ before its order.
- Set connection and statement timeouts. A silenced link must make the worker back off and retry, not hang.
- Keep batches modest so one bad event does not block a huge transaction.

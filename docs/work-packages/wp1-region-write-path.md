# WP1: Region write path

**Goal:** everything that changes data on one region node, each as one local transaction.

**You own:** `src/restaurantos/region/`, the functions in `db/region/`, `db/queries/region/`, `tests/unit/region/`, `tests/integration/region/`.

**Read first:** `docs/architecture.md` Sections 5.1 to 5.4; `docs/schema/data-dictionary.md` ("Amounts and units"); Phase 2 Sections 2.3, 2.4 (conflict resolution) and 2.6.

**Needs:** WP0 (tables and shared models). Nothing else: you can test against one plain PostgreSQL.

## Shared code you must use

| For | Use |
| --- | --- |
| request and result shapes | `common.models`: `OrderRequest`, `OrderResult`, `StatusResult`, `OrderStatus`, `RefusalReason` |
| outbox rows | `common.models`: `OutboxEvent`, `OrderCreatedPayload`, `StatusChangedPayload`, `ExceptionLoggedPayload` |
| the epoch check | the SQL function `assert_epoch` (in `db/region/`), called first in every transaction |
| errors | `common.errors`: `StaleEpochError`, `UnknownVendorError` |
| loading your SQL | `common.sqlfiles.load_query("region/<name>")`, files in `db/queries/region/` |

You never open a connection or a pool: callers hand you one. They call you through `common.db.run_on`, which turns a `ROS01` error into `StaleEpochError` for them.

## What to build

Five async functions (signatures in Section 5.2 of the architecture doc). Callers are the gateway, the edge API and the relay. They pass you an open connection and the epoch they believe is current. `place_order` also takes `received_at`, the wall-clock time the request first reached the system; store it on the order and put it in the `order.created` payload.

### `place_order`

One transaction, in this order:

1. Call `assert_epoch` with the caller's epoch. A mismatch raises SQLSTATE `ROS01`, which becomes `StaleEpochError`.
2. Generate the `order_id` first, then insert into `inbox` (outcome `accepted`, with that `order_id`, the request's `order_day`, and no `refusal_reason`) using `ON CONFLICT DO NOTHING`. The table's check constraints run before the conflict check, so the row must be complete. If no row was inserted, this is a duplicate: return the stored outcome (the existing order, or the earlier refusal) and stop. No other side effects.
3. Look up each item **for this vendor**: `products`, then `product_dish`, then `dishes.stock_mode`, always by (`vendor_id`, `product_id`). A product id this vendor does not have is unknown, even if another vendor has it. In the dataset every product has exactly one `product_dish` row; treat a product that has none as unknown as well. An unknown product (`unknown_product`) or a product switched off in `product_availability` (`unavailable`) refuses the order: see "Refusing" below. If the vendor itself is not on this node, raise `UnknownVendorError` instead. That rolls the transaction back and keeps no inbox row: it is a routing mistake, not a refusal.
4. Work out what the order consumes:
   - `per_ingredient`: for each row in `dish_ingredients`, `amount x multiplier x quantity` of that ingredient. Read `amount` and `multiplier` from the tables: in today's dataset every `amount` is 1, but that is a placeholder, and the tests must use other values;
   - `per_item`: `multiplier x quantity` of the dish itself;
   - `none`: nothing.
5. Lock the needed `stock` rows with `SELECT ... FOR UPDATE`, always in the same order (`item_kind`, `item_id`), so two orders cannot deadlock.
6. If any row has too little (`out_of_stock`): **refuse the whole order**. A missing `stock` row counts as zero.
7. Otherwise decrement stock, and add a `product_availability` row (`reason = 'stock'`) for products that can no longer be made because a stock row just reached zero. Insert with `ON CONFLICT DO NOTHING`, so a `manual` row is never overwritten.
8. Insert `orders` (status `received`, with `menu_version`, the caller's `epoch` and `received_at`) and `order_items`.
9. Insert one `outbox` row (`order.created`). Commit.

**Refusing** (from step 3 or step 6) is always the same: update the inbox row to `outcome = 'refused'`, `order_id = NULL` and a `refusal_reason`, all three together because the table checks them as a set; keep `order_day`; write nothing else; commit; return refused. If more than one reason applies, report the first one in step order. The three reasons are `RefusalReason` in `common/models.py`, and the table accepts no others.

### `change_status`

1. Epoch check.
2. Insert `event_id` into `processed_events`; if it is already there, return the current order unchanged (`StatusResult` with `duplicate = true`).
3. Check the move against the state machine: `received -> accepted -> preparing -> ready -> picked_up`, or `canceled` from any state before `picked_up`.
4. Allowed: update the order, add an `order.status_changed` outbox row.
5. Not allowed: leave the order alone, write both versions and timestamps to `exception_log`, add an `exception.logged` outbox row, return a `StatusResult` with `conflict = true`.

### The rest

- `restock`: set `on_hand = par_level` for one vendor, then delete that vendor's `product_availability` rows with `reason = 'stock'` (every counter is back at par; the stock check in `place_order` still decides each order).
- `set_availability`: manual sold-out switch. Off: upsert a row with `reason = 'manual'`. On: delete the row. A restock must not undo a manual sold-out.
- `apply_menu_version`: epoch check, then replace this region's `products` and `product_dish` rows where `origin = 'hq'`, and set `node_state.menu_version`, all in one transaction. Never touch rows with `origin = 'region'`.

`product_availability` holds a row only while a product is switched off, so `available` is `false` on every row you write. Readers (the menu, the nearby query) still test `available = false` rather than "a row exists".

## Run it on its own

There is no process to start. This package is a library that the gateway, the edge API and the relay call, and all it needs is one PostgreSQL.

```bash
uv run pytest tests/unit/region tests/integration/region
```

The integration tests need `ROS_TEST_DSN` (see "Running things" in `README.md` in this folder). Each test applies the region schema (`common.sqlfiles.schema_files("region")`, as `tests/integration/test_schema.py` does) and inserts the few rows it needs: a vendor, a product, its dish and recipe, and stock. Until WP6a's mini fixture exists, keep those rows in a fixture of your own, with an `amount` and a `multiplier` above 1.

## Done when

What each line needs is marked in front of it (the marks are explained in `README.md` in this folder).

- `db` The same order sent twice produces one order, one stock change and one outbox row.
- `db` Two orders racing for the last of an ingredient: exactly one succeeds.
- `db` A dish whose recipe has `amount` 250 and a product with `multiplier` 2, ordered with quantity 3, takes 1,500 off the counter.
- `db` A stale epoch is rejected and changes nothing.
- `db` A refused order sent again is refused again, with no side effects.
- `db` A conflicting status change lands in `exception_log` and the first change stands.
- `data (needs WP2 and WP6a)` Single-region sequential playback (WP6a) matches the counting oracle: orders stored, orders refused, final stock.

## Watch out for

- The epoch fence is cooperative. `assert_epoch` protects the transactions that call it, and nothing in the schema stops a write that skips it. So every function here calls it first, inside its transaction, and this package is the only code that writes a region's operational tables. If the gateway or the relay needs a new kind of write, it becomes a function here.
- Never call `now()` for business time. `order_day` and `order_time` come from the request. Use `clock_timestamp()` only for `committed_at`.
- Keep the availability update cheap: only look at stock rows that just crossed zero, not the whole menu.
- Hot path: the item lookup runs for every order. Check it uses the primary keys.

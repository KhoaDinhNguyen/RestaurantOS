# WP1: Region write path

**Goal:** everything that changes data on one region node, each as one local transaction.

**You own:** `src/restaurantos/region/`, the functions in `db/region/`, `tests/unit/region/`, `tests/integration/region/`.

**Read first:** `docs/architecture.md` Sections 5.1 to 5.4; Phase 2 Sections 2.3, 2.4 (conflict resolution) and 2.6.

**Needs:** WP0 (tables and shared models). Nothing else: you can test against one plain PostgreSQL.

## What to build

Five async functions (signatures in Section 5.2 of the architecture doc). Callers are the gateway, the edge API and the relay. They pass you an open connection and the epoch they believe is current.

### `place_order`

One transaction, in this order:

1. Call `assert_epoch` with the caller's epoch. A mismatch raises SQLSTATE `ROS01`, which becomes `StaleEpochError`.
2. Generate the `order_id` first, then insert into `inbox` (outcome `accepted`, with that `order_id`) using `ON CONFLICT DO NOTHING`. The table's check constraint runs before the conflict check, so the row must be complete. If no row was inserted, this is a duplicate: return the stored outcome (the existing order, or the earlier refusal) and stop. No other side effects.
3. Look up each item: `products` then `product_dish` then `dishes.stock_mode`. An unknown product or a product marked unavailable refuses the order.
4. Work out what the order consumes:
   - `per_ingredient`: for each row in `dish_ingredients`, `amount x multiplier x quantity` of that ingredient;
   - `per_item`: `multiplier x quantity` of the dish itself;
   - `none`: nothing.
5. Lock the needed `stock` rows with `SELECT ... FOR UPDATE`, always in the same order (`item_kind`, `item_id`), so two orders cannot deadlock.
6. If any row has too little: **refuse the whole order**. Update the inbox row to `outcome = 'refused'`, `order_id = NULL`, with a `refusal_reason`; write nothing else, commit, return refused.
7. Otherwise decrement stock, and add a `product_availability` row (`reason = 'stock'`) for products that can no longer be made because a stock row just reached zero. A product with no row is available.
8. Insert `orders` (status `received`, with `menu_version`) and `order_items`.
9. Insert one `outbox` row (`order.created`). Commit.

### `change_status`

1. Epoch check.
2. Insert `event_id` into `processed_events`; if it is already there, return the current order unchanged.
3. Check the move against the state machine: `received -> accepted -> preparing -> ready -> picked_up`, or `canceled` from any state before `picked_up`.
4. Allowed: update the order, add an `order.status_changed` outbox row.
5. Not allowed: leave the order alone, write both versions and timestamps to `exception_log`, add an `exception.logged` outbox row, return `Conflict`.

### The rest

- `restock`: set `on_hand = par_level` for one vendor, then make products available again where stock allows.
- `set_availability`: manual sold-out switch (`reason = 'manual'`). A restock must not undo a manual sold-out.
- `apply_menu_version`: replace this region's `products` and `product_dish` rows where `origin = 'hq'`, and set `node_state.menu_version`, all in one transaction. Never touch rows with `origin = 'region'`.

## Done when

- The same order sent twice produces one order, one stock change and one outbox row.
- Two orders racing for the last portion: exactly one succeeds.
- A stale epoch is rejected and changes nothing.
- A refused order sent again is refused again, with no side effects.
- A conflicting status change lands in `exception_log` and the first change stands.
- Single-region sequential playback (WP6a) matches the counting oracle: orders stored, orders refused, final stock.

## Watch out for

- Never call `now()` for business time. `order_day` and `order_time` come from the request. Use `clock_timestamp()` only for `committed_at`.
- Keep the availability update cheap: only look at stock rows that just crossed zero, not the whole menu.
- Hot path: the item lookup runs for every order. Check it uses the primary keys.

# WP3: Query router

**Goal:** answer questions that span regions by sending work only where the data is, and merging small partial results.

**You own:** `src/restaurantos/gateway/router/`, `db/queries/router/`, `tests/unit/router/`, `tests/integration/router/`.

**Read first:** `docs/architecture.md` Section 6.1 (the `/query/...` routes); Phase 2 Section 2.5.

**Needs:** WP0. You can develop against two or three plain PostgreSQL databases loaded with the mini fixture. End-to-end runs need WP6a (real data) and WP2 (the app your routes mount on).

## What to build

### Nearby: `GET /query/nearby?geohash=&dish=&limit=`

1. Take the customer's cell and its eight neighbours (`pygeohash`).
2. Look the nine cells up in the shard directory; contact only the regions that own at least one.
3. Run the same SQL on each, in parallel. A result row is one (vendor, product) where:
   - the vendor is in one of the requested cells;
   - the product's dish is `dish` (a `dish_id`);
   - the product is not switched off: no `product_availability` row with `available = false`;
   - the vendor has stock for **one portion** right now. `per_ingredient` dishes: `on_hand >= amount x multiplier` for every row in `dish_ingredients`. `per_item` dishes: `on_hand >= multiplier`. `none` dishes always pass. A missing `stock` row counts as zero.

   This is the rule `place_order` applies to an order line with quantity 1 (WP1 steps 3 to 6), so a vendor returned here would accept that order at this moment. Each region returns at most `limit` rows, in the order of step 4.
4. Merge and return the top `limit`, in one total order: distance between cell centres, then `vendor_id`, then `product_id`. Send each cell together with its distance, so every region sorts the same way. Without the tie-break, the cluster and the baseline can return different rows for the same `limit`.

### City-wide: `GET /query/top-dishes?day=&limit=&mode=`

- `mode=live`: every region groups and sums its own order lines for that day and returns **one row per dish**. The router adds the partial sums and sorts.
- Never take a local top-N in a region: a dish spread over regions can miss every local top-N and still be in the city-wide top-N.
- `mode=hq`: one query on HQ's analytics copy.

### Distributed join: `GET /query/ingredient-usage?day=&mode=&strategy=`

The number is the same for every strategy and mode: per `ingredient_id`, `sum(quantity x multiplier x amount)` over every order line with `order_day = day` whose dish has rows in `dish_ingredients`. Count canceled orders too: stock is not given back on a cancel, and a status filter would make `mode=hq` disagree with `mode=live` whenever a status event is still on its way to HQ.

The order lines are split across the regions. `product_dish` is split the same way, so the step from product to dish is always next to the lines. What differs is where `dish_ingredients` is read and what has to travel:

| Strategy | Where the join with `dish_ingredients` runs | What crosses a region link |
| --- | --- | --- |
| A, co-located | in each region, against the region's own copy | up: one row per ingredient |
| B, ship everything | at the router, against HQ's `product_dish` and `dish_ingredients` | up: every order line of that day |
| C, semi-join | in each region, against rows the router sends | round 1 up: the distinct `dish_id`s the region sold that day. Round 1 down: HQ's `dish_ingredients` rows for exactly those dishes. Round 2 up: one row per ingredient |

C is the textbook semi-join: take the join column from the fragment, use it to cut the other table down at its home site, and ship only what is left. It only means something if the region has no copy of the recipe table. So in strategy C the region's SQL **must not read its local `dish_ingredients`**; it joins against the rows it was sent (arrays with `unnest`, a JSON parameter, or a temporary table). Without that rule C is strategy A with extra steps.

What the three show together: A is the cheapest query but needs the recipe table copied to every region; B needs nothing copied and ships the most; C needs nothing copied and ships far less than B.

All three must return identical numbers. `mode=hq` is one query on HQ's analytics copy and ignores `strategy`.

### For every query

- A `meta` object, `QueryMeta` in `common/models.py`: `mode`, `nodes_contacted`, `missing_regions`, `rows_transferred`, `data_as_of`, `elapsed_ms`.
- `rows_transferred` counts every row that crosses a region link, in either direction, over all rounds.
- `mode=baseline` runs the same SQL on the single `baseline` node. Connect to it directly (`common.db.node_conninfo(cluster, "baseline", direct=True)`): it has no Toxiproxy proxy because it is not part of the system under test.
- The baseline holds a copy of what the regions hold, made by `rosctl baseline sync` after a run (architecture Section 7.1). The data on both sides is therefore identical, and a different answer can only come from how you split and merge. On the mini fixture the test loads the same rows into the regions and the baseline itself.
- Each scatter call goes through `common.db.run_on`, so one silent region costs the query the request deadline at most, and then shows up in `meta.missing_regions`.
- `day` is always a parameter. Never "today" from the clock.

## Done when

- Every query returns the same result on the cluster and on the baseline: on the mini fixture, where the expected answers are also written down by hand, and on real data after `rosctl baseline sync`.
- Strategies A, B and C return identical numbers, on the mini fixture and on real data. A test proves C never reads the region's own `dish_ingredients` (for example: empty that table in the test region and C still answers correctly).
- On a full day of real data, `rows_transferred` is recorded for all three and comes out A < C < B. Do not assert that order on the mini fixture: a handful of order lines can be fewer rows than their recipes.
- A nearby query in the middle of one region contacts one node; one on a border contacts two.
- A top-N test exists where a local top-N would give the wrong answer, and the router gets it right.

## Watch out for

- When a region is unreachable, answer `200` with what the other regions returned and list the missing ones in `meta.missing_regions` (the field already exists in `QueryMeta`). Do not fail the whole query, and do not fill the gap from HQ's copy.
- `orders` is partitioned by `order_day`: always filter on it so PostgreSQL prunes partitions.
- Averages: transfer sums and counts, divide at the router.

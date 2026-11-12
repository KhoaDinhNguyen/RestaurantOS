# WP3: Query router

**Goal:** answer questions that span regions by sending work only where the data is, and merging small partial results.

**You own:** `src/restaurantos/gateway/router/`, `db/queries/`, `tests/unit/router/`, `tests/integration/router/`.

**Read first:** `docs/architecture.md` Section 6.1 (the `/query/...` routes); Phase 2 Section 2.5.

**Needs:** WP0. You can develop against two or three plain PostgreSQL databases loaded with the mini fixture. End-to-end runs need WP6a (real data) and WP2 (the app your routes mount on).

## What to build

### Nearby: `GET /query/nearby?geohash=&dish=&limit=`

1. Take the customer's cell and its eight neighbours (`pygeohash`).
2. Look the nine cells up in the shard directory; contact only the regions that own at least one.
3. Run the same SQL on each, in parallel: vendors in those cells with a product whose dish matches, not sold out, with enough stock for every ingredient. Each region returns at most `limit` rows.
4. Merge, rank by distance between cell centres, return the top `limit`.

### City-wide: `GET /query/top-dishes?day=&limit=&mode=`

- `mode=live`: every region groups and sums its own order lines for that day and returns **one row per dish**. The router adds the partial sums and sorts.
- Never take a local top-N in a region: a dish spread over regions can miss every local top-N and still be in the city-wide top-N.
- `mode=hq`: one query on HQ's analytics copy.

### Distributed join: `GET /query/ingredient-usage?day=&mode=&strategy=`

| Strategy | What happens |
| --- | --- |
| A, co-located | each region joins its order lines with its local dish tables and returns one total per ingredient |
| B, ship everything | each region returns raw order lines; the router joins them with HQ's dish tables |
| C, semi-join | the router sends each region only the dish rows for dishes that region sold |

All three must return identical numbers.

### For every query

- A `meta` object: `mode`, `nodes_contacted`, `rows_transferred`, `data_as_of`, `elapsed_ms`.
- A baseline switch that runs the same SQL on the single `baseline` node.
- `day` is always a parameter. Never "today" from the clock.

## Done when

- Every query returns the same result on the cluster and on the baseline, on the mini fixture and on real data.
- Strategies A, B and C agree, and `rows_transferred` shows the expected ordering (A smallest, B largest).
- A nearby query in the middle of one region contacts one node; one on a border contacts two.
- A top-N test exists where a local top-N would give the wrong answer, and the router gets it right.

## Watch out for

- Decide what happens when a region is unreachable. Suggested: return what you have and list the missing regions in `meta`, rather than failing the whole query.
- `orders` is partitioned by `order_day`: always filter on it so PostgreSQL prunes partitions.
- Averages: transfer sums and counts, divide at the router.

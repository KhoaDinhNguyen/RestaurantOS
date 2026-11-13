# The mini fixture

A small, hand-checkable, public stand-in for the cleaned dataset. The real
dataset is restricted and never enters this repository; these files were
derived from it once, sanitized, and are now maintained by hand like any
other fixture.

Load it with `restaurantos.sim.mini.load_mini`, after applying a schema
(`common.sqlfiles.schema_files`, as `tests/integration/test_schema.py` does).

## Where the files come from

Vendors from three adjacent geohash cells (w21z7, w21z6, w21zt), chosen as
the busiest on days 14 to 16. Then each vendor's real orders on those days,
the products those orders actually bought, and the dishes and ingredients
behind those products. Row counts:

| File | Rows |
| --- | --- |
| vendors.csv | 4 |
| ingredients.csv | 13 |
| dishes.csv | 19 |
| dish_ingredients.csv | 19 |
| products.csv | 19 |
| product_dish.csv | 19 |
| orders.csv | 20 |
| order_items.csv | 31 |
| stock.csv | 18 |

The first seven files carry the exact headers of the real dataset files.
`stock.csv` is fixture-only: in the real system starting stock is derived
from the warm-up window, so there is no dataset file for it.

## What was sanitized

- Every id (vendor, product, dish, order, customer, chain) was replaced by a
  short synthetic id: `v001`, `p001`, `d001`, `o0001`, `c001`, `ch01`. One
  product keeps an all-digit id with a leading zero (`0017`, ordered four
  times): the shape that breaks a reader which guesses types.
- Vendor names do not exist in the dataset, but product and dish names do:
  they were replaced with `Menu item NNN` and `Sample dish NNN`.
- Kept as they are: geohash cells (coarse, and already public in the docs),
  cuisines, ingredient names (open RecipeNLG terms), stock modes, scopes
  (with chain ids remapped), prices, day-of-week and times.
- `dishes.n_products` and `n_vendors` were recomputed from this slice, so
  they describe the fixture, not the full dataset.
- `price_units` is the price the system uses. `unit_price` was shortened to
  4 decimals to keep the files readable; the real files carry up to 19, and
  the system never calculates with it.

## Deliberate traps

The real dataset has `amount` 1 and `unit` "portion" on every recipe row, so
it cannot catch a rule that forgets to multiply. The fixture plants the cases
the briefs ask for:

- `beef` appears once, with `amount` 2 and `unit` kg.
- `caramel` appears once, with `amount` 3 and `unit` piece.
- Each of those ingredients appears in exactly one recipe row (an ingredient
  must never appear with two different units).
- Two `product_dish` rows have `multiplier` above 1 (one real, one edited).
- One dish has an empty `dish_name` (the real data has 3,053 of those).
- One vendor has an empty `chain_id` (the real data has 1,499).
- One stock counter is deliberately low (`on_hand` 2, `par_level` 10 for
  caramel at v004): an order of a dish that uses caramel 3 piece at v004
  takes more than the counter holds, so tests can drive an out-of-stock
  refusal without touching anything else.

## Expected answers, in short

Orders per vendor: v001, v002, v003, v004 each have 5; days are split
14: 9, 15: 5, 16: 6. The rest of the query answers (top dishes per day,
ingredient usage, nearby) is what the work-package tests compute and then
write down; this file keeps only the counts that do not depend on a query's
parameters.

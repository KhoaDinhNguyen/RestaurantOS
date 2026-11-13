# Data dictionary: the cleaned dataset

What is in the CSV files under `data/`, column by column, and what a loader has to watch for. The numbers were measured on the export of 2026-11-12 with `scripts/profile_dataset.py`; run it again after a re-export and update this file. The dataset itself is never committed (DHRD is not public), so this file describes its shape only.

This file is about the dataset. Our own tables (orders as stored, stock, inbox, outbox and the rest) are described in `docs/architecture.md` Section 5.1 and defined in `db/`.

## Files

| File | Rows | One row is | Loaded into |
| --- | --- | --- | --- |
| `ingredients.csv` | 1,438 | an ingredient | `ingredients` |
| `dishes.csv` | 123,871 | a dish: what a product is, once names are normalised | `dishes` |
| `dish_ingredients.csv` | 156,918 | one ingredient of one dish | `dish_ingredients` |
| `vendors.csv` | 7,411 | a restaurant | `vendors` |
| `products.csv` | 256,496 | a menu item of one restaurant | `products` |
| `product_dish.csv` | 256,496 | which dish a product is, and how many of it | `product_dish` |
| `orders.csv` | 1,999,988 | an order | not loaded: playback sends it (see below) |
| `order_items.csv` | 3,923,435 | one line of an order | not loaded: sent with its order |
| `MANIFEST.csv` | 8 | one data file: `file`, `rows`, `sha256` | not loaded: bootstrap checks the files against it |

The six reference tables mirror their files one to one: same names, same columns. The only columns we add are `products.menu_version` and `products.origin`, which have defaults and are not in the file.

## Rules for anything that reads these files

1. **Check `MANIFEST.csv` first**: row count and SHA-256 of every file.
2. **Read every column as text.** The ids are short strings of digits and letters, and some of them happen to contain only digits, a few with leading zeros. A reader that guesses types turns them into numbers and breaks every join. Convert only the columns marked "whole number" or "decimal" below.
3. **Give `COPY` the column list from the file's header.** `products.csv` and `product_dish.csv` start with `product_id`, then `vendor_id`; the tables have them the other way round. Both are text, so nothing but a foreign key stops a swap.
4. **An empty field means "no value".** `COPY ... (FORMAT csv)` loads it as NULL, which is what `vendors.chain_id` and `dishes.dish_name` expect. No other column has empty fields.
5. **Load in this order**, because of the foreign keys: `ingredients`, `dishes`, `dish_ingredients`, `vendors`, `products`, `product_dish`.
6. **Never assume a value for `amount`, `unit` or `multiplier`.** Read them. See "Amounts and units".

## `ingredients.csv`

| Column | Type | Meaning |
| --- | --- | --- |
| `ingredient_id` | text, 3 to 35 characters | the ingredient's name in lower case; it is the key |
| `source` | text | where the ingredient came from: `recipenlg` (1,402) or `added` during cleaning (36) |

Only ingredients that some dish uses are listed.

## `dishes.csv`

| Column | Type | Meaning |
| --- | --- | --- |
| `dish_id` | text, 12 hexadecimal characters | key. Stable across re-cleaning while the dish's name and scope do not change |
| `dish_name` | text, may be empty | the normalised name, lower case. Empty for 3,053 dishes |
| `scope` | text | how widely the dish is shared: `global` (28,802 dishes), or a narrower scope written as a prefix and an id, such as `chain:` and a chain id |
| `stock_mode` | text | how stock is counted for the dish; see the table below |
| `n_products` | whole number, 1 to 168 | how many products are this dish |
| `n_vendors` | whole number, 1 to 117 | how many restaurants sell it |
| `example_name` | text | one product name as it appears on a menu, for people reading the data |

| `stock_mode` | Dishes | Rows in `dish_ingredients` | Stock is counted |
| --- | --- | --- | --- |
| `per_ingredient` | 89,715 | every one has at least one | per ingredient of the dish |
| `per_item` | 33,329 | none | for the dish itself, as one item |
| `none` | 827 | none | not at all (add-ons) |

## `dish_ingredients.csv`

| Column | Type | Meaning |
| --- | --- | --- |
| `dish_id` | text | the dish |
| `ingredient_id` | text | one of its ingredients |
| `amount` | whole number, above zero | how much of the ingredient one unit of the dish uses |
| `unit` | text | what `amount` is counted in |

Key: (`dish_id`, `ingredient_id`). Only `per_ingredient` dishes appear here: 156,918 rows for 89,715 dishes.

## `vendors.csv`

| Column | Type | Meaning |
| --- | --- | --- |
| `vendor_id` | text, 8 characters (digits and lower-case letters) | key |
| `chain_id` | text, 8 characters, may be empty | the chain the restaurant belongs to. Empty for 1,499 restaurants |
| `geohash` | text, exactly 5 characters | the cell the restaurant is in. 31 different cells |
| `primary_cuisine` | text | 78 different values, never empty |

The cell decides the region: `shard_directory` maps each of the 31 cells to one region.

## `products.csv`

| Column | Type | Meaning |
| --- | --- | --- |
| `product_id` | text, 12 characters (digits and lower-case letters) | key together with `vendor_id` |
| `vendor_id` | text | the restaurant whose menu it is on |
| `name` | text, up to 175 characters | the name on the menu |
| `unit_price` | decimal, 0 to 1, up to 19 decimal places | the price as a fraction. Kept as it is in the file; the system does not calculate with it |
| `price_units` | whole number, 0 to 2,500 | the price as a whole number. This is the one the system uses: an order line stores it, and sales totals add it up. Both price columns have 185 different values |

Key: (`vendor_id`, `product_id`). Note the file's column order (rule 3). Products nobody ever ordered were removed during cleaning.

## `product_dish.csv`

| Column | Type | Meaning |
| --- | --- | --- |
| `product_id`, `vendor_id` | text | the product |
| `dish_id` | text | the dish it is |
| `multiplier` | whole number, 1 to 50 | how many units of the dish one unit of the product is (a set of 6 has multiplier 6). 1 for 255,408 products |
| `multiplier_ambiguous` | `true` or `false` | the cleaning could not tell the multiplier for certain. True for 381 products |

Every product has exactly one row here, and every row has a product: the two files have the same 256,496 keys. Code may join `products` to `product_dish` without handling a missing row. Note the file's column order (rule 3).

## `orders.csv`

| Column | Type | Meaning |
| --- | --- | --- |
| `order_id` | whole number, 0 to about 2,000,000 | key. Not every number in the range is used |
| `customer_id` | text, 10 characters (digits and lower-case letters) | who ordered |
| `vendor_id` | text | the restaurant. All 7,411 restaurants have orders |
| `geohash` | text, 5 characters | the **customer's** cell, not the restaurant's. 45 different cells |
| `day_of_week` | whole number, 0 to 6 | as published by DHRD. The system does not use it |
| `order_day` | whole number, 0 to 90 | day number since the start of the data: 91 days |
| `order_time` | text, always `HH:MM:SS` | time of day |

- **Days.** Days 0 to 13 are the warm-up window (W = 14): 292,587 orders, used only to set starting stock and never replayed. Days 14 to 90 are replayed: 1,707,401 orders. A day has between 1,187 orders (day 90, which is cut short) and 30,966 (day 28). See `docs/replay-stock-parameters.md`.
- **Replay order.** Sort by `order_day`, then `order_time`, then `order_id` compared as a number. Many orders share a second, and the third key is what makes the order the same on every run.
- **Customer cells.** 14 of the 45 customer cells have no restaurant in them. A customer's cell is only used by the nearby query, which also looks at the eight cells around it; a cell that `shard_directory` does not know is simply skipped.

## `order_items.csv`

| Column | Type | Meaning |
| --- | --- | --- |
| `order_id` | whole number | the order the line belongs to |
| `product_id` | text | what was ordered |
| `quantity` | whole number, 1 to 15 | how many. 1 for 98% of lines |

The file has **no `vendor_id`, no line number and no price**. Take the restaurant from the order (`orders.vendor_id`); the product is then (`vendor_id`, `product_id`). The system adds the rest when the order is placed: `line_no` is the line's position within its order, and `price_units` is copied from `products`.

## How an order becomes a request

Playback and the oracle read `orders.csv` and `order_items.csv` together. One order becomes one `POST /orders` (`docs/architecture.md` Section 6.1):

| Request field | Comes from |
| --- | --- |
| `channel` | the constant `dhrd` |
| `external_order_id` | `orders.order_id`, as text |
| `vendor_id`, `customer_id`, `order_day`, `order_time` | the same columns of `orders.csv` |
| `customer_geohash` | `orders.geohash` |
| `items` | the order's lines: `product_id` and `quantity` |

## Amounts and units

In this export `dish_ingredients.amount` is 1 and `unit` is `portion` on every row. Those are placeholders: the way the dataset was built cannot recover how much of an ingredient a dish really uses, so each ingredient counts as one portion. A better dataset would carry real amounts and units (grams, millilitres, pieces), and the system must work with it unchanged.

- `amount`, `unit` and `multiplier` are data. The tables have no defaults for `amount` and `unit`, and no code may assume a value for any of the three.
- What an order line uses of an ingredient is `quantity x multiplier x amount`, in that row's `unit`.
- A stock counter for an ingredient is kept in that ingredient's unit and has no unit column of its own. So **every `dish_ingredients` row of one ingredient must use the same unit**. Bootstrap checks this after loading and stops if an ingredient appears with two units. There is no unit conversion; mixing units would need a conversion table first.
- Amounts are whole numbers. For fractions, use a smaller unit.
- Anything that reports an amount of an ingredient reports the unit with it.

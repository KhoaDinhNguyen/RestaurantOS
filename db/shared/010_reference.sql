-- Reference data. Applied to every node (HQ, regions, baseline).
-- These six tables mirror the cleaned CSV files one to one. The only added
-- columns are products.menu_version and products.origin.
-- HQ holds every row. A region holds all dishes and ingredients, but only the
-- vendors and products that belong to it.

CREATE TABLE ingredients (
    ingredient_id text PRIMARY KEY,
    source text NOT NULL
);

CREATE TABLE dishes (
    dish_id text PRIMARY KEY,
    dish_name text NOT NULL,
    scope text NOT NULL,
    stock_mode text NOT NULL CHECK (stock_mode IN ('per_ingredient', 'per_item', 'none')),
    n_products integer NOT NULL DEFAULT 0,
    n_vendors integer NOT NULL DEFAULT 0,
    example_name text
);

CREATE TABLE dish_ingredients (
    dish_id text NOT NULL REFERENCES dishes (dish_id),
    ingredient_id text NOT NULL REFERENCES ingredients (ingredient_id),
    amount integer NOT NULL DEFAULT 1 CHECK (amount > 0),
    unit text NOT NULL DEFAULT 'portion',
    PRIMARY KEY (dish_id, ingredient_id)
);

CREATE INDEX dish_ingredients_ingredient_idx ON dish_ingredients (ingredient_id);

CREATE TABLE vendors (
    vendor_id text PRIMARY KEY,
    chain_id text,
    geohash text NOT NULL CHECK (length(geohash) = 5),
    primary_cuisine text
);

CREATE INDEX vendors_geohash_idx ON vendors (geohash);
CREATE INDEX vendors_chain_idx ON vendors (chain_id);

CREATE TABLE products (
    vendor_id text NOT NULL REFERENCES vendors (vendor_id),
    product_id text NOT NULL,
    name text NOT NULL,
    unit_price numeric NOT NULL,
    price_units integer NOT NULL CHECK (price_units >= 0),
    menu_version integer NOT NULL DEFAULT 1,
    -- 'hq': from the dataset or published by HQ. 'region': created locally by
    -- a region (stretch goal D2). A menu publish never touches 'region' rows.
    origin text NOT NULL DEFAULT 'hq' CHECK (origin IN ('hq', 'region')),
    PRIMARY KEY (vendor_id, product_id)
);

CREATE TABLE product_dish (
    vendor_id text NOT NULL,
    product_id text NOT NULL,
    dish_id text NOT NULL REFERENCES dishes (dish_id),
    multiplier integer NOT NULL DEFAULT 1 CHECK (multiplier > 0),
    multiplier_ambiguous boolean NOT NULL DEFAULT false,
    PRIMARY KEY (vendor_id, product_id),
    FOREIGN KEY (vendor_id, product_id)
    REFERENCES products (vendor_id, product_id) ON DELETE CASCADE
);

CREATE INDEX product_dish_dish_idx ON product_dish (dish_id);

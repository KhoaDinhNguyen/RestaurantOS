-- Operational tables of one region node. Also applied to the baseline node.
-- The region primary is the only writer of everything in this file.

-- Exactly one row. Written by the init script, then only by the coordinator
-- (epoch) and the menu publisher (menu_version).
CREATE TABLE node_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    region_id text NOT NULL,
    epoch integer NOT NULL DEFAULT 1,
    menu_version integer NOT NULL DEFAULT 1
);

-- One counter per thing a vendor can run out of.
--   item_kind = 'ingredient': item_id is an ingredient_id (dishes counted per_ingredient)
--   item_kind = 'dish':       item_id is a dish_id        (dishes counted per_item)
-- Dishes with stock_mode = 'none' have no row.
CREATE TABLE stock (
    vendor_id text NOT NULL REFERENCES vendors (vendor_id),
    item_kind text NOT NULL CHECK (item_kind IN ('ingredient', 'dish')),
    item_id text NOT NULL,
    on_hand integer NOT NULL CHECK (on_hand >= 0),
    par_level integer NOT NULL CHECK (par_level >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (vendor_id, item_kind, item_id)
);

-- A product with no row here is available. A row exists only while the product
-- is switched off: reason 'stock' (set and cleared by the system) or 'manual'
-- (set and cleared by a person; a restock must not clear it).
CREATE TABLE product_availability (
    vendor_id text NOT NULL,
    product_id text NOT NULL,
    available boolean NOT NULL,
    reason text NOT NULL CHECK (reason IN ('stock', 'manual')),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (vendor_id, product_id)
);

-- order_day and order_time are simulated (business) time from the request.
-- received_at and committed_at are wall-clock time, for measurements only.
CREATE TABLE orders (
    order_day smallint NOT NULL CHECK (order_day >= 0),
    order_id uuid NOT NULL,
    vendor_id text NOT NULL REFERENCES vendors (vendor_id),
    channel text NOT NULL,
    external_order_id text NOT NULL,
    customer_id text,
    customer_geohash text CHECK (customer_geohash IS null OR length(customer_geohash) = 5),
    order_time time NOT NULL,
    status text NOT NULL DEFAULT 'received' CHECK (
        status IN ('received', 'accepted', 'preparing', 'ready', 'picked_up', 'canceled')
    ),
    status_changed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    menu_version integer NOT NULL,
    epoch integer NOT NULL,
    received_at timestamptz,
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (order_day, order_id)
) PARTITION BY RANGE (order_day);

CREATE TABLE orders_default PARTITION OF orders DEFAULT;

CREATE INDEX orders_order_id_idx ON orders (order_id);
CREATE INDEX orders_vendor_idx ON orders (vendor_id, order_day, status);

-- No foreign key to products on purpose: old orders must survive menu changes.
-- vendor_id is repeated here so order lines join to product_dish directly.
CREATE TABLE order_items (
    order_day smallint NOT NULL,
    order_id uuid NOT NULL,
    line_no smallint NOT NULL,
    vendor_id text NOT NULL,
    product_id text NOT NULL,
    quantity smallint NOT NULL CHECK (quantity > 0),
    price_units integer NOT NULL CHECK (price_units >= 0),
    PRIMARY KEY (order_day, order_id, line_no),
    FOREIGN KEY (order_day, order_id)
    REFERENCES orders (order_day, order_id) ON DELETE CASCADE
) PARTITION BY RANGE (order_day);

CREATE TABLE order_items_default PARTITION OF order_items DEFAULT;

CREATE INDEX order_items_product_idx ON order_items (vendor_id, product_id);

SELECT create_day_partitions('orders', 0, 90);
SELECT create_day_partitions('order_items', 0, 90);

-- Idempotency guard for order placement. One row per order ever seen,
-- accepted or refused, so a retry gets the same answer with no side effects.
-- order_day is the request's day on every row. An accepted row has an
-- order_id and no refusal_reason; a refused row has a refusal_reason and no
-- order_id. The reasons are restaurantos.common.models.RefusalReason.
CREATE TABLE inbox (
    channel text NOT NULL,
    external_order_id text NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('accepted', 'refused')),
    refusal_reason text CHECK (
        refusal_reason IN ('out_of_stock', 'unavailable', 'unknown_product')
    ),
    order_day smallint,
    order_id uuid,
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (channel, external_order_id),
    CHECK ((outcome = 'accepted') = (order_id IS NOT null)),
    CHECK ((outcome = 'refused') = (refusal_reason IS NOT null))
);

-- Refusals per day, for the regional summary. Small: refused rows only.
CREATE INDEX inbox_refused_day_idx ON inbox (order_day) WHERE outcome = 'refused';

-- Idempotency guard for status events.
CREATE TABLE processed_events (
    event_id uuid PRIMARY KEY,
    order_id uuid NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Events waiting to be copied to HQ. Written in the same transaction as the
-- change they describe. The relay sends rows where relayed_at IS NULL, in seq
-- order, and stamps relayed_at once HQ has confirmed them.
CREATE TABLE outbox (
    seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    event_type text NOT NULL CHECK (
        event_type IN ('order.created', 'order.status_changed', 'exception.logged')
    ),
    order_day smallint,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    relayed_at timestamptz
);

CREATE INDEX outbox_pending_idx ON outbox (seq) WHERE relayed_at IS null;

-- Status changes that lost to an earlier committed change (first writer wins).
CREATE TABLE exception_log (
    exception_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id uuid NOT NULL,
    order_day smallint NOT NULL,
    order_id uuid NOT NULL,
    kind text NOT NULL DEFAULT 'status_conflict',
    current_status text NOT NULL,
    current_changed_at timestamptz NOT NULL,
    attempted_status text NOT NULL,
    attempted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    detail jsonb NOT NULL DEFAULT '{}'::jsonb
);

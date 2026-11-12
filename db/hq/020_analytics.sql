-- HQ's read-only copy of every region's orders. Filled only by the relay
-- workers from outbox events. hq_applied_at minus committed_at is the HQ
-- staleness metric.

CREATE TABLE analytics_orders (
    order_day smallint NOT NULL,
    order_id uuid NOT NULL,
    region_id text NOT NULL,
    vendor_id text NOT NULL,
    channel text NOT NULL,
    external_order_id text NOT NULL,
    customer_id text,
    customer_geohash text,
    order_time time NOT NULL,
    status text NOT NULL,
    status_changed_at timestamptz,
    menu_version integer NOT NULL,
    received_at timestamptz,
    committed_at timestamptz NOT NULL,
    hq_applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (order_day, order_id)
) PARTITION BY RANGE (order_day);

CREATE TABLE analytics_orders_default PARTITION OF analytics_orders DEFAULT;

CREATE INDEX analytics_orders_region_idx ON analytics_orders (region_id, order_day);
CREATE INDEX analytics_orders_vendor_idx ON analytics_orders (vendor_id, order_day);

CREATE TABLE analytics_order_items (
    order_day smallint NOT NULL,
    order_id uuid NOT NULL,
    line_no smallint NOT NULL,
    vendor_id text NOT NULL,
    product_id text NOT NULL,
    quantity smallint NOT NULL,
    price_units integer NOT NULL,
    PRIMARY KEY (order_day, order_id, line_no)
) PARTITION BY RANGE (order_day);

CREATE TABLE analytics_order_items_default PARTITION OF analytics_order_items DEFAULT;

CREATE INDEX analytics_order_items_product_idx ON analytics_order_items (vendor_id, product_id);

SELECT create_day_partitions('analytics_orders', 0, 90);
SELECT create_day_partitions('analytics_order_items', 0, 90);

CREATE TABLE analytics_exceptions (
    event_id uuid PRIMARY KEY,
    region_id text NOT NULL,
    order_day smallint NOT NULL,
    order_id uuid NOT NULL,
    kind text NOT NULL,
    current_status text NOT NULL,
    current_changed_at timestamptz NOT NULL,
    attempted_status text NOT NULL,
    attempted_at timestamptz NOT NULL,
    detail jsonb NOT NULL DEFAULT '{}'::jsonb,
    hq_applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

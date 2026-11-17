INSERT INTO analytics_orders (
    order_day, order_id, region_id, vendor_id, channel, external_order_id,
    customer_id, customer_geohash, order_time, status, menu_version, received_at, committed_at
)
VALUES (
    %(order_day)s, %(order_id)s, %(region_id)s, %(vendor_id)s, %(channel)s, %(external_order_id)s,
    %(customer_id)s, %(customer_geohash)s, %(order_time)s, %(status)s, %(menu_version)s,
    %(received_at)s, %(committed_at)s
);

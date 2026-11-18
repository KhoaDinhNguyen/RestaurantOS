-- status ('received'), status_changed_at and committed_at come from
-- the column defaults; committed_at is clock_timestamp(), wall-clock time
-- for measurement only. menu_version is the one order_products read together
-- with the line prices, so the two always match.
INSERT INTO orders (
    order_day, order_id, vendor_id, channel, external_order_id, customer_id,
    customer_geohash, order_time, menu_version, epoch, received_at
)
VALUES (
    %(order_day)s, %(order_id)s, %(vendor_id)s, %(channel)s, %(external_order_id)s,
    %(customer_id)s, %(customer_geohash)s, %(order_time)s,
    %(menu_version)s, %(epoch)s, %(received_at)s
)
RETURNING status, menu_version, committed_at;

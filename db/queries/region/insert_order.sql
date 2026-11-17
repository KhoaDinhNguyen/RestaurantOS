-- status ('received'), status_changed_at and committed_at come from
-- the column defaults; committed_at is clock_timestamp(), wall-clock time
-- for measurement only. menu_version is the menu this node has right now.
INSERT INTO orders (
    order_day, order_id, vendor_id, channel, external_order_id, customer_id,
    customer_geohash, order_time, menu_version, epoch, received_at
)
VALUES (
    %(order_day)s, %(order_id)s, %(vendor_id)s, %(channel)s, %(external_order_id)s,
    %(customer_id)s, %(customer_geohash)s, %(order_time)s,
    (SELECT menu_version FROM node_state), %(epoch)s, %(received_at)s
)
RETURNING status, menu_version, committed_at;

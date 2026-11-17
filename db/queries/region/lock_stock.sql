-- Locks the counters the order needs. PostgreSQL locks rows in
-- ORDER BY order, so every transaction takes them as (item_kind, item_id)
-- and two orders cannot deadlock. Anything else in WP1 that locks stock
-- rows must use the same order. A counter with no row is simply not returned.
SELECT
    s.item_kind,
    s.item_id,
    s.on_hand
FROM stock AS s
WHERE
    s.vendor_id = %(vendor_id)s
    AND (s.item_kind, s.item_id) IN (
        SELECT * FROM unnest(%(item_kinds)s::text[], %(item_ids)s::text[])
    )
ORDER BY s.item_kind, s.item_id
FOR UPDATE OF s;

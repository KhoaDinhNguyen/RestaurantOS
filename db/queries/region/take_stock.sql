-- Takes the stock and returns what is left, so the caller knows
-- which counters just reached zero.
UPDATE stock AS s
SET
    on_hand = s.on_hand - n.need,
    updated_at = clock_timestamp()
FROM
    unnest(%(item_kinds)s::text[], %(item_ids)s::text[], %(needs)s::integer[])
        AS n (item_kind, item_id, need)
WHERE
    s.vendor_id = %(vendor_id)s
    AND s.item_kind = n.item_kind
    AND s.item_id = n.item_id
RETURNING s.item_kind, s.item_id, s.on_hand;

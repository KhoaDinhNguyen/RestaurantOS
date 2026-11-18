-- apply_menu_version step 6. An added product that cannot make one unit is
-- switched off with a stock mark; the next restock fills its counters and
-- switches it on. Never overwrites a row that is already there.
INSERT INTO product_availability (vendor_id, product_id, available, reason)
SELECT
    a.vendor_id,
    a.product_id,
    false AS available,
    'stock' AS reason
FROM unnest(%(vendor_ids)s::text[], %(product_ids)s::text[]) AS a (vendor_id, product_id)
WHERE NOT can_make_one(a.vendor_id, a.product_id)
ORDER BY a.vendor_id, a.product_id
ON CONFLICT (vendor_id, product_id) DO NOTHING;

-- restock step 2. Locks every counter of the vendor in the same order
-- place_order uses (item_kind, item_id), so a restock and an order waiting on
-- each other's counters cannot deadlock.
SELECT 1
FROM stock
WHERE vendor_id = %(vendor_id)s
ORDER BY item_kind, item_id
FOR UPDATE;

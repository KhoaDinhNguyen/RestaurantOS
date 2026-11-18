-- set_availability step 3, switching on. Only the manual mark; a stock mark
-- stays (it is the system's, and a restock clears it).
DELETE FROM product_availability
WHERE
    vendor_id = %(vendor_id)s
    AND product_id = %(product_id)s
    AND reason = 'manual';

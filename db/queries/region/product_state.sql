-- set_availability step 4, the answer. No row means available.
SELECT
    available,
    reason
FROM product_availability
WHERE
    vendor_id = %(vendor_id)s
    AND product_id = %(product_id)s;

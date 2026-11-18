-- restock step 4. Every stock mark of the vendor goes; manual marks stay.
-- The row count is the number of products back on.
DELETE FROM product_availability
WHERE
    vendor_id = %(vendor_id)s
    AND reason = 'stock';

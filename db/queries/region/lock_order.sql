-- change_status steps 2 and 3. The order as it is now, for this vendor only,
-- locked so that two changes to one order run one after the other: the first
-- to commit wins, the second sees its result. The request carries no day, so
-- every day partition is searched through the order_id index.
SELECT
    order_day,
    status,
    status_changed_at
FROM orders
WHERE
    vendor_id = %(vendor_id)s
    AND order_id = %(order_id)s
FOR UPDATE;

-- Is this vendor on this node? A row back means yes.
SELECT 1 FROM vendors
WHERE vendor_id = %(vendor_id)s;

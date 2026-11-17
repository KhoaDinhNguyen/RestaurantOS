INSERT INTO product_dish (
    vendor_id,
    product_id,
    dish_id,
    multiplier,
    multiplier_ambiguous
)
VALUES (
    %(vendor_id)s,
    %(product_id)s,
    %(dish_id)s,
    %(multiplier)s,
    false
);

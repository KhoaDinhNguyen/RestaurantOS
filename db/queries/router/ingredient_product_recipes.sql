WITH requested AS (
    SELECT *
    FROM unnest(
        %(vendor_ids)s::text[],
        %(product_ids)s::text[]
    ) AS item (vendor_id, product_id)
)

SELECT
    pd.vendor_id,
    pd.product_id,
    pd.multiplier,
    di.ingredient_id,
    di.amount,
    di.unit
FROM requested AS requested
INNER JOIN product_dish AS pd
    ON
        requested.vendor_id = pd.vendor_id
        AND requested.product_id = pd.product_id
INNER JOIN dish_ingredients AS di
    ON pd.dish_id = di.dish_id
ORDER BY
    pd.vendor_id ASC,
    pd.product_id ASC,
    di.ingredient_id ASC,
    di.unit ASC;

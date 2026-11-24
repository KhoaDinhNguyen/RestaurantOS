WITH requested_cells AS (
    SELECT *
    FROM unnest(
        %(geohashes)s::text[],
        %(distances_m)s::double precision[]
    ) AS requested (geohash, distance_m)
)

SELECT
    v.vendor_id,
    v.chain_id,
    v.primary_cuisine,
    v.geohash,
    requested.distance_m,
    p.product_id,
    p.name,
    p.price_units,
    pd.multiplier
FROM requested_cells AS requested
INNER JOIN vendors AS v
    ON requested.geohash = v.geohash
INNER JOIN products AS p
    ON v.vendor_id = p.vendor_id
INNER JOIN product_dish AS pd
    ON
        p.vendor_id = pd.vendor_id
        AND p.product_id = pd.product_id
LEFT JOIN product_availability AS pa
    ON
        p.vendor_id = pa.vendor_id
        AND p.product_id = pa.product_id
WHERE
    pd.dish_id = %(dish_id)s  -- noqa: RF02
    AND coalesce(pa.available, true)
    AND can_make_one(p.vendor_id, p.product_id)
ORDER BY
    requested.distance_m,
    v.vendor_id,
    p.product_id
LIMIT %(row_limit)s;

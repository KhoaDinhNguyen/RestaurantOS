SELECT
    v.vendor_id,
    v.chain_id,
    v.geohash,
    v.primary_cuisine
FROM vendors AS v
WHERE
    v.geohash = %(geohash)s  -- noqa: RF02
    AND (
        %(pattern)s::text IS NULL  -- noqa: RF02
        OR v.primary_cuisine ILIKE %(pattern)s ESCAPE '\'  -- noqa: RF02
        OR EXISTS (
            SELECT 1
            FROM products AS p
            WHERE
                p.vendor_id = v.vendor_id
                AND p.name ILIKE %(pattern)s ESCAPE '\'  -- noqa: RF02
        )
    )
ORDER BY v.vendor_id;

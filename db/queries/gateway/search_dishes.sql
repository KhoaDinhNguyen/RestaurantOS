SELECT
    dish_id,
    dish_name,
    example_name,
    scope,
    n_vendors
FROM dishes
WHERE
    dish_name ILIKE %(pattern)s ESCAPE '\'  -- noqa: RF02
    OR example_name ILIKE %(pattern)s ESCAPE '\'  -- noqa: RF02
ORDER BY n_vendors DESC, dish_id ASC
LIMIT %(row_limit)s;  -- noqa: RF02

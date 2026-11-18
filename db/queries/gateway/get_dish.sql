SELECT
    dish_id,
    dish_name,
    example_name,
    scope,
    n_vendors
FROM dishes
WHERE dish_id = %(dish_id)s;  -- noqa: RF02

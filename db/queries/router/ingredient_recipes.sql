SELECT
    di.dish_id,
    di.ingredient_id,
    di.amount,
    di.unit
FROM dish_ingredients AS di
WHERE di.dish_id = ANY(%(dish_ids)s::text[])  -- noqa: RF02
ORDER BY
    di.dish_id ASC,
    di.ingredient_id ASC,
    di.unit ASC;

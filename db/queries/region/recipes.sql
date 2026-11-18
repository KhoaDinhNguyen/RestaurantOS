-- The recipe rows of the per_ingredient dishes in the order.
SELECT
    dish_id,
    ingredient_id,
    amount
FROM dish_ingredients
WHERE dish_id = ANY(%(dish_ids)s::text[]);

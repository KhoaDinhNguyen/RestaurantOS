-- Can one unit of this product be made right now? One rule, written once
-- (WP1 brief, "The rest"):
--   per_ingredient: on_hand >= amount x multiplier for every recipe row
--   per_item:       on_hand >= multiplier
--   none:           always
-- A missing stock row counts as zero. A product with no dish link cannot be
-- made. The counters are read FOR SHARE, in the order place_order locks them
-- (item_kind, item_id), so an order committing at the same moment cannot slip
-- past, and the two cannot deadlock.

CREATE FUNCTION can_make_one(p_vendor_id text, p_product_id text)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    v_dish_id text;
    v_multiplier integer;
    v_stock_mode text;
BEGIN
    SELECT pd.dish_id, pd.multiplier, d.stock_mode
    INTO v_dish_id, v_multiplier, v_stock_mode
    FROM product_dish AS pd
    INNER JOIN dishes AS d ON pd.dish_id = d.dish_id
    WHERE pd.vendor_id = p_vendor_id AND pd.product_id = p_product_id;

    IF NOT FOUND THEN
        RETURN false;
    END IF;

    IF v_stock_mode = 'none' THEN
        RETURN true;
    END IF;

    IF v_stock_mode = 'per_item' THEN
        RETURN COALESCE((
            SELECT s.on_hand
            FROM stock AS s
            WHERE s.vendor_id = p_vendor_id AND s.item_kind = 'dish' AND s.item_id = v_dish_id
            FOR SHARE
        ), 0) >= v_multiplier;
    END IF;

    -- per_ingredient: lock the recipe's counters first, in place_order's order.
    PERFORM 1
    FROM stock AS s
    WHERE
        s.vendor_id = p_vendor_id
        AND s.item_kind = 'ingredient'
        AND s.item_id IN (
            SELECT di.ingredient_id FROM dish_ingredients AS di WHERE di.dish_id = v_dish_id
        )
    ORDER BY s.item_kind, s.item_id
    FOR SHARE;

    RETURN NOT EXISTS (
        SELECT 1
        FROM dish_ingredients AS di
        LEFT JOIN stock AS s
            ON s.vendor_id = p_vendor_id
            AND s.item_kind = 'ingredient'
            AND s.item_id = di.ingredient_id
        WHERE
            di.dish_id = v_dish_id
            AND COALESCE(s.on_hand, 0) < di.amount * v_multiplier
    );
END;
$$;

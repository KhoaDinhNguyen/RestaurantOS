WITH order_counts AS (
    SELECT
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE status = 'received') AS received,
        COUNT(*) FILTER (WHERE status = 'accepted') AS accepted,
        COUNT(*) FILTER (WHERE status = 'preparing') AS preparing,
        COUNT(*) FILTER (WHERE status = 'ready') AS ready,
        COUNT(*) FILTER (WHERE status = 'picked_up') AS picked_up,
        COUNT(*) FILTER (WHERE status = 'canceled') AS canceled
    FROM orders
    WHERE order_day = %(day)s
),

sales AS (
    SELECT
        COALESCE(
            SUM(oi.quantity::bigint * oi.price_units)
            0
        ) AS sales_units
    FROM order_items AS oi
    INNER JOIN orders AS o
        ON
            oi.order_day = o.order_day
            AND oi.order_id = o.order_id
    WHERE
        oi.order_day = %(day)s  -- noqa: RF02
        AND o.status <> 'canceled'
),

refusals AS (
    SELECT COUNT(*) AS refused
    FROM inbox
    WHERE
        order_day = %(day)s
        AND outcome = 'refused'
),

availability AS (
    SELECT COUNT(*) AS products_unavailable
    FROM product_availability
    WHERE available = false
),

backlog AS (
    SELECT COUNT(*) AS outbox_backlog
    FROM outbox
    WHERE relayed_at IS null
)

SELECT
    ns.region_id,
    ns.epoch,
    ns.menu_version,
    oc.total,
    oc.received,
    oc.accepted,
    oc.preparing,
    oc.ready,
    oc.picked_up,
    oc.canceled,
    r.refused,
    s.sales_units,
    a.products_unavailable,
    b.outbox_backlog,
    %(day)s::integer AS order_day,  -- noqa: RF02
    (
        SELECT COUNT(*)
        FROM vendors
    ) AS vendors,
    CLOCK_TIMESTAMP() AS data_as_of
FROM node_state AS ns
CROSS JOIN order_counts AS oc
CROSS JOIN sales AS s
CROSS JOIN refusals AS r
CROSS JOIN availability AS a
CROSS JOIN backlog AS b;

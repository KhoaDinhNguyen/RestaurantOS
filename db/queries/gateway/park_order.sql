INSERT INTO holding_queue (
    region_id,
    channel,
    external_order_id,
    request,
    received_at
)
VALUES (
    %(region_id)s,
    %(channel)s,
    %(external_order_id)s,
    %(request)s,
    %(received_at)s
)
ON CONFLICT (channel, external_order_id) DO NOTHING;

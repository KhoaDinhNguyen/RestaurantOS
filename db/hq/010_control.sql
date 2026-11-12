-- Control tables. HQ only. Written by the coordinator, bootstrap, the gateway
-- (holding queue) and the relay workers.

-- Current state of each region. epoch goes up by one at every promotion.
CREATE TABLE region_state (
    region_id text PRIMARY KEY,
    epoch integer NOT NULL DEFAULT 1,
    primary_node text NOT NULL,
    standby_node text,
    menu_version integer NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Which region owns each geohash cell.
CREATE TABLE shard_directory (
    geohash text PRIMARY KEY CHECK (length(geohash) = 5),
    region_id text NOT NULL REFERENCES region_state (region_id)
);

CREATE INDEX shard_directory_region_idx ON shard_directory (region_id);

CREATE TABLE menu_versions (
    version integer PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    note text
);

INSERT INTO menu_versions (version, note) VALUES (1, 'initial load from dataset');

-- Orders the gateway could not deliver to their region. request is the
-- original OrderRequest as JSON. Drained in holding_id order per region.
CREATE TABLE holding_queue (
    holding_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    region_id text NOT NULL REFERENCES region_state (region_id),
    channel text NOT NULL,
    external_order_id text NOT NULL,
    request jsonb NOT NULL,
    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    delivered_at timestamptz,
    delivered_outcome text CHECK (delivered_outcome IN ('accepted', 'refused')),
    UNIQUE (channel, external_order_id)
);

CREATE INDEX holding_queue_pending_idx ON holding_queue (region_id, holding_id)
WHERE delivered_at IS NULL;

-- Timeline for the admin page: failovers, partitions, recoveries, downgrades.
CREATE TABLE cluster_events (
    cluster_event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    kind text NOT NULL,
    region_id text,
    node_id text,
    detail jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX cluster_events_time_idx ON cluster_events (occurred_at);

-- Idempotency guard for events relayed from the regions.
CREATE TABLE hq_inbox (
    region_id text NOT NULL,
    event_id uuid NOT NULL,
    seq bigint NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (region_id, event_id)
);

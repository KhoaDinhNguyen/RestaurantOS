# WP2: Gateway and edge API

**Goal:** the HTTP front door. It finds the region that owns a restaurant and runs the request there. The same code also runs per region in "edge mode".

**You own:** `src/restaurantos/gateway/` except `router/` (that is WP3), `db/queries/gateway/`, `tests/unit/gateway/`, `tests/integration/gateway/`.

**Read first:** `docs/architecture.md` Sections 1, 4 and 6.1; Phase 2 Sections 2.1, 2.2 and 2.4 (epoch fencing).

**Needs:** WP0. Real writes need WP1; until then, stub `restaurantos.region` with fakes that return canned results. Keep the fakes under `tests/` (for example `tests/unit/gateway/fakes.py`). `src/restaurantos/region/` is WP1's folder, so nothing of yours goes there.

## Shared code you must use

| For | Use |
| --- | --- |
| which nodes and ports exist | `common.cluster.load_cluster`, `service_port` |
| connection strings and pools | `common.db.node_conninfo`, `make_pool` |
| every database call on a request path | `common.db.run_on` |
| edge mode: which local node is the primary | `common.db.find_local_primary` |
| request and response shapes | `common.models`: `OrderRequest`, `OrderResponse`, `StatusChangeRequest`, `StatusResult` |
| errors to turn into answers | `common.errors`: `RegionUnreachableError` (`503` or park), `StaleEpochError` (refresh and retry), `UnknownVendorError` (`404`) |
| the writes themselves | the functions of `restaurantos.region` (WP1); no SQL of your own on a region's tables |
| settings | shared ones from `common.config.get_settings()`; your own in `gateway/config.py` |

## What to build

One FastAPI app with two modes, chosen by config:

| | Gateway mode | Edge mode |
| --- | --- | --- |
| Runs | once, HQ side, port 8000 | once per region, port 8010 + n (`common.cluster.service_port`) |
| Serves | every region | its own region only |
| Reaches databases | through `gateway__<node>` proxies | its own region's nodes, directly |
| Knows which node is the primary from | `region_state.primary_node` at HQ | looking at its two local nodes: `common.db.find_local_primary` |
| Learns the epoch and the vendors from | HQ: `shard_directory`, `region_state`, `vendors` | its own node: `node_state`, the local `vendors` slice. It never reads HQ |
| Region unreachable | parks the order in HQ's `holding_queue` | answers `503` |

### Routing

- Keep two caches in memory: vendor to geohash cell (from `vendors`), and cell to region plus epoch plus primary node (from `shard_directory` and `region_state`).
- Refresh the directory on a timer and whenever a region answers `StaleEpochError`. The interval is a setting in your own `gateway/config.py`: `ROS_GATEWAY_DIRECTORY_REFRESH_S` (default 5).
- One psycopg async pool per node, made with `common.db.make_pool(common.db.node_conninfo(cluster, node))`. Do not build connection strings yourself: `node_conninfo` picks the proxy for your process and works both in a container and on the host (`ROS_NETWORK`, architecture Section 4).
- Every database call on a request path goes through `common.db.run_on(pool, work, region_id=...)`, never a bare `async with pool.connection()`. `run_on` keeps the deadline on your side of the link and raises `RegionUnreachableError` when there is no answer in time, for whatever reason. The pool's own timeouts cannot do that on a link that is silent or slow (architecture Section 5.2).

### Edge mode: which node to write to

The edge API sits next to its region's two nodes and connects to them directly, so nobody tells it about a failover. It has to look:

- Call `common.db.find_local_primary(cluster, region_id, current)` at start, on a timer (`ROS_GATEWAY_LOCAL_CHECK_S`, default 1), and after any `RegionUnreachableError` or `StaleEpochError`. The rule it applies is in architecture Section 4.
- Use the node it returns, and read your epoch from that node's `node_state`. When it returns a different node than before, switch pools.
- When it returns nothing, answer `503`. Do not keep using the old node.
- Single-region mode has no standby; the same call handles that.

### `POST /orders`

1. Stamp `received_at`.
2. Vendor to cell to region.
3. Call `region.place_order` on that region's primary with the cached epoch and `received_at`.
4. `StaleEpochError`: refresh the directory, retry once.
5. `RegionUnreachableError` (gateway mode): insert the request into `holding_queue` at HQ, with `received_at`, and answer `202`. Insert with `ON CONFLICT (channel, external_order_id) DO NOTHING`: playback resends some orders on purpose, and a resend that arrives while the first copy is still parked must answer `202` again, not fail on the unique constraint.
6. Stamp `acked_at` and answer `201` (new), `200` (duplicate) or `409` (refused, whatever the `refusal_reason`).

### Other routes

All the `/vendors/...` routes and `/regions` in Section 6.1, including `/regions/{region_id}/summary` (its fields are listed there). Reads of order status always go to the region primary. `POST /menu/products` writes at HQ only. In edge mode, serve the `/vendors/...` routes and the summary of your own region.

### When the region cannot be reached

Only orders are parked. Everything else fails fast and says which region is missing:

| Request | Answer |
| --- | --- |
| `POST /orders` | gateway mode: park it, `202` (step 5 above). Edge mode: `503` |
| any other write: status change, sold-out switch, restock | `503`, not parked. The holding queue holds orders only |
| any read of region data: order status, kitchen board, menu, `/vendors`, the summary | `503`. Never answered from HQ: its copy may be stale and has no stock or availability |
| `GET /regions`, `POST /menu/products` | not affected, they only touch HQ |

The `503` body is `{"error": "region_unreachable", "region_id": "r1"}`. `run_on` raises `RegionUnreachableError` with the `region_id` you gave it; turn it into the response in one exception handler. The other answers are in the table under the routes in Section 6.1: `404` for an unknown vendor, order or region, `409` for a refused order or a status change that lost.

Also add `GET /internal/probe/{node_id}`: run `SELECT 1` on that node and report up or down. The coordinator uses it as its second path before declaring a node down.

## Run it on its own

Needs the dev cluster (see "Running things" in `README.md` in this folder). The same package runs as the gateway or as one region's edge API:

```bash
ROS_MODE=gateway ROS_CLIENT=gateway uv run python -m restaurantos.gateway     # port 8000
ROS_MODE=edge ROS_REGION_ID=r1 uv run python -m restaurantos.gateway          # port 8011

curl -s localhost:8000/api/v1/regions
```

Until bootstrap (WP6a) exists the databases are empty. The gateway needs two control rows at HQ before it can route anything, plus whatever vendors and products you want to order from:

```sql
-- at HQ: psql -h 127.0.0.1 -p 5500 -U ros -d restaurantos   (password ros)
INSERT INTO region_state (region_id, primary_node, standby_node) VALUES ('r1', 'r1-primary', 'r1-standby');
INSERT INTO shard_directory (geohash, region_id) VALUES ('w21z7', 'r1');
```

Until WP1 exists, the write routes run against your fakes in tests only; the read routes and `GET /regions` work for real. To see the failure answers by hand, switch a proxy off and on through Toxiproxy:

```bash
curl -s -X POST 127.0.0.1:8474/proxies/gateway__r1-primary -d '{"enabled": false}'
curl -s -X POST 127.0.0.1:8474/reset          # everything back to normal
```

## Done when

What each line needs is marked in front of it (the marks are explained in `README.md` in this folder).

- `cluster (needs WP1; more than one region needs WP6a)` An order for a vendor in each region lands on the right node, and nowhere else.
- `cluster (needs WP1; WP5, or bump the epoch by hand)` After a promotion, the first request gets `StaleEpochError`, the gateway refreshes, and the retry succeeds without the client noticing.
- `cluster` With a region's links cut, `POST /orders` answers `202` within the timeout and the row is in `holding_queue`. The same order sent again while it is parked answers `202` again and adds no row.
- `cluster` With a region's links cut, a read or a status change for that region answers `503` within the timeout and names the region. Other regions are not affected.
- `cluster` The two points above also hold when the links are silenced or slowed instead of cut (`silence`, and `latency` above the deadline): the answer still comes within `ROS_REQUEST_DEADLINE_S`.
- `cluster` Edge mode refuses vendors from other regions and keeps working with HQ unreachable.
- `cluster (needs WP5, or promote by hand)` After a promotion in its region, the edge API moves to the new primary within one check interval, without a restart. With the old primary still running (T1), no request reaches it after that.
- `cluster (container mode also needs the service image)` The whole app runs unchanged with `ROS_NETWORK=host` and with `ROS_NETWORK=compose`.
- `unit` Every order response carries `received_at`, `committed_at` and `acked_at`.

## Watch out for

- The gateway holds no state that matters. Anything it must not lose goes into a database.
- Do not retry a write blindly after a timeout: the order may have committed. Retrying is safe only because `place_order` is idempotent on (`channel`, `external_order_id`), so always resend the same identifiers.
- No business logic here: stock, status rules and menus live in `restaurantos.region`.

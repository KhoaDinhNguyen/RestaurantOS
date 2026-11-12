# WP2: Gateway and edge API

**Goal:** the HTTP front door. It finds the region that owns a restaurant and runs the request there. The same code also runs per region in "edge mode".

**You own:** `src/restaurantos/gateway/` except `router/` (that is WP3), `tests/unit/gateway/`, `tests/integration/gateway/`.

**Read first:** `docs/architecture.md` Sections 1, 4 and 6.1; Phase 2 Sections 2.1, 2.2 and 2.4 (epoch fencing).

**Needs:** WP0. Real writes need WP1; until then, stub `restaurantos.region` with fakes that return canned results.

## What to build

One FastAPI app with two modes, chosen by config:

| | Gateway mode | Edge mode |
| --- | --- | --- |
| Runs | once, HQ side, port 8000 | once per region |
| Serves | every region | its own region only |
| Reaches databases | through `gateway__<node>` proxies | its own primary, directly |
| Region unreachable | parks the order in HQ's `holding_queue` | not applicable |

### Routing

- Keep two caches in memory: vendor to geohash cell (from `vendors`), and cell to region plus epoch plus primary node (from `shard_directory` and `region_state`).
- Refresh the directory on a timer and whenever a region answers `StaleEpoch`.
- One psycopg async pool per node, addressed by proxy name. Set short timeouts (suggest 1 s to connect, 2 s per statement). Without them a silenced link hangs the request forever.

### `POST /orders`

1. Stamp `received_at`.
2. Vendor to cell to region.
3. Call `region.place_order` on that region's primary with the cached epoch.
4. `StaleEpoch`: refresh the directory, retry once.
5. Cannot connect, or timeout (gateway mode): insert the request into `holding_queue` at HQ and answer `202`.
6. Stamp `acked_at` and answer `201` (new), `200` (duplicate) or `409` (refused).

### Other routes

All the `/vendors/...` routes and `/regions` in Section 6.1. Reads of order status always go to the region primary. `POST /menu/products` writes at HQ only.

Also add `GET /internal/probe/{node_id}`: run `SELECT 1` on that node and report up or down. The coordinator uses it as its second path before declaring a node down.

## Done when

- An order for a vendor in each region lands on the right node, and nowhere else.
- After a promotion, the first request gets `StaleEpoch`, the gateway refreshes, and the retry succeeds without the client noticing.
- With a region's links cut, `POST /orders` answers `202` within the timeout and the row is in `holding_queue`.
- Edge mode refuses vendors from other regions and keeps working with HQ unreachable.
- Every order response carries `received_at`, `committed_at` and `acked_at`.

## Watch out for

- The gateway holds no state that matters. Anything it must not lose goes into a database.
- Do not retry a write blindly after a timeout: the order may have committed. Retrying is safe only because `place_order` is idempotent on (`channel`, `external_order_id`), so always resend the same identifiers.
- No business logic here: stock, status rules and menus live in `restaurantos.region`.

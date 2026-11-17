# RestaurantOS architecture and contracts

Status: **draft for team review, 2026-11-11.** This turns the Phase 2 design into a repository layout and a set of contracts (what goes in, what comes out) so four people can build in parallel. Items marked **OPEN (Dn)** are decisions the team still has to make; they are collected in Section 11.

The Phase 2 document stays the source of truth for *why*. This file is the source of truth for *where code lives and how parts talk*.

## 1. The big picture

```mermaid
flowchart TB
  subgraph outside["Outside the system (simulation harness)"]
    PB["Playback<br/>replays dataset orders"]
    CP["Control plane<br/>faults, snapshots, setup"]
  end
  UI["Web UI (React)<br/>HQ, region, customer, ops views"]
  GW["Gateway and query router"]
  CO["Coordinator<br/>heartbeats, failover, epochs"]
  TP["Toxiproxy<br/>one named proxy per database link"]
  subgraph hq["HQ site"]
    HQP[("HQ primary")]
    HQS[("HQ standby")]
  end
  subgraph region["Region node (one per region)"]
    RP[("Region primary")]
    RS[("Region standby")]
    RL["Relay worker"]
    ED["Edge API"]
  end
  UI --> GW
  UI --> CO
  UI --> CP
  PB --> GW
  CP -. toxics .-> TP
  GW --> TP
  CO --> TP
  TP --> HQP
  TP --> RP
  HQP -- WAL --> HQS
  RP -- WAL --> RS
  ED --> RP
  RL --> RP
  RL -- outbox events --> TP
```

Two kinds of components:

- **Inside** (would exist in a real deployment): the PostgreSQL nodes, gateway and query router, coordinator, relay workers, edge API, web UI.
- **Outside** (exists only because this is a simulation): bootstrap, playback, fault injection, snapshot and restore, invariant checker, and the control-plane API that lets the UI drive them.

### How the parts communicate

Toxiproxy is a TCP proxy, so it does not care what protocol runs through it. In our design the links it sits on are **database connections**, not HTTP calls. That gives three kinds of link:

| Link | Protocol | Through Toxiproxy |
| --- | --- | --- |
| UI or playback to gateway, coordinator, control plane | HTTP + JSON (REST) | no |
| Any service to any PostgreSQL node | PostgreSQL wire protocol (psycopg 3) | **yes, always** |
| Primary to standby | PostgreSQL streaming replication | yes |

So "REST or WebSocket" only matters for the browser. How the UI stays live (polling on an interval, or a subscription such as server-sent events) is deferred to the UI owner. The backend keeps both possible: every read route is a cheap idempotent `GET`, list routes take `since=`, and responses carry `data_as_of`.

There is no service-to-service HTTP on the core paths. The gateway, relay and coordinator are stateless Python processes that open SQL connections to the nodes. A "node" is a PostgreSQL primary plus its standby.

## 2. Repository layout

### Options considered

| # | Layout | Typical user | Trade-off for us |
| --- | --- | --- | --- |
| 1 | **Split by language**: `backend/`, `frontend/`, `infra/` | small full-stack teams | simple, but all Python ends up one level deeper for no gain |
| 2 | **`apps/` + `packages/`** (Turborepo, Nx, pnpm workspaces) | JavaScript-first monorepos | built for many JS apps sharing JS libraries; we have one JS app |
| 3 | **One folder per deployable service**, each with its own `pyproject.toml` (uv workspace) | microservice teams with separate release cycles | four lock-step services would mean four configs and constant cross-package imports |
| 4 | **One Python package (`src/` layout) + sibling `frontend/`** | most Python projects with a web UI | one environment, one lock file, shared models imported directly |
| 5 | **Build-system monorepo** (Bazel, Pants) | large organisations | far too heavy |

**Chosen: option 4.** All Python services share the same models, SQL and database helpers, are released together, and are run by the same four people. One package with one sub-package per component gives folder-level ownership without workspace overhead.

### Layout

```text
RestaurantOS/
├── .github/workflows/ci.yml
├── .pre-commit-config.yaml   .markdownlint.yaml   .python-version   .gitignore
├── pyproject.toml   uv.lock
├── README.md   CONTRIBUTING.md   AGENTS.md
├── docs/
│   ├── architecture.md        this file
│   ├── work-packages/         one brief per work package: what to build, done-when
│   ├── setup.md               system components
│   ├── schema/                data dictionary of the cleaned dataset
│   ├── api/                   exported OpenAPI JSON per service
│   └── diagrams/
├── db/                        hand-written SQL (the schema contract)
│   ├── shared/                reference tables + helpers, applied to every node
│   ├── region/                DDL + functions for a region node
│   ├── hq/                    DDL + functions for the HQ node
│   └── queries/               named queries loaded by Python, one subfolder per component
├── src/restaurantos/
│   ├── common/                shared settings, models, db helpers, cluster naming, sim clock
│   ├── region/                the region transaction library
│   ├── gateway/               public API, routing; router/ = distributed queries
│   ├── relay/                 outbox relay, holding-queue drain, menu publisher
│   ├── coordinator/           heartbeats, failover, epochs, admin API
│   └── sim/                   OUTSIDE: bootstrap, playback, faults, snapshot, checker, control API, CLI
├── frontend/                  React + Vite (own package.json and lock file)
├── infra/
│   ├── cluster.toml           how many regions, standbys on/off, sync/async
│   ├── compose.yaml           dev baseline: HQ + one region, databases and Toxiproxy only
│   ├── toxiproxy/             proxy list for the dev baseline
│   ├── postgres/              init and replication scripts
│   └── generated/             git-ignored: Compose file and proxy list written by rosctl bootstrap
├── scripts/                   small helper scripts
├── tests/
│   ├── unit/<component>/      no database
│   ├── integration/           one PostgreSQL
│   ├── cluster/               full cluster + Toxiproxy (T1 to T6)
│   └── fixtures/mini/         tiny hand-made dataset, safe to commit
├── data/                      git-ignored: the cleaned *.csv files and MANIFEST.csv (path set by ROS_DATA_DIR)
├── snapshots/                 git-ignored
└── runs/                      git-ignored: per-run measurements
```

### uv and React together

uv manages Python only. The frontend is a normal Node project in `frontend/` with its own `package.json`, lock file and `.nvmrc`. The two never share an environment; they meet at the HTTP API. The repository root has no `package.json`. Decided: npm, TypeScript, current Node LTS pinned in `frontend/.nvmrc`.

## 3. Components and ownership

Each row is a work package someone can claim. Each has a brief in `docs/work-packages/`; start with `docs/work-packages/README.md`, which has the reading order, the rules every package shares and how to run things. "Needs" lists what must exist first.

| WP | Component | Folder | What it does | Needs |
| --- | --- | --- | --- | --- |
| 0 | Foundations | `common/`, `db/`, `infra/` | Schema DDL, shared models, config, Compose base, PostgreSQL replication setup | nothing; **lands first** |
| 1 | Region write path | `region/`, `db/region/` | Order transaction, status state machine, stock, availability, inbox, outbox | WP0 |
| 2 | Gateway | `gateway/` (not `router/`) | Public API, shard-directory cache, epoch handling, holding queue, timestamps | WP0; WP1 for real writes |
| 3 | Query router | `gateway/router/`, `db/queries/router/` | Nearby query, city-wide scatter-gather, join strategies A/B/C, single-node baseline | WP0 |
| 4 | Relay | `relay/` | Region to HQ outbox relay (one process per region); HQ to region holding-queue drain and menu publisher (one HQ-side process, `hq-worker`) | WP0; WP1 for the outbox |
| 5 | Coordinator | `coordinator/`, `infra/postgres/` | Heartbeats, suspect/down, promotion, epoch bump, admin API | WP0 |
| 6a | Bootstrap and playback | `sim/`, `infra/generated/` | Cluster generation, Toxiproxy client (proxy setup), partition ranges, data and stock loading, order replay with speed control, single-region mode | WP0 |
| 6b | Faults, snapshot, checker | `sim/` | Toxiproxy client (toxics, link groups), T1 to T6 scripts, snapshot/restore, invariant checker, control API | WP6a |
| 7 | Web UI | `frontend/` | Four views (Section 8) | nothing to start: the API tables in Section 6, against mocked JSON |

Everyone codes against the contracts in Sections 4 to 7, not against each other's internals.

Two things that look shared belong to each component. Its settings live in its own `<component>/config.py` (prefix `ROS_<COMPONENT>_`); `common/config.py` holds only what several components share. Its named queries live in `db/queries/<component>/`.

Solid arrows: cannot start before. Dashed arrows: can start, but needs the other one to be tested end to end.

```mermaid
flowchart LR
  WP0["WP0 Foundations"]
  WP1["WP1 Region write path"]
  WP2["WP2 Gateway + edge mode"]
  WP3["WP3 Query router"]
  WP4["WP4 Relay"]
  WP5["WP5 Coordinator"]
  WP6a["WP6a Bootstrap + playback"]
  WP6b["WP6b Faults, snapshot, checker"]
  WP7["WP7 Web UI"]
  WP0 --> WP1 & WP2 & WP3 & WP4 & WP5 & WP6a & WP7
  WP6a --> WP6b
  WP1 -.-> WP2
  WP1 -.-> WP4
  WP6a -.-> WP1
  WP6a -.-> WP3
  WP2 -.-> WP3
  WP2 -.-> WP5
  WP4 -.-> WP6b
  WP5 -.-> WP6b
  WP2 -.-> WP7
  WP5 -.-> WP7
  WP6b -.-> WP7
```

## 4. Runtime topology

Generated by `rosctl bootstrap` from `infra/cluster.toml`, the only cluster input a person edits:

| Files | Written by | In git | Used for |
| --- | --- | --- | --- |
| `infra/generated/compose.yaml`, `infra/generated/proxies.json` | `rosctl bootstrap` (WP6a) | no | every bootstrapped cluster, of any size |
| `infra/compose.yaml`, `infra/toxiproxy/proxies.json` | by hand (WP0); the proxy list with `python -m restaurantos.common.cluster --write` | yes | the dev baseline: HQ and `r1`, databases and Toxiproxy only |

The generated Compose file is complete on its own. It does not include or extend the committed one, and bootstrap never writes to a committed file. For one region its database services must match the baseline. Both use the Compose project name `restaurantos`, so they share container and volume names: run one at a time. Application services are never added to `infra/compose.yaml`.

**Where the Python services run.** One setting, `ROS_NETWORK`, chooses between two modes. The code is the same in both; only the addresses that `common.cluster.conninfo` returns differ.

| Mode | Python services | They reach a proxy at | For |
| --- | --- | --- | --- |
| `host` | processes on the Docker host, started with `uv run` (or all at once with `rosctl up`) | `127.0.0.1:<proxy port>` | developing and debugging: no image to rebuild, a debugger attaches |
| `compose` (the default) | containers in the Compose network, all from one image | `toxiproxy:<proxy port>` | measured runs and the demo: no extra hop through Docker's port forwarding |

PostgreSQL and Toxiproxy run in Docker in both modes, and every connection a fault test can cut still goes through its proxy. Host mode is built. Container mode still needs its image (`infra/Dockerfile`, one for all Python services, a WP0 task) and the service entries in the generated Compose file; do not report performance numbers from host mode. `rosctl` and the control plane always run on the host, because they drive Docker.

Services never build a connection string by hand: `common.db.node_conninfo(cluster, node)` reads the mode, the caller's proxy name and the credentials from the settings.

**How each service starts.** Every service package has a `__main__.py`, so a service is started with `uv run python -m restaurantos.<component>`. Who the process is comes from settings, not from arguments. `rosctl up` and, later, the Compose file use exactly these commands:

| Service | Command | Settings that say who it is | HTTP port | Brief |
| --- | --- | --- | --- | --- |
| `gateway` | `python -m restaurantos.gateway` | `ROS_MODE=gateway`, `ROS_CLIENT=gateway` | 8000 | WP2 |
| `edge-r{n}` | `python -m restaurantos.gateway` | `ROS_MODE=edge`, `ROS_REGION_ID=r{n}` | 8010 + n | WP2 |
| `coordinator` | `python -m restaurantos.coordinator` | `ROS_CLIENT=coordinator` | 8001 | WP5 |
| `relay-r{n}` | `python -m restaurantos.relay` | `ROS_RELAY_ROLE=region`, `ROS_CLIENT=relay-r{n}`, `ROS_REGION_ID=r{n}` | none | WP4 |
| `hq-worker` | `python -m restaurantos.relay` | `ROS_RELAY_ROLE=hq`, `ROS_CLIENT=hq` | none | WP4 |
| `control` | `rosctl control` | none (it is outside the system) | 8002 | WP6b |
| `web` | `npm run dev` in `frontend/` | none | Vite's | WP7 |

HTTP ports come from `common.cluster.service_port`. Add `ROS_NETWORK=host` (once, in `.env`) when the process runs on your machine instead of in a container.

With three regions:

| Container | Count | Notes |
| --- | --- | --- |
| `hq-primary`, `hq-standby` | 2 | PostgreSQL 18 |
| `r{n}-primary`, `r{n}-standby` | 2 per region | PostgreSQL 18 |
| `toxiproxy` | 1 | API on 8474 |
| `gateway` | 1 | port 8000, HQ side |
| `coordinator` | 1 | port 8001, HQ side |
| `relay-r{n}` | 1 per region | region side; the outbox relay |
| `hq-worker` | 1 | HQ side; the holding-queue drainer and the menu publisher |
| `edge-r{n}` | 1 per region | port 8010 + n, region side; the gateway code in edge mode |
| `control` | 1 | port 8002, outside the system |
| `web` | 1 | Vite dev server or static build |
| `baseline` | 1 | single PostgreSQL with all data, for comparisons |

Region IDs are `r1`, `r2`, ... Node IDs are `hq-primary`, `r1-primary`, `r1-standby`, and so on. Pin the exact PostgreSQL image tag (same minor everywhere) in `infra/compose.yaml`.

### Toxiproxy proxy names (contract)

One proxy per (client, target) pair, named `<client>__<target>`:

| Proxy | Carries |
| --- | --- |
| `gateway__r1-primary`, `gateway__r1-standby` | orders, reads, queries |
| `coordinator__r1-primary`, `coordinator__r1-standby` | heartbeats, promotion |
| `r1-standby__r1-primary`, `r1-primary__r1-standby` | streaming replication; the second one once the roles have swapped |
| `relay-r1__hq-primary`, `relay-r1__hq-standby` | outbox events going up |
| `hq__r1-primary`, `hq__r1-standby` | menu versions and holding-queue orders going down, sent by `hq-worker` |
| `gateway__hq-primary`, `gateway__hq-standby`, `coordinator__hq-primary`, `coordinator__hq-standby`, `hq-standby__hq-primary`, `hq-primary__hq-standby` | HQ equivalents |

A standby has the same proxies as its primary, because after a promotion it is the primary. Reach a region's primary through the proxy for the node named in `region_state.primary_node`; never assume it is the node called `-primary`.

Names, ports and groups are produced by `restaurantos.common.cluster` from `infra/cluster.toml`; nobody types them by hand. HQ proxies listen from port 21000, region n from 21000 + 10n.

The Compose file publishes three kinds of port, all on `127.0.0.1` only so that nothing is exposed to the network the machine is on. `common.cluster.published_ports` is the full list.

- The Toxiproxy API, 8474.
- Every proxy port, so that services in host mode take exactly the routes that containers take.
- Every node's PostgreSQL port, for batch tools that run on the host (bootstrap loads, the checker, snapshots): `common.cluster.host_port` gives 5500 and 5501 for HQ, 5500 + 10n and 5501 + 10n for region n, 5509 for `baseline`, and `common.cluster.host_conninfo` builds the connection string. These connections bypass Toxiproxy, so they are never used on a request path.

Service ports come from `common.cluster.service_port`. Each edge API has its own (8010 + n) so that several can run on one host.

Fault scenarios never name single proxies. They name **link groups** that the harness expands:

| Group | Proxies | Used by |
| --- | --- | --- |
| `node:r1-primary` | every proxy whose target is `r1-primary` | T1 |
| `link:r1<->hq` | `gateway__r1-*`, `coordinator__r1-*`, `hq__r1-*`, `relay-r1__hq-*` | T2 |
| `repl:r1` | `r1-standby__r1-primary`, `coordinator__r1-standby` | standby loss (roles as at bootstrap) |

The edge API and the relay talk to their own region's primary on the local network, not through an HQ-side proxy. That is what lets a region keep working during T2.

**Which local node is the primary.** It also means HQ cannot tell these two processes about a failover, and the epoch fence cannot stop them: they read the epoch from the very node they write to, so they always agree with it. In T1 the old primary is cut off from HQ but alive, and without a rule they would keep using it. So they look for themselves, with `common.db.find_local_primary`: at start, on a timer, and after any database error.

| What the two local nodes show | Use |
| --- | --- |
| one is writable (not in recovery) | that one |
| both are writable, different `node_state.epoch` | the higher epoch. The other is an old primary that was cut off, or restarted without being rejoined |
| both are writable, same epoch | stay where you are. A promotion is half done: the coordinator has promoted the standby but not yet written its epoch. With no node in use yet, wait |
| neither is writable | none. The edge API answers `503`, the relay sleeps and looks again |

The rule itself is `common.localnode.pick_local_primary`, a function with no I/O, so both processes apply exactly the same one. Two things follow from it. Writes that reach the old primary between the promotion and the next look are lost with that node; the window is one timer interval. And a promotion must write the new epoch to the promoted node straight after `pg_promote()`, because that write is what moves the local processes.

### How vendors are assigned to regions

Static, and decided once at bootstrap. A vendor belongs to its geohash cell (`vendors.geohash`), and each cell belongs to one region (`shard_directory`). There is no per-vendor placement and nothing moves while the system runs. The number of regions comes from `infra/cluster.toml`, so the ranges are computed, not hard-coded. The only dynamic case is the stretch goal: move one hot cell to another region, flip its directory row and bump the epoch.

## 5. Data contracts

### 5.1 Tables

Reference tables mirror the cleaned CSV files **one to one** (same names, same columns; see `docs/schema/data-dictionary.md`, which also lists what a loader must watch for). The only added columns are `products.menu_version` and `products.origin` (`hq` or `region`, see D2 in Section 11).

Note: the Phase 2 document (Table 12) says `recipe` and `recipe_ingredient`. The cleaned dataset calls them `dishes`, `dish_ingredients` and `product_dish`. The code uses the dataset names.

#### On every region node

| Table | Key | Contents |
| --- | --- | --- |
| `ingredients`, `dishes`, `dish_ingredients` | as in CSV | full copy |
| `vendors`, `products`, `product_dish` | as in CSV | this region's slice only |
| `node_state` | single row | `region_id`, `epoch`, `menu_version` |
| `stock` | (`vendor_id`, `item_kind`, `item_id`) | `on_hand`, `par_level`. `item_kind` is `ingredient` (dishes with `stock_mode = per_ingredient`) or `dish` (`per_item`). Dishes with `stock_mode = none` have no row. |
| `product_availability` | (`vendor_id`, `product_id`) | `available`, `reason` (`stock` or `manual`). A product with no row is available. |
| `orders` | (`order_day`, `order_id`), partitioned by `order_day` | `order_id` uuid, `vendor_id`, `channel`, `external_order_id`, `customer_id`, `customer_geohash`, `order_time`, `status`, `status_changed_at`, `menu_version`, `epoch`, `received_at`, `committed_at` |
| `order_items` | (`order_day`, `order_id`, `line_no`), partitioned by `order_day` | `vendor_id`, `product_id`, `quantity`, `price_units` |
| `inbox` | (`channel`, `external_order_id`) | `outcome` (`accepted` or `refused`), `order_day` always, `order_id` only when accepted, `refusal_reason` only when refused (the table checks both pairings); the idempotency guard, so a retried refused order is refused again without side effects |
| `processed_events` | `event_id` | dedupes status events |
| `outbox` | `seq` bigint, UNIQUE `event_id` | `event_type`, `order_day`, `payload` jsonb, `created_at`, `relayed_at` (NULL until HQ has confirmed the event) |
| `exception_log` | `exception_id` | rejected conflicting updates: both versions and timestamps |

#### On HQ

| Table | Key | Contents |
| --- | --- | --- |
| all six reference tables | as in CSV | master copy, all vendors and products |
| `shard_directory` | `geohash` | `region_id` |
| `region_state` | `region_id` | `epoch`, `primary_node`, `standby_node`, `menu_version` |
| `menu_versions` | `version` | `created_at`, `note` |
| `analytics_orders`, `analytics_order_items` | as regional + `region_id` | the read-only copy; adds `hq_applied_at` |
| `analytics_exceptions` | `event_id` | copy of regional exception-log entries; `seq` numbers them in arrival order (added 2026-11-16: a cluster bootstrapped before that needs a fresh `rosctl bootstrap`) |
| `hq_inbox` | (`region_id`, `event_id`) | `seq`, `applied_at`; dedupes relayed events |
| `holding_queue` | `holding_id`, UNIQUE (`channel`, `external_order_id`) | `region_id`, `request` jsonb, `received_at`, `delivered_at`, `delivered_outcome` |
| `cluster_events` | `cluster_event_id` | timeline: failovers, partitions, recoveries |

### 5.2 Region write functions (contract between WP1 and WP2/WP4)

`restaurantos.region` exposes these async functions. Each runs **one local transaction** and takes the epoch the caller believes is current.

| Function | Does | Returns |
| --- | --- | --- |
| `place_order(conn, req, epoch, *, received_at=None)` | epoch check, inbox insert (`ON CONFLICT DO NOTHING`), order + items, stock decrement, availability update, one outbox event | `OrderResult` |
| `change_status(conn, vendor_id, order_id, event_id, to_status, epoch)` | state-machine check; conflict goes to `exception_log` | `StatusResult` (`applied`, `duplicate` or `conflict`) |
| `set_availability(conn, vendor_id, product_id, available, epoch)` | manual sold-out switch; switching on re-checks stock | the product's state: `available`, `reason` |
| `restock(conn, vendor_id, epoch, *, items=None)` | top `on_hand` up to `par_level` (or add `items`), clear `stock` marks | `counters_restocked`, `products_back_on` |
| `apply_menu_version(conn, version, rows, epoch)` | bring this region's product slice to `version` atomically; skip a version it already has; create stock counters for new products | none |

`OrderResult` and `StatusResult` are in `common/models.py`.

`received_at` is the wall-clock time the request first reached the system. The gateway or the edge API stamps it; the holding-queue drainer passes `holding_queue.received_at`. It is stored on the order and travels to HQ in the `order.created` payload. It is not part of `OrderRequest`, which is only what the client sends.

A stale epoch raises `StaleEpochError` (the SQL function `assert_epoch` raises SQLSTATE `ROS01`). The caller refreshes the shard directory and retries once.

The fence is cooperative: `assert_epoch` protects the transactions that call it, and nothing in the schema stops a write that skips it. So every function above calls it first, inside its transaction, and `restaurantos.region` is the only code that writes a region's operational tables. Two writers are outside it on purpose: bootstrap, which loads data before the system runs, and the coordinator, which sets `node_state.epoch` during a promotion.

**How a request path calls a database.** Through `common.db.run_on(pool, work, region_id=...)`, never with a bare `async with pool.connection()`. `run_on` adds a deadline kept by the caller's own clock (`ROS_REQUEST_DEADLINE_S`, default 3 s) and turns every way of not getting an answer in time into `RegionUnreachableError`: no connection, an empty pool, a statement timeout, or the deadline itself. The deadline is what covers a link that is silent or slow while a statement is in flight: `statement_timeout` runs on the server, and on such a link its error does not come back in time. A call that misses the deadline is not cancelled: it is left to finish in the background once the fault is gone, and is then rolled back. Cancelling it would make psycopg send a cancel request over the same dead link, which is pointless at best. The pool also sets `idle_in_transaction_session_timeout` (5 s), so a caller that was cut off half-way through a transaction does not keep its row locks for as long as the fault lasts.

After `RegionUnreachableError` the caller does not know whether its write was committed. That is safe here because the writes that are retried are repeatable: `place_order` through the region's inbox, the HQ apply through `hq_inbox`.

Status state machine: `received -> accepted -> preparing -> ready -> picked_up`, or `canceled` from any state before `picked_up`. First committed change wins.

### 5.3 Outbox event envelope (contract between WP1 and WP4)

```json
{
  "event_id": "uuid",
  "region_id": "r1",
  "seq": 1042,
  "event_type": "order.created | order.status_changed | exception.logged",
  "order_day": 14,
  "created_at": "2026-11-20T18:03:11.482Z",
  "payload": { }
}
```

The relay takes the rows where `relayed_at IS NULL`, in `seq` order. HQ applies each event and its `hq_inbox` row in one transaction and stamps `hq_applied_at`. The relay then sets `relayed_at` on those rows.

Why a per-row flag and not a "last sequence number sent" cursor: two orders committing at the same moment can commit in the opposite order to their `seq` numbers. A cursor that has already moved past the higher number would skip the lower one for ever. A per-row flag cannot skip anything. Payload shapes are the `*Payload` models in `common/models.py`.

### 5.4 Time

Two clocks, never mixed:

- **Simulated time** is business time. It comes from the dataset: `order_day` (0 to 90) and `order_time`. Every request carries it. "Today" in any query is an explicit `day` parameter. No service calls `now()` to decide what day it is.
- **Wall-clock time** is for measurement only: `received_at` and `acked_at` (gateway), `committed_at` (region), `hq_applied_at` (HQ). All containers run on one host, so the clocks agree.

The current simulated time lives in the playback component and is readable at `GET /sim/clock`.

## 6. HTTP APIs (contract for the UI and the harness)

All JSON. No authentication (simulation). FastAPI serves OpenAPI at `/docs` on each service; exported copies go in `docs/api/`. Every response from a query route includes a `meta` object (`QueryMeta` in `common/models.py`): `{"mode", "nodes_contacted", "missing_regions", "rows_transferred", "data_as_of", "elapsed_ms"}`.

### 6.1 Gateway, port 8000, prefix `/api/v1`

| Method and path | Purpose | Notes |
| --- | --- | --- |
| `POST /orders` | place an order | `201` committed; `200` duplicate (same body as the first time); `202` parked in the holding queue; `409` whole order refused (`refusal_reason` says why) |
| `GET /vendors/{vendor_id}/orders/{order_id}?day=` | order status | always reads the region primary; `day` is optional |
| `POST /vendors/{vendor_id}/orders/{order_id}/status` | status change | body `{event_id, to_status}`; `409` on conflict |
| `GET /vendors/{vendor_id}/orders?day=&status=` | kitchen board: one restaurant's orders of a day | `status` takes one or several, comma-separated |
| `GET /vendors/{vendor_id}/menu` | menu with availability | each row carries its `dish_id` |
| `GET /vendors/{vendor_id}/stock` | stock counters | `on_hand` and `par_level` per counter; details below |
| `PUT /vendors/{vendor_id}/products/{product_id}/availability` | manual sold-out switch | body `{available}`; changes the manual mark only; details below |
| `POST /vendors/{vendor_id}/restock` | restock to par | also called by playback; no body needed; details below |
| `GET /vendors?geohash=&q=` | find restaurants in one cell | exact cell only, no neighbours (that is `/query/nearby`); details below |
| `GET /dishes?q=&limit=` | search dishes by name | gateway reads HQ, edge reads its own node; details below |
| `GET /dishes/{dish_id}` | one dish | `404` if unknown |
| `GET /query/nearby?geohash=&dish=&limit=` | "who near me can make X now" | geohash cell + 8 neighbours; `dish` is a `dish_id`; "can make" means stock for an order of one |
| `GET /query/top-dishes?day=&limit=&rank_by=&mode=live\|hq\|baseline` | city-wide top dishes | takes a day range and a time window; details below |
| `GET /query/ingredient-usage?day=&mode=live\|hq\|baseline&strategy=A\|B\|C` | distributed join | `strategy` applies to `mode=live`; takes a day range and a time window; details below |
| `GET /regions` | shard directory and epochs | reads HQ only; shape below |
| `GET /regions/{region_id}/summary?day=` | regional office numbers | fields below; reads the region primary |
| `POST /menu/products` | add or change products | gateway only; writes at HQ and creates a new menu version; `202`; details below |

`POST /orders` request:

```json
{
  "channel": "dhrd",
  "external_order_id": "1234567",
  "vendor_id": "212753d2",
  "customer_id": "1ba124d4e5",
  "customer_geohash": "w21z7",
  "order_day": 85,
  "order_time": "12:03:29",
  "items": [ { "product_id": "783e85338f1c", "quantity": 1 } ]
}
```

Response:

```json
{
  "order_id": "uuid",
  "status": "received",
  "duplicate": false,
  "region_id": "r2",
  "menu_version": 3,
  "received_at": "...", "committed_at": "...", "acked_at": "..."
}
```

The edge API (`edge-r{n}`) serves the same `/vendors/...` routes, and `/regions/{region_id}/summary`, for its own region only.

`GET /vendors?geohash=&q=`, the customer's restaurant search:

- `geohash`: required, one 5-character cell. Only that cell is searched, never its neighbours; "near me" is `/query/nearby` (WP3). One cell belongs to one region, so the route goes cell to region through the shard directory and reads that region's primary.
- `q`: optional, case-insensitive substring (`ILIKE`). A vendor matches when `q` is in its `primary_cuisine` or in the `name` of any of its `products`. Without `q`, every vendor in the cell matches. Stock and sold-out are ignored: "who can make it right now" is `/query/nearby`. Fuzzy matching may come later; not needed now.
- Response: a JSON list, one entry per vendor (a vendor matched by several products appears once), ordered by `vendor_id`. No `meta`: this is not a `/query` route.

```json
[
  { "vendor_id": "212753d2", "chain_id": null, "geohash": "w21z7", "primary_cuisine": "noodles" }
]
```

- A cell the shard directory does not know has no restaurants: `200` with `[]`, no region contacted.
- Edge mode: searches its own region only. A cell owned by another region gives `200` with `[]`.
- The owning region cannot be reached: `503`, as for every other `/vendors` route.

`PUT /vendors/{vendor_id}/products/{product_id}/availability`, the kitchen's sold-out switch. It changes availability; the menu route is what reads it.

- Body: `{"available": true}` or `{"available": false}`.
- Answer: `200` with the product's state after the change. `reason` is `manual`, `stock` or `null`. Sending the same value twice is harmless. `404` when this vendor does not sell that product.

```json
{ "vendor_id": "212753d2", "product_id": "783e85338f1c", "available": false, "reason": "stock" }
```

- The switch only touches the **manual** mark. Off writes a `manual` mark. On removes it and then checks whether one unit of the product can be made right now; if not, a `stock` mark takes its place and the answer says `available: false, reason: "stock"`. That is not an error.
- The `stock` mark looks after itself: an order that empties a counter sets it, and a restock clears it. Nobody switches it by hand.
- The mark is set only when a counter reaches zero. A product that needs 6 of a counter holding 3 still shows as available; its orders are refused as `out_of_stock` all the same, because the stock check at order time is the real gate.

What availability does to orders:

- It blocks **new** orders only. An order is checked once, when it is placed.
- Orders already placed carry on: their stock was taken when they were placed. Nothing cancels them automatically.
- To drop one, the kitchen cancels it with a status change. A cancel does not give stock back.
- An order parked in the holding queue is checked when HQ delivers it, so it can be refused as `unavailable` although the product was on when the customer ordered.

`POST /vendors/{vendor_id}/restock`:

- No body: every counter of the vendor is **topped up** to its par level, `on_hand = max(on_hand, par_level)`. A restock never removes stock. Every `stock` mark of the vendor is cleared; `manual` marks stay.
- Optional body, and **optional to build** (playback and the demonstration do not use it): a delivery of chosen amounts.

```json
{ "items": [ { "item_kind": "ingredient", "item_id": "garlic", "add": 20 } ] }
```

- With a body, each `add` (a whole number above zero) is added to that counter and may take it above par. The item need not be used by any product the vendor sells today: a restaurant can stock up before a menu item exists. A counter the vendor does not have yet is created with `par_level = 0`. The item must exist in `ingredients` (or in `dishes` for `item_kind: "dish"`), otherwise `404` with `{"error": "unknown_item"}`. Only the `stock` marks of products that can now make one unit are cleared.
- Answer: `200`. Playback counts only `200` as success and reads nothing else.

```json
{ "vendor_id": "212753d2", "region_id": "r1", "counters_restocked": 14, "products_back_on": 3 }
```

- A no-body restock can be sent twice safely. A body restock adds, so sending it twice adds twice.

`GET /vendors/{vendor_id}/stock`, the vendor's counters, ordered by `item_kind`, then `item_id`:

```json
[
  { "item_kind": "ingredient", "item_id": "garlic", "name": "garlic", "unit": "portion", "on_hand": 12, "par_level": 35, "updated_at": "..." }
]
```

`name` is the ingredient's id (it is its name), or for a dish counter the dish's `dish_name`, falling back to `example_name`. `unit` is the ingredient's unit from `dish_ingredients`, and `null` for a dish counter (it counts servings) or an ingredient no recipe uses.

`GET /dishes?q=&limit=` and `GET /dishes/{dish_id}`:

- `q`: required, case-insensitive substring on `dish_name` or `example_name` (3,053 dishes have no `dish_name`). `limit`: default 20, at most 100. Ordered by `n_vendors` descending, then `dish_id`.
- Each dish: `{dish_id, dish_name, example_name, scope, n_vendors}`. The single-dish route returns one such object.
- Dishes are reference data with no stock, and every region holds a full copy, so the gateway reads HQ and the edge API reads its own node; both give the same answer.
- A dish is not always shared: `scope` is `global`, or narrower such as one chain. "Chicken rice" can be several dishes, and `/query/nearby` on one of them finds only the restaurants that sell that one. A screen runs the nearby query once per dish it wants.
- `n_vendors` is the dataset's count; products added later do not change it.

Dish names in answers: where a dish is what the answer is about and nothing else names it, its `dish_id` comes with `dish_name` and `example_name` (dish search, the top level of a nearby answer, top-dishes rows). A row that is a product with its own `name` (the menu, a nearby row) carries only the `dish_id`.

`POST /menu/products`, add or change products. Gateway only: the edge API never reaches HQ, so it does not serve this route, and a region cut off from HQ cannot add products until the link is back (D2). One call makes one menu version:

```json
{
  "note": "add laksa",
  "products": [
    { "vendor_id": "212753d2", "name": "Laksa", "price_units": 30, "dish_id": "0a1b2c3d4e5f", "multiplier": 1 },
    { "vendor_id": "212753d2", "product_id": "783e85338f1c", "name": "Kaya toast set", "price_units": 12, "dish_id": "9f8e7d6c5b4a", "multiplier": 2 }
  ]
}
```

- **No `product_id`: a new product.** The gateway makes the id: the first 12 hexadecimal characters of SHA-1 of `vendor_id|name`. Dataset ids come from DHRD; an id only has to be unique within its vendor. The same request sent again lands on the same id and creates nothing (`created: false`); a different price in it is applied as a change. If the id exists under a different name, the answer is `409` with `{"error": "product_exists", "vendor_id": "...", "product_id": "..."}`.
- **With `product_id`: a change** to that product's name or price. `404` when the vendor does not have it. Renaming keeps the id. Products cannot be removed; the sold-out switch hides one.
- **A product's dish link never changes.** `dish_id` and `multiplier` are set when the product is created. On a change they may be left out; if they are sent and differ from the stored ones, the answer is `409` with `{"error": "dish_link_fixed"}`. A product that should count as another dish is a new product. This is what keeps history true: stock, `top-dishes` and `ingredient-usage` all read the dish and multiplier of an old order line from `product_dish`, so those rows must still say what they said when the order was placed.
- `dish_id` must be an existing dish (find it with `GET /dishes?q=`); new dishes and recipes are out of scope. `multiplier` is a whole number, 1 or more. `unit_price` is stored as `price_units / 2500`. New rows get `origin = 'hq'`.
- At HQ, in one transaction: lock `menu_versions` so versions are numbered in commit order, insert the next version, upsert `products` and `product_dish`.
- Answer: `202`. HQ has stored the version; a region has it once its `menu_version` (in `GET /regions` or its summary) reaches that number.

```json
{ "menu_version": 4, "products": [ { "vendor_id": "212753d2", "product_id": "a1b2c3d4e5f6", "region_id": "r1", "created": true } ] }
```

- `hq-worker` sends a version only to the regions that own a changed vendor, so another region's `menu_version` can skip numbers. A region ignores a version at or below its own.
- What a region has is what its own primary says (`node_state.menu_version`), not what HQ recorded. After a failover the new primary can be a version behind, and `hq-worker` sends it again.
- When a region applies a version, each added product gets the stock counters it lacks, with `on_hand = 0` and a par level of at least `max(F, the product's need for one unit)` (Section 7.1). An added product that cannot make one unit is marked unavailable (`stock`); the next restock fills its counters and switches it on. A changed product keeps its marks: a new name or price does not change what it needs.

The three `/query/...` routes answer with the parameters they were given, a `results` list and `meta`.

**Day range and time window** (`top-dishes` and `ingredient-usage`):

- `from_day=&to_day=`, both included. `day=N` is the short form of `from_day=N&to_day=N`; sending `day` together with either of the others is a `422`. There is no "today" on the server: for the last 7 days a caller reads the day from `/sim/clock` and sends `from_day = today - 6`, `to_day = today`.
- `from_time=&to_time=` (`HH:MM:SS`), optional and only together; `from_time` is included, `to_time` is not. The window repeats on **each day** of the range, so a week of lunches is one request and a week of dinners is another.
- A window that crosses midnight (`from_time` later than `to_time`) belongs to the day it starts on:

| Request, with `from_time=22:00:00&to_time=04:00:00` | Covers |
| --- | --- |
| `day=20`, or `from_day=20&to_day=20` | one night: day 20 at 22:00 up to day 21 at 04:00 |
| `from_day=20&to_day=21` | two nights: that one, and day 21 at 22:00 up to day 22 at 04:00 |

Day 20's own hours before 04:00 are not in either: they belong to the night of day 19.

`GET /query/nearby?geohash=&dish=&limit=`:

```json
{
  "geohash": "w21z7", "dish_id": "0a1b2c3d4e5f", "dish_name": "laksa", "example_name": "Laksa", "limit": 10,
  "results": [
    { "vendor_id": "212753d2", "chain_id": null, "primary_cuisine": "noodles", "geohash": "w21z6", "distance_m": 4890.0,
      "product_id": "783e85338f1c", "name": "Laksa", "price_units": 30, "multiplier": 1, "region_id": "r1" }
  ],
  "meta": { "mode": "live", "nodes_contacted": ["r1-primary"], "missing_regions": [], "rows_transferred": 1, "data_as_of": "...", "elapsed_ms": 12.5 }
}
```

`distance_m` is the distance in metres between the centre of the customer's cell and the centre of the vendor's cell, so every vendor in the customer's own cell has 0. Rows are ordered by distance, then `vendor_id`, then `product_id`.

`GET /query/top-dishes`, with `limit`, `mode`, the range and window above, and `rank_by=units|quantity|order_lines|sales_units` (default `units`):

```json
{
  "from_day": 14, "to_day": 20, "from_time": null, "to_time": null, "limit": 10, "rank_by": "units",
  "results": [
    { "dish_id": "0a1b2c3d4e5f", "dish_name": "laksa", "example_name": "Laksa", "units": 412, "quantity": 398, "order_lines": 371, "sales_units": 11940 }
  ],
  "meta": { "...": "..." }
}
```

| Field | Sum over the order lines in the range and window | One line "3 x set of 6" at 20 adds |
| --- | --- | --- |
| `units` | `quantity x multiplier`: units of the dish | 18 |
| `quantity` | `quantity`: menu items | 3 |
| `order_lines` | number of lines | 1 |
| `sales_units` | `quantity x price_units` | 60 |

- Canceled orders are left out, as in the region summary's `sales_units`: the sale is void. So while a cancel is still on its way to HQ, `mode=hq` can differ from `mode=live`; `meta.data_as_of` shows how old HQ's copy is.
- Order: the `rank_by` number descending, then `dish_id`. The second key decides which dishes make the cut at `limit` when several tie, so the cluster and the baseline return the same rows. A screen may show ties in any order.

`GET /query/ingredient-usage`, with `mode`, `strategy` and the range and window above:

```json
{
  "from_day": 14, "to_day": 14, "from_time": null, "to_time": null, "strategy": "C",
  "results": [ { "ingredient_id": "garlic", "amount": 1830, "unit": "portion" } ],
  "meta": { "...": "..." }
}
```

`amount` is `sum(quantity x multiplier x amount)`, with `multiplier` and the recipe read from `product_dish` and `dish_ingredients`; a product's dish link never changes (see `POST /menu/products`), so this is the multiplier the order was placed with. Canceled orders count here, on purpose: the sale is void but the ingredients were used. Rows are grouped by (`ingredient_id`, `unit`) and ordered by `amount` descending, then `ingredient_id`, then `unit`; there is no `limit`. `strategy` is `null` unless `mode=live`. Strategy B ships every order line in the range, so a long range is slow; that cost is what B is there to show.

The read routes for orders, the menu and the regions:

`GET /vendors/{vendor_id}/orders/{order_id}` returns one order:

```json
{
  "order_id": "uuid", "order_day": 85, "order_time": "12:03:29", "vendor_id": "212753d2",
  "customer_id": "1ba124d4e5", "channel": "dhrd", "status": "preparing", "status_changed_at": "...",
  "menu_version": 3, "received_at": "...", "committed_at": "...",
  "items": [ { "line_no": 1, "product_id": "783e85338f1c", "name": "Laksa", "quantity": 1, "price_units": 30 } ]
}
```

- `items` is ordered by `line_no`. `price_units` is the price stored on the order line; `name` is the product's name today, so an old order shows a renamed product's new name.
- `orders` has one partition per day. With `?day=` (the `order_day` from the `POST /orders` answer) the lookup reads one partition; without it, all of them.

`GET /vendors/{vendor_id}/orders?day=&status=`, the kitchen board, returns a list of the same objects, ordered by `order_time`, then `order_id`. `status` is one status or several separated by commas (`status=received,accepted,preparing` is "everything not finished yet"); without it, every status. An unknown status is a `422`.

`GET /vendors/{vendor_id}/menu` returns a list ordered by `product_id`:

```json
[
  { "vendor_id": "212753d2", "product_id": "783e85338f1c", "name": "Laksa", "price_units": 30, "menu_version": 1,
    "dish_id": "0a1b2c3d4e5f", "available": true, "reason": null }
]
```

`GET /regions` returns a list ordered by `region_id`:

```json
[
  { "region_id": "r1", "epoch": 1, "primary_node": "r1-primary", "standby_node": "r1-standby", "menu_version": 1,
    "updated_at": "...", "geohashes": ["w21z6", "w21z7"] }
]
```

Answers every route shares:

| Status | When | Body |
| --- | --- | --- |
| `404` | unknown vendor, product, order, dish or region | `{"error": "not_found"}` |
| `409` | an order was refused, or a status change lost to an earlier one | the normal response body |
| `503` | the region that owns the data cannot be reached in time | `{"error": "region_unreachable", "region_id": "r1"}` |
| `503` | HQ's database cannot be reached in time (`POST /menu/products`, `GET /regions`, `GET /dishes` on the gateway) | `{"error": "hq_unreachable"}` |

Only `POST /orders` is parked when its region is unreachable (`202`). Every other route for that region, read or write, answers `503`; none falls back to HQ's copy, which may be stale and has no stock or availability. The `/query/...` routes are the exception: they answer `200` with what the reachable regions returned and name the rest in `meta.missing_regions`.

`GET /regions/{region_id}/summary?day=` response:

```json
{
  "region_id": "r1",
  "day": 14,
  "epoch": 1,
  "menu_version": 1,
  "vendors": 3120,
  "orders": {
    "total": 7211,
    "received": 4, "accepted": 9, "preparing": 31, "ready": 12, "picked_up": 7120, "canceled": 35
  },
  "refused": 58,
  "sales_units": 5120340,
  "products_unavailable": 17,
  "outbox_backlog": 0,
  "data_as_of": "..."
}
```

| Field | Source, all on the region primary |
| --- | --- |
| `epoch`, `menu_version` | `node_state` |
| `vendors` | number of rows in `vendors` |
| `orders` | `orders` with `order_day = day`, counted per `status`; every status is present, zero included |
| `refused` | `inbox` rows with `outcome = 'refused'` and `order_day = day` |
| `sales_units` | `sum(quantity x price_units)` over that day's `order_items`, leaving out canceled orders |
| `products_unavailable` | `product_availability` rows with `available = false`, at the time of the read |
| `outbox_backlog` | `outbox` rows with `relayed_at IS NULL`, at the time of the read |
| `data_as_of` | wall-clock time of the read |

### 6.2 Coordinator, port 8001

| Method and path | Purpose |
| --- | --- |
| `GET /admin/cluster` | which nodes are alive, and each region's figures |
| `GET /admin/events?since=&limit=` | timeline of failovers, partitions, recoveries |
| `GET /admin/exceptions?since=&limit=` | status changes that lost to an earlier one |
| `POST /admin/regions/{region_id}/failover` | manual promotion from the UI |

`GET /admin/cluster`:

```json
{
  "as_of": "...",
  "nodes": [
    { "node_id": "r1-primary", "region_id": "r1", "role": "standby", "status": "up", "last_heartbeat_at": "..." }
  ],
  "regions": [
    { "region_id": "r1", "epoch": 2, "menu_version": 4, "primary_node": "r1-standby", "standby_node": "r1-primary",
      "replication": "async", "orders_per_s": 41.5, "standby_lag_s": 0.2, "hq_lag_s": 1.1,
      "outbox_backlog": 12, "holding_queue": 0, "figures_as_of": "..." }
  ]
}
```

- `status` is `up`, `suspect` or `down`. HQ's two nodes are listed with `region_id: null`.
- `replication` is `sync` or `async` as it runs now (synchronous mode can fall back). `standby_lag_s` is `null` when the standby is not connected; that is not zero lag.
- `holding_queue` is the number of orders parked at HQ for the region and not yet delivered.
- `figures_as_of` is when the region's figures were last read. When the coordinator cannot reach a region primary it keeps showing the last values, and this field shows their age.

`GET /admin/events` and `GET /admin/exceptions` page the same way: `since` is the `event_id` (events) or `seq` (exceptions) of the last entry the caller has, and the answer is the entries after it, oldest first, at most `limit` (default 100, at most 1,000). Without `since`, the newest `limit` entries, oldest first. All times are wall-clock: the coordinator does not know simulated time.

A `since` cursor is only safe when numbers are handed out in commit order (Section 5.3 explains why the relay does not use one). So every transaction that inserts into `cluster_events` or `analytics_exceptions` first takes a transaction-level advisory lock, one fixed key per table (`pg_advisory_xact_lock`). Both tables get few rows, so writers queueing behind each other costs nothing.

```json
[ { "event_id": 17, "occurred_at": "...", "kind": "failover", "region_id": "r1", "node_id": "r1-standby", "detail": {} } ]
```

`kind` is one of `node_suspect`, `node_down`, `node_up`, `failover`, `partition`, `sync_downgrade`; the coordinator's owner may add more.

```json
[ { "seq": 3, "region_id": "r1", "order_day": 85, "order_id": "uuid", "kind": "status_conflict",
    "current_status": "picked_up", "current_changed_at": "...", "attempted_status": "ready", "attempted_at": "...",
    "hq_applied_at": "..." } ]
```

Exceptions are read from HQ's copy, so an entry appears once the relay has delivered it.

`POST /admin/regions/{region_id}/failover` takes no body. It answers `200` when the promotion's steps are done:

```json
{ "region_id": "r1", "epoch": 3, "primary_node": "r1-standby", "standby_node": "r1-primary", "event_id": 18 }
```

- `409` with `{"error": "standby_unavailable"}` when the standby is not up; `409` with `{"error": "failover_in_progress"}` when a promotion of that region is already running; `404` for an unknown region.
- It is allowed while the primary is healthy: the operator decides, and the old primary is fenced off by the epoch. With asynchronous replication the last orders may be lost, which the checker reports as "lost at failover".
- HQ has no such route; promoting HQ's standby is a `rosctl` command (D10).

### 6.3 Control plane, port 8002 (outside the system)

The Ops view is the only HTTP caller. Everything here is also a `rosctl` command. Commands that create or read shared state (links, faults, nodes) are clients of this API, not second implementations: they call the control process on port 8002, so the fault list stays in one place. Commands that read the simulated clock or the run's counters are clients of the process that runs playback, because that state lives in memory there. Only commands that hold no run state run the same code directly, so tests and scripts need no HTTP server for them.

| Method and path | Purpose | Over HTTP |
| --- | --- | --- |
| `GET /sim/clock` | current simulated day and time, speed, state | built (WP6a) |
| `GET /sim/playback` | the run's state and counters | built (WP6a) |
| `POST /sim/playback` | `{action: start\|pause\|resume\|stop, from_day, to_day, speed, seed, run_id}` | built (WP6a) |
| `PATCH /sim/playback` | `{speed}`: change speed on the fly (UI slider) | built (WP6a) |
| `GET /sim/links` | link groups and the faults on them | required |
| `POST /sim/faults` | break a link group | required |
| `DELETE /sim/faults/{fault_id}`, `POST /sim/faults/heal-all` | remove faults | required |
| `POST /sim/nodes/{node_id}/kill`, `POST /sim/nodes/{node_id}/start` | real crash (T1b) and restart | required |
| `POST /sim/scenarios/{name}` | run a scripted scenario (`T1`, `T1b`, `T2` to `T6`) | optional |
| `GET /sim/snapshots`, `POST /sim/snapshots`, `POST /sim/snapshots/{name}/restore` | Section 7.4 | optional |
| `POST /sim/check` | run the invariant checker | optional |
| `GET /sim/jobs/{job_id}` | progress of a scenario, snapshot, restore or check | with the optional routes |

"Optional" routes must work as `rosctl` commands; putting them on the Ops view is extra. Their shapes are fixed here either way.

**Playback.** `GET /sim/clock` answers `{"state", "sim_day", "sim_time", "speed"}`. Before the first run the last three are `null`: there is no "today" yet, and a screen should ask for a day. While a run is paused, and after it has ended, the clock stands at its last moment. `state` is `idle`, `running`, `paused`, `stopping`, `done`, `stopped` or `failed`.

`GET /sim/playback`, and every `POST` and `PATCH` on it, answer:

```json
{
  "state": "running", "run_id": "2026-11-20-a", "seed": 0, "from_day": 14, "to_day": 20, "speed": 1000.0,
  "orders_sent": 0, "orders_acked": 0, "orders_parked": 0, "orders_refused": 0, "orders_failed": 0,
  "events_sent": 0, "events_acked": 0, "events_conflicted": 0, "events_failed": 0, "events_skipped": 0,
  "restocks_sent": 0, "restocks_acked": 0, "restocks_failed": 0, "inflight": 0
}
```

- `start` needs `from_day`; `to_day`, `speed`, `seed` and `run_id` are optional. `duplicate_rate` and `shuffle` (Section 7.2) will join the body when they are built. Status events and restocks are switched on when the playback process starts, not per run.
- A command the current state does not allow answers `409` with `{"error": "invalid_state", "state": "idle", "message": "..."}`.

**Faults.** `POST /sim/faults` takes `{"target": "<link group>", "fault": "...", "params": {}}`:

| `fault` | `params` |
| --- | --- |
| `cut` | none |
| `silence` | none |
| `latency` | `latency_ms` (required), `jitter_ms` (default 0) |
| `reset` | `timeout_ms` (default 0: reset at once) |

It answers `201` with the fault; `404` for an unknown link group; `409` with `{"error": "fault_exists"}` when the group already has a fault of that kind (different kinds may be on one group together).

```json
{ "fault_id": "f3", "target": "link:r1<->hq", "fault": "latency", "params": { "latency_ms": 4000, "jitter_ms": 0 },
  "proxies": ["gateway__r1-primary", "gateway__r1-standby"], "created_at": "..." }
```

`DELETE /sim/faults/{fault_id}` answers `200`, or `404`. `POST /sim/faults/heal-all` answers `200` with `{"healed": 3}`. `GET /sim/links` lists every link group as `{"group", "proxies", "faults"}`, each fault in the shape above.

**Nodes.** `POST /sim/nodes/{node_id}/kill` and `/start` answer `200` with `{"node_id": "r1-primary", "running": false}`, or `404` for an unknown node. Killing a stopped node or starting a running one changes nothing.

**Jobs.** A scenario, a snapshot, a restore and a check take from seconds to minutes. Their `POST` answers `202` with a job, and `GET /sim/jobs/{job_id}` reports on it:

```json
{ "job_id": "j7", "kind": "scenario", "name": "T2", "state": "running", "started_at": "...", "finished_at": null, "result": null, "error": null }
```

- `kind` is `scenario`, `snapshot`, `restore` or `check`; `state` is `running`, `done` or `failed`. One job runs at a time: a second `POST` answers `409` with `{"error": "busy", "job_id": "j7"}`.
- `POST /sim/scenarios/{name}` takes an optional `{"region_id": "r1"}`, the region to break (default `r1`). Its `result` is the checker's result plus the scenario's own numbers, such as T1's failover time.
- `GET /sim/snapshots` lists `{"name", "created_at", "sim_day", "sim_time", "regions"}`. `POST /sim/snapshots` takes `{"name": "before-t2"}` (`409` when the name is taken). Restoring an unknown name is a `404`.
- The `result` of `POST /sim/check`:

```json
{ "passed": true, "acked": 21840, "stored": 21840, "hq": 21840,
  "lost_at_failover": { "count": 0, "order_ids": [] }, "duplicates": [], "unaccounted": [] }
```

## 7. Simulation harness

### 7.1 Bootstrap

`rosctl bootstrap` reads `infra/cluster.toml` (number of regions, standbys on or off, sync or async) and:

1. generates the Compose file and the Toxiproxy proxy list into `infra/generated/` (Section 4);
2. starts the nodes from empty volumes. There are no schema migrations: after a change under `db/`, bootstrap again. Each primary applies its own schema on first start (`infra/postgres/init-primary.sh`) and each standby clones its primary through its proxy, so bootstrap applies no schema itself. It then writes one `region_state` row per region at HQ;
3. counts **vendors** per geohash cell from `vendors.csv`, sorts the cells, cuts them into N contiguous ranges with roughly equal vendor counts, and writes `shard_directory`. Order data is deliberately not used here: sizing regions from the orders we then replay would leak future information. (Phase 2 Section 2.2 says "order volume"; update that wording in the final report.);
4. loads reference data: masters into HQ, slices into each region, everything into `baseline` (orders and stock reach the baseline later, see below);
5. initialises `stock` from the warm-up window (see `docs/replay-stock-parameters.md`: W = 14 days, C = 7, F = 5, R = 7). No counter starts below what one unit of the largest product using it needs, so every product can be sold at least once per restock.

**The baseline node.** `baseline` is one PostgreSQL that holds, on a single node, exactly what the regions hold between them. `rosctl baseline sync` fills it after a run: it refuses to start unless playback is paused and nothing is in flight, then copies `orders`, `order_items`, `stock` and `product_availability` out of every region primary and replaces the baseline's copies with them.

The baseline has two uses. It checks the query router: the same SQL run through the router (`mode=live`) and on the baseline (`mode=baseline`) must give the same answer, and since the data is identical by construction, a difference can only come from how the router splits and merges. And it gives a fair speed comparison of one node against scatter-gather on the same data. It does not test whether orders arrive (the oracle and the invariant checker do that), and it holds whatever the run produced, faults and refusals included.

Stretch goal: replay the same orders into a one-node system to compare write throughput. The cheap way needs no baseline at all: bootstrap with `regions = 1`, run in single-region mode (Section 7.2), and compare the run log with a `regions = 3` run.

### 7.2 Playback and speed-up

Playback reads `orders.csv` and `order_items.csv` in (`order_day`, `order_time`) order and posts each order to the gateway. With speed factor S, an order whose simulated time is t seconds after the start is sent t / S wall-clock seconds after the run starts.

The dataset averages about 0.25 orders per second, so the speed factor decides both run length and load:

| Speed | Average load | Wall-clock time for one simulated day | For all 77 replayed days |
| --- | --- | --- | --- |
| 1x | 0.25 orders/s | 24 h | not practical |
| 10x | 2.5 orders/s | 2.4 h | 7.7 days |
| 50x | 13 orders/s | 29 min | 37 h |
| 1,000x | 250 orders/s | 86 s | 1.8 h |
| 5,000x | 1,300 orders/s | 17 s | 22 min |

So every run needs a **window** (`from_day`, `to_day`) as well as a speed. Default speed is **1,000x**. Speed can be changed while a run is in progress (`PATCH /sim/playback`), so playback must schedule incrementally: on a speed change it re-anchors (wall-clock now, simulated now) and continues from there, rather than precomputing send times.

Playback also sends status events for each order, triggers restocks every R simulated days, resends a configurable share of orders, and shuffles delivery. Per-order timestamps go to `runs/<run_id>/orders.jsonl`.

**Status events.** On by default (`status_events: all`); a flag can turn them off or sample them for a measurement run. Each event fires at a baseline simulated offset after the order (+1, +3, +15, +25 minutes) plus a random jitter. They go to the region's own API, because the kitchen is on the region side.

**Repeatability.** Random values are never drawn from one shared generator in send order. Each is computed from a hash of (`seed`, `order_id`, purpose), for example the jitter of the "ready" event or whether an order is resent. The same seed then gives the same run whatever the concurrency, speed changes, pauses or snapshot restores.

**Single-region mode.** `rosctl playback --region r1 --target edge` replays only the orders whose vendor belongs to `r1` and posts them straight to that region's API. It needs only `r1-primary` and `edge-r1` running: no HQ, gateway, relay or Toxiproxy. With `--sequential` (one order in flight at a time) the result is exactly predictable, so the checker compares the region against an oracle computed from the CSV files by plain counting: orders stored, orders refused, and final stock per vendor and item.

### 7.3 Faults

We talk to Toxiproxy through **our own small client** over its HTTP API (port 8474), built on `httpx`, in `src/restaurantos/sim/toxiproxy.py`. No third-party Toxiproxy library. WP6a writes the part bootstrap needs (list, create, update and delete proxies); WP6b adds toxics and link-group operations on top. Faults are addressed by link group (Section 4):

| Fault | Toxiproxy action |
| --- | --- |
| `cut` | disable the proxies |
| `silence` | `timeout` toxic, `timeout: 0` |
| `latency` | `latency` toxic with `latency`, `jitter` |
| `reset` | `reset_peer` toxic |

Real crashes use `docker kill`. Scenarios T1 to T6 are scripts built from these.

### 7.4 Snapshot and restore

A snapshot is the state of the whole system at a point in simulated time:

1. pause playback and wait for in-flight requests;
2. record the control state: playback cursor, seed, speed, simulated clock, Toxiproxy proxies and toxics, `cluster.toml`;
3. stop every PostgreSQL container and archive each data volume;
4. start everything again.

Restore reverses it. Because primaries and standbys are captured stopped and together, replication state, epochs and outboxes come back consistent.

### 7.5 Invariant checker

After a run and after queues drain: orders acknowledged by the gateway = orders stored across regions = orders in HQ's analytics copy; no `external_order_id` stored twice; every refused or parked order is accounted for.

After a failover the three numbers may differ by a small, explainable amount, and the checker reports it as "lost at failover" instead of failing: orders the old primary acknowledged, or its relay forwarded to HQ, before they were replicated to the standby. With asynchronous replication that is the expected cost, and it is the number the sync-versus-async comparison is about. Anything beyond it is a bug.

## 8. Web UI

The UI holds no business logic. It calls the routes in Section 6 and renders the JSON. Layout is the UI owners' decision; `docs/work-packages/wp7-web-ui.md` collects what they need to know about the APIs. The backend guarantees these four views have the data they need:

| View | Who it stands for | Reads | Can do |
| --- | --- | --- | --- |
| HQ | company head office | `/query/top-dishes`, `/query/ingredient-usage` (live or HQ copy, with "data as of"), `/admin/cluster`, `/admin/events`, `/admin/exceptions` | manual failover; add products (D2) |
| Region | regional office and kitchens | `/regions/{id}/summary`, kitchen board, menu, stock (`/vendors/{id}/stock`) | status changes, sold-out toggle, restock |
| Customer | delivery-app user | `/vendors`, `/dishes`, menu, `/query/nearby`, order status | place an order |
| Ops | outside the system | `/sim/*` | start, pause and seek playback, set speed, inject and heal faults, kill nodes, snapshots |

Adding a product (`POST /menu/products`) is a gateway route that any view may call; which screen shows the form is the UI owners' choice.

Where the logic sits: joins, aggregation and merging happen in PostgreSQL and in the query router. HQ does not do UI work and the UI does not do data work.

## 9. Testing

| Level | Folder | Needs | In CI |
| --- | --- | --- | --- |
| unit | `tests/unit/` | nothing | yes |
| integration | `tests/integration/` | one PostgreSQL 18 | yes |
| cluster | `tests/cluster/` | full cluster + Toxiproxy | no |

CI cannot use the real dataset (it must not be published), so tests use `tests/fixtures/mini/`: a hand-made dataset with the same files and columns, a few vendors in three cells.

## 10. Decisions made

Everything in the Phase 2 document and working notes, plus:

| Topic | Decision |
| --- | --- |
| Repository | public monorepo; one Python package + sibling `frontend/`; data git-ignored, never committed, no Git LFS |
| Tooling | uv; **Python 3.14**; ruff (no black, no isort); hand-written SQL; async services; `pygeohash` for geohash maths |
| Toxiproxy | driven by our own small `httpx` client over the HTTP API; no third-party client library. Proxy calls in WP6a, toxic calls in WP6b |
| Frontend | npm, TypeScript, current Node LTS; live-update mechanism left to the UI owner |
| D1 | every region runs its own API process (the gateway code in edge mode) |
| D2 | build (b): the region asks HQ to add a product, through `POST /menu/products` on the gateway (Section 6.1). (c) region-owned products is a stretch goal; `products.origin` is in the schema for it |
| D8 | status events on by default, switchable per run; seeded jitter on top of fixed offsets |
| D10 | HQ standby with manual promotion; automatic HQ failover is a stretch goal |
| D3 | an order containing an out-of-stock dish is refused whole (`409`) and counted |
| D6 | default playback speed 1,000x, adjustable on the fly |
| D7 | snapshots stop the containers and archive the volumes |
| Workflow | anyone may merge; rebase onto latest `main` first; Conventional Commits; agents never merge; WP0 may go straight to `main` |
| Cluster files | `infra/cluster.toml` is the only hand-edited input. `rosctl bootstrap` writes `infra/generated/` (git-ignored) and never touches a committed file. `infra/compose.yaml` stays as the one-region dev baseline |
| Proxies | a standby has every proxy its primary has, and replication is proxied both ways, so a promoted standby is reachable by the gateway, the HQ-side workers and the relays |
| Settings | shared ones in `common/config.py`; each component's own in `<component>/config.py` with prefix `ROS_<COMPONENT>_` |
| Region unreachable | orders are parked (`202`); every other route answers `503`; queries answer with `missing_regions`; nothing falls back to HQ's copy |
| Semi-join (strategy C) | the region joins against recipe rows sent by the router and must not read its own `dish_ingredients` |
| Epoch fence | cooperative: enforced by `restaurantos.region` calling `assert_epoch`, not by the schema |
| Amounts and units | `dish_ingredients.amount`, `unit` and `product_dish.multiplier` are data, never assumed. Today's dataset has amount 1 and unit `portion` everywhere only as placeholders. A stock counter is kept in its ingredient's unit, so one ingredient has one unit (checked at bootstrap); anything that reports an ingredient amount reports the unit too |
| Availability | the sold-out switch changes the manual mark only; the stock mark is set by orders and cleared by restocks. Availability blocks new orders only; existing orders are never cancelled by it |
| Restock | tops up to par and never removes stock; an optional body adds chosen amounts (optional to build) |
| Stock model, known limits | par levels are fixed at bootstrap, not a rolling average rounded to pack sizes; ingredient loss (spills, spoilage, staff meals, unsold prepared food) is not modelled; an ingredient has one unit and nothing converts between units, so data with two units for one ingredient is refused at bootstrap |
| libpq | psycopg is built against libpq 17 or newer (`docs/setup.md`); a unit test checks it. Older versions cancel queries with a blocking call that freezes the process on a silenced link |
| D16 | two modes, one setting (`ROS_NETWORK`). `host`: Python services run on the Docker host, for development; built. `compose`: services run in containers from one image, for measured runs; the image is a WP0 task still to do. Published ports are bound to `127.0.0.1` |
| D17 | the edge API and the relay find their region's primary themselves, by the rule in Section 4 (`common.localnode`), at start, on a timer and after any error |
| D18 | the baseline is filled by `rosctl baseline sync`, a copy of the regions after a run. Replaying into a one-node system for a write-throughput number is a stretch goal |
| D19 | request paths call databases through `common.db.run_on`, which keeps a deadline on the caller's side; a call that misses it is abandoned and later rolled back, never cancelled; pools also set `idle_in_transaction_session_timeout`. Checked by `tests/cluster/test_deadline.py` under `silence` and `latency` |
| HQ worker | the holding-queue drainer and the menu publisher run in their own HQ-side process, `hq-worker` (WP4), not inside the coordinator or the gateway: a large drain must not share an event loop with heartbeats or with live requests, and T3 must be able to kill it alone. It reaches the regions through the `hq__` proxies and HQ's own database directly |
| Starting a service | `python -m restaurantos.<component>`; who the process is comes from settings (table in Section 4) |

## 11. Open decisions

| ID | Question |
| --- | --- |
| D15 | Owners for WP1 to WP7 (WP0: Mark). To be claimed in the group chat. |

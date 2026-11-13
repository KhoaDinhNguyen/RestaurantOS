# WP7: Web UI

**Goal:** four views that show the system working: what head office sees, what a region sees, what a customer does, and the operator's controls. The UI holds no logic of its own; it calls the HTTP APIs and shows the JSON.

**You own:** `frontend/` and everything in it. How you build it is your decision: layout, component library, state handling, how the screen stays up to date. This brief does not prescribe any of that. It tells you where the information is and what the rest of the repository expects from your folder.

**Read first:** `docs/architecture.md` Section 6 (every route) and Section 8 (the four views); Section 1 for the big picture. `src/restaurantos/common/models.py` for the exact shape of an order request and response.

**Needs:** nothing to start. You can build every screen against mocked JSON today. Real data needs WP2 (gateway and edge API), WP5 (coordinator) and WP6b (control plane), in that order of usefulness.

## Shared code you must use

None of the Python packages. What you share with the backend is the HTTP API ("Where the data comes from" below) and a few fixed names that CI and the commit hooks rely on.

Decided already: React, TypeScript, npm, the current Node LTS. CI and the commit hooks call these, so they must exist once you add `frontend/package.json`:

| What | Used by |
| --- | --- |
| `frontend/.nvmrc` | `docs/setup.md` (`nvm install`) and CI |
| `frontend/package-lock.json` | CI runs `npm ci` |
| `npm run lint` | CI, `CONTRIBUTING.md` Section 4 |
| `npx prettier --check .` passes | CI; the `frontend-prettier` commit hook |
| `npm test -- --run` | CI (the flag is Vitest's "run once") |
| `npm run build` | CI |

The frontend CI job is skipped until `frontend/package.json` exists, so nothing fails before you start. You may add npm packages without asking; list them in the pull request.

## What to build

### Where the data comes from

Three services, plus one extra API per region. All answer JSON, none needs a login.

| Service | Port | Routes | Listed in |
| --- | --- | --- | --- |
| Gateway | 8000 | `/api/v1/...`: orders, restaurants, menus, queries, regions | Section 6.1 |
| Edge API of region n | 8010 + n (8011, 8012, ...) | the same `/vendors/...` routes and `/regions/{id}/summary`, for that region only | Section 6.1 |
| Coordinator | 8001 | `/admin/...`: node health, lag, events, exceptions, manual failover | Section 6.2 |
| Control plane | 8002 | `/sim/...`: clock, playback, faults, snapshots, checker | Section 6.3 |

**Reaching them from the browser.** No brief asks the services to send CORS headers, so assume a page served from the Vite dev server cannot call `localhost:8000` directly. The simplest fix stays inside your folder: let the dev server forward the calls (Vite's `server.proxy`), for example `/api` to 8000, `/admin` to 8001, `/sim` to 8002, and one prefix per region for the edge APIs. Your code then only uses relative paths, and the same build works behind any reverse proxy later.

**Which API for the Region view.** Use the region's own edge API, not the gateway. The point of the edge API is that a region keeps working when it is cut off from head office (test T2). During that test the gateway answers `503` for the region while its edge API still serves the kitchen board. A Region view that talks to the edge API shows exactly that.

### How to find out what a response looks like

In order of reliability:

1. **A running service.** Every service publishes its OpenAPI description at `/docs` (a page) and `/openapi.json`. This is always right, because it is generated from the code.
2. **`docs/api/`.** Exported copies of those descriptions. The folder is empty today; each backend owner fills it with their first pull request.
3. **`docs/architecture.md` Section 6.** Full examples exist for `POST /orders`, for `GET /regions/{region_id}/summary`, for the shared error answers and for the `meta` object. For the other routes only the path and purpose are fixed so far.
4. **`src/restaurantos/common/models.py`.** `OrderRequest`, `OrderResponse`, `StatusChangeRequest`, `StatusResult`, `OrderStatus`, `RefusalReason` and `QueryMeta` are the contract for orders, status changes and query metadata.

If a shape you need is not written down anywhere, ask the owner of that route before you build on a guess: WP2 for restaurants, menus and the kitchen board, WP3 for the three queries, WP5 for `/admin`, WP6b for `/sim`.

### The four views

What each view reads and can do is the contract (Section 8). The notes are things the data makes possible; whether and how you show them is up to you.

| View | Reads | Can do | Worth knowing |
| --- | --- | --- | --- |
| **HQ** (head office) | `/query/top-dishes`, `/query/ingredient-usage`, `/admin/cluster`, `/admin/events`, `/admin/exceptions` | manual failover of a region; add a product | Both queries take `mode=live`, `hq` or `baseline`, and ingredient usage also takes `strategy=A`, `B` or `C`. Every answer carries `meta`: how fresh the data is, which nodes were asked, which regions did not answer, how many rows travelled, how long it took. Showing `meta` next to the result is the demonstration |
| **Region** (regional office, kitchens) | `/regions/{id}/summary`, the kitchen board, menu with availability | change an order's status, switch a product to sold out, restock | Talk to the edge API (see above). The summary's fields are defined in Section 6.1 |
| **Customer** | `/vendors`, a menu, `/query/nearby`, one order's status | place an order | See "Placing an order" below |
| **Ops** (the operator, outside the system) | `/sim/clock`, `/sim/playback`, `/sim/links`, `/sim/snapshots` | start, pause and stop playback, change speed, inject and remove faults, kill and start nodes, run a scenario, take and restore snapshots, run the checker | The professor asked for an admin screen showing which nodes are alive and their state. `/admin/cluster` has exactly that: per node its role, `up`, `suspect` or `down`, and last heartbeat; per region its epoch, orders per second, standby lag, lag to HQ and outbox backlog |

### Rules of the data

- **Ids are text.** Restaurant, product, dish and customer ids are short strings of digits and letters. Some contain only digits and a few start with zeros. Never turn one into a number.
- **Time is simulated.** "Today" is a day number from 0 to 90, and the clock runs at whatever speed playback is set to. Read it from `GET /sim/clock`. Do not use the browser's clock for anything except showing how long a request took. Routes that take `day=` expect that day number; the replay covers days 14 to 90.
- **Order status** is one of `received`, `accepted`, `preparing`, `ready`, `picked_up`, `canceled`.
- **No calculations.** No sums, no joins, no ranking, no choosing a region in the browser. If a screen needs a number the API does not return, ask for it in the API.

#### Placing an order

The body is `OrderRequest` (example in Section 6.1). Three fields need care:

- `external_order_id`: make up a unique id per order (a UUID is fine) and **send the same id again if you retry**. That is what stops a double click or a timeout from creating two orders.
- `channel`: any fixed name for the UI, such as `web`. Playback uses `dhrd`.
- `order_day` and `order_time`: the simulated day and time from `/sim/clock`, not the real ones.

The answers, all of which are normal and worth showing:

| Status | Meaning | The body tells you |
| --- | --- | --- |
| `201` | stored | `order_id`, `region_id`, the three timestamps |
| `200` | this order was sent before; here is the first answer again | `duplicate` is true |
| `202` | the region cannot be reached; the order is parked and will be delivered later | `queued` is true, no `order_id` yet. Send the same request again later to learn the outcome |
| `409` | refused as a whole | `refusal_reason`: `out_of_stock`, `unavailable` or `unknown_product` |

#### Changing a status

`POST /vendors/{vendor_id}/orders/{order_id}/status` with `{event_id, to_status}`. Make a new `event_id` (a UUID) for each click and reuse it on a retry. A `409` means the change lost to an earlier one, for example "ready" arriving after "picked up"; the body is a `StatusResult` with `conflict` set.

#### Answers every route can give

`404` for an unknown restaurant, order or region. `503` with `{"error": "region_unreachable", "region_id": "r1"}` when the region that owns the data does not answer in time. During the fault tests `503` and `202` are the expected, correct behaviour, so show them clearly; do not hide them behind endless retries or a blank screen. The three `/query/...` routes do not fail when one region is missing: they answer with what they have and name the missing regions in `meta.missing_regions`.

## Run it on its own

**Before the backend exists.** Everything above can be built against mocks. Suggested, not required:

- Keep mock answers as JSON files next to the code that uses them, shaped exactly like the examples in Section 6, and switch between mock and real with one setting.
- Start with the answers that are already fully defined: an order (all four outcomes), the region summary, the error bodies, a query answer with its `meta`.
- When a backend route appears, compare your mock with its `/openapi.json` and fix the mock, not the server.

**Against the real system**, once the other work packages land:

```bash
docker compose -f infra/compose.yaml up -d    # databases and Toxiproxy (one region)
uv run rosctl up                              # gateway, coordinator, HQ worker, relay, edge API (WP6a)
cd frontend && npm run dev
```

`README.md` in this folder ("Running things") and `docs/setup.md` have the ports and what must be free. Until `rosctl up` exists, each backend brief shows the one command that starts its service.

## Done when

What each line needs is marked in front of it (the marks are explained in `README.md` in this folder).

- `data (needs WP2, WP5 and WP6b)` The four views exist and each can do what Section 8 lists for it, against the real services.
- `data (needs WP1 and WP2)` An order placed from the Customer view shows up on that restaurant's kitchen board in the Region view, and a status change made there shows up on the customer's order.
- `data (needs WP2, WP4 and WP6b)` With a region cut off (T2): the Customer view shows the order as queued, the Region view (on the edge API) keeps working, and the HQ view shows which regions a query could not reach.
- `data (needs WP5 and WP6b)` After a failover (T1), the HQ view shows the node states changing and the new epoch, without a page reload.
- `unit` Query results show their `meta`: mode, freshness, nodes asked, rows transferred, time taken.
- `unit` Nothing on any screen is computed in the browser, and nothing uses the browser's clock as the simulated time.
- `unit` `npm run lint`, `npm test -- --run` and `npm run build` pass, locally and in CI.

## Watch out for

- The numbers on screen change quickly at 1,000x playback. Polling every second or two is enough for a demonstration; how you keep the screen current is your choice.
- An id that looks like a number is still text (see above). JSON parsers keep strings as strings; the danger is your own code, sort functions and table libraries.
- Do not build on a response shape you have not seen written down. Ask its owner.
- Keep dataset content out of the repository: no real menu or order data in mock files. Make up a few restaurants and products.

## Questions to raise with the team

These are not yours to solve alone, but you will hit them first:

1. **Choosing a dish for the nearby search.** `/query/nearby` takes a `dish_id`, and no route lists or searches dishes. This one is yours to design and then ask the backend for. Two ways it could go: the menu answer carries each product's `dish_id` ("who else near me makes this?"); or a search route returns every dish that matches what the customer typed, the UI runs the nearby query once per dish, and shows each dish's matches separately.
2. **Response shapes** for the restaurant list, the menu, the kitchen board, the three queries, `/admin/cluster` and `/sim/...` are not written down yet. The earlier their owners export them to `docs/api/`, the less you rebuild.
3. **The `web` container** for the final demonstration (Section 4 lists it as "Vite dev server or static build"): a static build needs something in front of it that forwards `/api`, `/admin` and `/sim`, or the services need CORS headers.
4. **The clock when playback is stopped.** What `/sim/clock` returns then decides what "today" is on your date pickers.

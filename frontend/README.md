# RestaurantOS web UI (WP7)

Four views, HQ, Region, Customer and Ops (`docs/architecture.md` Section 8). The UI holds no business logic: it calls the HTTP API (Section 6) and renders the JSON. `PAGES.md` explains what each view shows and does, with a walkthrough in mock mode.

## Run it

```bash
nvm install            # the Node version in .nvmrc
npm ci
npm run dev:mock       # made-up data, no backend needed
npm run dev            # against the real services (see below)
```

`npm run dev` forwards every call through the Vite dev server, because the services send no CORS headers:

| Path in the browser | Goes to                                              |
| ------------------- | ---------------------------------------------------- |
| `/api/...`          | gateway, port 8000                                   |
| `/admin/...`        | coordinator, port 8001                               |
| `/sim/...`          | control plane, port 8002                             |
| `/edge/r{n}/...`    | edge API of region n, port 8010 + n (prefix removed) |

Two settings, read from the repository's `.env`: `ROS_UI_PROXY_HOST` (default `127.0.0.1`) and `ROS_UI_REGIONS` (default 3).

In mock mode region `r2` is cut off from HQ. Through the gateway its routes answer `503` and its orders are parked (`202`), while its edge API keeps working, as in test T2.

## Checks (the same as CI)

```bash
npm run lint
npx prettier --check .
npm test -- --run
npm run build
```

## Layout

| Folder                 | What is in it                                                                                                                           |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `src/api/`             | `client.ts` (one `request` that never throws for an HTTP status), `endpoints.ts` (one function per route), `types.ts` (response shapes) |
| `src/hooks/`           | `usePolling`, `useAction`, `useSimClock`                                                                                                |
| `src/features/<area>/` | the panels of each view: `hq`, `region`, `customer`, `ops`, plus `cluster` (shared by HQ and Ops) and `clock`                           |
| `src/pages/`           | one page per view; `layout/` holds the header with the clock (`RootLayout`) and the page frame (`PageLayout`)                           |
| `src/components/`      | small shared pieces: `Panel`, `ApiStatus`, `QueryMetaView`, `StatusBadge`                                                               |
| `src/mocks/`           | the mock server and its made-up JSON. No dataset content                                                                                |

## Rules this code keeps

- Ids are strings, never numbers.
- Simulated day and time come from `GET /sim/clock`. When there is no run yet, the user picks a day in the header. The browser clock is used only to time requests.
- No sums, joins or ranking in the browser. Rows are shown in the order the server sent them.
- Each order has one `external_order_id`, and each status click has one `event_id`. A retry sends the same id again.
- `503` and `202` are shown plainly; nothing retries them on its own.

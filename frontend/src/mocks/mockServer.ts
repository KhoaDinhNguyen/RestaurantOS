// A stand-in for the backend, used by `npm run dev:mock` and by tests.
// It answers the routes of docs/architecture.md Section 6 with made-up data
// (never dataset content) shaped like the examples there. It plays the
// server's part, so the little bookkeeping it does here is the server's job,
// not the UI's.
//
// To show the fault cases, region r2 is cut off from HQ: through the gateway
// its routes answer 503 and its orders are parked (202), while its own edge
// API (/edge/r2/...) keeps working, as in test T2.
import type { RawResponse, Transport } from "../api/client";
import type {
  ClusterEvent,
  ClusterStatus,
  Dish,
  ExceptionEntry,
  Fault,
  LinkGroup,
  MenuRow,
  Order,
  OrderRequest,
  OrderResponse,
  OrderStatus,
  PlaybackState,
  QueryMeta,
  SimClock,
  StockRow,
} from "../api/types";
import clusterJson from "./data/cluster.json";
import dishesJson from "./data/dishes.json";
import linksJson from "./data/links.json";
import menusJson from "./data/menus.json";
import stockJson from "./data/stock.json";
import vendorsJson from "./data/vendors.json";

const CUT_OFF_REGION = "r2";
const STAMP = "2026-11-23T10:00:00Z";

const NEXT: Record<OrderStatus, OrderStatus[]> = {
  received: ["accepted", "canceled"],
  accepted: ["preparing", "canceled"],
  preparing: ["ready", "canceled"],
  ready: ["picked_up", "canceled"],
  picked_up: [],
  canceled: [],
};

const clone = <T>(value: T): T => structuredClone(value);
const reply = (status: number, body: unknown): RawResponse => ({
  status,
  body,
});
const notFound = () => reply(404, { error: "not_found" });
const unreachable = (regionId: string) =>
  reply(503, { error: "region_unreachable", region_id: regionId });

function addSeconds(time: string, seconds: number): string {
  const [h, m, s] = time.split(":").map(Number);
  const total = (h * 3600 + m * 60 + s + seconds) % 86400;
  const pad = (n: number) => String(Math.floor(n)).padStart(2, "0");
  return `${pad(total / 3600)}:${pad((total % 3600) / 60)}:${pad(total % 60)}`;
}

function meta(mode: string, rows: number): QueryMeta {
  const live = mode === "live";
  return {
    mode: mode as QueryMeta["mode"],
    nodes_contacted: live
      ? ["r1-primary"]
      : mode === "hq"
        ? ["hq-primary"]
        : ["baseline"],
    missing_regions: live ? [CUT_OFF_REGION] : [],
    rows_transferred: rows,
    data_as_of: STAMP,
    elapsed_ms: 12.5,
  };
}

export function createMockServer(): Transport {
  const vendors = clone(vendorsJson);
  const dishes = clone(dishesJson) as Dish[];
  const menus = clone(menusJson) as MenuRow[];
  const stockByVendor = new Map<string, StockRow[]>();
  const orders: Order[] = [];
  const answers = new Map<string, OrderResponse>();
  const cluster = clone(clusterJson) as ClusterStatus;
  const events: ClusterEvent[] = [
    {
      event_id: 1,
      occurred_at: "2026-11-23T09:59:40Z",
      kind: "partition",
      region_id: "r2",
      node_id: null,
      detail: {},
    },
    {
      event_id: 2,
      occurred_at: "2026-11-23T09:59:52Z",
      kind: "node_suspect",
      region_id: "r2",
      node_id: "r2-primary",
      detail: {},
    },
  ];
  const exceptions: ExceptionEntry[] = [
    {
      seq: 1,
      region_id: "r1",
      order_day: 20,
      order_id: "5f0c2b1e-0000-4000-8000-000000000001",
      kind: "status_conflict",
      current_status: "picked_up",
      current_changed_at: STAMP,
      attempted_status: "ready",
      attempted_at: STAMP,
      hq_applied_at: STAMP,
    },
  ];
  const links = clone(linksJson) as LinkGroup[];
  const clock: SimClock = {
    state: "idle",
    sim_day: null,
    sim_time: null,
    speed: null,
  };
  const playback: PlaybackState = {
    state: "idle",
    run_id: null,
    seed: null,
    from_day: null,
    to_day: null,
    speed: null,
    orders_sent: 0,
    orders_acked: 0,
    orders_parked: 0,
    orders_refused: 0,
  };
  let nextFault = 2;
  let nextOrder = 1;

  const vendorRegion = (id: string) =>
    vendors.find((v) => v.vendor_id === id)?.region_id;

  const stockOf = (vendorId: string) => {
    if (!stockByVendor.has(vendorId))
      stockByVendor.set(vendorId, clone(stockJson) as StockRow[]);
    return stockByVendor.get(vendorId)!;
  };

  /** Resolves the `/vendors/{id}` region for a gateway or an edge call. */
  function vendorGuard(
    edgeRegion: string | null,
    vendorId: string,
  ): RawResponse | null {
    const region = vendorRegion(vendorId);
    if (!region) return notFound();
    if (edgeRegion !== null && edgeRegion !== region) return notFound();
    if (edgeRegion === null && region === CUT_OFF_REGION)
      return unreachable(region);
    return null;
  }

  function placeOrder(body: OrderRequest): RawResponse {
    const first = answers.get(body.external_order_id);
    if (first) return reply(200, { ...first, duplicate: true });
    const region = vendorRegion(body.vendor_id);
    if (!region) return notFound();
    const base = {
      duplicate: false,
      region_id: region,
      received_at: STAMP,
      acked_at: STAMP,
      committed_at: null,
      menu_version: null,
      order_day: null,
      order_id: null,
      status: null,
      refusal_reason: null,
      outcome: null,
      queued: false,
    } satisfies OrderResponse;
    if (region === CUT_OFF_REGION) return reply(202, { ...base, queued: true });
    const rows = body.items.map((item) =>
      menus.find(
        (m) =>
          m.vendor_id === body.vendor_id && m.product_id === item.product_id,
      ),
    );
    const refuse = (reason: OrderResponse["refusal_reason"]) => {
      const answer = {
        ...base,
        outcome: "refused" as const,
        refusal_reason: reason,
      };
      answers.set(body.external_order_id, answer);
      return reply(409, answer);
    };
    if (rows.some((r) => !r)) return refuse("unknown_product");
    if (rows.some((r) => !r!.available)) return refuse("unavailable");
    const order: Order = {
      order_id: `5f0c2b1e-0000-4000-8000-${String(nextOrder++).padStart(12, "0")}`,
      order_day: body.order_day,
      order_time: body.order_time,
      vendor_id: body.vendor_id,
      customer_id: body.customer_id,
      channel: body.channel,
      status: "received",
      status_changed_at: STAMP,
      menu_version: 1,
      received_at: STAMP,
      committed_at: STAMP,
      items: body.items.map((item, i) => ({
        line_no: i + 1,
        product_id: item.product_id,
        name: rows[i]!.name,
        quantity: item.quantity,
        price_units: rows[i]!.price_units,
      })),
    };
    orders.push(order);
    const answer: OrderResponse = {
      ...base,
      outcome: "accepted",
      order_id: order.order_id,
      order_day: order.order_day,
      status: "received",
      menu_version: 1,
      committed_at: STAMP,
    };
    answers.set(body.external_order_id, answer);
    return reply(201, answer);
  }

  function vendorRoute(
    method: string,
    edgeRegion: string | null,
    rest: string,
    params: URLSearchParams,
    body: unknown,
  ): RawResponse {
    const parts = rest.split("/").map(decodeURIComponent);
    const vendorId = parts[0];
    const guard = vendorGuard(edgeRegion, vendorId);
    if (guard) return guard;
    const route = parts.slice(1).join("/");
    const mine = menus.filter((m) => m.vendor_id === vendorId);

    if (method === "GET" && route === "menu") return reply(200, mine);
    if (method === "GET" && route === "stock")
      return reply(200, stockOf(vendorId));
    if (method === "POST" && route === "restock") {
      const counters = stockOf(vendorId);
      counters.forEach((c) => (c.on_hand = Math.max(c.on_hand, c.par_level)));
      const back = mine.filter((m) => m.reason === "stock");
      back.forEach((m) => {
        m.available = true;
        m.reason = null;
      });
      return reply(200, {
        vendor_id: vendorId,
        region_id: vendorRegion(vendorId),
        counters_restocked: counters.length,
        products_back_on: back.length,
      });
    }
    if (
      method === "PUT" &&
      parts[1] === "products" &&
      parts[3] === "availability"
    ) {
      const row = mine.find((m) => m.product_id === parts[2]);
      if (!row) return notFound();
      const { available } = body as { available: boolean };
      row.available = available;
      row.reason = available ? null : "manual";
      return reply(200, {
        vendor_id: vendorId,
        product_id: row.product_id,
        available: row.available,
        reason: row.reason,
      });
    }
    if (parts[1] === "orders") {
      const mineOrders = orders.filter((o) => o.vendor_id === vendorId);
      if (method === "GET" && parts.length === 2) {
        const day = params.get("day");
        const wanted = params.get("status")?.split(",").filter(Boolean) ?? [];
        return reply(
          200,
          mineOrders.filter(
            (o) =>
              (day === null || String(o.order_day) === day) &&
              (wanted.length === 0 || wanted.includes(o.status)),
          ),
        );
      }
      const order = mineOrders.find((o) => o.order_id === parts[2]);
      if (!order) return notFound();
      if (method === "GET" && parts.length === 3) return reply(200, order);
      if (method === "POST" && parts[3] === "status") {
        const { to_status } = body as { to_status: OrderStatus };
        if (!NEXT[order.status].includes(to_status)) {
          return reply(409, {
            order_id: order.order_id,
            status: order.status,
            applied: false,
            duplicate: false,
            conflict: true,
          });
        }
        order.status = to_status;
        return reply(200, {
          order_id: order.order_id,
          status: order.status,
          applied: true,
          duplicate: false,
          conflict: false,
        });
      }
    }
    return notFound();
  }

  function apiRoute(
    method: string,
    edgeRegion: string | null,
    path: string,
    params: URLSearchParams,
    body: unknown,
  ): RawResponse {
    if (path.startsWith("/vendors/"))
      return vendorRoute(method, edgeRegion, path.slice(9), params, body);

    if (method === "POST" && path === "/orders")
      return placeOrder(body as OrderRequest);

    if (method === "GET" && path === "/vendors") {
      const geohash = params.get("geohash") ?? "";
      const q = (params.get("q") ?? "").toLowerCase();
      return reply(
        200,
        vendors
          .filter((v) => v.geohash === geohash)
          .filter((v) => edgeRegion === null || v.region_id === edgeRegion)
          .filter(
            (v) =>
              !q ||
              (v.primary_cuisine ?? "").includes(q) ||
              menus.some(
                (m) =>
                  m.vendor_id === v.vendor_id &&
                  m.name.toLowerCase().includes(q),
              ),
          )
          .map((v) => ({
            vendor_id: v.vendor_id,
            chain_id: v.chain_id,
            geohash: v.geohash,
            primary_cuisine: v.primary_cuisine,
          })),
      );
    }

    if (method === "GET" && path === "/dishes") {
      const q = (params.get("q") ?? "").toLowerCase();
      return reply(
        200,
        dishes.filter(
          (d) =>
            (d.dish_name ?? "").includes(q) ||
            (d.example_name ?? "").toLowerCase().includes(q),
        ),
      );
    }
    if (method === "GET" && path.startsWith("/dishes/")) {
      const dish = dishes.find((d) => d.dish_id === path.slice(8));
      return dish ? reply(200, dish) : notFound();
    }

    if (method === "GET" && path === "/query/nearby") {
      const dish = dishes.find((d) => d.dish_id === params.get("dish"));
      if (!dish) return notFound();
      const results = menus
        .filter((m) => m.dish_id === dish.dish_id && m.available)
        .map((m) => ({
          m,
          v: vendors.find((v) => v.vendor_id === m.vendor_id)!,
        }))
        .filter(({ v }) => v.region_id !== CUT_OFF_REGION)
        .map(({ m, v }) => ({
          vendor_id: v.vendor_id,
          chain_id: v.chain_id,
          primary_cuisine: v.primary_cuisine,
          geohash: v.geohash,
          distance_m: v.geohash === params.get("geohash") ? 0 : 4890,
          product_id: m.product_id,
          name: m.name,
          price_units: m.price_units,
          multiplier: 1,
          region_id: v.region_id,
        }));
      return reply(200, {
        geohash: params.get("geohash"),
        dish_id: dish.dish_id,
        dish_name: dish.dish_name,
        example_name: dish.example_name,
        limit: Number(params.get("limit") ?? 10),
        results,
        meta: meta("live", results.length),
      });
    }

    if (method === "GET" && path === "/query/top-dishes") {
      const mode = params.get("mode") ?? "live";
      const results = dishes.map((d, i) => ({
        dish_id: d.dish_id,
        dish_name: d.dish_name,
        example_name: d.example_name,
        units: 400 - i * 90,
        quantity: 380 - i * 85,
        order_lines: 350 - i * 80,
        sales_units: 11900 - i * 2500,
      }));
      return reply(200, {
        ...rangeEcho(params),
        limit: Number(params.get("limit") ?? 10),
        rank_by: params.get("rank_by") ?? "units",
        results,
        meta: meta(mode, results.length),
      });
    }

    if (method === "GET" && path === "/query/ingredient-usage") {
      const mode = params.get("mode") ?? "live";
      const results = [
        { ingredient_id: "mock-broth", amount: 1830, unit: "ladle" },
        { ingredient_id: "mock-noodles", amount: 920, unit: "g" },
        { ingredient_id: "mock-rice", amount: 410, unit: "g" },
      ];
      return reply(200, {
        ...rangeEcho(params),
        strategy: mode === "live" ? (params.get("strategy") ?? "C") : null,
        results,
        meta: meta(mode, results.length),
      });
    }

    if (method === "GET" && path === "/regions") {
      return reply(
        200,
        cluster.regions.map((r) => ({
          region_id: r.region_id,
          epoch: r.epoch,
          primary_node: r.primary_node,
          standby_node: r.standby_node,
          menu_version: r.menu_version,
          updated_at: STAMP,
          geohashes: [
            ...new Set(
              vendors
                .filter((v) => v.region_id === r.region_id)
                .map((v) => v.geohash),
            ),
          ],
        })),
      );
    }

    const summary = path.match(/^\/regions\/([^/]+)\/summary$/);
    if (method === "GET" && summary) {
      const regionId = summary[1];
      const region = cluster.regions.find((r) => r.region_id === regionId);
      if (!region || (edgeRegion !== null && edgeRegion !== regionId))
        return notFound();
      if (edgeRegion === null && regionId === CUT_OFF_REGION)
        return unreachable(regionId);
      return reply(200, {
        region_id: regionId,
        day: Number(params.get("day")),
        epoch: region.epoch,
        menu_version: region.menu_version,
        vendors: vendors.filter((v) => v.region_id === regionId).length,
        orders: {
          total: 7211,
          received: 4,
          accepted: 9,
          preparing: 31,
          ready: 12,
          picked_up: 7120,
          canceled: 35,
        },
        refused: 58,
        sales_units: 5120340,
        products_unavailable: menus.filter((m) => !m.available).length,
        outbox_backlog: region.outbox_backlog,
        data_as_of: STAMP,
      });
    }

    if (method === "POST" && path === "/menu/products") {
      if (edgeRegion !== null) return notFound();
      const { products } = body as {
        products: { vendor_id: string; product_id?: string }[];
      };
      return reply(202, {
        menu_version: 2,
        products: products.map((p, i) => ({
          vendor_id: p.vendor_id,
          product_id: p.product_id ?? `f00d${String(i).padStart(8, "0")}`,
          region_id: vendorRegion(p.vendor_id) ?? "r1",
          created: !p.product_id,
        })),
      });
    }
    return notFound();
  }

  function rangeEcho(params: URLSearchParams) {
    const day = params.get("day");
    return {
      from_day: Number(day ?? params.get("from_day")),
      to_day: Number(day ?? params.get("to_day")),
      from_time: params.get("from_time"),
      to_time: params.get("to_time"),
    };
  }

  function adminRoute(method: string, path: string): RawResponse {
    if (method === "GET" && path === "/admin/cluster")
      return reply(200, cluster);
    if (method === "GET" && path === "/admin/events") return reply(200, events);
    if (method === "GET" && path === "/admin/exceptions")
      return reply(200, exceptions);
    const m = path.match(/^\/admin\/regions\/([^/]+)\/failover$/);
    if (method === "POST" && m) {
      const region = cluster.regions.find((r) => r.region_id === m[1]);
      if (!region) return notFound();
      [region.primary_node, region.standby_node] = [
        region.standby_node,
        region.primary_node,
      ];
      region.epoch += 1;
      for (const node of cluster.nodes) {
        if (node.region_id !== region.region_id) continue;
        node.role =
          node.node_id === region.primary_node ? "primary" : "standby";
        if (node.role === "primary") node.status = "up";
      }
      const event: ClusterEvent = {
        event_id: events.length + 1,
        occurred_at: STAMP,
        kind: "failover",
        region_id: region.region_id,
        node_id: region.primary_node,
        detail: {},
      };
      events.push(event);
      return reply(200, {
        region_id: region.region_id,
        epoch: region.epoch,
        primary_node: region.primary_node,
        standby_node: region.standby_node,
        event_id: event.event_id,
      });
    }
    return notFound();
  }

  function simRoute(method: string, path: string, body: unknown): RawResponse {
    if (method === "GET" && path === "/sim/clock") {
      if (clock.state === "running" && clock.sim_time) {
        // Each read moves the mock clock on by one simulated minute per 1,000x.
        clock.sim_time = addSeconds(
          clock.sim_time,
          60 * ((clock.speed ?? 1000) / 1000),
        );
      }
      return reply(200, clock);
    }
    if (method === "GET" && path === "/sim/playback")
      return reply(200, playback);
    if (method === "POST" && path === "/sim/playback") {
      const cmd = body as {
        action: string;
        from_day?: number;
        to_day?: number;
        speed?: number;
        seed?: number;
        run_id?: string;
      };
      const allowed: Record<string, string[]> = {
        start: ["idle", "done", "stopped", "failed"],
        pause: ["running"],
        resume: ["paused"],
        stop: ["running", "paused"],
      };
      if (!allowed[cmd.action]?.includes(playback.state)) {
        return reply(409, {
          error: "invalid_state",
          state: playback.state,
          message: `cannot ${cmd.action} while ${playback.state}`,
        });
      }
      if (cmd.action === "start") {
        if (cmd.from_day === undefined)
          return reply(422, { error: "validation" });
        Object.assign(playback, {
          run_id: cmd.run_id ?? "mock-run",
          seed: cmd.seed ?? 0,
          from_day: cmd.from_day,
          to_day: cmd.to_day ?? 90,
          speed: cmd.speed ?? 1000,
        });
        Object.assign(clock, {
          sim_day: cmd.from_day,
          sim_time: "00:00:00",
          speed: playback.speed,
        });
      }
      const next = {
        start: "running",
        pause: "paused",
        resume: "running",
        stop: "stopped",
      };
      playback.state = next[
        cmd.action as keyof typeof next
      ] as PlaybackState["state"];
      clock.state = playback.state;
      return reply(200, playback);
    }
    if (method === "PATCH" && path === "/sim/playback") {
      if (playback.state !== "running" && playback.state !== "paused")
        return reply(409, { error: "invalid_state", state: playback.state });
      playback.speed = (body as { speed: number }).speed;
      clock.speed = playback.speed;
      return reply(200, playback);
    }
    if (method === "GET" && path === "/sim/links") return reply(200, links);
    if (method === "POST" && path === "/sim/faults") {
      const req = body as Pick<Fault, "target" | "fault" | "params">;
      const group = links.find((l) => l.group === req.target);
      if (!group) return notFound();
      if (group.faults.some((f) => f.fault === req.fault))
        return reply(409, { error: "fault_exists" });
      const fault: Fault = {
        fault_id: `f${nextFault++}`,
        target: req.target,
        fault: req.fault,
        params: req.params,
        proxies: group.proxies,
        created_at: STAMP,
      };
      group.faults.push(fault);
      return reply(201, fault);
    }
    if (method === "POST" && path === "/sim/faults/heal-all") {
      let healed = 0;
      links.forEach((l) => {
        healed += l.faults.length;
        l.faults = [];
      });
      return reply(200, { healed });
    }
    const del = path.match(/^\/sim\/faults\/([^/]+)$/);
    if (method === "DELETE" && del) {
      const group = links.find((l) =>
        l.faults.some((f) => f.fault_id === del[1]),
      );
      if (!group) return notFound();
      group.faults = group.faults.filter((f) => f.fault_id !== del[1]);
      return reply(200, {});
    }
    const node = path.match(/^\/sim\/nodes\/([^/]+)\/(kill|start)$/);
    if (method === "POST" && node) {
      if (!cluster.nodes.some((n) => n.node_id === node[1])) return notFound();
      const target = cluster.nodes.find((n) => n.node_id === node[1])!;
      target.status = node[2] === "start" ? "up" : "down";
      return reply(200, { node_id: node[1], running: node[2] === "start" });
    }
    return notFound();
  }

  return async (method, url, body) => {
    const parsed = new URL(url, "http://mock.local");
    const path = parsed.pathname;
    const params = parsed.searchParams;
    const edge = path.match(/^\/edge\/([^/]+)\/api\/v1(\/.*)$/);
    if (edge)
      return apiRoute(
        method,
        decodeURIComponent(edge[1]),
        edge[2],
        params,
        body,
      );
    if (path.startsWith("/api/v1/"))
      return apiRoute(method, null, path.slice(7), params, body);
    if (path.startsWith("/admin/")) return adminRoute(method, path);
    if (path.startsWith("/sim/")) return simRoute(method, path, body);
    return notFound();
  };
}

export const mockTransport: Transport = createMockServer();

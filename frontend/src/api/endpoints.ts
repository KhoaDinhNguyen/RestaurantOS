// One function per route in docs/architecture.md Section 6. Paths are
// relative; the dev server forwards them (vite.config.ts).
import { qs, request } from "./client";
import type {
  AvailabilityResult,
  CheckRequest,
  ClusterEvent,
  ClusterStatus,
  Dish,
  ExceptionEntry,
  FailoverResult,
  Fault,
  FaultKind,
  IngredientUsageAnswer,
  Job,
  LinkGroup,
  MenuPublishRequest,
  MenuPublishResponse,
  MenuRow,
  NearbyAnswer,
  NodeRunState,
  Order,
  OrderRequest,
  OrderResponse,
  OrderStatus,
  PlaybackCommand,
  PlaybackState,
  QueryMode,
  RankBy,
  RegionDirectoryRow,
  RegionSummary,
  RestockResult,
  RestoreRequest,
  ScenarioInfo,
  ScenarioParam,
  SimClock,
  SnapshotInfo,
  StatusResult,
  StockRow,
  Strategy,
  TopDishesAnswer,
  Vendor,
} from "./types";

const GATEWAY = "/api/v1";

/** Region ids the dev server can reach an edge API for. */
export const REGION_IDS: readonly string[] = __ROS_REGIONS__;

/**
 * Where the `/vendors/...` routes are served: the gateway, or one region's
 * edge API. The Region view uses the edge so it keeps working while the
 * region is cut off from HQ (test T2).
 */
export type Api = { kind: "gateway" } | { kind: "edge"; regionId: string };

export const gatewayApi: Api = { kind: "gateway" };
export const edgeApi = (regionId: string): Api => ({ kind: "edge", regionId });

function base(api: Api): string {
  return api.kind === "gateway"
    ? GATEWAY
    : `/edge/${encodeURIComponent(api.regionId)}/api/v1`;
}

const seg = encodeURIComponent;

// --- orders ---------------------------------------------------------------

export const placeOrder = (body: OrderRequest) =>
  request<OrderResponse>("POST", `${GATEWAY}/orders`, {
    body,
    dataStatuses: [409],
  });

export const getOrder = (
  api: Api,
  vendorId: string,
  orderId: string,
  day?: number | null,
) =>
  request<Order>(
    "GET",
    `${base(api)}/vendors/${seg(vendorId)}/orders/${seg(orderId)}${qs({ day })}`,
  );

export const listOrders = (
  api: Api,
  vendorId: string,
  day: number | null,
  statuses: readonly OrderStatus[],
) =>
  request<Order[]>(
    "GET",
    `${base(api)}/vendors/${seg(vendorId)}/orders${qs({
      day,
      status: statuses.join(","),
    })}`,
  );

export const changeStatus = (
  api: Api,
  vendorId: string,
  orderId: string,
  eventId: string,
  toStatus: OrderStatus,
) =>
  request<StatusResult>(
    "POST",
    `${base(api)}/vendors/${seg(vendorId)}/orders/${seg(orderId)}/status`,
    { body: { event_id: eventId, to_status: toStatus }, dataStatuses: [409] },
  );

// --- menu, stock, availability ------------------------------------------

export const getMenu = (api: Api, vendorId: string) =>
  request<MenuRow[]>("GET", `${base(api)}/vendors/${seg(vendorId)}/menu`);

export const getStock = (api: Api, vendorId: string) =>
  request<StockRow[]>("GET", `${base(api)}/vendors/${seg(vendorId)}/stock`);

export const setAvailability = (
  api: Api,
  vendorId: string,
  productId: string,
  available: boolean,
) =>
  request<AvailabilityResult>(
    "PUT",
    `${base(api)}/vendors/${seg(vendorId)}/products/${seg(productId)}/availability`,
    { body: { available } },
  );

export const restock = (api: Api, vendorId: string) =>
  request<RestockResult>(
    "POST",
    `${base(api)}/vendors/${seg(vendorId)}/restock`,
  );

export const publishMenu = (body: MenuPublishRequest) =>
  request<MenuPublishResponse>("POST", `${GATEWAY}/menu/products`, { body });

// --- search -------------------------------------------------------------

export const searchVendors = (geohash: string, q: string) =>
  request<Vendor[]>("GET", `${GATEWAY}/vendors${qs({ geohash, q })}`);

export const searchDishes = (q: string, limit = 20) =>
  request<Dish[]>("GET", `${GATEWAY}/dishes${qs({ q, limit })}`);

// --- queries ------------------------------------------------------------

export interface RangeParams {
  mode: QueryMode;
  from_day: string;
  to_day: string;
  from_time: string;
  to_time: string;
}

export const nearby = (geohash: string, dishId: string, limit = 10) =>
  request<NearbyAnswer>(
    "GET",
    `${GATEWAY}/query/nearby${qs({ geohash, dish: dishId, limit })}`,
  );

export const topDishes = (range: RangeParams, rankBy: RankBy, limit: string) =>
  request<TopDishesAnswer>(
    "GET",
    `${GATEWAY}/query/top-dishes${qs({ ...range, rank_by: rankBy, limit })}`,
  );

export const ingredientUsage = (range: RangeParams, strategy: Strategy) =>
  request<IngredientUsageAnswer>(
    "GET",
    `${GATEWAY}/query/ingredient-usage${qs({
      ...range,
      strategy: range.mode === "live" ? strategy : null,
    })}`,
  );

// --- regions ------------------------------------------------------------

export const listRegions = () =>
  request<RegionDirectoryRow[]>("GET", `${GATEWAY}/regions`);

export const regionSummary = (api: Api, regionId: string, day: number) =>
  request<RegionSummary>(
    "GET",
    `${base(api)}/regions/${seg(regionId)}/summary${qs({ day })}`,
  );

// --- coordinator ----------------------------------------------------------

export const getCluster = () => request<ClusterStatus>("GET", "/admin/cluster");

export const getEvents = (limit = 20) =>
  request<ClusterEvent[]>("GET", `/admin/events${qs({ limit })}`);

export const getExceptions = (limit = 20) =>
  request<ExceptionEntry[]>("GET", `/admin/exceptions${qs({ limit })}`);

export const failover = (regionId: string) =>
  request<FailoverResult>("POST", `/admin/regions/${seg(regionId)}/failover`);

// --- control plane --------------------------------------------------------

export const getClock = () => request<SimClock>("GET", "/sim/clock");

export const getPlayback = () => request<PlaybackState>("GET", "/sim/playback");

export const commandPlayback = (body: PlaybackCommand) =>
  request<PlaybackState>("POST", "/sim/playback", { body });

export const setSpeed = (speed: number) =>
  request<PlaybackState>("PATCH", "/sim/playback", { body: { speed } });

export const getLinks = () => request<LinkGroup[]>("GET", "/sim/links");

export const injectFault = (
  target: string,
  fault: FaultKind,
  params: Record<string, number>,
) => request<Fault>("POST", "/sim/faults", { body: { target, fault, params } });

export const removeFault = (faultId: string) =>
  request<Fault>("DELETE", `/sim/faults/${seg(faultId)}`);

export const healAll = () =>
  request<{ healed: number }>("POST", "/sim/faults/heal-all");

export const killNode = (nodeId: string) =>
  request<NodeRunState>("POST", `/sim/nodes/${seg(nodeId)}/kill`);

export const startNode = (nodeId: string) =>
  request<NodeRunState>("POST", `/sim/nodes/${seg(nodeId)}/start`);

// --- control plane jobs: scenarios, snapshots, checker ----------------------
// Each POST answers 202 with a Job (or 409 `busy` naming the running one);
// GET /sim/jobs/{id} reports on it.

export const getJob = (jobId: string) =>
  request<Job>("GET", `/sim/jobs/${seg(jobId)}`);

export const listScenarios = () =>
  request<ScenarioInfo[]>("GET", "/sim/scenarios");

export const runScenario = (
  name: string,
  params: Record<string, ScenarioParam>,
) => request<Job>("POST", `/sim/scenarios/${seg(name)}`, { body: params });

export const listSnapshots = () =>
  request<{ snapshots: SnapshotInfo[] }>("GET", "/sim/snapshots");

export const saveSnapshot = (name: string) =>
  request<Job>("POST", "/sim/snapshots", { body: { name } });

export const restoreSnapshot = (name: string, options: RestoreRequest) =>
  request<Job>("POST", `/sim/snapshots/${seg(name)}/restore`, {
    body: options,
  });

export const runCheck = (body: CheckRequest) =>
  request<Job>("POST", "/sim/check", { body });

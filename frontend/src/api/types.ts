// Shapes of the HTTP API, copied from docs/architecture.md Section 6 and
// src/restaurantos/common/models.py. Ids are always strings: never turn one
// into a number. Times named *_at are wall-clock ISO strings; order_day and
// order_time are simulated time.

export type OrderStatus =
  "received" | "accepted" | "preparing" | "ready" | "picked_up" | "canceled";

/** Every status, in the order the API documents them. For display only. */
export const ORDER_STATUSES: readonly OrderStatus[] = [
  "received",
  "accepted",
  "preparing",
  "ready",
  "picked_up",
  "canceled",
];

export type RefusalReason = "out_of_stock" | "unavailable" | "unknown_product";

export type QueryMode = "live" | "hq" | "baseline";

/** Any error body the services send. */
export interface ErrorBody {
  error: string;
  region_id?: string;
  message?: string;
  state?: string;
  detail?: unknown;
  [key: string]: unknown;
}

// --- orders (models.py) -------------------------------------------------

export interface OrderItemIn {
  product_id: string;
  quantity: number;
}

export interface OrderRequest {
  channel: string;
  external_order_id: string;
  vendor_id: string;
  customer_id: string | null;
  customer_geohash: string | null;
  order_day: number;
  order_time: string;
  items: OrderItemIn[];
}

export interface OrderResponse {
  outcome: "accepted" | "refused" | null;
  duplicate: boolean;
  order_id: string | null;
  order_day: number | null;
  status: OrderStatus | null;
  menu_version: number | null;
  committed_at: string | null;
  refusal_reason: RefusalReason | null;
  queued: boolean;
  region_id: string;
  received_at: string;
  acked_at: string;
}

export interface StatusChangeRequest {
  event_id: string;
  to_status: OrderStatus;
}

export interface StatusResult {
  order_id: string;
  status: OrderStatus;
  applied: boolean;
  duplicate: boolean;
  conflict: boolean;
}

export interface OrderLine {
  line_no: number;
  product_id: string;
  name: string;
  quantity: number;
  price_units: number;
}

export interface Order {
  order_id: string;
  order_day: number;
  order_time: string;
  vendor_id: string;
  customer_id: string | null;
  channel: string;
  status: OrderStatus;
  status_changed_at: string;
  menu_version: number;
  received_at: string | null;
  committed_at: string;
  items: OrderLine[];
}

// --- restaurants, menus, stock (Section 6.1) ----------------------------

export interface Vendor {
  vendor_id: string;
  chain_id: string | null;
  geohash: string;
  primary_cuisine: string | null;
}

export interface MenuRow {
  vendor_id: string;
  product_id: string;
  name: string;
  price_units: number;
  menu_version: number;
  dish_id: string;
  available: boolean;
  reason: "manual" | "stock" | null;
}

export interface AvailabilityResult {
  vendor_id: string;
  product_id: string;
  available: boolean;
  reason: "manual" | "stock" | null;
}

export interface StockRow {
  item_kind: "ingredient" | "dish";
  item_id: string;
  name: string;
  unit: string | null;
  on_hand: number;
  par_level: number;
  updated_at: string;
}

export interface RestockResult {
  vendor_id: string;
  region_id: string;
  counters_restocked: number;
  products_back_on: number;
}

export interface Dish {
  dish_id: string;
  dish_name: string | null;
  example_name: string | null;
  scope: string;
  n_vendors: number;
}

export interface MenuProductInput {
  vendor_id: string;
  product_id?: string;
  name: string;
  price_units: number;
  dish_id?: string;
  multiplier?: number;
}

export interface MenuPublishRequest {
  note: string | null;
  products: MenuProductInput[];
}

export interface MenuPublishResponse {
  menu_version: number;
  products: {
    vendor_id: string;
    product_id: string;
    region_id: string;
    created: boolean;
  }[];
}

// --- queries (Section 6.1) ----------------------------------------------

export interface QueryMeta {
  mode: QueryMode;
  nodes_contacted: string[];
  missing_regions: string[];
  rows_transferred: number;
  data_as_of: string | null;
  elapsed_ms: number;
}

export interface NearbyRow {
  vendor_id: string;
  chain_id: string | null;
  primary_cuisine: string | null;
  geohash: string;
  distance_m: number;
  product_id: string;
  name: string;
  price_units: number;
  multiplier: number;
  region_id: string;
}

export interface NearbyAnswer {
  geohash: string;
  dish_id: string;
  dish_name: string | null;
  example_name: string | null;
  limit: number;
  results: NearbyRow[];
  meta: QueryMeta;
}

export type RankBy = "units" | "quantity" | "order_lines" | "sales_units";

export interface TopDishRow {
  dish_id: string;
  dish_name: string | null;
  example_name: string | null;
  units: number;
  quantity: number;
  order_lines: number;
  sales_units: number;
}

export interface TopDishesAnswer {
  from_day: number;
  to_day: number;
  from_time: string | null;
  to_time: string | null;
  limit: number;
  rank_by: RankBy;
  results: TopDishRow[];
  meta: QueryMeta;
}

export type Strategy = "A" | "B" | "C";

export interface IngredientUsageAnswer {
  from_day: number;
  to_day: number;
  from_time: string | null;
  to_time: string | null;
  strategy: Strategy | null;
  results: { ingredient_id: string; amount: number; unit: string | null }[];
  meta: QueryMeta;
}

// --- regions (Section 6.1) ----------------------------------------------

export interface RegionDirectoryRow {
  region_id: string;
  epoch: number;
  primary_node: string;
  standby_node: string;
  menu_version: number;
  updated_at: string;
  geohashes: string[];
}

export interface RegionSummary {
  region_id: string;
  day: number;
  epoch: number;
  menu_version: number;
  vendors: number;
  orders: Record<OrderStatus | "total", number>;
  refused: number;
  sales_units: number;
  products_unavailable: number;
  outbox_backlog: number;
  data_as_of: string;
}

// --- coordinator (Section 6.2) ------------------------------------------

export type NodeHealth = "up" | "suspect" | "down";

export interface NodeStatus {
  node_id: string;
  region_id: string | null;
  role: "primary" | "standby";
  status: NodeHealth;
  last_heartbeat_at: string | null;
}

export interface RegionStatus {
  region_id: string;
  epoch: number;
  menu_version: number;
  primary_node: string;
  standby_node: string;
  replication: "sync" | "async";
  orders_per_s: number | null;
  standby_lag_s: number | null;
  hq_lag_s: number | null;
  outbox_backlog: number | null;
  holding_queue: number | null;
  figures_as_of: string | null;
}

export interface ClusterStatus {
  as_of: string;
  nodes: NodeStatus[];
  regions: RegionStatus[];
}

export interface ClusterEvent {
  event_id: number;
  occurred_at: string;
  kind: string;
  region_id: string | null;
  node_id: string | null;
  detail: Record<string, unknown>;
}

export interface ExceptionEntry {
  seq: number;
  region_id: string;
  order_day: number;
  order_id: string;
  kind: string;
  current_status: OrderStatus;
  current_changed_at: string;
  attempted_status: OrderStatus;
  attempted_at: string;
  hq_applied_at: string;
}

export interface FailoverResult {
  region_id: string;
  epoch: number;
  primary_node: string;
  standby_node: string;
  event_id: number;
}

// --- control plane (Section 6.3) ----------------------------------------

export type PlaybackStateName =
  "idle" | "running" | "paused" | "stopping" | "done" | "stopped" | "failed";

export interface SimClock {
  state: PlaybackStateName;
  sim_day: number | null;
  sim_time: string | null;
  speed: number | null;
}

export interface PlaybackState {
  state: PlaybackStateName;
  run_id: string | null;
  seed: number | null;
  from_day: number | null;
  to_day: number | null;
  speed: number | null;
  [counter: string]: string | number | null;
}

export interface PlaybackCommand {
  action: "start" | "pause" | "resume" | "stop";
  from_day?: number;
  to_day?: number;
  speed?: number;
  seed?: number;
  run_id?: string;
}

export type FaultKind = "cut" | "silence" | "latency" | "reset";

export interface Fault {
  fault_id: string;
  target: string;
  fault: FaultKind;
  params: Record<string, number>;
  proxies: string[];
  created_at: string;
}

export interface LinkGroup {
  group: string;
  proxies: string[];
  faults: Fault[];
}

export interface NodeRunState {
  node_id: string;
  running: boolean;
}

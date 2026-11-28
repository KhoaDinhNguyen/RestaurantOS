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
  /** Null while the region runs without a standby. */
  standby_node: string | null;
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
  /** Null while the region runs without a standby (after T1, until rejoin). */
  standby_node: string | null;
  replication: "sync" | "async" | null;
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
  /** The dispatch frontier: the simulated instant of the last item sent. */
  dispatched_sim_day?: number | null;
  dispatched_sim_time?: string | null;
  /** The earliest order pulled from the stream but not sent yet. */
  held_sim_day?: number | null;
  held_sim_time?: string | null;
  /** The earliest item of any kind still waiting to go out. */
  next_due_sim_day?: number | null;
  next_due_sim_time?: string | null;
  /** What every run on this playback process shares (Section 6.3). */
  engine?: Record<string, unknown>;
  /** The counters: orders_sent, events_acked, restocks_failed, inflight, ... */
  [field: string]: unknown;
}

export interface PlaybackCommand {
  action: "start" | "pause" | "resume" | "stop";
  from_day?: number;
  to_day?: number;
  speed?: number;
  seed?: number;
  run_id?: string;
  /** Share of orders sent twice, in [0, 1]. */
  duplicate_rate?: number;
  shuffle?: boolean;
}

export type FaultKind = "cut" | "silence" | "latency" | "reset";

export interface Fault {
  fault_id: string;
  target: string;
  fault: FaultKind;
  params: Record<string, number>;
  proxies: string[];
  created_at: string;
  /** On a cut's create answer: proxies that were disabled before it came. */
  already_disabled?: string[];
  /** On a cut's heal answer: proxies it left disabled (not its to enable). */
  left_disabled?: string[];
}

export interface StrayToxic {
  proxy: string;
  name: string | null;
  type: string | null;
  stream: string | null;
}

export interface LinkGroup {
  group: string;
  proxies: string[];
  faults: Fault[];
  /** Proxies a live cut holds that were already disabled when it came. */
  preexisting_disabled?: string[];
  /** What Toxiproxy holds that no known fault explains. */
  unexplained?: {
    disabled: string[];
    toxics: StrayToxic[];
    missing: string[];
  };
}

export interface NodeRunState {
  node_id: string;
  running: boolean;
}

// --- jobs: scenarios, snapshots, restore, checker (Section 6.3) ------------

export type JobKind = "scenario" | "snapshot" | "restore" | "check";

export interface Job {
  job_id: string;
  kind: JobKind;
  name: string | null;
  state: "running" | "done" | "failed";
  started_at: string;
  finished_at: string | null;
  /** The job's answer once done: a ScenarioResult, a CheckResult, ... */
  result: unknown;
  error: string | null;
  /** A short line on where a running job is, e.g. a scenario's phase. */
  progress: string | null;
}

export type ScenarioParam = string | number | boolean | null;

export interface ScenarioInfo {
  name: string;
  summary: string;
  /** Every parameter the scenario takes, with its default. */
  params: Record<string, ScenarioParam>;
}

export interface ScenarioCondition {
  name: string;
  passed: boolean;
  detail: string | null;
}

export interface ScenarioResult {
  scenario: string;
  summary: string;
  passed: boolean;
  error: string | null;
  region_id: string;
  params: Record<string, ScenarioParam>;
  run_id: string | null;
  measurements: Record<string, unknown>;
  conditions: ScenarioCondition[];
  check: CheckResult | { passed: false; error: string } | null;
  warnings: string[];
  next_steps: string[];
  [field: string]: unknown;
}

export interface CheckResult {
  passed: boolean;
  acked: number;
  stored: number;
  hq: number;
  lost_at_failover: { count: number; order_ids: string[] };
  duplicates: Record<string, unknown>[];
  unaccounted: {
    external_order_id: string;
    reason: string;
    region_id: string | null;
  }[];
  stored_unacked?: { count: number; order_ids: string[]; allowed: boolean };
  runs?: string[];
  days?: number[];
}

export interface CheckRequest {
  runs: string[];
  allow_unacked: boolean;
}

export interface SnapshotInfo {
  name: string;
  created_at: string | null;
  sim_day: number | null;
  sim_time: string | null;
  regions: string[] | null;
}

export type RestoreServices = "auto" | "host" | "containers" | "none";

export interface RestoreRequest {
  replace_cluster_file: boolean;
  services: RestoreServices;
}

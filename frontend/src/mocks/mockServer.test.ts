import { describe, expect, it } from "vitest";
import type { OrderRequest } from "../api/types";
import { createMockServer } from "./mockServer";

const order = (over: Partial<OrderRequest>): OrderRequest => ({
  channel: "web",
  external_order_id: "e-1",
  vendor_id: "00a1f3c2",
  customer_id: null,
  customer_geohash: "w21z7",
  order_day: 20,
  order_time: "12:00:00",
  items: [{ product_id: "0012ab34cd56", quantity: 2 }],
  ...over,
});

describe("mock server", () => {
  it("answers the four order outcomes", async () => {
    const server = createMockServer();
    const post = (body: OrderRequest) => server("POST", "/api/v1/orders", body);

    const stored = await post(order({}));
    expect(stored.status).toBe(201);

    const again = await post(order({}));
    expect(again.status).toBe(200);
    expect(again.body).toMatchObject({ duplicate: true });

    const parked = await post(
      order({
        external_order_id: "e-2",
        vendor_id: "9e02b5f4",
        items: [{ product_id: "e5f6a7b8c9d0", quantity: 1 }],
      }),
    );
    expect(parked.status).toBe(202);
    expect(parked.body).toMatchObject({ queued: true, order_id: null });

    const refused = await post(
      order({
        external_order_id: "e-3",
        items: [{ product_id: "7a8b9c0d1e2f", quantity: 1 }],
      }),
    );
    expect(refused.status).toBe(409);
    expect(refused.body).toMatchObject({ refusal_reason: "unavailable" });

    // Like the gateway, an order with no items is refused before anything,
    // even for a region that cannot be reached.
    const empty = await post(
      order({ external_order_id: "e-4", vendor_id: "9e02b5f4", items: [] }),
    );
    expect(empty.status).toBe(422);
  });

  it("moves the clock into the next day at midnight", async () => {
    const server = createMockServer();
    await server("POST", "/sim/playback", {
      action: "start",
      from_day: 20,
      speed: 1000,
    });
    let clock = (await server("GET", "/sim/clock")).body as {
      sim_day: number;
    };
    // one simulated minute per read at 1000x: a day is 1,440 reads
    for (let i = 0; i < 1440; i++)
      clock = (await server("GET", "/sim/clock")).body as { sim_day: number };
    expect(clock.sim_day).toBe(21);
  });

  it("serves a cut-off region on its edge API but not through the gateway", async () => {
    const server = createMockServer();
    const gateway = await server("GET", "/api/v1/vendors/9e02b5f4/menu");
    expect(gateway).toEqual({
      status: 503,
      body: { error: "region_unreachable", region_id: "r2" },
    });
    const edge = await server("GET", "/edge/r2/api/v1/vendors/9e02b5f4/menu");
    expect(edge.status).toBe(200);
  });

  it("runs one control-plane job at a time and reports on it", async () => {
    const server = createMockServer();
    const first = await server("POST", "/sim/scenarios/T2", {
      region_id: "r1",
    });
    expect(first.status).toBe(202);
    expect(first.body).toMatchObject({ job_id: "j1", state: "running" });

    const second = await server("POST", "/sim/check", { runs: ["a"] });
    expect(second).toMatchObject({
      status: 409,
      body: { error: "busy", job_id: "j1" },
    });

    let job = (await server("GET", "/sim/jobs/j1")).body as { state: string };
    while (job.state === "running")
      job = (await server("GET", "/sim/jobs/j1")).body as { state: string };
    expect(job).toMatchObject({
      state: "done",
      result: { scenario: "T2", passed: true },
    });

    const typo = await server("POST", "/sim/scenarios/T2", { kill_after: 1 });
    expect(typo).toMatchObject({
      status: 422,
      body: { error: "invalid_params" },
    });
  });
});

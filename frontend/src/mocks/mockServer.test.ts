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
});

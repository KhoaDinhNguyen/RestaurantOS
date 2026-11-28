import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchTransport, qs, request, setTransport } from "./client";

afterEach(() => {
  setTransport(fetchTransport);
  vi.unstubAllGlobals();
});

describe("request", () => {
  it("returns a 503 as an error without throwing", async () => {
    setTransport(async () => ({
      status: 503,
      body: { error: "region_unreachable", region_id: "r1" },
    }));
    const result = await request("GET", "/api/v1/vendors/x/menu");
    expect(result.status).toBe(503);
    expect(result.data).toBeNull();
    expect(result.error).toEqual({
      error: "region_unreachable",
      region_id: "r1",
    });
  });

  it("treats a listed status as a normal body", async () => {
    setTransport(async () => ({
      status: 409,
      body: { refusal_reason: "out_of_stock" },
    }));
    const result = await request("POST", "/api/v1/orders", {
      dataStatuses: [409],
    });
    expect(result.data).toEqual({ refusal_reason: "out_of_stock" });
    expect(result.error).toBeNull();
  });

  it("reports no answer as status 0", async () => {
    setTransport(async () => {
      throw new TypeError("Failed to fetch");
    });
    const result = await request("GET", "/admin/cluster");
    expect(result.status).toBe(0);
    expect(result.error?.error).toBe("network");
  });

  it("keeps ids that look like numbers as text", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response('[{"vendor_id": "00123", "product_id": "0042"}]', {
            status: 200,
          }),
      ),
    );
    const result = await request<{ vendor_id: string; product_id: string }[]>(
      "GET",
      "/api/v1/vendors?geohash=w21z7",
    );
    expect(result.data?.[0].vendor_id).toBe("00123");
    expect(result.data?.[0].product_id).toBe("0042");
  });
});

describe("qs", () => {
  it("leaves out empty values and keeps ids as written", () => {
    expect(qs({ day: 14, status: "", vendor: "007", q: null })).toBe(
      "?day=14&vendor=007",
    );
    expect(qs({})).toBe("");
  });
});

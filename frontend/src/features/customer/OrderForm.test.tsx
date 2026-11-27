import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchTransport, setTransport, type Transport } from "../../api/client";
import type { OrderRequest } from "../../api/types";
import { SimClockContext, type SimClockValue } from "../clock/context";
import { createMockServer } from "../../mocks/mockServer";
import { OrderForm } from "./OrderForm";

const clock: SimClockValue = {
  clock: null,
  day: 20,
  time: "12:03:29",
  fromClock: true,
  pickedDay: "",
  pickedTime: "",
  setPickedDay: () => {},
  setPickedTime: () => {},
};

afterEach(() => setTransport(fetchTransport));

function renderForm(vendorId: string) {
  const server = createMockServer();
  const orders: OrderRequest[] = [];
  const spy: Transport = async (method, url, body) => {
    if (method === "POST" && url.endsWith("/orders"))
      orders.push(body as OrderRequest);
    return server(method, url, body);
  };
  setTransport(spy);
  const onPlaced = vi.fn();
  render(
    <SimClockContext.Provider value={clock}>
      <OrderForm vendorId={vendorId} geohash="w21zk" onPlaced={onPlaced} />
    </SimClockContext.Provider>,
  );
  return { orders, onPlaced };
}

describe("OrderForm", () => {
  it("sends the simulated day and time and shows the stored order", async () => {
    const { orders, onPlaced } = renderForm("00a1f3c2");
    const user = userEvent.setup();
    await user.type(
      await screen.findByLabelText("quantity of Noodle soup"),
      "2",
    );
    await user.click(screen.getByRole("button", { name: "Place order" }));

    expect(await screen.findByText(/201: stored in r1/)).toBeInTheDocument();
    expect(orders).toHaveLength(1);
    expect(orders[0]).toMatchObject({
      channel: "web",
      vendor_id: "00a1f3c2",
      order_day: 20,
      order_time: "12:03:29",
      items: [{ product_id: "0012ab34cd56", quantity: 2 }],
    });
    expect(onPlaced).toHaveBeenCalledOnce();
  });

  it("shows a parked order and asks again with the same order id", async () => {
    const { orders } = renderForm("9e02b5f4");
    const user = userEvent.setup();
    // The gateway cannot reach r2, so even the menu answers 503 ...
    expect(
      await screen.findByText(/503: region r2 cannot be reached/),
    ).toBeInTheDocument();
    // ... but an order is still accepted for later delivery.
    setTransport(async (method, _url, body) => {
      if (method === "POST") orders.push(body as OrderRequest);
      return method === "POST"
        ? {
            status: 202,
            body: { queued: true, region_id: "r2", order_id: null },
          }
        : {
            status: 503,
            body: { error: "region_unreachable", region_id: "r2" },
          };
    });
    await user.click(screen.getByRole("button", { name: "Place order" }));
    expect(await screen.findByText(/202: region r2/)).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /Ask again/ }));
    expect(orders).toHaveLength(2);
    expect(orders[1].external_order_id).toBe(orders[0].external_order_id);
  });
});

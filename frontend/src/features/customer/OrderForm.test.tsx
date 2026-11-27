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
    // ... and with no item there is nothing to send.
    const place = screen.getByRole("button", { name: "Place order" });
    expect(place).toBeDisabled();

    // An item typed by product id is still accepted for later delivery.
    await user.type(screen.getByLabelText("product id"), "e5f6a7b8c9d0");
    await user.type(screen.getByLabelText("quantity"), "1");
    await user.click(place);
    expect(await screen.findByText(/202: region r2/)).toBeInTheDocument();
    expect(orders[0].items).toEqual([
      { product_id: "e5f6a7b8c9d0", quantity: 1 },
    ]);

    await user.click(screen.getByRole("button", { name: /Ask again/ }));
    await screen.findByText(/202: region r2/);
    expect(orders).toHaveLength(2);
    expect(orders[1]).toEqual(orders[0]);
  });

  it("keeps New order disabled while the order is on its way", async () => {
    let answer: (() => void) | undefined;
    const server = createMockServer();
    setTransport(async (method, url, body) => {
      if (method === "POST")
        await new Promise<void>((resolve) => (answer = resolve));
      return server(method, url, body);
    });
    render(
      <SimClockContext.Provider value={clock}>
        <OrderForm vendorId="00a1f3c2" geohash="w21z7" onPlaced={vi.fn()} />
      </SimClockContext.Provider>,
    );
    const user = userEvent.setup();
    await user.type(
      await screen.findByLabelText("quantity of Noodle soup"),
      "1",
    );
    await user.click(screen.getByRole("button", { name: "Place order" }));
    expect(screen.getByRole("button", { name: "New order" })).toBeDisabled();
    answer?.();
    expect(await screen.findByText(/201: stored in r1/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "New order" })).toBeEnabled();
  });
});

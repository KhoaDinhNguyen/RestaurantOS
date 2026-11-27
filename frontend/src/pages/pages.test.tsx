import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createMemoryRouter, RouterProvider } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fetchTransport, setTransport } from "../api/client";
import { createMockServer } from "../mocks/mockServer";
import { routes } from "../routes";

beforeEach(() => setTransport(createMockServer()));
afterEach(() => setTransport(fetchTransport));

const open = (path: string) =>
  render(
    <RouterProvider
      router={createMemoryRouter(routes, { initialEntries: [path] })}
    />,
  );

describe("the four views render against the mock server", () => {
  it("HQ shows node health and the failover button", async () => {
    open("/hq");
    expect(await screen.findAllByText("r2-primary")).not.toHaveLength(0);
    expect(screen.getByText("suspect")).toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: "Failover" })).toHaveLength(2);
  });

  it("Region reads the edge API, which works for a cut-off region", async () => {
    open("/region?region=r2&vendor=9e02b5f4");
    expect(await screen.findByText("Restock to par")).toBeInTheDocument();
    expect(await screen.findByText("Noodle soup")).toBeInTheDocument();
    expect(screen.queryByText(/503/)).not.toBeInTheDocument();
  });

  it("Customer shows the searches", async () => {
    open("/customer");
    expect(
      await screen.findByRole("heading", { name: "Restaurants in my cell" }),
    ).toBeInTheDocument();
  });

  it("each view keeps its state when you switch tabs", async () => {
    const user = userEvent.setup();
    open("/customer");
    const cell = await screen.findByLabelText("my geohash cell");
    await user.clear(cell);
    await user.type(cell, "w21zk");
    await user.click(screen.getByRole("button", { name: "Search w21zk" }));
    expect(await screen.findByText("9e02b5f4")).toBeVisible();

    await user.click(screen.getByRole("link", { name: "Region" }));
    await user.selectOptions(screen.getByLabelText("region"), "r2");
    await user.type(
      screen.getByLabelText("restaurant (vendor id)"),
      "9e02b5f4",
    );
    await user.click(screen.getByRole("button", { name: "Open" }));
    expect(
      await screen.findByRole("heading", { name: "Kitchen board: 9e02b5f4" }),
    ).toBeVisible();

    await user.click(screen.getByRole("link", { name: "Customer" }));
    expect(screen.getByLabelText("my geohash cell")).toHaveValue("w21zk");
    expect(screen.getByText("9e02b5f4")).toBeVisible();

    await user.click(screen.getByRole("link", { name: "Region" }));
    expect(
      screen.getByRole("heading", { name: "Kitchen board: 9e02b5f4" }),
    ).toBeVisible();
    expect(screen.getByLabelText("region")).toHaveValue("r2");
  });

  it("a hidden view stops polling", async () => {
    const calls: string[] = [];
    const server = createMockServer();
    setTransport(async (method, url, body) => {
      calls.push(url);
      return server(method, url, body);
    });
    open("/ops");
    await screen.findByRole("option", { name: "link:r2<->hq" });
    // HQ is hidden: its events and exceptions are never asked for.
    expect(calls.some((u) => u.startsWith("/admin/events"))).toBe(false);
    expect(calls.some((u) => u.startsWith("/sim/links"))).toBe(true);
  });

  it("Ops runs a scenario as a job and shows its verdict", async () => {
    const confirm = vi.spyOn(window, "confirm").mockReturnValue(true);
    const user = userEvent.setup();
    open("/ops");
    await user.selectOptions(await screen.findByLabelText("scenario"), "T2");
    await user.click(screen.getByRole("button", { name: "Run T2" }));
    expect(confirm).toHaveBeenCalled();
    // the progress line while it runs, then the verdict
    expect(await screen.findByText("r1 broken: 25 s left")).toBeInTheDocument();
    expect(
      await screen.findAllByText("PASSED", {}, { timeout: 5000 }),
    ).not.toHaveLength(0);
    expect(screen.getByText("failover_s")).toBeInTheDocument();
    confirm.mockRestore();
  }, 10000);

  it("Ops lists the snapshots", async () => {
    open("/ops");
    expect(await screen.findByText("before-t2")).toBeInTheDocument();
    expect(screen.getByText("day 20 · 00:30:00")).toBeInTheDocument();
  });

  it("Ops shows the link groups and their faults", async () => {
    open("/ops");
    expect(
      await screen.findByRole("option", { name: "link:r2<->hq" }),
    ).toBeInTheDocument();
    expect(
      screen.getByText("f1 {} since 2026-11-23T09:59:40Z"),
    ).toBeInTheDocument();
  });
});

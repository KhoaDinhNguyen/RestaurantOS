import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { QueryMetaView } from "./QueryMetaView";

describe("QueryMetaView", () => {
  it("shows mode, freshness, nodes, missing regions, rows and time", () => {
    render(
      <QueryMetaView
        meta={{
          mode: "live",
          nodes_contacted: ["r1-primary", "r3-primary"],
          missing_regions: ["r2"],
          rows_transferred: 42,
          data_as_of: "2026-11-23T10:00:00Z",
          elapsed_ms: 12.5,
        }}
      />,
    );
    expect(screen.getByText("live")).toBeInTheDocument();
    expect(screen.getByText("2026-11-23T10:00:00Z")).toBeInTheDocument();
    expect(screen.getByText("r1-primary, r3-primary")).toBeInTheDocument();
    expect(screen.getByText("r2")).toBeInTheDocument();
    expect(screen.getByText("42")).toBeInTheDocument();
    expect(screen.getByText("12.5 ms")).toBeInTheDocument();
  });
});

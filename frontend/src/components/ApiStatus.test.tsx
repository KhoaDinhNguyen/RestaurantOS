import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ApiStatus } from "./ApiStatus";

const failed = (status: number, body: unknown) => ({
  status,
  data: null,
  error: { error: "http_error", detail: body },
  elapsedMs: 1,
});

describe("ApiStatus", () => {
  it("explains the control plane's origin guard", () => {
    render(
      <ApiStatus
        result={failed(403, { detail: "cross-origin request rejected" })}
      />,
    );
    expect(screen.getByRole("alert")).toHaveTextContent(
      "ROS_SIM_CONTROL_ALLOWED_ORIGINS",
    );
  });

  it("shows FastAPI's detail text", () => {
    render(
      <ApiStatus result={failed(422, { detail: "start needs from_day" })} />,
    );
    expect(screen.getByRole("alert")).toHaveTextContent(
      "422: start needs from_day",
    );
  });
});

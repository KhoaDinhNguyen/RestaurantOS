import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";
import type { ApiResult } from "../api/client";
import { ViewActiveContext } from "../pages/layout/viewActiveContext";
import { usePolling } from "./usePolling";

const answer = (data: string): ApiResult<string> => ({
  status: 200,
  data,
  error: null,
  elapsedMs: 1,
});

/** A one-off load per key whose answers the test hands out by hand. */
function setup() {
  const calls: { key: string; resolve: (r: ApiResult<string>) => void }[] = [];
  const hook = renderHook(
    ({ k }: { k: string }) =>
      usePolling(
        k,
        () =>
          new Promise<ApiResult<string>>((resolve) =>
            calls.push({ key: k, resolve }),
          ),
        0,
      ),
    { initialProps: { k: "A" } },
  );
  return { ...hook, calls };
}

describe("usePolling (one-off)", () => {
  it("asks again for a key that comes back while another was being loaded", async () => {
    const { result, rerender, calls } = setup();
    await act(async () => calls[0].resolve(answer("A1")));
    expect(result.current.result?.data).toBe("A1");

    rerender({ k: "B" }); // B is asked for, and never answers
    rerender({ k: "A" }); // back to A before B is in
    expect(calls.map((c) => c.key)).toEqual(["A", "B", "A"]);
    await act(async () => calls[2].resolve(answer("A2")));
    expect(result.current.result?.data).toBe("A2");
  });

  it("does not repeat a finished load when only the view comes back", async () => {
    const calls: string[] = [];
    let active = true;
    const { rerender } = renderHook(
      () =>
        usePolling(
          "A",
          async () => {
            calls.push("A");
            return answer("A1");
          },
          0,
        ),
      {
        wrapper: ({ children }: { children: ReactNode }) => (
          <ViewActiveContext.Provider value={active}>
            {children}
          </ViewActiveContext.Provider>
        ),
      },
    );
    await act(async () => {});
    active = false;
    rerender();
    active = true;
    rerender();
    await act(async () => {});
    expect(calls).toEqual(["A"]);
  });
});

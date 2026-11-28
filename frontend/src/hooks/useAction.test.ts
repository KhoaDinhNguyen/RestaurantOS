import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { ApiResult } from "../api/client";
import { useAction } from "./useAction";

const answer = (status: number): ApiResult<number> => ({
  status,
  data: status,
  error: null,
  elapsedMs: 1,
});

describe("useAction", () => {
  it("stays pending until every call is back, and keeps the latest call's answer", async () => {
    const waiting: ((value: ApiResult<number>) => void)[] = [];
    const call = () =>
      new Promise<ApiResult<number>>((resolve) => waiting.push(resolve));
    const { result } = renderHook(() => useAction(call));

    let first!: Promise<unknown>;
    let second!: Promise<unknown>;
    act(() => {
      first = result.current.run();
      second = result.current.run();
    });
    expect(result.current.pending).toBe(true);

    // the second (newer) call answers first
    await act(async () => {
      waiting[1](answer(202));
      await second;
    });
    expect(result.current.pending).toBe(true);
    expect(result.current.result?.status).toBe(202);

    // the older call answering last does not replace it
    await act(async () => {
      waiting[0](answer(200));
      await first;
    });
    expect(result.current.pending).toBe(false);
    expect(result.current.result?.status).toBe(202);
  });
});

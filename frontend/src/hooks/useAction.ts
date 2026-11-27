import { useCallback, useRef, useState } from "react";
import type { ApiResult } from "../api/client";

export interface Action<A extends unknown[], T> {
  run: (...args: A) => Promise<ApiResult<T>>;
  /** True while any call is still waiting for its answer. */
  pending: boolean;
  /** The answer to the most recent call (an older call finishing later never replaces it). */
  result: ApiResult<T> | null;
  reset: () => void;
}

/** Wraps one write call (POST, PUT, ...) and remembers its last answer. */
export function useAction<A extends unknown[], T>(
  call: (...args: A) => Promise<ApiResult<T>>,
): Action<A, T> {
  const [inFlight, setInFlight] = useState(0);
  const [result, setResult] = useState<ApiResult<T> | null>(null);
  /** Numbers each call, so only the latest one may set `result`. */
  const latest = useRef(0);

  const run = useCallback(
    async (...args: A) => {
      const id = ++latest.current;
      setInFlight((n) => n + 1);
      try {
        const answer = await call(...args);
        if (id === latest.current) setResult(answer);
        return answer;
      } finally {
        setInFlight((n) => n - 1);
      }
    },
    [call],
  );
  const reset = useCallback(() => {
    latest.current += 1;
    setResult(null);
  }, []);
  return { run, pending: inFlight > 0, result, reset };
}

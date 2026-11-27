import { useCallback, useState } from "react";
import type { ApiResult } from "../api/client";

export interface Action<A extends unknown[], T> {
  run: (...args: A) => Promise<ApiResult<T>>;
  pending: boolean;
  /** The answer to the last call. */
  result: ApiResult<T> | null;
  reset: () => void;
}

/** Wraps one write call (POST, PUT, ...) and remembers its last answer. */
export function useAction<A extends unknown[], T>(
  call: (...args: A) => Promise<ApiResult<T>>,
): Action<A, T> {
  const [pending, setPending] = useState(false);
  const [result, setResult] = useState<ApiResult<T> | null>(null);

  const run = useCallback(
    async (...args: A) => {
      setPending(true);
      const answer = await call(...args);
      setResult(answer);
      setPending(false);
      return answer;
    },
    [call],
  );
  const reset = useCallback(() => setResult(null), []);
  return { run, pending, result, reset };
}

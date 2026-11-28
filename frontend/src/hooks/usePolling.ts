import { useCallback, useEffect, useRef, useState } from "react";
import type { ApiResult } from "../api/client";
import { useViewActive } from "./useViewActive";

export interface Polled<T> {
  /** The latest answer for the current key, or null before the first one. */
  result: ApiResult<T> | null;
  loading: boolean;
  /** Ask again now, without waiting for the next tick. */
  refresh: () => void;
}

/**
 * Calls `load` now and then every `intervalMs` (0 = once), and keeps the
 * latest answer. `key` names what is being loaded: when it changes the old
 * answer is dropped and loading starts over; `null` pauses loading.
 * The next call starts only after the previous answer came back.
 *
 * While the view is hidden (another tab is open) nothing is loaded and the
 * last answer stays. Coming back, a polled load starts again at once; a
 * one-off load (`intervalMs` 0) is not repeated, so a query result stays
 * exactly as it was left.
 */
export function usePolling<T>(
  key: string | null,
  load: () => Promise<ApiResult<T>>,
  intervalMs = 2000,
): Polled<T> {
  const active = useViewActive();
  const loadRef = useRef(load);
  useEffect(() => {
    loadRef.current = load;
  });

  const [state, setState] = useState<{
    key: string | null;
    result: ApiResult<T> | null;
  }>({ key: null, result: null });
  const [tick, setTick] = useState(0);
  /** What the last finished load was for, so a one-off load is not repeated. */
  const loaded = useRef<{ key: string; tick: number } | null>(null);

  useEffect(() => {
    if (key === null || !active) return;
    if (
      intervalMs === 0 &&
      loaded.current?.key === key &&
      loaded.current.tick === tick
    )
      return;
    // A new load is asked for: the last finished one no longer stands for
    // what is shown. Without this, going A, then B, then back to A before B
    // answers would find "A loaded" and skip the load.
    loaded.current = null;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const run = async () => {
      const result = await loadRef.current();
      if (stopped) return;
      loaded.current = { key, tick };
      setState({ key, result });
      if (intervalMs > 0) timer = setTimeout(run, intervalMs);
    };
    void run();
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  }, [key, intervalMs, tick, active]);

  const refresh = useCallback(() => setTick((t) => t + 1), []);
  const result = state.key === key ? state.result : null;
  return { result, loading: key !== null && result === null, refresh };
}

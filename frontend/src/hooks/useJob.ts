import { useEffect, useState } from "react";
import type { ApiResult } from "../api/client";
import { getJob } from "../api/endpoints";
import type { Job } from "../api/types";
import { useViewActive } from "./useViewActive";

/**
 * Follows one control-plane job (`GET /sim/jobs/{id}`): asks every
 * `intervalMs` while it runs, and stops once it is done or failed. A call
 * that got no answer, or a 5xx, is asked again; a 404 (the control process
 * restarted and forgot the job) is not. `null` follows nothing.
 */
export function useJob(
  jobId: string | null,
  intervalMs = 1000,
): ApiResult<Job> | null {
  const active = useViewActive();
  const [state, setState] = useState<{
    id: string | null;
    result: ApiResult<Job> | null;
  }>({ id: null, result: null });

  const result = state.id === jobId ? state.result : null;
  const settled =
    result !== null &&
    (result.data ? result.data.state !== "running" : result.status === 404);

  useEffect(() => {
    if (jobId === null || !active || settled) return;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const run = async () => {
      const answer = await getJob(jobId);
      if (stopped) return;
      setState({ id: jobId, result: answer });
      const again =
        answer.data?.state === "running" ||
        answer.status === 0 ||
        answer.status >= 500;
      if (again) timer = setTimeout(run, intervalMs);
    };
    void run();
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  }, [jobId, active, settled, intervalMs]);

  return result;
}

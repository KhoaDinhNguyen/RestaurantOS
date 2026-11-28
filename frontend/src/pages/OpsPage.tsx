import { useCallback, useState } from "react";
import type { ApiResult } from "../api/client";
import type { Job } from "../api/types";
import { ClusterPanel } from "../features/cluster/ClusterPanel";
import { CheckPanel } from "../features/ops/CheckPanel";
import { FaultsPanel } from "../features/ops/FaultsPanel";
import { JobPanel } from "../features/ops/JobPanel";
import { NodesPanel } from "../features/ops/NodesPanel";
import { PlaybackPanel } from "../features/ops/PlaybackPanel";
import { ScenariosPanel } from "../features/ops/ScenariosPanel";
import { SnapshotsPanel } from "../features/ops/SnapshotsPanel";
import { useJob } from "../hooks/useJob";
import { PageLayout } from "./layout/PageLayout";

/**
 * The operator, outside the system: playback, faults, nodes, and the jobs
 * (scenarios, snapshots, restore, checker) that the control plane runs one
 * at a time.
 */
export function OpsPage() {
  const [jobId, setJobId] = useState<string | null>(null);
  const job = useJob(jobId);
  const busy = job?.data?.state === "running";

  // A 202 names the new job; a 409 `busy` names the one already running,
  // which is followed instead.
  const onJob = useCallback((answer: ApiResult<Job>) => {
    if (answer.data) setJobId(answer.data.job_id);
    else if (
      answer.status === 409 &&
      answer.error?.error === "busy" &&
      typeof answer.error.job_id === "string"
    )
      setJobId(answer.error.job_id);
  }, []);

  return (
    <PageLayout
      title="Ops"
      subtitle="Outside the system: drive playback, break links, kill and start nodes, run scenarios, snapshots and the checker."
    >
      <PlaybackPanel />
      <ClusterPanel />
      <div className="grid gap-4 lg:grid-cols-2">
        <FaultsPanel />
        <NodesPanel />
      </div>
      <JobPanel jobId={jobId} result={job} />
      <ScenariosPanel busy={busy} onJob={onJob} />
      <div className="grid gap-4 lg:grid-cols-2">
        <SnapshotsPanel busy={busy} onJob={onJob} />
        <CheckPanel busy={busy} onJob={onJob} />
      </div>
    </PageLayout>
  );
}

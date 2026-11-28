import type { ApiResult } from "../../api/client";
import type { CheckResult, Job, ScenarioResult } from "../../api/types";
import { ApiStatus } from "../../components/ApiStatus";
import { Panel } from "../../components/Panel";
import { StatusBadge } from "../../components/StatusBadge";

/** What the three job panels (scenarios, snapshots, checker) share. */
export interface JobStarter {
  /** A job started from this page is still running. */
  busy: boolean;
  /** Hand over a POST answer: a 202 job, or a 409 `busy` naming one. */
  onJob: (answer: ApiResult<Job>) => void;
}

const text = (value: unknown) =>
  value === null || value === undefined
    ? "—"
    : typeof value === "object"
      ? JSON.stringify(value)
      : String(value);

const Verdict = ({ passed }: { passed: boolean }) => (
  <StatusBadge value={passed ? "PASSED" : "FAILED"} />
);

function List({ title, items }: { title: string; items: string[] }) {
  if (items.length === 0) return null;
  return (
    <div>
      <div className="text-xs text-slate-500">{title}</div>
      <ul className="list-disc pl-5 text-xs">
        {items.map((item) => (
          <li key={item}>{item}</li>
        ))}
      </ul>
    </div>
  );
}

/** `POST /sim/check`'s result, also nested in a scenario's result. */
export function CheckResultView({ check }: { check: CheckResult }) {
  return (
    <div className="space-y-2 text-sm">
      <div className="flex flex-wrap items-center gap-3">
        <Verdict passed={check.passed} />
        <span>
          acked <b>{check.acked}</b>
        </span>
        <span>
          stored <b>{check.stored}</b>
        </span>
        <span>
          at HQ <b>{check.hq}</b>
        </span>
        <span>
          lost at failover <b>{check.lost_at_failover.count}</b>
        </span>
        {check.stored_unacked && (
          <span>
            stored but never answered <b>{check.stored_unacked.count}</b>
            {check.stored_unacked.allowed ? " (allowed)" : ""}
          </span>
        )}
      </div>
      {check.runs && (
        <p className="font-mono text-xs text-slate-500">
          runs {check.runs.join(", ")}
          {check.days ? ` · days ${check.days.join("–")}` : ""}
        </p>
      )}
      <List
        title="lost at failover (order ids)"
        items={check.lost_at_failover.order_ids}
      />
      <List
        title="unaccounted"
        items={check.unaccounted.map(
          (u) =>
            `${u.external_order_id}: ${u.reason}${u.region_id ? ` (${u.region_id})` : ""}`,
        )}
      />
      <List title="duplicates" items={check.duplicates.map(text)} />
    </div>
  );
}

function ScenarioResultView({ result }: { result: ScenarioResult }) {
  const check = result.check;
  return (
    <div className="space-y-3 text-sm">
      <div className="flex flex-wrap items-center gap-2">
        <Verdict passed={result.passed} />
        <span className="font-medium">{result.scenario}</span>
        <span className="text-slate-500">{result.summary}</span>
      </div>
      <p className="font-mono text-xs text-slate-500">
        region {result.region_id} · run {result.run_id ?? "—"}
      </p>
      {result.error && <p className="text-rose-700">{result.error}</p>}
      {result.conditions.length > 0 && (
        <ul className="space-y-1">
          {result.conditions.map((c) => (
            <li key={c.name} className="flex flex-wrap gap-2 text-xs">
              <span className={c.passed ? "text-emerald-700" : "text-rose-700"}>
                {c.passed ? "ok" : "FAIL"}
              </span>
              <span>{c.name}</span>
              {c.detail && <span className="text-slate-500">{c.detail}</span>}
            </li>
          ))}
        </ul>
      )}
      {Object.keys(result.measurements ?? {}).length > 0 && (
        <dl className="grid grid-cols-2 gap-2 sm:grid-cols-4">
          {Object.entries(result.measurements).map(([key, value]) => (
            <div key={key} className="rounded bg-slate-50 px-2 py-1">
              <dt className="text-xs text-slate-500">{key}</dt>
              <dd className="font-mono text-xs">{text(value)}</dd>
            </div>
          ))}
        </dl>
      )}
      {check && "acked" in check && (
        <div>
          <div className="text-xs text-slate-500">checker</div>
          <CheckResultView check={check} />
        </div>
      )}
      {check && !("acked" in check) && check.error && (
        <p className="text-xs text-rose-700">checker: {check.error}</p>
      )}
      <List title="warnings" items={result.warnings ?? []} />
      <List title="next steps" items={result.next_steps ?? []} />
    </div>
  );
}

/** A restore's warnings and, when it could not resume the run itself, the command that does. */
function RestoreResultView({ result }: { result: Record<string, unknown> }) {
  const resume = (result.resume ?? null) as Record<string, unknown> | null;
  const warnings = [
    ...((result.warnings as string[] | undefined) ?? []),
    ...((resume?.warnings as string[] | undefined) ?? []),
  ];
  return (
    <div className="space-y-2 text-sm">
      {resume && (
        <p>
          Playback {resume.started ? "resumed" : "not resumed"}
          {resume.run_id ? ` as run ${text(resume.run_id)}` : ""}.
        </p>
      )}
      {typeof resume?.manual_command === "string" && (
        <div>
          <div className="text-xs text-slate-500">resume it by hand</div>
          <pre className="overflow-x-auto rounded bg-slate-50 p-2 text-xs">
            {resume.manual_command}
          </pre>
        </div>
      )}
      <List title="warnings" items={warnings} />
    </div>
  );
}

function ResultView({ job }: { job: Job }) {
  if (job.result === null || job.result === undefined) return null;
  const result = job.result as Record<string, unknown>;
  if (job.kind === "scenario" && "scenario" in result)
    return <ScenarioResultView result={job.result as ScenarioResult} />;
  if (job.kind === "check" && "acked" in result)
    return <CheckResultView check={job.result as CheckResult} />;
  if (job.kind === "restore") return <RestoreResultView result={result} />;
  return null;
}

/** The job started from this page, polled until it is done. */
export function JobPanel({
  jobId,
  result,
}: {
  jobId: string | null;
  result: ApiResult<Job> | null;
}) {
  const job = result?.data ?? null;
  return (
    <Panel title="Job">
      {jobId === null ? (
        <p className="text-sm text-slate-400">
          No job started from this page yet. Scenarios, snapshots, restores and
          checks run as jobs, one at a time.
        </p>
      ) : (
        <ApiStatus result={result} loading />
      )}
      {job && (
        <div className="space-y-3">
          <div className="flex flex-wrap items-center gap-2 text-sm">
            <span className="font-mono">{job.job_id}</span>
            <span>
              {job.kind}
              {job.name ? ` ${job.name}` : ""}
            </span>
            <StatusBadge value={job.state} />
            <span className="text-xs text-slate-500">
              started {job.started_at}
              {job.finished_at ? ` · finished ${job.finished_at}` : ""}
            </span>
          </div>
          {job.progress && job.state === "running" && (
            <p className="rounded bg-sky-50 px-3 py-2 font-mono text-sm text-sky-900">
              {job.progress}
            </p>
          )}
          {job.error && (
            <p role="alert" className="text-sm text-rose-700">
              {job.error}
            </p>
          )}
          <ResultView job={job} />
          {job.result !== null && job.result !== undefined && (
            <details className="text-xs">
              <summary className="cursor-pointer text-slate-500">
                full result
              </summary>
              <pre className="mt-1 max-h-96 overflow-auto rounded bg-slate-50 p-2">
                {JSON.stringify(job.result, null, 2)}
              </pre>
            </details>
          )}
        </div>
      )}
    </Panel>
  );
}

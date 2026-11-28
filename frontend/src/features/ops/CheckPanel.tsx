import { useState } from "react";
import { runCheck } from "../../api/endpoints";
import { ApiStatus } from "../../components/ApiStatus";
import { Field, Panel } from "../../components/Panel";
import { buttonClass, inputClass } from "../../components/styles";
import { useAction } from "../../hooks/useAction";
import type { JobStarter } from "./JobPanel";

/**
 * `POST /sim/check`: the invariant checker (architecture Section 7.5) on
 * one or more finished runs. It waits for the outbox and the holding queue
 * to drain, then compares acknowledged, stored and HQ orders.
 */
export function CheckPanel({ busy, onJob }: JobStarter) {
  const check = useAction(runCheck);
  const [runs, setRuns] = useState("");
  const [allowUnacked, setAllowUnacked] = useState(false);

  const ids = runs.split(/[\s,]+/).filter(Boolean);

  return (
    <Panel title="Checker">
      <form
        className="space-y-2"
        onSubmit={(e) => {
          e.preventDefault();
          void check
            .run({ runs: ids, allow_unacked: allowUnacked })
            .then(onJob);
        }}
      >
        <Field label="run ids (comma-separated)">
          <input
            className={inputClass}
            placeholder="2026-11-20-a"
            value={runs}
            onChange={(e) => setRuns(e.target.value)}
            required
          />
        </Field>
        <label className="flex items-center gap-1 text-xs text-slate-600">
          <input
            type="checkbox"
            checked={allowUnacked}
            onChange={(e) => setAllowUnacked(e.target.checked)}
          />
          allow stored-but-unanswered orders (normal under faults)
        </label>
        <p className="text-xs text-slate-500">
          Check a run once it has stopped. A restored run brings the run it
          continues by itself.
        </p>
        <button
          className={buttonClass}
          type="submit"
          disabled={busy || check.pending || ids.length === 0}
        >
          Run checker
        </button>
      </form>
      <div className="mt-2">
        <ApiStatus result={check.result} />
      </div>
    </Panel>
  );
}

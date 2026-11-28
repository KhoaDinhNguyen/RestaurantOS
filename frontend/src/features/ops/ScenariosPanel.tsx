import { useState } from "react";
import { listScenarios, runScenario } from "../../api/endpoints";
import type { ScenarioInfo, ScenarioParam } from "../../api/types";
import { ApiStatus } from "../../components/ApiStatus";
import { Field, Panel } from "../../components/Panel";
import { dangerButtonClass, inputClass } from "../../components/styles";
import { useAction } from "../../hooks/useAction";
import { usePolling } from "../../hooks/usePolling";
import type { JobStarter } from "./JobPanel";

/** What the user typed for one scenario; an absent key means "default". */
type Typed = Record<string, string | boolean>;

/**
 * Turns the typed values into a request body. Only filled-in fields are
 * sent, so the server applies its own defaults to the rest. A value that is
 * not a number where one is expected is sent as typed, and the server's
 * `422` says what is wrong with it.
 */
function bodyFor(scenario: ScenarioInfo, typed: Typed) {
  const body: Record<string, ScenarioParam> = {};
  for (const [key, fallback] of Object.entries(scenario.params)) {
    const value = typed[key];
    if (value === undefined || value === "") continue;
    if (typeof value === "boolean" || typeof fallback === "string") {
      body[key] = value;
    } else {
      const n = Number(value);
      body[key] = Number.isFinite(n) ? n : value;
    }
  }
  return body;
}

/**
 * `GET /sim/scenarios` and `POST /sim/scenarios/{name}`: the scripted
 * failure tests T1 to T6 (architecture Section 7.3). Each starts its own
 * playback run, breaks something under load, heals it and runs the checker,
 * as one job.
 */
export function ScenariosPanel({ busy, onJob }: JobStarter) {
  const list = usePolling("scenarios", listScenarios, 0);
  const run = useAction(runScenario);
  const [name, setName] = useState("");
  const [typed, setTyped] = useState<Record<string, Typed>>({});

  const scenarios = list.result?.data ?? [];
  const chosen = scenarios.find((s) => s.name === name) ?? scenarios[0];
  const values = chosen ? (typed[chosen.name] ?? {}) : {};
  const setValue = (key: string, value: string | boolean) => {
    if (!chosen) return;
    setTyped({ ...typed, [chosen.name]: { ...values, [key]: value } });
  };

  const onRun = async () => {
    if (!chosen) return;
    const body = bodyFor(chosen, values);
    const region = body.region_id ?? chosen.params.region_id ?? "the region";
    if (
      !window.confirm(
        `Run ${chosen.name} on ${String(region)}? It starts a playback run and breaks the cluster on purpose.`,
      )
    )
      return;
    onJob(await run.run(chosen.name, body));
  };

  return (
    <Panel title="Scenarios">
      <ApiStatus result={list.result} loading={list.loading} />
      {chosen && (
        <form
          className="space-y-3"
          onSubmit={(e) => {
            e.preventDefault();
            void onRun();
          }}
        >
          <div className="flex flex-wrap items-end gap-2">
            <Field label="scenario">
              <select
                className={inputClass}
                value={chosen.name}
                onChange={(e) => setName(e.target.value)}
              >
                {scenarios.map((s) => (
                  <option key={s.name} value={s.name}>
                    {s.name}
                  </option>
                ))}
              </select>
            </Field>
            <p className="pb-1 text-sm text-slate-600">{chosen.summary}</p>
          </div>
          <div className="flex flex-wrap items-end gap-2">
            {Object.entries(chosen.params).map(([key, fallback]) =>
              typeof fallback === "boolean" ? (
                <label
                  key={key}
                  className="flex items-center gap-1 pb-1 text-xs text-slate-600"
                >
                  <input
                    type="checkbox"
                    checked={(values[key] as boolean | undefined) ?? fallback}
                    onChange={(e) => setValue(key, e.target.checked)}
                  />
                  {key}
                </label>
              ) : (
                <Field key={key} label={key}>
                  <input
                    className={`${inputClass} w-28`}
                    placeholder={
                      fallback === null ? "default" : String(fallback)
                    }
                    value={(values[key] as string | undefined) ?? ""}
                    onChange={(e) => setValue(key, e.target.value)}
                  />
                </Field>
              ),
            )}
          </div>
          <p className="text-xs text-slate-500">
            Empty fields use the default shown in grey. A scenario at the
            defaults takes up to an hour; a lower speed (for example 300) makes
            it fit in one simulated day.
          </p>
          <button
            className={dangerButtonClass}
            type="submit"
            disabled={busy || run.pending}
          >
            Run {chosen.name}
          </button>
        </form>
      )}
      <div className="mt-2">
        <ApiStatus result={run.result} />
      </div>
    </Panel>
  );
}

import { useState } from "react";
import {
  getLinks,
  healAll,
  injectFault,
  removeFault,
} from "../../api/endpoints";
import type { FaultKind } from "../../api/types";
import { ApiStatus } from "../../components/ApiStatus";
import { Field, Panel } from "../../components/Panel";
import {
  buttonClass,
  dangerButtonClass,
  inputClass,
  subtleButtonClass,
} from "../../components/styles";
import { useAction } from "../../hooks/useAction";
import { usePolling } from "../../hooks/usePolling";

/** The parameters each fault takes (architecture Section 6.3). */
const PARAMS: Record<FaultKind, string[]> = {
  cut: [],
  silence: [],
  latency: ["latency_ms", "jitter_ms"],
  reset: ["timeout_ms"],
};

/** `/sim/links` and `/sim/faults`: break and heal link groups. */
export function FaultsPanel() {
  const links = usePolling("links", getLinks, 2000);
  const inject = useAction(injectFault);
  const remove = useAction(removeFault);
  const heal = useAction(healAll);
  const [target, setTarget] = useState("");
  const [fault, setFault] = useState<FaultKind>("cut");
  const [params, setParams] = useState<Record<string, string>>({});

  const groups = links.result?.data ?? [];
  const chosen = target || groups[0]?.group || "";

  const onInject = async () => {
    const body: Record<string, number> = {};
    for (const key of PARAMS[fault])
      if (params[key]) body[key] = Number(params[key]);
    await inject.run(chosen, fault, body);
    links.refresh();
  };

  return (
    <Panel
      title="Links and faults"
      actions={
        <button
          className={dangerButtonClass}
          disabled={heal.pending}
          onClick={() => void heal.run().then(links.refresh)}
        >
          Heal all
        </button>
      }
    >
      <form
        className="mb-3 flex flex-wrap items-end gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          void onInject();
        }}
      >
        <Field label="link group">
          <select
            className={inputClass}
            value={chosen}
            onChange={(e) => setTarget(e.target.value)}
          >
            {groups.map((g) => (
              <option key={g.group} value={g.group}>
                {g.group}
              </option>
            ))}
          </select>
        </Field>
        <Field label="fault">
          <select
            className={inputClass}
            value={fault}
            onChange={(e) => setFault(e.target.value as FaultKind)}
          >
            {Object.keys(PARAMS).map((k) => (
              <option key={k} value={k}>
                {k}
              </option>
            ))}
          </select>
        </Field>
        {PARAMS[fault].map((key) => (
          <Field key={key} label={key}>
            <input
              className={`${inputClass} w-24`}
              inputMode="numeric"
              value={params[key] ?? ""}
              onChange={(e) => setParams({ ...params, [key]: e.target.value })}
            />
          </Field>
        ))}
        <button
          className={buttonClass}
          type="submit"
          disabled={!chosen || inject.pending}
        >
          Inject
        </button>
      </form>
      <ApiStatus result={inject.result} />
      <ApiStatus result={remove.result} />
      {heal.result?.data && (
        <p className="text-sm text-emerald-700">
          Healed {heal.result.data.healed} faults.
        </p>
      )}

      <ApiStatus result={links.result} loading={links.loading} />
      <ul className="space-y-2">
        {groups.map((g) => (
          <li key={g.group} className="rounded border border-slate-200 p-2">
            <div className="flex flex-wrap items-center gap-2 text-sm">
              <span className="font-mono font-medium">{g.group}</span>
              <span className="font-mono text-xs text-slate-400">
                {g.proxies.join(", ")}
              </span>
            </div>
            {g.faults.length === 0 ? (
              <p className="text-xs text-slate-400">healthy</p>
            ) : (
              <ul className="mt-1 space-y-1">
                {g.faults.map((f) => (
                  <li
                    key={f.fault_id}
                    className="flex flex-wrap items-center gap-2 text-sm"
                  >
                    <span className="rounded bg-rose-100 px-1.5 text-rose-800">
                      {f.fault}
                    </span>
                    <span className="font-mono text-xs">
                      {f.fault_id} {JSON.stringify(f.params)} since{" "}
                      {f.created_at}
                    </span>
                    <button
                      className={subtleButtonClass}
                      onClick={() =>
                        void remove.run(f.fault_id).then(links.refresh)
                      }
                    >
                      Remove
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </li>
        ))}
      </ul>
    </Panel>
  );
}

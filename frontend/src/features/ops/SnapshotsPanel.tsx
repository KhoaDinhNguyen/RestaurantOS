import { useState } from "react";
import {
  listSnapshots,
  restoreSnapshot,
  saveSnapshot,
} from "../../api/endpoints";
import type { RestoreServices } from "../../api/types";
import { ApiStatus } from "../../components/ApiStatus";
import { Field, Panel } from "../../components/Panel";
import {
  buttonClass,
  dangerButtonClass,
  inputClass,
  tableClass,
  tdClass,
  thClass,
} from "../../components/styles";
import { useAction } from "../../hooks/useAction";
import { usePolling } from "../../hooks/usePolling";
import type { JobStarter } from "./JobPanel";

const SERVICES: RestoreServices[] = ["auto", "host", "containers", "none"];

/**
 * `GET /sim/snapshots`, `POST /sim/snapshots` and
 * `POST /sim/snapshots/{name}/restore` (architecture Section 7.4). Saving
 * pauses playback and stops the databases for a moment; restoring puts the
 * whole cluster back and replays the snapshot's day as a new run.
 */
export function SnapshotsPanel({ busy, onJob }: JobStarter) {
  const list = usePolling("snapshots", listSnapshots, 5000);
  const save = useAction(saveSnapshot);
  const restore = useAction(restoreSnapshot);
  const [name, setName] = useState("");
  const [services, setServices] = useState<RestoreServices>("auto");
  const [replace, setReplace] = useState(false);

  const snapshots = list.result?.data?.snapshots ?? [];

  const onSave = async () => {
    onJob(await save.run(name.trim()));
    list.refresh();
  };

  const onRestore = async (snapshot: string) => {
    if (
      !window.confirm(
        `Restore ${snapshot}? This stops the live run and the services, and puts every database back as it was.`,
      )
    )
      return;
    onJob(
      await restore.run(snapshot, { services, replace_cluster_file: replace }),
    );
  };

  return (
    <Panel title="Snapshots">
      <form
        className="mb-3 flex flex-wrap items-end gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          void onSave();
        }}
      >
        <Field label="new snapshot name">
          <input
            className={`${inputClass} w-40`}
            placeholder="before-t2"
            value={name}
            onChange={(e) => setName(e.target.value)}
            required
          />
        </Field>
        <button
          className={buttonClass}
          type="submit"
          disabled={busy || save.pending || !name.trim()}
        >
          Take snapshot
        </button>
      </form>
      <ApiStatus result={save.result} />

      <div className="mb-2 flex flex-wrap items-end gap-2">
        <Field label="services after restore">
          <select
            className={inputClass}
            value={services}
            onChange={(e) => setServices(e.target.value as RestoreServices)}
          >
            {SERVICES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </Field>
        <label className="flex items-center gap-1 pb-1 text-xs text-slate-600">
          <input
            type="checkbox"
            checked={replace}
            onChange={(e) => setReplace(e.target.checked)}
          />
          replace cluster file
        </label>
      </div>
      <ApiStatus result={restore.result} />

      <ApiStatus result={list.result} loading={list.loading} />
      {list.result?.data &&
        (snapshots.length === 0 ? (
          <p className="text-sm text-slate-400">No snapshots yet.</p>
        ) : (
          <table className={tableClass}>
            <thead>
              <tr>
                <th className={thClass}>name</th>
                <th className={thClass}>simulated point</th>
                <th className={thClass}>regions</th>
                <th className={thClass}>taken</th>
                <th className={thClass} />
              </tr>
            </thead>
            <tbody>
              {snapshots.map((s) => (
                <tr key={s.name}>
                  <td className={`${tdClass} font-mono`}>{s.name}</td>
                  <td className={`${tdClass} font-mono text-xs`}>
                    {s.sim_day === null
                      ? "—"
                      : `day ${s.sim_day} · ${s.sim_time}`}
                  </td>
                  <td className={`${tdClass} text-xs`}>
                    {(s.regions ?? []).join(", ")}
                  </td>
                  <td className={`${tdClass} text-xs`}>
                    {s.created_at ?? "—"}
                  </td>
                  <td className={tdClass}>
                    <button
                      className={dangerButtonClass}
                      disabled={busy || restore.pending}
                      onClick={() => void onRestore(s.name)}
                    >
                      Restore
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ))}
    </Panel>
  );
}

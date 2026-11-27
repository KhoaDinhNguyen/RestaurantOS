import { useState } from "react";
import { getCluster, killNode, startNode } from "../../api/endpoints";
import { ApiStatus } from "../../components/ApiStatus";
import { Field, Panel } from "../../components/Panel";
import { StatusBadge } from "../../components/StatusBadge";
import {
  dangerButtonClass,
  inputClass,
  subtleButtonClass,
} from "../../components/styles";
import { useAction } from "../../hooks/useAction";
import { usePolling } from "../../hooks/usePolling";

/**
 * `POST /sim/nodes/{id}/kill` and `/start`: a real crash (T1b) and a
 * restart. The node list comes from the coordinator; a node id can also be
 * typed, for when the coordinator itself is not answering.
 */
export function NodesPanel() {
  const cluster = usePolling("cluster", getCluster, 2000);
  const kill = useAction(killNode);
  const start = useAction(startNode);
  const [typed, setTyped] = useState("");

  const nodes = cluster.result?.data?.nodes ?? [];
  const last = kill.result ?? start.result;

  const act = async (action: "kill" | "start", nodeId: string) => {
    if (action === "kill" && !window.confirm(`Kill ${nodeId}?`)) return;
    await (action === "kill" ? kill : start).run(nodeId);
    cluster.refresh();
  };

  return (
    <Panel title="Nodes">
      <ApiStatus result={cluster.result} loading={cluster.loading} />
      <ul className="mb-3 space-y-1">
        {nodes.map((n) => (
          <li key={n.node_id} className="flex items-center gap-2 text-sm">
            <span className="w-28 font-mono">{n.node_id}</span>
            <StatusBadge value={n.role} />
            <StatusBadge value={n.status} />
            <button
              className={subtleButtonClass}
              onClick={() => void act("kill", n.node_id)}
            >
              Kill
            </button>
            <button
              className={subtleButtonClass}
              onClick={() => void act("start", n.node_id)}
            >
              Start
            </button>
          </li>
        ))}
      </ul>
      <div className="flex flex-wrap items-end gap-2">
        <Field label="node id">
          <input
            className={inputClass}
            placeholder="r1-primary"
            value={typed}
            onChange={(e) => setTyped(e.target.value)}
          />
        </Field>
        <button
          className={dangerButtonClass}
          disabled={!typed}
          onClick={() => void act("kill", typed.trim())}
        >
          Kill
        </button>
        <button
          className={subtleButtonClass}
          disabled={!typed}
          onClick={() => void act("start", typed.trim())}
        >
          Start
        </button>
      </div>
      {last?.data && (
        <p className="mt-2 text-sm">
          {last.data.node_id} is {last.data.running ? "running" : "stopped"}.
        </p>
      )}
      <div className="mt-2">
        <ApiStatus result={last} />
      </div>
    </Panel>
  );
}

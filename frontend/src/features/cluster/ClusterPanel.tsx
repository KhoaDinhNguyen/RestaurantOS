import { failover, getCluster } from "../../api/endpoints";
import { ApiStatus } from "../../components/ApiStatus";
import { Panel } from "../../components/Panel";
import { StatusBadge } from "../../components/StatusBadge";
import {
  dangerButtonClass,
  tableClass,
  tdClass,
  thClass,
} from "../../components/styles";
import { useAction } from "../../hooks/useAction";
import { usePolling } from "../../hooks/usePolling";

const show = (value: number | string | null) =>
  value === null ? "—" : String(value);

/**
 * `GET /admin/cluster`, polled: every node's role and health, every region's
 * epoch and figures. With `canFailover`, each region gets the manual
 * failover button (`POST /admin/regions/{id}/failover`).
 */
export function ClusterPanel({
  canFailover = false,
}: {
  canFailover?: boolean;
}) {
  const { result, loading, refresh } = usePolling("cluster", getCluster, 2000);
  const promote = useAction(failover);
  const cluster = result?.data;

  const onFailover = async (regionId: string) => {
    if (!window.confirm(`Promote the standby of ${regionId}?`)) return;
    await promote.run(regionId);
    refresh();
  };

  return (
    <Panel
      title="Cluster"
      actions={
        cluster && (
          <span className="text-xs text-slate-500">as of {cluster.as_of}</span>
        )
      }
    >
      <ApiStatus result={result} loading={loading} />
      {cluster && (
        <div className="space-y-4">
          <table className={tableClass}>
            <thead>
              <tr>
                <th className={thClass}>node</th>
                <th className={thClass}>region</th>
                <th className={thClass}>role</th>
                <th className={thClass}>status</th>
                <th className={thClass}>last heartbeat</th>
              </tr>
            </thead>
            <tbody>
              {cluster.nodes.map((n) => (
                <tr key={n.node_id}>
                  <td className={`${tdClass} font-mono`}>{n.node_id}</td>
                  <td className={tdClass}>{n.region_id ?? "HQ"}</td>
                  <td className={tdClass}>
                    <StatusBadge value={n.role} />
                  </td>
                  <td className={tdClass}>
                    <StatusBadge value={n.status} />
                  </td>
                  <td className={`${tdClass} text-xs`}>
                    {show(n.last_heartbeat_at)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          <div className="overflow-x-auto">
            <table className={tableClass}>
              <thead>
                <tr>
                  <th className={thClass}>region</th>
                  <th className={thClass}>epoch</th>
                  <th className={thClass}>primary / standby</th>
                  <th className={thClass}>replication</th>
                  <th className={thClass}>orders/s</th>
                  <th className={thClass}>standby lag s</th>
                  <th className={thClass}>HQ lag s</th>
                  <th className={thClass}>outbox</th>
                  <th className={thClass}>holding queue</th>
                  <th className={thClass}>menu v.</th>
                  <th className={thClass}>figures as of</th>
                  {canFailover && <th className={thClass} />}
                </tr>
              </thead>
              <tbody>
                {cluster.regions.map((r) => (
                  <tr key={r.region_id}>
                    <td className={`${tdClass} font-mono`}>{r.region_id}</td>
                    <td className={`${tdClass} font-semibold`}>{r.epoch}</td>
                    <td className={`${tdClass} font-mono text-xs`}>
                      {r.primary_node} / {r.standby_node}
                    </td>
                    <td className={tdClass}>{r.replication}</td>
                    <td className={tdClass}>{show(r.orders_per_s)}</td>
                    <td className={tdClass}>
                      {r.standby_lag_s === null
                        ? "not connected"
                        : r.standby_lag_s}
                    </td>
                    <td className={tdClass}>{show(r.hq_lag_s)}</td>
                    <td className={tdClass}>{show(r.outbox_backlog)}</td>
                    <td className={tdClass}>{show(r.holding_queue)}</td>
                    <td className={tdClass}>{r.menu_version}</td>
                    <td className={`${tdClass} text-xs`}>
                      {show(r.figures_as_of)}
                    </td>
                    {canFailover && (
                      <td className={tdClass}>
                        <button
                          className={dangerButtonClass}
                          disabled={promote.pending}
                          onClick={() => onFailover(r.region_id)}
                        >
                          Failover
                        </button>
                      </td>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
      {promote.result && (
        <div className="mt-3 text-sm">
          {promote.result.data ? (
            <p className="text-emerald-700">
              {promote.result.data.region_id} promoted: primary is now{" "}
              {promote.result.data.primary_node}, epoch{" "}
              {promote.result.data.epoch}.
            </p>
          ) : (
            <ApiStatus result={promote.result} />
          )}
        </div>
      )}
    </Panel>
  );
}

import { getEvents, getExceptions } from "../../api/endpoints";
import { ApiStatus } from "../../components/ApiStatus";
import { Panel } from "../../components/Panel";
import { StatusBadge } from "../../components/StatusBadge";
import { tableClass, tdClass, thClass } from "../../components/styles";
import { usePolling } from "../../hooks/usePolling";

/** `GET /admin/events`: failovers, partitions, recoveries (newest 20, oldest first). */
export function EventsPanel() {
  const { result, loading } = usePolling("events", () => getEvents(20), 2000);
  return (
    <Panel title="Cluster events">
      <ApiStatus result={result} loading={loading} />
      {result?.data && (
        <table className={tableClass}>
          <thead>
            <tr>
              <th className={thClass}>#</th>
              <th className={thClass}>when (wall clock)</th>
              <th className={thClass}>kind</th>
              <th className={thClass}>region</th>
              <th className={thClass}>node</th>
            </tr>
          </thead>
          <tbody>
            {result.data.map((e) => (
              <tr key={e.event_id}>
                <td className={tdClass}>{e.event_id}</td>
                <td className={`${tdClass} text-xs`}>{e.occurred_at}</td>
                <td className={tdClass}>{e.kind}</td>
                <td className={tdClass}>{e.region_id ?? "—"}</td>
                <td className={`${tdClass} font-mono`}>{e.node_id ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {result?.data?.length === 0 && (
        <p className="text-sm text-slate-400">no events</p>
      )}
    </Panel>
  );
}

/** `GET /admin/exceptions`: status changes that lost to an earlier one. */
export function ExceptionsPanel() {
  const { result, loading } = usePolling(
    "exceptions",
    () => getExceptions(20),
    2000,
  );
  return (
    <Panel title="Exceptions">
      <ApiStatus result={result} loading={loading} />
      {result?.data && (
        <table className={tableClass}>
          <thead>
            <tr>
              <th className={thClass}>#</th>
              <th className={thClass}>region</th>
              <th className={thClass}>order</th>
              <th className={thClass}>was</th>
              <th className={thClass}>attempted</th>
            </tr>
          </thead>
          <tbody>
            {result.data.map((x) => (
              <tr key={x.seq}>
                <td className={tdClass}>{x.seq}</td>
                <td className={tdClass}>{x.region_id}</td>
                <td className={`${tdClass} font-mono text-xs`}>
                  day {x.order_day} · {x.order_id}
                </td>
                <td className={tdClass}>
                  <StatusBadge value={x.current_status} />
                </td>
                <td className={tdClass}>
                  <StatusBadge value={x.attempted_status} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {result?.data?.length === 0 && (
        <p className="text-sm text-slate-400">no exceptions</p>
      )}
    </Panel>
  );
}

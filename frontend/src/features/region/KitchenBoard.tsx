import { useState } from "react";
import { changeStatus, edgeApi, listOrders } from "../../api/endpoints";
import type { ApiResult } from "../../api/client";
import {
  ORDER_STATUSES,
  type OrderStatus,
  type StatusResult,
} from "../../api/types";
import { ApiStatus } from "../../components/ApiStatus";
import { Panel } from "../../components/Panel";
import { StatusBadge } from "../../components/StatusBadge";
import {
  subtleButtonClass,
  tableClass,
  tdClass,
  thClass,
} from "../../components/styles";
import { usePolling } from "../../hooks/usePolling";
import { newId } from "../../lib/ids";

const OPEN: OrderStatus[] = ["received", "accepted", "preparing", "ready"];

interface Attempt {
  orderId: string;
  toStatus: OrderStatus;
  eventId: string;
  result: ApiResult<StatusResult> | null;
}

/**
 * The kitchen board: `GET /vendors/{id}/orders?day=&status=` on the edge API,
 * and a status change per click. Each click gets a new event id; "retry"
 * sends the same one again, so a retry never counts twice. The server
 * decides which changes are allowed: one that lost answers 409.
 */
export function KitchenBoard({
  regionId,
  vendorId,
  day,
}: {
  regionId: string;
  vendorId: string;
  day: number | null;
}) {
  const api = edgeApi(regionId);
  const [statuses, setStatuses] = useState<OrderStatus[]>(OPEN);
  const [attempt, setAttempt] = useState<Attempt | null>(null);

  const { result, loading, refresh } = usePolling(
    day === null
      ? null
      : `board|${regionId}|${vendorId}|${day}|${statuses.join(",")}`,
    () => listOrders(api, vendorId, day, statuses),
    2000,
  );

  const send = async (next: Omit<Attempt, "result">) => {
    setAttempt({ ...next, result: null });
    const answer = await changeStatus(
      api,
      vendorId,
      next.orderId,
      next.eventId,
      next.toStatus,
    );
    setAttempt({ ...next, result: answer });
    refresh();
  };

  const toggle = (status: OrderStatus) =>
    setStatuses((current) =>
      current.includes(status)
        ? current.filter((s) => s !== status)
        : ORDER_STATUSES.filter((s) => s === status || current.includes(s)),
    );

  const failed =
    attempt?.result &&
    (attempt.result.status === 0 || attempt.result.status >= 500);

  return (
    <Panel title={`Kitchen board: ${vendorId}`}>
      <div className="mb-3 flex flex-wrap gap-3 text-xs">
        {ORDER_STATUSES.map((s) => (
          <label key={s} className="flex items-center gap-1">
            <input
              type="checkbox"
              checked={statuses.includes(s)}
              onChange={() => toggle(s)}
            />
            {s}
          </label>
        ))}
      </div>

      {attempt?.result && (
        <div className="mb-3 text-sm">
          {attempt.result.data ? (
            <AttemptOutcome attempt={attempt} data={attempt.result.data} />
          ) : (
            <ApiStatus result={attempt.result} />
          )}
          {failed && (
            <button
              className={`${subtleButtonClass} mt-1`}
              onClick={() => void send(attempt)}
            >
              Retry {attempt.toStatus} (same event id)
            </button>
          )}
        </div>
      )}

      <ApiStatus result={result} loading={loading} />
      {result?.data && (
        <table className={tableClass}>
          <thead>
            <tr>
              <th className={thClass}>time</th>
              <th className={thClass}>order</th>
              <th className={thClass}>items</th>
              <th className={thClass}>status</th>
              <th className={thClass}>change to</th>
            </tr>
          </thead>
          <tbody>
            {result.data.map((o) => (
              <tr key={o.order_id}>
                <td className={`${tdClass} font-mono`}>{o.order_time}</td>
                <td className={`${tdClass} font-mono text-xs`}>
                  {o.order_id}
                  <div className="text-slate-400">{o.channel}</div>
                </td>
                <td className={`${tdClass} text-xs`}>
                  {o.items.map((i) => (
                    <div key={i.line_no}>
                      {i.quantity} × {i.name}
                    </div>
                  ))}
                </td>
                <td className={tdClass}>
                  <StatusBadge value={o.status} />
                </td>
                <td className={tdClass}>
                  <div className="flex flex-wrap gap-1">
                    {ORDER_STATUSES.filter((s) => s !== o.status).map((s) => (
                      <button
                        key={s}
                        className={subtleButtonClass}
                        onClick={() =>
                          void send({
                            orderId: o.order_id,
                            toStatus: s,
                            eventId: newId(),
                          })
                        }
                      >
                        {s}
                      </button>
                    ))}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {result?.data?.length === 0 && (
        <p className="text-sm text-slate-400">No orders with those statuses.</p>
      )}
    </Panel>
  );
}

function AttemptOutcome({
  attempt,
  data,
}: {
  attempt: Attempt;
  data: StatusResult;
}) {
  if (data.conflict)
    return (
      <p className="text-amber-800">
        409: {attempt.toStatus} lost to an earlier change; the order is{" "}
        {data.status}.
      </p>
    );
  if (data.duplicate)
    return (
      <p className="text-slate-600">
        Already applied (same event id); the order is {data.status}.
      </p>
    );
  return (
    <p className="text-emerald-700">
      Order {data.order_id} is now {data.status}.
    </p>
  );
}

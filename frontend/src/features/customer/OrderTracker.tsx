import { gatewayApi, getOrder } from "../../api/endpoints";
import { ApiStatus } from "../../components/ApiStatus";
import { Panel } from "../../components/Panel";
import { StatusBadge } from "../../components/StatusBadge";
import { usePolling } from "../../hooks/usePolling";
import type { PlacedOrder } from "./OrderForm";

/** One order's status, `GET /vendors/{id}/orders/{order_id}?day=`, polled. */
export function OrderTracker({ order }: { order: PlacedOrder }) {
  const { result, loading } = usePolling(
    `order|${order.vendorId}|${order.orderId}`,
    () => getOrder(gatewayApi, order.vendorId, order.orderId, order.day),
    2000,
  );
  const o = result?.data;
  return (
    <Panel title="My order">
      <ApiStatus result={result} loading={loading} />
      {o && (
        <div className="space-y-1 text-sm">
          <p>
            <StatusBadge value={o.status} />{" "}
            <span className="font-mono text-xs">{o.order_id}</span>
          </p>
          <p className="text-xs text-slate-500">
            {o.vendor_id} · day {o.order_day} {o.order_time} · status changed{" "}
            {o.status_changed_at}
          </p>
          <ul className="list-inside list-disc text-xs">
            {o.items.map((i) => (
              <li key={i.line_no}>
                {i.quantity} × {i.name} ({i.price_units} price units each)
              </li>
            ))}
          </ul>
        </div>
      )}
    </Panel>
  );
}

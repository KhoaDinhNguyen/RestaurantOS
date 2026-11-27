import { useState } from "react";
import { gatewayApi, getMenu, placeOrder } from "../../api/endpoints";
import type { ApiResult } from "../../api/client";
import type { OrderRequest, OrderResponse } from "../../api/types";
import { ApiStatus } from "../../components/ApiStatus";
import { Panel } from "../../components/Panel";
import {
  buttonClass,
  inputClass,
  subtleButtonClass,
  tableClass,
  tdClass,
  thClass,
} from "../../components/styles";
import { usePolling } from "../../hooks/usePolling";
import { useSimClock } from "../../hooks/useSimClock";
import { newId } from "../../lib/ids";

/** The fixed `channel` for orders placed from this UI (playback uses `dhrd`). */
const CHANNEL = "web";

export interface PlacedOrder {
  vendorId: string;
  orderId: string;
  day: number | null;
}

/**
 * A vendor's menu (through the gateway) and `POST /orders`. One draft has one
 * `external_order_id`; every retry of it, including asking again after a
 * 202, sends the identical request, so it can never become two orders.
 */
export function OrderForm({
  vendorId,
  geohash,
  onPlaced,
}: {
  vendorId: string;
  geohash: string;
  onPlaced: (order: PlacedOrder) => void;
}) {
  const { day, time } = useSimClock();
  const menu = usePolling(
    `menu|${vendorId}`,
    () => getMenu(gatewayApi, vendorId),
    5000,
  );
  const [quantities, setQuantities] = useState<Record<string, string>>({});
  const [sent, setSent] = useState<OrderRequest | null>(null);
  const [answer, setAnswer] = useState<ApiResult<OrderResponse> | null>(null);
  const [pending, setPending] = useState(false);

  const send = async (request: OrderRequest) => {
    setSent(request);
    setPending(true);
    const result = await placeOrder(request);
    setAnswer(result);
    setPending(false);
    const body = result.data;
    if (body?.order_id)
      onPlaced({ vendorId, orderId: body.order_id, day: body.order_day });
  };

  const place = () => {
    if (day === null) return;
    const items = Object.entries(quantities)
      .filter(([, q]) => /^\d+$/.test(q) && q !== "0")
      .map(([product_id, q]) => ({ product_id, quantity: Number(q) }));
    void send({
      channel: CHANNEL,
      external_order_id: newId(),
      vendor_id: vendorId,
      customer_id: null,
      customer_geohash: /^[0-9a-z]{5}$/.test(geohash) ? geohash : null,
      order_day: day,
      order_time: time,
      items,
    });
  };

  const newOrder = () => {
    setSent(null);
    setAnswer(null);
    setQuantities({});
  };

  // A draft is open until it gets a final answer; a 202, a 5xx or no answer
  // leave it open, and the only way on is to send the same request again.
  const open =
    sent !== null &&
    (answer === null ||
      answer.status === 202 ||
      answer.status === 0 ||
      answer.status >= 500);

  return (
    <Panel title={`Menu of ${vendorId}`}>
      <ApiStatus result={menu.result} loading={menu.loading} />
      {menu.result?.data && (
        <table className={tableClass}>
          <thead>
            <tr>
              <th className={thClass}>product</th>
              <th className={thClass}>price units</th>
              <th className={thClass}>available</th>
              <th className={thClass}>quantity</th>
            </tr>
          </thead>
          <tbody>
            {menu.result.data.map((m) => (
              <tr key={m.product_id}>
                <td className={tdClass}>{m.name}</td>
                <td className={tdClass}>{m.price_units}</td>
                <td className={tdClass}>
                  {m.available ? "yes" : `no (${m.reason})`}
                </td>
                <td className={tdClass}>
                  <input
                    aria-label={`quantity of ${m.name}`}
                    className={`${inputClass} w-16`}
                    inputMode="numeric"
                    disabled={sent !== null}
                    value={quantities[m.product_id] ?? ""}
                    onChange={(e) =>
                      setQuantities({
                        ...quantities,
                        [m.product_id]: e.target.value,
                      })
                    }
                  />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <div className="mt-3 flex flex-wrap items-center gap-2">
        {sent === null ? (
          <button
            className={buttonClass}
            disabled={pending || day === null}
            onClick={place}
          >
            Place order
          </button>
        ) : (
          <>
            {open && (
              <button
                className={buttonClass}
                disabled={pending}
                onClick={() => void send(sent)}
              >
                {answer?.status === 202 ? "Ask again" : "Retry"} (same order id)
              </button>
            )}
            <button className={subtleButtonClass} onClick={newOrder}>
              New order
            </button>
          </>
        )}
        {day === null && (
          <span className="text-xs text-amber-700">
            Pick a simulated day first.
          </span>
        )}
        {sent && (
          <span className="font-mono text-xs text-slate-400">
            external_order_id {sent.external_order_id} · day {sent.order_day}{" "}
            {sent.order_time}
          </span>
        )}
      </div>

      {answer && (
        <div className="mt-3">
          <OrderOutcome result={answer} />
        </div>
      )}
    </Panel>
  );
}

function OrderOutcome({ result }: { result: ApiResult<OrderResponse> }) {
  const body = result.data;
  if (!body) return <ApiStatus result={result} />;
  if (result.status === 202 || body.queued)
    return (
      <p className="rounded border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-900">
        202: region {body.region_id} cannot be reached; the order is parked and
        will be delivered later. Ask again to learn the outcome.
      </p>
    );
  if (result.status === 409)
    return (
      <p className="rounded border border-rose-300 bg-rose-50 px-3 py-2 text-sm text-rose-800">
        409: refused ({body.refusal_reason}).
      </p>
    );
  return (
    <p className="rounded border border-emerald-300 bg-emerald-50 px-3 py-2 text-sm text-emerald-800">
      {result.status}: {body.duplicate ? "already placed before" : "stored"} in{" "}
      {body.region_id} as <span className="font-mono">{body.order_id}</span> ·
      received {body.received_at} · committed {body.committed_at} · acked{" "}
      {body.acked_at}
    </p>
  );
}

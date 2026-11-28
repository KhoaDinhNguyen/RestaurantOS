import { useState } from "react";
import { gatewayApi, getMenu, placeOrder } from "../../api/endpoints";
import type { ApiResult } from "../../api/client";
import type { OrderRequest, OrderResponse } from "../../api/types";
import { ApiStatus } from "../../components/ApiStatus";
import { Field, Panel } from "../../components/Panel";
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
import { newId } from "../../api/ids";

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
  // The quantity typed per product, with the product's name, so the line can
  // still be shown (and changed) if the menu stops loading mid-draft.
  const [quantities, setQuantities] = useState<
    Record<string, { name: string; quantity: string }>
  >({});
  const setQuantity = (productId: string, name: string, quantity: string) =>
    setQuantities((current) => ({
      ...current,
      [productId]: { name, quantity },
    }));
  // An item typed by product id, for when the menu cannot be loaded (the
  // gateway cannot reach the region): the order is then parked, not lost.
  const [typed, setTyped] = useState({ productId: "", quantity: "" });
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

  // The lines of the order are exactly the rows on screen: the menu's rows,
  // or, while the menu cannot be loaded, the quantities already typed plus
  // the typed item. An order needs at least one (POST /orders refuses an
  // empty one).
  const wanted = (q: string) => /^\d+$/.test(q) && Number(q) > 0;
  const menuRows = menu.result?.data ?? null;
  const menuMissing = menu.result !== null && menuRows === null;
  const shownIds = menuRows
    ? menuRows.map((m) => m.product_id)
    : menuMissing
      ? Object.keys(quantities)
      : [];
  const typedId = menuMissing ? typed.productId.trim() : "";
  const items = [
    ...shownIds
      .filter((id) => wanted(quantities[id]?.quantity ?? ""))
      .map((id) => ({
        product_id: id,
        quantity: Number(quantities[id].quantity),
      })),
    ...(typedId && wanted(typed.quantity)
      ? [{ product_id: typedId, quantity: Number(typed.quantity) }]
      : []),
  ];

  const place = () => {
    if (day === null || items.length === 0) return;
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
    setTyped({ productId: "", quantity: "" });
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
                    value={quantities[m.product_id]?.quantity ?? ""}
                    onChange={(e) =>
                      setQuantity(m.product_id, m.name, e.target.value)
                    }
                  />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {menuMissing && shownIds.length > 0 && (
        <table className={tableClass}>
          <caption className="text-left text-xs text-slate-600">
            Already in this order
          </caption>
          <thead>
            <tr>
              <th className={thClass}>product</th>
              <th className={thClass}>quantity</th>
            </tr>
          </thead>
          <tbody>
            {shownIds.map((id) => (
              <tr key={id}>
                <td className={tdClass}>
                  {quantities[id].name}{" "}
                  <span className="font-mono text-xs text-slate-400">{id}</span>
                </td>
                <td className={tdClass}>
                  <input
                    aria-label={`quantity of ${quantities[id].name}`}
                    className={`${inputClass} w-16`}
                    inputMode="numeric"
                    disabled={sent !== null}
                    value={quantities[id].quantity}
                    onChange={(e) =>
                      setQuantity(id, quantities[id].name, e.target.value)
                    }
                  />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {menuMissing && (
        <div className="mt-2 flex flex-wrap items-end gap-2">
          <p className="w-full text-xs text-slate-600">
            The menu cannot be loaded. You can still order by product id: if the
            region cannot be reached, the order is parked and delivered later.
          </p>
          <Field label="product id">
            <input
              className={`${inputClass} w-36 font-mono`}
              disabled={sent !== null}
              value={typed.productId}
              onChange={(e) =>
                setTyped({ ...typed, productId: e.target.value })
              }
            />
          </Field>
          <Field label="quantity">
            <input
              className={`${inputClass} w-16`}
              inputMode="numeric"
              disabled={sent !== null}
              value={typed.quantity}
              onChange={(e) => setTyped({ ...typed, quantity: e.target.value })}
            />
          </Field>
        </div>
      )}

      <div className="mt-3 flex flex-wrap items-center gap-2">
        {sent === null ? (
          <button
            className={buttonClass}
            disabled={pending || day === null || items.length === 0}
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
            {/* Not while a request is out: its answer belongs to this draft. */}
            <button
              className={subtleButtonClass}
              disabled={pending}
              onClick={newOrder}
            >
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

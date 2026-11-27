import { edgeApi, getMenu, setAvailability } from "../../api/endpoints";
import { ApiStatus } from "../../components/ApiStatus";
import { Panel } from "../../components/Panel";
import {
  subtleButtonClass,
  tableClass,
  tdClass,
  thClass,
} from "../../components/styles";
import { useAction } from "../../hooks/useAction";
import { usePolling } from "../../hooks/usePolling";

/**
 * The menu with availability, and the kitchen's sold-out switch
 * (`PUT /vendors/{id}/products/{pid}/availability`). Switching on can still
 * answer `available: false, reason: "stock"`: that is the server's answer,
 * not an error.
 */
export function MenuAvailabilityPanel({
  regionId,
  vendorId,
}: {
  regionId: string;
  vendorId: string;
}) {
  const api = edgeApi(regionId);
  const { result, loading, refresh } = usePolling(
    `menu|${regionId}|${vendorId}`,
    () => getMenu(api, vendorId),
    2000,
  );
  const toggle = useAction(setAvailability);

  const onToggle = async (productId: string, available: boolean) => {
    await toggle.run(api, vendorId, productId, available);
    refresh();
  };

  const last = toggle.result;
  return (
    <Panel title="Menu and availability">
      <ApiStatus result={result} loading={loading} />
      {last?.data && (
        <p className="mb-2 text-sm">
          {last.data.product_id}:{" "}
          {last.data.available
            ? "available"
            : `unavailable (${last.data.reason})`}
        </p>
      )}
      <ApiStatus result={last} />
      {result?.data && (
        <table className={tableClass}>
          <thead>
            <tr>
              <th className={thClass}>product</th>
              <th className={thClass}>price units</th>
              <th className={thClass}>dish</th>
              <th className={thClass}>available</th>
              <th className={thClass} />
            </tr>
          </thead>
          <tbody>
            {result.data.map((m) => (
              <tr key={m.product_id}>
                <td className={tdClass}>
                  {m.name}{" "}
                  <span className="font-mono text-xs text-slate-400">
                    {m.product_id}
                  </span>
                </td>
                <td className={tdClass}>{m.price_units}</td>
                <td className={`${tdClass} font-mono text-xs`}>{m.dish_id}</td>
                <td className={tdClass}>
                  {m.available ? (
                    <span className="text-emerald-700">yes</span>
                  ) : (
                    <span className="text-rose-700">no ({m.reason})</span>
                  )}
                </td>
                <td className={tdClass}>
                  <button
                    className={subtleButtonClass}
                    disabled={toggle.pending}
                    onClick={() =>
                      void onToggle(m.product_id, m.reason === "manual")
                    }
                  >
                    {m.reason === "manual" ? "Switch on" : "Mark sold out"}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  );
}

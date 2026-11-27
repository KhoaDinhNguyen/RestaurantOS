import { edgeApi, getStock, restock } from "../../api/endpoints";
import { ApiStatus } from "../../components/ApiStatus";
import { Panel } from "../../components/Panel";
import {
  buttonClass,
  tableClass,
  tdClass,
  thClass,
} from "../../components/styles";
import { useAction } from "../../hooks/useAction";
import { usePolling } from "../../hooks/usePolling";

/** `GET /vendors/{id}/stock` and the restock-to-par button. */
export function StockPanel({
  regionId,
  vendorId,
}: {
  regionId: string;
  vendorId: string;
}) {
  const api = edgeApi(regionId);
  const { result, loading, refresh } = usePolling(
    `stock|${regionId}|${vendorId}`,
    () => getStock(api, vendorId),
    2000,
  );
  const doRestock = useAction(restock);

  const onRestock = async () => {
    await doRestock.run(api, vendorId);
    refresh();
  };

  const last = doRestock.result;
  return (
    <Panel
      title="Stock"
      actions={
        <button
          className={buttonClass}
          disabled={doRestock.pending}
          onClick={() => void onRestock()}
        >
          Restock to par
        </button>
      }
    >
      {last?.data && (
        <p className="mb-2 text-sm text-emerald-700">
          Restocked {last.data.counters_restocked} counters;{" "}
          {last.data.products_back_on} products back on.
        </p>
      )}
      <ApiStatus result={last} />
      <ApiStatus result={result} loading={loading} />
      {result?.data && (
        <table className={tableClass}>
          <thead>
            <tr>
              <th className={thClass}>kind</th>
              <th className={thClass}>item</th>
              <th className={thClass}>on hand</th>
              <th className={thClass}>par level</th>
              <th className={thClass}>unit</th>
              <th className={thClass}>updated</th>
            </tr>
          </thead>
          <tbody>
            {result.data.map((s) => (
              <tr key={`${s.item_kind}|${s.item_id}`}>
                <td className={tdClass}>{s.item_kind}</td>
                <td className={tdClass}>
                  {s.name}
                  {s.name !== s.item_id && (
                    <span className="ml-1 font-mono text-xs text-slate-400">
                      {s.item_id}
                    </span>
                  )}
                </td>
                <td className={tdClass}>{s.on_hand}</td>
                <td className={tdClass}>{s.par_level}</td>
                <td className={tdClass}>{s.unit ?? "servings"}</td>
                <td className={`${tdClass} text-xs`}>{s.updated_at}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  );
}

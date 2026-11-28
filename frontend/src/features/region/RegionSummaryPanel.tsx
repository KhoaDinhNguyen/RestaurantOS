import { edgeApi, regionSummary } from "../../api/endpoints";
import { ORDER_STATUSES } from "../../api/types";
import { ApiStatus } from "../../components/ApiStatus";
import { Panel } from "../../components/Panel";
import { usePolling } from "../../hooks/usePolling";

/** `GET /regions/{id}/summary?day=` on the region's own edge API. */
export function RegionSummaryPanel({
  regionId,
  day,
}: {
  regionId: string;
  day: number | null;
}) {
  const { result, loading } = usePolling(
    day === null ? null : `summary|${regionId}|${day}`,
    () => regionSummary(edgeApi(regionId), regionId, day!),
    2000,
  );
  const s = result?.data;

  return (
    <Panel
      title={`Region ${regionId}, day ${day ?? "?"}`}
      actions={
        s && (
          <span className="text-xs text-slate-500">
            data as of {s.data_as_of}
          </span>
        )
      }
    >
      {day === null && (
        <p className="text-sm text-slate-500">Pick a day in the header.</p>
      )}
      <ApiStatus result={result} loading={loading} />
      {s && (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-4 lg:grid-cols-8">
          <Stat label="epoch" value={s.epoch} />
          <Stat label="menu version" value={s.menu_version} />
          <Stat label="restaurants" value={s.vendors} />
          <Stat label="orders" value={s.orders.total} />
          <Stat label="refused" value={s.refused} />
          <Stat label="sales units" value={s.sales_units} />
          <Stat label="products off" value={s.products_unavailable} />
          <Stat label="outbox backlog" value={s.outbox_backlog} />
          {ORDER_STATUSES.map((status) => (
            <Stat key={status} label={status} value={s.orders[status]} small />
          ))}
        </div>
      )}
    </Panel>
  );
}

function Stat({
  label,
  value,
  small = false,
}: {
  label: string;
  value: number;
  small?: boolean;
}) {
  return (
    <div className="rounded bg-slate-50 px-3 py-2">
      <div className="text-xs text-slate-500">{label}</div>
      <div className={small ? "text-base" : "text-xl font-semibold"}>
        {value}
      </div>
    </div>
  );
}

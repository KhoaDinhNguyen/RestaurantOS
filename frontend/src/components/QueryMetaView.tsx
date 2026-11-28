import type { QueryMeta } from "../api/types";

/** The `meta` of a query answer, shown next to its result. */
export function QueryMetaView({ meta }: { meta: QueryMeta }) {
  const missing = meta.missing_regions;
  return (
    <dl className="grid grid-cols-2 gap-x-4 gap-y-1 rounded bg-slate-50 p-3 text-xs sm:grid-cols-3">
      <Item label="mode" value={meta.mode} />
      <Item label="data as of" value={meta.data_as_of ?? "—"} />
      <Item label="took" value={`${meta.elapsed_ms} ms`} />
      <Item label="rows transferred" value={String(meta.rows_transferred)} />
      <Item
        label="nodes asked"
        value={meta.nodes_contacted.join(", ") || "none"}
      />
      <div>
        <dt className="text-slate-500">missing regions</dt>
        <dd
          className={
            missing.length ? "font-semibold text-amber-700" : "text-slate-800"
          }
        >
          {missing.length ? missing.join(", ") : "none"}
        </dd>
      </div>
    </dl>
  );
}

function Item({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-slate-500">{label}</dt>
      <dd className="font-mono text-slate-800">{value}</dd>
    </div>
  );
}

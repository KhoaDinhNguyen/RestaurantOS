import { useState } from "react";
import { searchVendors } from "../../api/endpoints";
import { ApiStatus } from "../../components/ApiStatus";
import { Field, Panel } from "../../components/Panel";
import {
  buttonClass,
  inputClass,
  subtleButtonClass,
} from "../../components/styles";
import { usePolling } from "../../hooks/usePolling";

/** `GET /vendors?geohash=&q=`: restaurants in one cell, no neighbours. */
export function VendorSearch({
  geohash,
  selected,
  onSelect,
}: {
  geohash: string;
  selected: string | null;
  onSelect: (vendorId: string) => void;
}) {
  const [q, setQ] = useState("");
  const [submitted, setSubmitted] = useState<{
    geohash: string;
    q: string;
  } | null>(null);
  const { result, loading } = usePolling(
    submitted && `vendors|${submitted.geohash}|${submitted.q}`,
    () => searchVendors(submitted!.geohash, submitted!.q),
    0,
  );

  return (
    <Panel title="Restaurants in my cell">
      <form
        className="mb-3 flex flex-wrap items-end gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          setSubmitted({ geohash, q });
        }}
      >
        <Field label="cuisine or product (optional)">
          <input
            className={inputClass}
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
        </Field>
        <button className={buttonClass} type="submit" disabled={!geohash}>
          Search {geohash || "…"}
        </button>
      </form>
      <ApiStatus result={result} loading={loading} />
      {result?.data && (
        <ul className="space-y-1">
          {result.data.map((v) => (
            <li key={v.vendor_id} className="flex items-center gap-2 text-sm">
              <button
                className={subtleButtonClass}
                disabled={selected === v.vendor_id}
                onClick={() => onSelect(v.vendor_id)}
              >
                {selected === v.vendor_id ? "selected" : "Open menu"}
              </button>
              <span className="font-mono">{v.vendor_id}</span>
              <span className="text-slate-500">
                {v.primary_cuisine ?? "—"}
                {v.chain_id && ` · chain ${v.chain_id}`}
              </span>
            </li>
          ))}
          {result.data.length === 0 && (
            <li className="text-sm text-slate-400">No restaurants found.</li>
          )}
        </ul>
      )}
    </Panel>
  );
}

import { useState } from "react";
import { nearby, searchDishes } from "../../api/endpoints";
import { ApiStatus } from "../../components/ApiStatus";
import { Field, Panel } from "../../components/Panel";
import { QueryMetaView } from "../../components/QueryMetaView";
import {
  buttonClass,
  inputClass,
  subtleButtonClass,
  tableClass,
  tdClass,
  thClass,
} from "../../components/styles";
import { usePolling } from "../../hooks/usePolling";

/**
 * "Who near me can make this now?": `GET /dishes?q=` finds the dishes that
 * match a name, and `GET /query/nearby` runs once per dish picked. One name
 * can be several dishes (see `scope`), so each dish's matches stay separate.
 */
export function DishNearbySearch({ geohash }: { geohash: string }) {
  const [q, setQ] = useState("");
  const [submitted, setSubmitted] = useState<string | null>(null);
  const [picked, setPicked] = useState<string[]>([]);
  const dishes = usePolling(
    submitted && `dishes|${submitted}`,
    () => searchDishes(submitted!),
    0,
  );

  const togglePick = (dishId: string) =>
    setPicked((current) =>
      current.includes(dishId)
        ? current.filter((d) => d !== dishId)
        : [...current, dishId],
    );

  return (
    <Panel title="Find a dish nearby">
      <form
        className="mb-3 flex flex-wrap items-end gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          setPicked([]);
          setSubmitted(q);
        }}
      >
        <Field label="dish name">
          <input
            className={inputClass}
            value={q}
            onChange={(e) => setQ(e.target.value)}
            required
          />
        </Field>
        <button className={buttonClass} type="submit">
          Search dishes
        </button>
      </form>
      <ApiStatus result={dishes.result} loading={dishes.loading} />
      {dishes.result?.data && (
        <ul className="mb-3 space-y-1">
          {dishes.result.data.map((d) => (
            <li key={d.dish_id} className="flex items-center gap-2 text-sm">
              <button
                className={subtleButtonClass}
                disabled={!geohash}
                onClick={() => togglePick(d.dish_id)}
              >
                {picked.includes(d.dish_id) ? "Hide" : "Nearby"}
              </button>
              {d.dish_name ?? d.example_name}
              <span className="font-mono text-xs text-slate-400">
                {d.dish_id} · {d.scope} · {d.n_vendors} restaurants
              </span>
            </li>
          ))}
          {dishes.result.data.length === 0 && (
            <li className="text-sm text-slate-400">No dish matches.</li>
          )}
        </ul>
      )}
      <div className="space-y-4">
        {picked.map((dishId) => (
          <NearbyResults key={dishId} geohash={geohash} dishId={dishId} />
        ))}
      </div>
    </Panel>
  );
}

function NearbyResults({
  geohash,
  dishId,
}: {
  geohash: string;
  dishId: string;
}) {
  const { result, loading } = usePolling(
    `nearby|${geohash}|${dishId}`,
    () => nearby(geohash, dishId),
    0,
  );
  const answer = result?.data;
  return (
    <div className="rounded border border-slate-200 p-3">
      <ApiStatus result={result} loading={loading} />
      {answer && (
        <div className="space-y-2">
          <h3 className="text-sm font-semibold">
            {answer.dish_name ?? answer.example_name}{" "}
            <span className="font-mono text-xs text-slate-400">
              {answer.dish_id}
            </span>{" "}
            near {answer.geohash}
          </h3>
          <QueryMetaView meta={answer.meta} />
          <table className={tableClass}>
            <thead>
              <tr>
                <th className={thClass}>restaurant</th>
                <th className={thClass}>product</th>
                <th className={thClass}>price units</th>
                <th className={thClass}>distance m</th>
                <th className={thClass}>region</th>
              </tr>
            </thead>
            <tbody>
              {answer.results.map((r) => (
                <tr key={`${r.vendor_id}|${r.product_id}`}>
                  <td className={`${tdClass} font-mono`}>{r.vendor_id}</td>
                  <td className={tdClass}>{r.name}</td>
                  <td className={tdClass}>{r.price_units}</td>
                  <td className={tdClass}>{r.distance_m}</td>
                  <td className={tdClass}>{r.region_id}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {answer.results.length === 0 && (
            <p className="text-sm text-slate-400">
              Nobody nearby can make it right now.
            </p>
          )}
        </div>
      )}
    </div>
  );
}

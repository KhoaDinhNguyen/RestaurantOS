import { useState } from "react";
import { topDishes, type RangeParams } from "../../api/endpoints";
import type { RankBy } from "../../api/types";
import { ApiStatus } from "../../components/ApiStatus";
import { Field, Panel } from "../../components/Panel";
import { QueryMetaView } from "../../components/QueryMetaView";
import {
  buttonClass,
  inputClass,
  tableClass,
  tdClass,
  thClass,
} from "../../components/styles";
import { usePolling } from "../../hooks/usePolling";
import { useSimClock } from "../../hooks/useSimClock";
import { emptyRange, withDefaultDays } from "./range";
import { RangeFields } from "./RangeFields";

interface Submitted {
  range: RangeParams;
  rankBy: RankBy;
  limit: string;
}

/** `GET /query/top-dishes`, rows in the order the server ranked them. */
export function TopDishesPanel() {
  const { day } = useSimClock();
  const [range, setRange] = useState<RangeParams>(emptyRange);
  const [rankBy, setRankBy] = useState<RankBy>("units");
  const [limit, setLimit] = useState("10");
  const [auto, setAuto] = useState(false);
  const [submitted, setSubmitted] = useState<Submitted | null>(null);

  const { result, loading, refresh } = usePolling(
    submitted && JSON.stringify(submitted),
    () => topDishes(submitted!.range, submitted!.rankBy, submitted!.limit),
    auto ? 2000 : 0,
  );
  const answer = result?.data;

  return (
    <Panel title="Top dishes">
      <form
        className="mb-3 flex flex-wrap items-end gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          const next = { range: withDefaultDays(range, day), rankBy, limit };
          // Run with unchanged fields asks again (after a 503, or for fresh data).
          if (JSON.stringify(next) === JSON.stringify(submitted)) refresh();
          else setSubmitted(next);
        }}
      >
        <RangeFields value={range} onChange={setRange} simDay={day} />
        <Field label="rank by">
          <select
            className={inputClass}
            value={rankBy}
            onChange={(e) => setRankBy(e.target.value as RankBy)}
          >
            <option value="units">units</option>
            <option value="quantity">quantity</option>
            <option value="order_lines">order_lines</option>
            <option value="sales_units">sales_units</option>
          </select>
        </Field>
        <Field label="limit">
          <input
            className={`${inputClass} w-16`}
            value={limit}
            onChange={(e) => setLimit(e.target.value)}
          />
        </Field>
        <label className="flex items-center gap-1 text-xs">
          <input
            type="checkbox"
            checked={auto}
            onChange={(e) => setAuto(e.target.checked)}
          />
          refresh every 2 s
        </label>
        <button className={buttonClass} type="submit">
          Run
        </button>
      </form>

      <ApiStatus result={result} loading={loading} />
      {answer && (
        <div className="space-y-3">
          <QueryMetaView meta={answer.meta} />
          <p className="text-xs text-slate-500">
            days {answer.from_day}–{answer.to_day}
            {answer.from_time && `, ${answer.from_time} to ${answer.to_time}`} ·
            ranked by {answer.rank_by}
          </p>
          <table className={tableClass}>
            <thead>
              <tr>
                <th className={thClass}>dish</th>
                <th className={thClass}>units</th>
                <th className={thClass}>quantity</th>
                <th className={thClass}>order lines</th>
                <th className={thClass}>sales units</th>
              </tr>
            </thead>
            <tbody>
              {answer.results.map((r) => (
                <tr key={r.dish_id}>
                  <td className={tdClass}>
                    {r.dish_name ?? r.example_name}{" "}
                    <span className="font-mono text-xs text-slate-400">
                      {r.dish_id}
                    </span>
                  </td>
                  <td className={tdClass}>{r.units}</td>
                  <td className={tdClass}>{r.quantity}</td>
                  <td className={tdClass}>{r.order_lines}</td>
                  <td className={tdClass}>{r.sales_units}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}

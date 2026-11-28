import { useState } from "react";
import { ingredientUsage, type RangeParams } from "../../api/endpoints";
import type { Strategy } from "../../api/types";
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

/** `GET /query/ingredient-usage`: the distributed join, with its strategy. */
export function IngredientUsagePanel() {
  const { day } = useSimClock();
  const [range, setRange] = useState<RangeParams>(emptyRange);
  const [strategy, setStrategy] = useState<Strategy>("C");
  const [submitted, setSubmitted] = useState<{
    range: RangeParams;
    strategy: Strategy;
  } | null>(null);

  const { result, loading, refresh } = usePolling(
    submitted && JSON.stringify(submitted),
    () => ingredientUsage(submitted!.range, submitted!.strategy),
    0,
  );
  const answer = result?.data;

  return (
    <Panel title="Ingredient usage">
      <form
        className="mb-3 flex flex-wrap items-end gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          const next = { range: withDefaultDays(range, day), strategy };
          // Run with unchanged fields asks again (after a 503, or for fresh data).
          if (JSON.stringify(next) === JSON.stringify(submitted)) refresh();
          else setSubmitted(next);
        }}
      >
        <RangeFields value={range} onChange={setRange} simDay={day} />
        <Field label="strategy (live only)">
          <select
            className={inputClass}
            value={strategy}
            disabled={range.mode !== "live"}
            onChange={(e) => setStrategy(e.target.value as Strategy)}
          >
            <option value="A">A</option>
            <option value="B">B</option>
            <option value="C">C</option>
          </select>
        </Field>
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
            {answer.from_time && `, ${answer.from_time} to ${answer.to_time}`}
            {" · "}strategy {answer.strategy ?? "—"}
          </p>
          <table className={tableClass}>
            <thead>
              <tr>
                <th className={thClass}>ingredient</th>
                <th className={thClass}>amount</th>
                <th className={thClass}>unit</th>
              </tr>
            </thead>
            <tbody>
              {answer.results.map((r) => (
                <tr key={`${r.ingredient_id}|${r.unit}`}>
                  <td className={`${tdClass} font-mono`}>{r.ingredient_id}</td>
                  <td className={tdClass}>{r.amount}</td>
                  <td className={tdClass}>{r.unit ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}

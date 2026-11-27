import type { RangeParams } from "../../api/endpoints";
import type { QueryMode } from "../../api/types";
import { Field } from "../../components/Panel";
import { inputClass } from "../../components/styles";

/**
 * Mode, day range and time window shared by top-dishes and ingredient-usage
 * (architecture Section 6.1, "Day range and time window"). Values are sent as
 * typed; the server checks them.
 */
export function RangeFields({
  value,
  onChange,
  simDay,
}: {
  value: RangeParams;
  onChange: (next: RangeParams) => void;
  simDay: number | null;
}) {
  const set = (key: keyof RangeParams) => (v: string) =>
    onChange({ ...value, [key]: v });
  const dayHint = simDay === null ? "day" : `day (${simDay})`;
  return (
    <>
      <Field label="mode">
        <select
          className={inputClass}
          value={value.mode}
          onChange={(e) => set("mode")(e.target.value as QueryMode)}
        >
          <option value="live">live</option>
          <option value="hq">hq</option>
          <option value="baseline">baseline</option>
        </select>
      </Field>
      <Field label="from day">
        <input
          className={`${inputClass} w-20`}
          placeholder={dayHint}
          value={value.from_day}
          onChange={(e) => set("from_day")(e.target.value)}
        />
      </Field>
      <Field label="to day">
        <input
          className={`${inputClass} w-20`}
          placeholder={dayHint}
          value={value.to_day}
          onChange={(e) => set("to_day")(e.target.value)}
        />
      </Field>
      <Field label="from time">
        <input
          className={`${inputClass} w-24`}
          placeholder="HH:MM:SS"
          value={value.from_time}
          onChange={(e) => set("from_time")(e.target.value)}
        />
      </Field>
      <Field label="to time">
        <input
          className={`${inputClass} w-24`}
          placeholder="HH:MM:SS"
          value={value.to_time}
          onChange={(e) => set("to_time")(e.target.value)}
        />
      </Field>
    </>
  );
}

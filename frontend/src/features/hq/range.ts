import type { RangeParams } from "../../api/endpoints";

export const emptyRange: RangeParams = {
  mode: "live",
  from_day: "",
  to_day: "",
  from_time: "",
  to_time: "",
};

/** Empty day fields mean "the current simulated day". */
export function withDefaultDays(
  range: RangeParams,
  simDay: number | null,
): RangeParams {
  const day = simDay === null ? "" : String(simDay);
  return {
    ...range,
    from_day: range.from_day || day,
    to_day: range.to_day || day,
  };
}

import { useContext } from "react";
import { SimClockContext, type SimClockValue } from "../features/clock/context";

/**
 * The simulated day and time every screen works in. They come from
 * `GET /sim/clock`; when the clock has no day yet (before the first run) or
 * the control plane does not answer, they are the day and time the user
 * picked in the header. Never the browser's clock.
 */
export function useSimClock(): SimClockValue {
  return useContext(SimClockContext);
}

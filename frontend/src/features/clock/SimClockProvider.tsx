import { useMemo, useState, type ReactNode } from "react";
import { getClock } from "../../api/endpoints";
import { usePolling } from "../../hooks/usePolling";
import { SimClockContext, type SimClockValue } from "./context";

/** Polls `GET /sim/clock` once a second and shares the simulated day and time. */
export function SimClockProvider({ children }: { children: ReactNode }) {
  const { result } = usePolling("clock", getClock, 1000);
  const [pickedDay, setPickedDay] = useState("");
  const [pickedTime, setPickedTime] = useState("12:00:00");

  const value = useMemo<SimClockValue>(() => {
    const clock = result?.data ?? null;
    const fromClock = clock?.sim_day !== null && clock?.sim_day !== undefined;
    const picked = /^\d+$/.test(pickedDay) ? Number(pickedDay) : null;
    return {
      clock: result,
      day: fromClock ? clock!.sim_day : picked,
      time: fromClock && clock!.sim_time ? clock!.sim_time : pickedTime,
      fromClock,
      pickedDay,
      pickedTime,
      setPickedDay,
      setPickedTime,
    };
  }, [result, pickedDay, pickedTime]);

  return (
    <SimClockContext.Provider value={value}>
      {children}
    </SimClockContext.Provider>
  );
}

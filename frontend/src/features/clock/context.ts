import { createContext } from "react";
import type { ApiResult } from "../../api/client";
import type { SimClock } from "../../api/types";

export interface SimClockValue {
  /** The last `GET /sim/clock` answer. */
  clock: ApiResult<SimClock> | null;
  /** Simulated day to use: the clock's, else the one the user picked. */
  day: number | null;
  /** Simulated time to use (HH:MM:SS): the clock's, else the picked one. */
  time: string;
  /** True when `day` and `time` come from the clock, not from the user. */
  fromClock: boolean;
  pickedDay: string;
  pickedTime: string;
  setPickedDay: (day: string) => void;
  setPickedTime: (time: string) => void;
}

export const SimClockContext = createContext<SimClockValue>({
  clock: null,
  day: null,
  time: "12:00:00",
  fromClock: false,
  pickedDay: "",
  pickedTime: "12:00:00",
  setPickedDay: () => {},
  setPickedTime: () => {},
});

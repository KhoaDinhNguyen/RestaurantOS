import { useSimClock } from "../../hooks/useSimClock";
import { inputClass } from "../../components/styles";

/** Header strip: the simulated clock, or a day picker when there is none. */
export function ClockBar() {
  const {
    clock,
    fromClock,
    pickedDay,
    pickedTime,
    setPickedDay,
    setPickedTime,
  } = useSimClock();
  const data = clock?.data;

  if (fromClock && data) {
    return (
      <div className="flex items-center gap-3 text-sm">
        <span className="rounded bg-slate-800 px-2 py-1 font-mono">
          day {data.sim_day} · {data.sim_time}
        </span>
        <span>
          {data.state} · {data.speed}x
        </span>
      </div>
    );
  }

  const reason =
    clock === null
      ? "reading the clock…"
      : clock.data
        ? "no run yet: pick a day"
        : "control plane not answering: pick a day";

  return (
    <div className="flex flex-wrap items-center gap-2 text-sm">
      <span className="text-amber-300">{reason}</span>
      <label className="flex items-center gap-1">
        day
        <input
          aria-label="simulated day"
          className={`${inputClass} w-20 text-slate-900`}
          inputMode="numeric"
          placeholder="14–90"
          value={pickedDay}
          onChange={(e) => setPickedDay(e.target.value)}
        />
      </label>
      <label className="flex items-center gap-1">
        time
        <input
          aria-label="simulated time"
          className={`${inputClass} w-28 text-slate-900`}
          placeholder="HH:MM:SS"
          value={pickedTime}
          onChange={(e) => setPickedTime(e.target.value)}
        />
      </label>
    </div>
  );
}

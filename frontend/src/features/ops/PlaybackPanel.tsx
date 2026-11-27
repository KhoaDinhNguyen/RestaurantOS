import { useState } from "react";
import { commandPlayback, getPlayback, setSpeed } from "../../api/endpoints";
import type { PlaybackCommand } from "../../api/types";
import { ApiStatus } from "../../components/ApiStatus";
import { Field, Panel } from "../../components/Panel";
import {
  buttonClass,
  inputClass,
  subtleButtonClass,
} from "../../components/styles";
import { useAction } from "../../hooks/useAction";
import { usePolling } from "../../hooks/usePolling";
import { useSimClock } from "../../hooks/useSimClock";

/** Fields shown in their own places, not in the counter grid. */
const NOT_COUNTERS = new Set([
  "state",
  "run_id",
  "seed",
  "from_day",
  "to_day",
  "speed",
  "engine",
  "dispatched_sim_day",
  "dispatched_sim_time",
  "held_sim_day",
  "held_sim_time",
  "next_due_sim_day",
  "next_due_sim_time",
]);

const instant = (day: unknown, time: unknown) =>
  day === null || day === undefined
    ? "—"
    : `day ${String(day)} · ${String(time)}`;

/** `/sim/clock` and `/sim/playback`: start, pause, resume, stop, speed. */
export function PlaybackPanel() {
  const { clock } = useSimClock();
  const playback = usePolling("playback", getPlayback, 1000);
  const command = useAction(commandPlayback);
  const speedChange = useAction(setSpeed);
  const [start, setStart] = useState({
    from_day: "14",
    to_day: "",
    speed: "1000",
    seed: "",
    run_id: "",
    duplicate_rate: "",
    shuffle: false,
  });
  const [speed, setSpeedText] = useState("1000");

  const send = async (cmd: PlaybackCommand) => {
    await command.run(cmd);
    playback.refresh();
  };

  const onStart = () => {
    const cmd: PlaybackCommand = {
      action: "start",
      from_day: Number(start.from_day),
    };
    if (start.to_day) cmd.to_day = Number(start.to_day);
    if (start.speed) cmd.speed = Number(start.speed);
    if (start.seed) cmd.seed = Number(start.seed);
    if (start.run_id) cmd.run_id = start.run_id;
    if (start.duplicate_rate) cmd.duplicate_rate = Number(start.duplicate_rate);
    if (start.shuffle) cmd.shuffle = true;
    void send(cmd);
  };

  const state = playback.result?.data;
  const c = clock?.data;
  const set =
    (key: Exclude<keyof typeof start, "shuffle">) =>
    (e: { target: { value: string } }) =>
      setStart({ ...start, [key]: e.target.value });

  return (
    <Panel title="Clock and playback">
      <div className="mb-3 flex flex-wrap gap-4 text-sm">
        <div>
          <div className="text-xs text-slate-500">simulated clock</div>
          <div className="font-mono text-lg">
            {c
              ? c.sim_day === null
                ? "no run yet"
                : `day ${c.sim_day} · ${c.sim_time}`
              : "—"}
          </div>
        </div>
        <div>
          <div className="text-xs text-slate-500">state</div>
          <div className="text-lg">{c?.state ?? "—"}</div>
        </div>
        <div>
          <div className="text-xs text-slate-500">speed</div>
          <div className="text-lg">{c?.speed ?? "—"}x</div>
        </div>
      </div>
      <ApiStatus result={clock} />

      <form
        className="mb-3 flex flex-wrap items-end gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          onStart();
        }}
      >
        <Field label="from day">
          <input
            className={`${inputClass} w-16`}
            value={start.from_day}
            onChange={set("from_day")}
            required
          />
        </Field>
        <Field label="to day">
          <input
            className={`${inputClass} w-16`}
            value={start.to_day}
            onChange={set("to_day")}
          />
        </Field>
        <Field label="speed">
          <input
            className={`${inputClass} w-20`}
            value={start.speed}
            onChange={set("speed")}
          />
        </Field>
        <Field label="seed">
          <input
            className={`${inputClass} w-16`}
            value={start.seed}
            onChange={set("seed")}
          />
        </Field>
        <Field label="run id">
          <input
            className={`${inputClass} w-32`}
            value={start.run_id}
            onChange={set("run_id")}
          />
        </Field>
        <Field label="duplicate rate">
          <input
            className={`${inputClass} w-16`}
            placeholder="0"
            inputMode="decimal"
            value={start.duplicate_rate}
            onChange={set("duplicate_rate")}
          />
        </Field>
        <label className="flex items-center gap-1 pb-1 text-xs text-slate-600">
          <input
            type="checkbox"
            checked={start.shuffle}
            onChange={(e) => setStart({ ...start, shuffle: e.target.checked })}
          />
          shuffle
        </label>
        <button
          className={buttonClass}
          type="submit"
          disabled={command.pending}
        >
          Start
        </button>
        <button
          type="button"
          className={subtleButtonClass}
          onClick={() => void send({ action: "pause" })}
        >
          Pause
        </button>
        <button
          type="button"
          className={subtleButtonClass}
          onClick={() => void send({ action: "resume" })}
        >
          Resume
        </button>
        <button
          type="button"
          className={subtleButtonClass}
          onClick={() => void send({ action: "stop" })}
        >
          Stop
        </button>
      </form>

      <form
        className="mb-3 flex items-end gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          void speedChange.run(Number(speed)).then(playback.refresh);
        }}
      >
        <Field label="change speed while running">
          <input
            className={`${inputClass} w-24`}
            value={speed}
            onChange={(e) => setSpeedText(e.target.value)}
          />
        </Field>
        <button className={subtleButtonClass} type="submit">
          Set speed
        </button>
      </form>
      <ApiStatus result={command.result} />
      <ApiStatus result={speedChange.result} />

      <ApiStatus result={playback.result} loading={playback.loading} />
      {state && (
        <div className="space-y-2 text-sm">
          <p className="font-mono text-xs text-slate-500">
            run {state.run_id ?? "—"} · seed {state.seed ?? "—"} · days{" "}
            {state.from_day ?? "—"}–{state.to_day ?? "—"}
          </p>
          <dl className="grid gap-2 text-xs sm:grid-cols-3">
            <div>
              <dt className="text-slate-500">dispatched up to</dt>
              <dd className="font-mono">
                {instant(state.dispatched_sim_day, state.dispatched_sim_time)}
              </dd>
            </div>
            <div>
              <dt className="text-slate-500">earliest held order</dt>
              <dd className="font-mono">
                {instant(state.held_sim_day, state.held_sim_time)}
              </dd>
            </div>
            <div>
              <dt className="text-slate-500">next due</dt>
              <dd className="font-mono">
                {instant(state.next_due_sim_day, state.next_due_sim_time)}
              </dd>
            </div>
          </dl>
          <dl className="grid grid-cols-2 gap-2 sm:grid-cols-4 lg:grid-cols-6">
            {Object.entries(state)
              .filter(([key]) => !NOT_COUNTERS.has(key))
              .map(([key, value]) => (
                <div key={key} className="rounded bg-slate-50 px-2 py-1">
                  <dt className="text-xs text-slate-500">{key}</dt>
                  <dd className="font-mono">
                    {value !== null && typeof value === "object"
                      ? JSON.stringify(value)
                      : String(value)}
                  </dd>
                </div>
              ))}
          </dl>
          {state.engine && (
            <details className="text-xs">
              <summary className="cursor-pointer text-slate-500">
                engine (fixed when the playback process started)
              </summary>
              <pre className="mt-1 overflow-x-auto rounded bg-slate-50 p-2">
                {JSON.stringify(state.engine, null, 2)}
              </pre>
            </details>
          )}
        </div>
      )}
    </Panel>
  );
}

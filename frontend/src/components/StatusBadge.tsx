const TONES: Record<string, string> = {
  received: "bg-slate-100 text-slate-700",
  accepted: "bg-sky-100 text-sky-800",
  preparing: "bg-indigo-100 text-indigo-800",
  ready: "bg-emerald-100 text-emerald-800",
  picked_up: "bg-emerald-600 text-white",
  canceled: "bg-rose-100 text-rose-800",
  up: "bg-emerald-100 text-emerald-800",
  suspect: "bg-amber-100 text-amber-900",
  down: "bg-rose-600 text-white",
  primary: "bg-slate-800 text-white",
  standby: "bg-slate-200 text-slate-700",
  running: "bg-sky-100 text-sky-800",
  done: "bg-emerald-100 text-emerald-800",
  failed: "bg-rose-600 text-white",
  PASSED: "bg-emerald-600 text-white",
  FAILED: "bg-rose-600 text-white",
};

/**
 * A coloured label for an order status, a node health or role, a job state
 * or a verdict.
 */
export function StatusBadge({ value }: { value: string }) {
  return (
    <span
      className={`inline-block rounded px-1.5 py-0.5 text-xs font-medium ${
        TONES[value] ?? "bg-slate-100 text-slate-700"
      }`}
    >
      {value}
    </span>
  );
}

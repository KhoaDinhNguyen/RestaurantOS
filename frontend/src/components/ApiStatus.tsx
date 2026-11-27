import type { ApiResult } from "../api/client";

/**
 * Shows why a call did not give data. A 503 is the expected answer while a
 * region is cut off, so it is shown plainly, not hidden.
 */
export function ApiStatus<T>({
  result,
  loading,
}: {
  result: ApiResult<T> | null;
  loading?: boolean;
}) {
  if (result === null)
    return loading ? <p className="text-sm text-slate-400">loading…</p> : null;
  if (result.data !== null) return null;

  const { status, error } = result;
  let tone = "border-rose-300 bg-rose-50 text-rose-800";
  let text: string;
  if (status === 0) {
    text = "No answer: the service could not be reached.";
  } else if (status === 503 && error?.error === "region_unreachable") {
    tone = "border-amber-300 bg-amber-50 text-amber-900";
    text = `503: region ${error.region_id} cannot be reached right now.`;
  } else if (status === 503 && error?.error === "hq_unreachable") {
    tone = "border-amber-300 bg-amber-50 text-amber-900";
    text = "503: HQ's database cannot be reached right now.";
  } else if (status === 404) {
    text = "404: not found.";
  } else if (status >= 500) {
    text = `${status}: the service did not answer (is it running?).`;
  } else {
    text = `${status}: ${error?.error ?? "error"}${error?.message ? ` (${error.message})` : ""}`;
  }

  return (
    <p role="alert" className={`rounded border px-3 py-2 text-sm ${tone}`}>
      {text}
    </p>
  );
}

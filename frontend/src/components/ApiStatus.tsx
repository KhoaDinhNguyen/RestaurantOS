import type { ApiResult } from "../api/client";
import type { ErrorBody } from "../api/types";

/**
 * FastAPI answers some errors as `{"detail": ...}` instead of the project's
 * `{"error", "message"}`; the client keeps that body under `detail`.
 */
function fastApiDetail(error: ErrorBody | null): string | null {
  const body = error?.detail;
  if (!body || typeof body !== "object" || !("detail" in body)) return null;
  const detail = (body as { detail: unknown }).detail;
  return typeof detail === "string" ? detail : JSON.stringify(detail);
}

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
  const detail = fastApiDetail(error);
  let tone = "border-rose-300 bg-rose-50 text-rose-800";
  let text: string;
  if (status === 403 && detail?.includes("cross-origin")) {
    // The control plane's origin guard (architecture Section 6.3).
    text =
      `403: the control plane refused this page's origin (${window.location.origin}). ` +
      "List it in ROS_SIM_CONTROL_ALLOWED_ORIGINS in the repository's .env and restart `rosctl control`.";
  } else if (status === 0) {
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
  } else if (detail !== null) {
    text = `${status}: ${detail}`;
  } else {
    text = `${status}: ${error?.error ?? "error"}${error?.message ? ` (${error.message})` : ""}`;
  }

  return (
    <p role="alert" className={`rounded border px-3 py-2 text-sm ${tone}`}>
      {text}
    </p>
  );
}

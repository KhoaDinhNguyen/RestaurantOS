import type { ErrorBody } from "./types";

/** What one HTTP call produced. A call never throws for an HTTP status. */
export interface ApiResult<T> {
  /** HTTP status, or 0 when no answer came back at all. */
  status: number;
  /** The body, for 2xx answers and for the statuses listed in `dataStatuses`. */
  data: T | null;
  /** The body of any other answer, or a made-up one when nothing came back. */
  error: ErrorBody | null;
  /** Wall-clock time the request took. The only use of the browser clock. */
  elapsedMs: number;
}

export interface RawResponse {
  status: number;
  body: unknown;
}

/** Sends one request. Swapped for the mock server in `--mode mock` and in tests. */
export type Transport = (
  method: string,
  url: string,
  body?: unknown,
) => Promise<RawResponse>;

export const fetchTransport: Transport = async (method, url, body) => {
  const response = await fetch(url, {
    method,
    headers:
      body === undefined ? undefined : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  let parsed: unknown = null;
  if (text) {
    try {
      parsed = JSON.parse(text);
    } catch {
      parsed = { error: "not_json", message: text.slice(0, 200) };
    }
  }
  return { status: response.status, body: parsed };
};

let transport: Transport = fetchTransport;
let mockReady: Promise<void> | null = null;

if (import.meta.env.MODE === "mock") {
  mockReady = import("../mocks/mockServer").then((m) => {
    transport = m.mockTransport;
  });
}

export function setTransport(next: Transport): void {
  transport = next;
  mockReady = null;
}

export interface RequestOptions {
  body?: unknown;
  /** Non-2xx statuses whose body is the normal response, e.g. 409 for an order. */
  dataStatuses?: number[];
}

export async function request<T>(
  method: string,
  url: string,
  options: RequestOptions = {},
): Promise<ApiResult<T>> {
  if (mockReady) await mockReady;
  const started = performance.now();
  try {
    const { status, body } = await transport(method, url, options.body);
    const elapsedMs = performance.now() - started;
    const isData =
      (status >= 200 && status < 300) ||
      (options.dataStatuses ?? []).includes(status);
    if (isData) return { status, data: body as T, error: null, elapsedMs };
    const error =
      body && typeof body === "object" && "error" in body
        ? (body as ErrorBody)
        : { error: "http_error", detail: body };
    return { status, data: null, error, elapsedMs };
  } catch (e) {
    return {
      status: 0,
      data: null,
      error: { error: "network", message: String(e) },
      elapsedMs: performance.now() - started,
    };
  }
}

/** Builds `?a=1&b=x`, leaving out empty values. Values are sent as typed. */
export function qs(
  params: Record<string, string | number | null | undefined>,
): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === null || value === undefined || value === "") continue;
    search.set(key, String(value));
  }
  const text = search.toString();
  return text ? `?${text}` : "";
}

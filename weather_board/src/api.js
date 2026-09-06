export async function api(path, options = {}) {
  const response = await fetch(path, {
    cache: "no-store",
    headers: {
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
    },
    ...options,
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.error || response.statusText);
  return payload;
}

export const fetchOverview = () => api("/api/overview");

export function fetchEvents({ status = "", q = "", limit = 1000 } = {}) {
  const params = new URLSearchParams({ status, q, limit: String(limit) });
  return api("/api/events?" + params);
}

export const fetchCandidates = () => api("/api/candidates?limit=200");
export const fetchStatus = () => api("/api/status");

export function scanOnce() {
  return api("/api/scan/once", { method: "POST", body: "{}" });
}

export const startService = () => api("/api/start", { method: "POST", body: "{}" });
export const stopService = () => api("/api/stop", { method: "POST", body: "{}" });


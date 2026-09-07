export const $ = (id) => document.getElementById(id);

export function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

export function pct(value) {
  const n = Number(value);
  return Number.isFinite(n) ? (n * 100).toFixed(1) + "%" : "—";
}

export function price(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(3) : "—";
}

export function num(value, digits = 2) {
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(digits) : "—";
}

export function fmtTime(value) {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
}

export function fmtStationTime(value, timeZone) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  const options = {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  };
  try {
    return new Intl.DateTimeFormat("sv-SE", timeZone ? { ...options, timeZone } : options)
      .format(date)
      .replace("T", " ");
  } catch (_error) {
    return date.toISOString().slice(0, 16).replace("T", " ");
  }
}

export function fmtTemp(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  const rounded = Math.round(n * 10) / 10;
  return Number.isInteger(rounded) ? String(rounded) : rounded.toFixed(1);
}

export function age(value) {
  if (!value) return "—";
  const stamp = new Date(value).getTime();
  if (Number.isNaN(stamp)) return "—";
  const seconds = Math.max(0, (Date.now() - stamp) / 1000);
  if (seconds < 60) return Math.round(seconds) + "s";
  if (seconds < 3600) return Math.round(seconds / 60) + "m";
  return Math.round(seconds / 3600) + "h";
}

export function statusClass(value) {
  const text = String(value || "").toLowerCase();
  if (text.includes("opportunity") || text === "final" || text === "matched") return "ok";
  if (text.includes("review") || text.includes("provisional") || text.includes("waiting")) return "warn";
  if (text.includes("no_trade") || text.includes("error") || text.includes("missing") || text.includes("unavailable")) return "bad";
  return "muted";
}

export function statusLabel(value) {
  return String(value || "unknown").replaceAll("_", " ");
}

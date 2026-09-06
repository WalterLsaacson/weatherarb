import { fetchCandidates, fetchEvents, fetchOverview, fetchStatus, scanOnce, startService, stopService } from "./api.js";
import { state } from "./state.js";
import { $, escapeHtml } from "./utils.js";
import { renderAll, renderDetail } from "./render.js";

function applySnapshot(snapshot) {
  const payload = snapshot || {};
  state.summary = payload.summary || state.summary;
  state.events = payload.event_groups || state.events;
  state.source = "sse";
  renderAll();
}

async function reload() {
  const [overview, events, candidates, status] = await Promise.all([
    fetchOverview(),
    fetchEvents({ status: state.eventFilter, q: state.query }),
    fetchCandidates(),
    fetchStatus(),
  ]);
  state.summary = overview.summary || {};
  state.service = status || overview.service || {};
  state.events = events.events || [];
  state.candidates = candidates.candidates || [];
  renderAll();
}

function connectStream() {
  if (state.stream) state.stream.close();
  if (!$("autoRefresh").checked) return;
  const stream = new EventSource("/api/stream");
  state.stream = stream;
  stream.onopen = () => {
    state.connected = true;
    renderAll();
  };
  stream.onerror = () => {
    state.connected = false;
    renderAll();
  };
  stream.addEventListener("snapshot", (event) => {
    try {
      const snapshot = JSON.parse(event.data);
      state.service = snapshot.status || state.service;
      applySnapshot(snapshot);
      fetchCandidates().then((payload) => {
        state.candidates = payload.candidates || [];
        renderAll();
      }).catch(() => {});
    } catch (error) {
      console.warn("invalid weather SSE snapshot", error);
    }
  });
  stream.addEventListener("source_update", (event) => {
    try {
      const payload = JSON.parse(event.data);
      state.events = payload.event_groups || state.events;
      renderAll();
    } catch (error) {
      console.warn("invalid weather source update", error);
    }
  });
  stream.addEventListener("candidate_update", (event) => {
    try {
      const payload = JSON.parse(event.data);
      state.candidates = payload.candidates || state.candidates;
      renderAll();
    } catch (error) {
      console.warn("invalid weather candidate update", error);
    }
  });
}

function toast(message, error = false) {
  const node = $("toast");
  node.textContent = message;
  node.className = "toast is-visible" + (error ? " toast--error" : "");
  window.setTimeout(() => node.classList.remove("is-visible"), 2600);
}

function bind() {
  $("search").addEventListener("input", async (event) => {
    state.query = event.target.value;
    try {
      const payload = await fetchEvents({ status: state.eventFilter, q: state.query });
      state.events = payload.events || [];
      renderAll();
    } catch (error) {
      toast(error.message, true);
    }
  });
  $("statusFilter").addEventListener("change", async (event) => {
    state.eventFilter = event.target.value;
    try {
      const payload = await fetchEvents({ status: state.eventFilter, q: state.query });
      state.events = payload.events || [];
      renderAll();
    } catch (error) {
      toast(error.message, true);
    }
  });
  $("autoRefresh").addEventListener("change", () => {
    if ($("autoRefresh").checked) connectStream();
    else if (state.stream) state.stream.close();
  });
  $("btnScan").addEventListener("click", async () => {
    try {
      $("btnScan").disabled = true;
      await scanOnce();
      await reload();
      toast("扫描完成");
    } catch (error) {
      toast(error.message, true);
    } finally {
      $("btnScan").disabled = false;
    }
  });
  $("btnStart").addEventListener("click", async () => {
    try {
      await startService();
      await reload();
      connectStream();
      toast("轮询已启动");
    } catch (error) {
      toast(error.message, true);
    }
  });
  $("btnStop").addEventListener("click", async () => {
    try {
      await stopService();
      await reload();
      toast("轮询已停止");
    } catch (error) {
      toast(error.message, true);
    }
  });
  $("btnCloseDetail").addEventListener("click", () => renderDetail(null));
  window.addEventListener("weather:select", async (event) => {
    try {
      const response = await fetch("/api/events/" + encodeURIComponent(event.detail), { cache: "no-store" });
      if (!response.ok) throw new Error("event " + response.status);
      renderDetail(await response.json());
    } catch (error) {
      toast(error.message, true);
    }
  });
}

async function init() {
  bind();
  try {
    await reload();
    connectStream();
  } catch (error) {
    $("eventRows").innerHTML = `<tr><td colspan="8" class="empty">连接失败：${escapeHtml(error.message)}</td></tr>`;
    toast(error.message, true);
  }
}

init();

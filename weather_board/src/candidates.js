import { fetchCandidates, fetchStatus } from "./api.js";
import { $, age, escapeHtml, fmtTemp, fmtTime, num, price } from "./utils.js";

const REASON_LABEL = {
  intradaily_impossible_no: "盘中已死，买 No",
  intraday_impossible_no: "盘中已死，买 No",
  provisional_loser_no: "暂定输家 No",
  source_final_loser_no: "源已终局，输家 No",
  intradaily_impossible_sell_yes: "盘中已死，卖 Yes",
  intraday_impossible_sell_yes: "盘中已死，卖 Yes",
  provisional_loser_sell_yes: "暂定输家，卖 Yes",
  source_final_loser_sell_yes: "源已终局，卖 Yes",
  dry_run_candidate_only: "匹配 Yes（dry-run）",
};

const state = {
  service: {},
  candidates: [],
  selected: null,
  query: "",
  side: "",
  reason: "",
  connected: false,
  stream: null,
};

function reasonLabel(value) {
  const key = String(value || "");
  return REASON_LABEL[key] || key.replaceAll("_", " ") || "—";
}

function question(row) {
  return (row.market || {}).question || row.event_group_id || "—";
}

function station(row) {
  return (row.observation || {}).station_id || (row.rule || {}).station_id || (row.lock || {}).station_id || "—";
}

function metric(row) {
  const rule = row.rule || {};
  const obs = row.observation || {};
  return obs.aggregation || rule.metric || "—";
}

function unit(row) {
  return (row.observation || {}).unit || (row.rule || {}).unit || "";
}

function obsDate(row) {
  const start = (row.rule || {}).observation_start || "";
  return start ? String(start).slice(0, 10) : "—";
}

function runningText(row) {
  const obs = row.observation || {};
  const value = obs.value;
  const u = unit(row);
  if (value == null || value === "") return "—";
  return fmtTemp(value) + (u ? " " + u : "");
}

function matches(row) {
  if (state.side && String(row.trade_side || "").toUpperCase() !== state.side) return false;
  if (state.reason && String(row.reason || "") !== state.reason) return false;
  const q = state.query.trim().toLowerCase();
  if (!q) return true;
  const blob = [
    station(row),
    question(row),
    row.event_group_id,
    row.target_outcome,
    row.matched_outcome,
    row.trade_side,
    row.reason,
    reasonLabel(row.reason),
  ]
    .join(" ")
    .toLowerCase();
  return blob.includes(q);
}

function sortedRows() {
  return (state.candidates || [])
    .filter(matches)
    .slice()
    .sort((a, b) => {
      const ae = Number((a.economics || {}).net_edge);
      const be = Number((b.economics || {}).net_edge);
      return (Number.isFinite(be) ? be : -1) - (Number.isFinite(ae) ? ae : -1);
    });
}

function renderKpis() {
  const rows = state.candidates || [];
  const nos = rows.filter((row) => String(row.trade_side || "").toUpperCase() === "NO").length;
  const yes = rows.filter((row) => String(row.trade_side || "").toUpperCase() === "YES").length;
  const locked = rows.filter((row) => row.lock && row.lock.locked).length;
  const edges = rows
    .map((row) => Number((row.economics || {}).net_edge))
    .filter((n) => Number.isFinite(n));
  const best = edges.length ? Math.max(...edges) : null;
  $("kpis").innerHTML = [
    kpi("候选", String(rows.length), "通过全部门"),
    kpi("买 No", String(nos), locked ? locked + " 已锁定" : "终局延迟"),
    kpi("买 Yes", String(yes), "须 source final"),
    kpi("最佳净边际", best == null ? "—" : num(best, 4), "VWAP 后扣费"),
  ].join("");
}

function kpi(title, value, detail) {
  return `<div class="kpi">
    <span class="kpi__title">${escapeHtml(title)}</span>
    <strong>${escapeHtml(value)}</strong>
    <small>${escapeHtml(detail || "")}</small>
  </div>`;
}

function renderMeta() {
  const service = state.service || {};
  $("pillConnection").textContent = state.connected ? "SSE 已连接" : "REST / 重连中";
  $("pillConnection").className = "pill " + (state.connected ? "" : "pill--warn");
  $("pillMode").textContent = service.dry_run === false ? "LIVE" : "DRY-RUN / READ-ONLY";
  $("pillScan").textContent = "最近扫描 " + fmtTime(service.last_scan_at);
  $("pillCount").textContent = "候选 " + String((state.candidates || []).length);
}

function renderTable() {
  const rows = sortedRows();
  const body = $("candidateRows");
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="9" class="empty">${
      (state.candidates || []).length ? "没有符合筛选的候选" : "当前没有通过盘口经济门的候选"
    }</td></tr>`;
    return;
  }
  body.innerHTML = rows
    .map((row) => {
      const eco = row.economics || {};
      const obs = row.observation || {};
      const side = String(row.trade_side || "YES").toUpperCase();
      const orderSide = String(row.order_side || (row.economics || {}).order_side || "BUY").toUpperCase();
      const locked = Boolean(row.lock && row.lock.locked);
      const selected =
        state.selected
        && state.selected.market_id === row.market_id
        && String(state.selected.order_side || "BUY") === orderSide;
      const sideLabel =
        orderSide === "SELL" ? side + " SELL" : side;
      const lockLabel = locked
        ? (orderSide === "SELL" ? "锁定卖 Yes" : "锁定 No")
        : "";
      return `<tr class="event-row desk-row${selected ? " is-selected" : ""}" data-market-id="${escapeHtml(
        row.market_id || "",
      )}" data-order-side="${escapeHtml(orderSide)}" tabindex="0">
        <td><strong>${escapeHtml(station(row))}</strong><small>${escapeHtml(question(row))}</small></td>
        <td>${escapeHtml(obsDate(row))}<small>${escapeHtml(metric(row))} · ${escapeHtml(obs.status || "—")}</small></td>
        <td><strong class="side side--${side === "NO" ? "no" : "yes"}">${escapeHtml(
          (row.target_outcome || row.matched_outcome || "—") + " " + sideLabel,
        )}</strong>${lockLabel ? `<small>${escapeHtml(lockLabel)}</small>` : ""}</td>
        <td>${escapeHtml(reasonLabel(row.reason))}</td>
        <td class="mono">${escapeHtml(runningText(row))}</td>
        <td class="mono">${price(eco.execution_price)}</td>
        <td class="mono">${num(eco.net_edge, 4)}</td>
        <td class="mono">${num(eco.ask_depth_at_limit ?? eco.bid_depth_at_floor, 1)}</td>
        <td><small>${escapeHtml(age((row.book || {}).fetched_at))}</small></td>
      </tr>`;
    })
    .join("");
  body.querySelectorAll(".desk-row").forEach((node) => {
    node.addEventListener("click", () => {
      const id = node.dataset.marketId || "";
      const orderSide = node.dataset.orderSide || "BUY";
      state.selected =
        (state.candidates || []).find(
          (row) =>
            String(row.market_id || "") === id
            && String(row.order_side || (row.economics || {}).order_side || "BUY") === orderSide,
        ) || null;
      renderTable();
      renderDetail();
    });
  });
}

function fact(label, value) {
  return `<span>${escapeHtml(label)} <b>${escapeHtml(value)}</b></span>`;
}

function renderDetail() {
  const drawer = $("detail");
  const row = state.selected;
  if (!row) {
    drawer.setAttribute("aria-hidden", "true");
    drawer.classList.remove("is-open");
    return;
  }
  const eco = row.economics || {};
  const obs = row.observation || {};
  const rule = row.rule || {};
  const book = row.book || {};
  const lock = row.lock || {};
  const asks = (book.asks || []).slice(0, 8);
  const side = String(row.trade_side || "YES").toUpperCase();
  const orderSide = String(row.order_side || eco.order_side || "BUY").toUpperCase();
  $("detailTitle").textContent =
    station(row)
    + " · "
    + (row.target_outcome || row.matched_outcome || "—")
    + " "
    + side
    + (orderSide === "SELL" ? " SELL" : "");
  $("detailBody").innerHTML = `
    <section class="detail-card">
      <div class="detail-title"><span class="eyebrow">WHY</span>${escapeHtml(reasonLabel(row.reason))}</div>
      <p>${escapeHtml(question(row))}</p>
      <div class="facts">
        ${fact("order", orderSide === "SELL" ? "SELL Yes（需持仓）" : "BUY " + side)}
        ${fact("station", station(row))}
        ${fact("metric", metric(row))}
        ${fact("盘中", runningText(row))}
        ${fact("源状态", obs.status || "—")}
        ${fact("sample_set", (rule.source || {}).sample_set || "all")}
        ${lock.locked ? fact("锁", "No 已锁定") : ""}
      </div>
    </section>
    <section class="detail-card">
      <div class="section-head"><div><span class="eyebrow">ECONOMICS</span><h3>费用与边际</h3></div></div>
      <div class="facts">
        ${fact("best ask", price(eco.best_ask))}
        ${fact("VWAP", price(eco.execution_price))}
        ${fact("slippage", num(eco.slippage, 4))}
        ${fact("fee", num(eco.fee_rate, 3))}
        ${fact("毛边际", num(eco.gross_edge, 4))}
        ${fact("净边际", num(eco.net_edge, 4))}
        ${fact("门槛", num(eco.min_net_edge, 4))}
        ${fact("max ask", price(eco.max_ask))}
        ${fact("shares", num(eco.execution_shares, 2))}
        ${fact("depth", num(eco.ask_depth_at_limit, 2))}
      </div>
    </section>
    <section class="detail-card">
      <div class="section-head"><div><span class="eyebrow">CLOB</span><h3>盘口</h3></div><span class="muted">${escapeHtml(
        book.book_missing ? book.error || "missing" : "age " + age(book.fetched_at),
      )}</span></div>
      <div class="ladder">${
        asks.length
          ? asks
              .map(
                (level) =>
                  `<div><span>${price(level.price)}</span><b>${num(level.size, 2)}</b></div>`,
              )
              .join("")
          : `<small>${escapeHtml(book.error || "无 asks")}</small>`
      }</div>
    </section>
    <section class="detail-card">
      <div class="section-head"><div><span class="eyebrow">SOURCE</span><h3>观测</h3></div></div>
      <div class="facts">
        ${fact("provider", obs.provider || "—")}
        ${fact("源时间", fmtTime(obs.source_timestamp))}
        ${fact("扫描", fmtTime(obs.observed_at || row.created_at))}
        ${fact("reason", obs.reason || "—")}
      </div>
      <p class="muted" style="margin-top:10px"><a href="/?event=${encodeURIComponent(
        row.event_group_id || "",
      )}">在总览打开事件</a></p>
    </section>`;
  drawer.setAttribute("aria-hidden", "false");
  drawer.classList.add("is-open");
}

function renderAll() {
  renderMeta();
  renderKpis();
  renderTable();
  if (state.selected) {
    const id = state.selected.market_id;
    const orderSide = String(state.selected.order_side || "BUY");
    state.selected =
      (state.candidates || []).find(
        (row) =>
          row.market_id === id
          && String(row.order_side || (row.economics || {}).order_side || "BUY") === orderSide,
      ) || state.selected;
    renderDetail();
  }
}

function toast(message, error = false) {
  const node = $("toast");
  node.textContent = message;
  node.className = "toast is-visible" + (error ? " toast--error" : "");
  window.setTimeout(() => node.classList.remove("is-visible"), 2600);
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
  stream.addEventListener("candidate_update", (event) => {
    try {
      const payload = JSON.parse(event.data);
      state.candidates = payload.candidates || [];
      if (payload.scanned_at) {
        state.service = { ...state.service, last_scan_at: payload.scanned_at };
      }
      renderAll();
    } catch (error) {
      console.warn("invalid candidate update", error);
    }
  });
  stream.addEventListener("health_update", (event) => {
    try {
      const payload = JSON.parse(event.data);
      state.service = payload.status || payload || state.service;
      renderMeta();
    } catch (error) {
      console.warn("invalid health update", error);
    }
  });
}

async function reload() {
  const [candidates, status] = await Promise.all([fetchCandidates(), fetchStatus()]);
  state.candidates = candidates.candidates || [];
  state.service = status || {};
  renderAll();
}

function bind() {
  $("search").addEventListener("input", (event) => {
    state.query = event.target.value;
    renderTable();
  });
  $("sideFilter").addEventListener("change", (event) => {
    state.side = event.target.value;
    renderTable();
  });
  $("reasonFilter").addEventListener("change", (event) => {
    state.reason = event.target.value;
    renderTable();
  });
  $("autoRefresh").addEventListener("change", () => {
    if ($("autoRefresh").checked) connectStream();
    else if (state.stream) state.stream.close();
  });
  $("btnCloseDetail").addEventListener("click", () => {
    state.selected = null;
    renderTable();
    renderDetail();
  });
}

async function init() {
  bind();
  try {
    await reload();
    connectStream();
  } catch (error) {
    $("candidateRows").innerHTML = `<tr><td colspan="9" class="empty">连接失败：${escapeHtml(error.message)}</td></tr>`;
    toast(error.message, true);
  }
}

init();

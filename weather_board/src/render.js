import { state } from "./state.js";
import { $, age, escapeHtml, fmtStationTime, fmtTemp, fmtTime, num, pct, price, statusClass, statusLabel } from "./utils.js";

function badge(value, label = value) {
  return `<span class="badge badge--${statusClass(value)}">${escapeHtml(statusLabel(label))}</span>`;
}

function groupMeta(group) {
  const first = group.rows?.[0] || {};
  const market = first.market || {};
  const rule = first.rule || {};
  const source = first.observation || {};
  const sampleSet = String(rule.source?.sample_set || source.sample_set || "all").toLowerCase();
  return {
    market,
    rule,
    source,
    station: source.station_id || rule.source?.station_id || group.station_id || "—",
    date: rule.observation_start ? String(rule.observation_start).slice(0, 10) : group.local_date || "—",
    metric: source.aggregation || rule.metric || group.metric || "—",
    question: market.question || group.question || group.event_group_id,
    sampleSet: sampleSet === "hourly" ? "hourly" : "all",
  };
}

function sampleSetLabel(meta) {
  return meta.sampleSet === "hourly" ? "Hourly Data" : "All data";
}

export function renderMeta() {
  const service = state.service || {};
  $("pillConnection").textContent = state.connected ? "SSE 已连接" : "REST / 重连中";
  $("pillConnection").className = "pill " + (state.connected ? "" : "pill--warn");
    $("pillMode").textContent = (service.trading && service.trading.live_orders) || service.dry_run === false
      ? "LIVE · max " + (service.trading && service.trading.max_order_usdc != null ? service.trading.max_order_usdc : "5") + " USDC"
      : "DRY-RUN / READ-ONLY";
  $("pillCircuit").textContent = "Circuit " + (service.circuit || "—");
  $("pillCircuit").className = "pill " + (service.circuit === "OPEN" ? "pill--bad" : "pill--muted");
  $("pillScan").textContent = "最近扫描 " + fmtTime(service.last_scan_at);
  $("pillProxy").textContent = "proxy " + (service.proxy || "direct");
  $("btnStart").disabled = !!service.running;
  $("btnStop").disabled = !service.running;
}

function kpi(title, value, detail, tone = "") {
  return `<div class="kpi ${tone ? "kpi--" + tone : ""}">
    <span class="kpi__title">${escapeHtml(title)}</span>
    <strong>${escapeHtml(value)}</strong>
    <small>${escapeHtml(detail || "")}</small>
  </div>`;
}

export function renderKpis() {
  const s = state.summary || {};
  $("kpis").innerHTML = [
    kpi(
      "事件组",
      num(s.event_groups, 0),
      (s.catalog_events != null ? num(s.catalog_events, 0) + " catalog · " : num(s.markets, 0) + " markets / ")
        + num(s.approved_event_groups ?? s.rules, 0) + " 已批规则"
        + (s.horizon_hours != null ? " · ±" + num(s.horizon_hours, 0) + "h" : ""),
    ),
    kpi("市场匹配率", pct(s.market_match_rate), (s.matched_markets ?? 0) + " / " + (s.eligible_markets ?? 0), s.market_match_rate > 0 ? "ok" : ""),
    kpi("Source final", num(s.source_final, 0), (s.final_event_groups ?? 0) + " 个事件组"),
    kpi("桶匹配率", pct(s.bucket_match_rate), (s.mapped_event_groups ?? 0) + " / " + (s.final_event_groups ?? 0), s.bucket_match_rate === 1 && s.final_event_groups ? "ok" : ""),
    kpi("Book-ready", num(s.book_ready, 0), "匹配候选 " + pct(s.book_ready_rate), s.book_ready ? "ok" : ""),
    kpi("Dry-run 候选", num(s.opportunities, 0), "waiting " + (s.waiting ?? 0) + " · review " + (s.review ?? 0)),
  ].join("");
}

function bestRow(group) {
  return (group.rows || []).find((row) => row.status === "opportunity")
    || (group.rows || []).find((row) => row.rule_status === "matched")
    || (group.rows || [])[0]
    || {};
}

export function renderEvents() {
  const rows = state.events || [];
  const body = $("eventRows");
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="8" class="empty">没有符合条件的事件</td></tr>`;
    return;
  }
  body.innerHTML = rows.map((group) => {
    const meta = groupMeta(group);
    const row = bestRow(group);
    const observation = row.observation || {};
    const economics = row.economics || {};
    const book = row.book || {};
    const lifecycle = row.lifecycle?.state || row.status || group.source_status;
    const bucket = group.matched_bucket?.outcome || row.matched_outcome || "—";
    const sourceAge = age(observation.observed_at || observation.source_timestamp);
    const bookState = book.fetched_at
      ? "fresh " + age(book.fetched_at)
      : (book.book_missing ? "missing" : "—");
    return `<tr class="event-row" data-event="${escapeHtml(group.event_group_id)}">
      <td><strong>${escapeHtml(meta.station)}</strong><small>${escapeHtml(meta.question)}</small></td>
      <td>${escapeHtml(meta.date)}<small>${escapeHtml(meta.metric)} · ${escapeHtml(meta.rule.unit || meta.source.unit || "")}</small></td>
      <td>${badge(row.rule?.manual_approval ? "approved" : "review", row.rule?.manual_approval ? "approved" : "review")}</td>
      <td>${badge(observation.status || group.source_status, observation.provider || "source")}<small>${escapeHtml(sourceAge)}</small></td>
      <td><strong>${escapeHtml(bucket)}</strong><small>${group.matched_market_count ?? 0}/${group.market_count ?? 0} target${(group.candidate_count || 0) ? " · " + group.candidate_count + " cand" : ""}</small></td>
      <td><span class="mono">${price(economics.execution_price || book.best_ask)}</span><small>${escapeHtml(row.trade_side || "VWAP")} · ${num(economics.net_edge, 4)}</small></td>
      <td>${badge(lifecycle)}<small>${escapeHtml(row.reason || "")}</small></td>
      <td><span class="mono">${escapeHtml(bookState)}</span><small>${escapeHtml(fmtTime(observation.observed_at))}</small></td>
    </tr>`;
  }).join("");
  body.querySelectorAll(".event-row").forEach((row) => {
    row.addEventListener("click", () => window.dispatchEvent(new CustomEvent("weather:select", { detail: row.dataset.event })));
  });
}

export function renderCandidates() {
  const rows = state.candidates || [];
  $("candidateCount").textContent = String(rows.length);
  $("candidates").innerHTML = rows.length
    ? rows.map((row) => {
        const eco = row.economics || {};
        const source = row.observation || {};
        return `<article class="candidate">
          <div class="candidate__head"><span>${escapeHtml(source.station_id || row.event_group_id)}</span>${badge("opportunity", "dry-run")}</div>
          <strong>${escapeHtml((row.target_outcome || row.matched_outcome || "—") + " " + (row.trade_side || "YES"))}</strong>
          <div class="candidate__grid">
            <span>VWAP <b>${price(eco.execution_price)}</b></span>
            <span>Net <b>${num(eco.net_edge, 4)}</b></span>
            <span>Fee <b>${num(eco.fee_rate, 3)}</b></span>
            <span>Depth <b>${num(eco.ask_depth_at_limit, 2)}</b></span>
          </div>
          <small>${escapeHtml(row.event_group_id)} · ${escapeHtml(fmtTime(row.created_at))}</small>
        </article>`;
      }).join("")
    : `<div class="empty">当前没有通过盘口经济门的候选</div>`;
}

function seriesPoints(detail) {
  let points = [];
  if (Array.isArray(detail.source_series) && detail.source_series.length) {
    points = detail.source_series;
  } else {
    const rows = detail.rows || [];
    for (const row of rows) {
      const obs = row.observation || {};
      if (Array.isArray(obs.series) && obs.series.length) {
        points = obs.series;
        break;
      }
    }
  }
  if (groupMeta(detail).sampleSet !== "hourly") return points;
  return points.filter((item) => item.counts_for_resolution !== false);
}

function observationEmptyMessage(detail) {
  const meta = groupMeta(detail);
  const reason = meta.source.reason || "";
  const tz = meta.rule.timezone || "";
  if (reason === "observation_window_not_started") {
    return `站点当地日尚未开始（${tz || "当地时区"}），现在没有该日小时序列是正常的，不是漏拉。`;
  }
  if (reason === "no_observations_in_window") {
    return "窗口已开，源还没有返回这一天窗口内的观测点。";
  }
  if (reason === "hourly_filter_empty") {
    return "盘口按 Hourly Data 结算，但过滤后这一天还没有计入结算的观测点。";
  }
  if (reason === "observation_window_open") {
    return "窗口已开，但这一轮没有拿到窗口内观测。";
  }
  if (String(detail.source_status || meta.source.status || "").includes("waiting")) {
    return `窗口未开${reason ? "：" + reason : ""}${tz ? "（" + tz + "）" : ""}。`;
  }
  return reason ? `当前没有小时序列：${reason}` : "当前没有小时序列。";
}

function wrhPageHref(meta) {
  const page = String(meta.rule.source?.resolution_source || "");
  if (!page) return "";
  if (meta.sampleSet !== "hourly" || /[?&]hourly=/i.test(page)) return page;
  return page + (page.includes("?") ? "&" : "?") + "hourly=true";
}

function renderObservationTable(detail) {
  const meta = groupMeta(detail);
  const tz = meta.rule.timezone || "";
  const values = seriesPoints(detail).slice(-96);
  const page = wrhPageHref(meta);
  const hourlyNote = meta.sampleSet === "hourly"
    ? "（WRH Show Hourly Data，只列出计入结算的小时点）"
    : /wunderground\.com|weather\.com/i.test(page)
      ? "（Wunderground Daily Observations 全部观测点，含 :30）"
      : "（同一 WRH 页全部观测点）";
  const caption = `<p class="obs-caption">结算序列：<b>${escapeHtml(sampleSetLabel(meta))}</b>${hourlyNote}${
    page ? `<br><span class="muted">${escapeHtml(page)}</span>` : ""
  }</p>`;
  if (!values.length) {
    return caption + `<div class="empty">${escapeHtml(observationEmptyMessage(detail))}</div>`;
  }
  return `${caption}<table class="compact"><thead><tr><th>站点当地时间${tz ? "（" + escapeHtml(tz) + "）" : ""}</th><th>温度</th></tr></thead><tbody>${
    values.map((item) => {
      const when = item.local_time || fmtStationTime(item.timestamp, tz);
      const temp = fmtTemp(item.temp ?? item.value);
      const unit = meta.rule.unit || meta.source.unit || "";
      return `<tr><td>${escapeHtml(when)}</td><td>${escapeHtml(temp)}${unit ? " " + escapeHtml(unit) : ""}</td></tr>`;
    }).join("")
  }</tbody></table>`;
}

function renderBooks(rows) {
  return `<div class="book-grid">${rows.map((row) => {
    const book = row.book || {};
    const asks = (book.asks || []).slice(0, 6);
    const side = row.trade_side || "YES";
    const empty = book.book_missing
      ? `<small>${escapeHtml(book.error || "盘口未拉到")}</small>`
      : `<small>无 asks</small>`;
    return `<article class="book">
      <div class="book__head"><strong>${escapeHtml((row.outcome || "—") + " " + side)}</strong><span>${escapeHtml(row.market_id || "")}</span></div>
      <div class="book__meta">ask ${price(book.best_ask)} · bid ${price(book.best_bid)} · age ${age(book.fetched_at)}</div>
      <div class="ladder">${asks.length ? asks.map((level) => `<div><span>${price(level.price)}</span><b>${num(level.size, 2)}</b></div>`).join("") : empty}</div>
    </article>`;
  }).join("")}</div>`;
}

export function renderDetail(detail) {
  const drawer = $("detail");
  if (!detail) {
    drawer.setAttribute("aria-hidden", "true");
    drawer.classList.remove("is-open");
    return;
  }
  state.selectedEvent = detail;
  const meta = groupMeta(detail);
  const running = meta.source.value;
  const runningText = running == null || running === "" ? "—" : fmtTemp(running) + " " + (meta.rule.unit || meta.source.unit || "");
  $("detailTitle").textContent = (meta.station || "事件") + " · " + meta.date;
  $("detailBody").innerHTML = `
    <section class="detail-card">
      <div class="detail-title"><span class="eyebrow">RULE / FINALITY</span>${badge(detail.source_status, detail.source_status)}</div>
      <p>${escapeHtml(meta.question)}</p>
      <div class="facts"><span>station <b>${escapeHtml(meta.station)}</b></span><span>metric <b>${escapeHtml(meta.metric)}</b></span><span>unit <b>${escapeHtml(meta.rule.unit || meta.source.unit || "—")}</b></span><span>sample_set <b>${escapeHtml(sampleSetLabel(meta))}</b></span><span>盘中 <b>${escapeHtml(runningText)}</b></span><span>bucket <b>${escapeHtml(detail.matched_bucket?.outcome || "—")}</b></span></div>
    </section>
    <section class="detail-card"><div class="section-head"><div><span class="eyebrow">SOURCE OBSERVATIONS</span><h3>实时数据</h3></div><span class="muted">${escapeHtml(meta.source.provider || "—")} · 扫描 ${escapeHtml(fmtTime(meta.source.observed_at))}${meta.rule.timezone ? " · " + escapeHtml(meta.rule.timezone) : ""}</span></div>${renderObservationTable(detail)}</section>
    <section class="detail-card"><div class="section-head"><div><span class="eyebrow">CLOB</span><h3>盘口深度</h3></div><span class="muted">先拉新死掉的 No，再拉其余交易盘，Yes 展示盘最后</span></div>${renderBooks(detail.markets || [])}</section>
    <section class="detail-card"><span class="eyebrow">AUDIT</span><div class="audit">${(detail.rows || []).map((row) => `<div><b>${escapeHtml((row.target_outcome || row.matched_outcome || "—") + (row.trade_side ? " " + row.trade_side : ""))}</b><span>${escapeHtml(row.status || "")}</span><small>${escapeHtml(row.reason || "")} · ${escapeHtml(row.observation?.evidence_hash || "no evidence hash")}</small></div>`).join("")}</div></section>`;
  drawer.setAttribute("aria-hidden", "false");
  drawer.classList.add("is-open");
}

export function renderAll() {
  renderMeta();
  renderKpis();
  renderEvents();
  renderCandidates();
}


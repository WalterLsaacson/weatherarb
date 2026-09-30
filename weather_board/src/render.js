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
    const trading = service.trading || {};
    const liveParts = [];
    if (trading.live_orders || service.dry_run === false) {
      liveParts.push("锁定No " + (trading.max_order_usdc != null ? trading.max_order_usdc : "5"));
    }
    const diurnalYes = trading.diurnal_yes_orders != null ? trading.diurnal_yes_orders : trading.diurnal_orders;
    const diurnalSides = [
      diurnalYes ? "Yes" : "",
      trading.diurnal_no_orders ? "No" : "",
    ].filter(Boolean);
    if (diurnalSides.length) {
      liveParts.push(
        "日变化 " + diurnalSides.join("/") + " " + (trading.diurnal_order_usdc != null ? trading.diurnal_order_usdc : "5"),
      );
    }
    $("pillMode").textContent = liveParts.length
      ? "LIVE · " + liveParts.join(" · ") + " USDC"
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
    kpi(
      "Dry-run 候选",
      num(s.opportunities, 0),
      "日变化 " + num(s.diurnal_opportunities, 0) + " · waiting " + (s.waiting ?? 0) + " · review " + (s.review ?? 0),
    ),
  ].join("");
}

function isDiurnalReason(reason) {
  const text = String(reason || "");
  return text === "diurnal_max_winner_yes"
    || text === "diurnal_min_winner_yes"
    || text === "diurnal_max_loser_no"
    || text === "diurnal_min_loser_no";
}

function bestRow(group) {
  const rows = group.rows || [];
  return rows.find((row) => row.status === "opportunity" && !isDiurnalReason(row.reason))
    || rows.find((row) => !isDiurnalReason(row.reason) && row.rule_status === "matched")
    || rows.find((row) => !isDiurnalReason(row.reason))
    || rows.find((row) => row.status === "opportunity")
    || rows[0]
    || {};
}

function clock(value) {
  const match = String(value || "").match(/(\d{2}:\d{2})/);
  return match ? match[1] : "—";
}

const DIURNAL_INFLUENCE_LABELS = {
  lake_breeze: "湖风",
  sea_breeze: "海风",
  heat_island: "热岛",
};

function diurnalPhaseLabel(diurnal) {
  if (!diurnal) return "—";
  if (diurnal.city_class !== "A") return "非A类";
  if (diurnal.phase === "regime_change" || (diurnal.continuity || {}).status === "regime_change") return "变天不买";
  if (diurnal.phase === "trend_unlearned" || diurnal.phase === "weather_mismatch") return "不可买";
  if (diurnal.trigger || diurnal.phase === "ready") return "可买";
  if (diurnal.phase === "passed") return "已过";
  return "未到";
}

function diurnalInfluenceLabel(diurnal) {
  const notes = (diurnal && diurnal.influences) || [];
  if (!notes.length) return "—";
  return notes.map((item) => DIURNAL_INFLUENCE_LABELS[item] || item).join(" · ");
}

function diurnalCell(group) {
  const diurnal = group.diurnal;
  if (!diurnal) return `<td class="muted">—</td>`;
  const continuity = diurnal.continuity || {};
  const delta = continuity.delta == null ? "" : ` Δ${continuity.delta}${continuity.unit || ""}`;
  const phase = diurnalPhaseLabel(diurnal);
  const high = diurnal.learned_max_valid
    ? `${clock(diurnal.learned_max_start_local)}–${clock(diurnal.learned_max_end_local)}`
    : "—";
  const low = diurnal.learned_min_valid
    ? `${clock(diurnal.learned_min_start_local)}–${clock(diurnal.learned_min_end_local)}`
    : "—";
  return `<td class="diurnal-cell"><span>高 ${escapeHtml(high)}</span><span>低 ${escapeHtml(low)}</span><small>${escapeHtml(phase)}${escapeHtml(delta)}</small></td>`;
}

export function renderEvents() {
  const rows = state.events || [];
  const body = $("eventRows");
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="9" class="empty">没有符合条件的事件</td></tr>`;
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
      ${diurnalCell(group)}
      <td><span class="mono">${price(economics.execution_price || book.best_ask)}</span><small>${escapeHtml(row.side_label || row.trade_side || "VWAP")} · ${num(economics.net_edge, 4)}</small></td>
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
        const orderSide = String(row.order_side || eco.order_side || "BUY").toUpperCase();
        const sideLabel = row.side_label || ((row.trade_side || "YES") + (orderSide === "SELL" ? " SELL" : ""));
        const diurnalNote = isDiurnalReason(row.reason) ? "日变化 · DRY-RUN" : "";
        const inventoryNote = diurnalNote || (orderSide === "SELL" ? "需持有 Yes · live 不下单" : "DRY-RUN");
        return `<article class="candidate">
          <div class="candidate__head"><span>${escapeHtml(source.station_id || row.event_group_id)}</span>${badge("opportunity", inventoryNote)}</div>
          <strong>${escapeHtml((row.target_outcome || row.matched_outcome || "—") + " " + sideLabel)}</strong>
          <div class="candidate__grid">
            <span>VWAP <b>${price(eco.execution_price)}</b></span>
            <span>Net <b>${num(eco.net_edge, 4)}</b></span>
            <span>Fee <b>${num(eco.fee_rate, 3)}</b></span>
            <span>Depth <b>${num(eco.ask_depth_at_limit ?? eco.bid_depth_at_floor, 2)}</b></span>
          </div>
          <small>${escapeHtml(row.reason || "")} · ${escapeHtml(row.event_group_id)} · ${escapeHtml(fmtTime(row.created_at))}</small>
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

function diurnalFacts(detail) {
  const diurnal = detail.diurnal;
  if (!diurnal) return "";
  const continuity = diurnal.continuity || {};
  const delta = continuity.delta == null ? "—" : continuity.delta + (continuity.unit || "");
  const status = continuity.status || "—";
  const cityClass = diurnal.city_class === "A" ? "A类" : "非A类";
  const yesterdayExtreme = diurnal.metric === "daily_min" ? diurnal.yesterday_min_at_local : diurnal.yesterday_max_at_local;
  const learnedStart = diurnal.metric === "daily_min" ? diurnal.learned_min_start_local : diurnal.learned_max_start_local;
  const learnedEnd = diurnal.metric === "daily_min" ? diurnal.learned_min_end_local : diurnal.learned_max_end_local;
  const learnedBuy = diurnal.metric === "daily_min" ? diurnal.learned_min_buy_local : diurnal.learned_max_buy_local;
  return `<span>城市 <b>${escapeHtml(cityClass)}</b></span><span>潜在影响 <b>${escapeHtml(diurnalInfluenceLabel(diurnal))}</b></span><span>昨日趋势 <b>${escapeHtml(clock(yesterdayExtreme) || "—")}</b></span><span>学习时段 <b>${learnedStart ? escapeHtml(clock(learnedStart)) + "–" + escapeHtml(clock(learnedEnd)) : "—"}</b></span><span>买入 <b>${escapeHtml(clock(learnedBuy) || "—")}</b></span><span>日出 <b>${escapeHtml(clock(diurnal.sunrise_local))}</b></span><span>太阳正午 <b>${escapeHtml(clock(diurnal.solar_noon_local))}</b></span><span>日落 <b>${escapeHtml(clock(diurnal.sunset_local))}</b></span><span>天气 <b>${escapeHtml(diurnal.weather_class || "—")}</b></span><span>昨日天气 <b>${escapeHtml(diurnal.yesterday_weather_class || "—")}</b></span><span>几何最高温 <b>${escapeHtml(clock(diurnal.max_window_start_local))}–${escapeHtml(clock(diurnal.max_window_end_local))}</b></span><span>几何最低温 <b>${escapeHtml(clock(diurnal.min_window_start_local))}–${escapeHtml(clock(diurnal.min_window_end_local))}</b></span><span>昨日温差 <b>${escapeHtml(delta)}</b></span><span>对照 <b>${escapeHtml(status)}</b></span><span>日变化 <b>${escapeHtml(diurnalPhaseLabel(diurnal))}</b></span>`;
}

function clockStamp(value) {
  return String(value || "").slice(0, 16);
}

function clockMs(value) {
  const match = clockStamp(value).match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})$/);
  if (!match) return null;
  return Date.UTC(+match[1], +match[2] - 1, +match[3], +match[4], +match[5]);
}

function inPredictedWindow(when, start, end, hourly) {
  const t = clockMs(when);
  const a = clockMs(start);
  const b = clockMs(end);
  if (t == null || a == null || b == null) return false;
  if (t >= a && t <= b) return true;
  if (!hourly) return false;
  const hourStart = clockMs(clockStamp(when).slice(0, 14) + "00");
  if (hourStart == null) return false;
  const overlap = Math.min(hourStart + 60 * 60 * 1000, b) - Math.max(hourStart, a);
  return overlap >= 15 * 60 * 1000;
}

function diurnalTag(kind, text) {
  return `<span class="diurnal-tag diurnal-tag--${kind}">${escapeHtml(text)}</span>`;
}

function renderObservationTable(detail) {
  const meta = groupMeta(detail);
  const tz = meta.rule.timezone || "";
  const diurnal = detail.diurnal || {};
  const hourly = meta.sampleSet === "hourly";
  const suppressed = diurnalPhaseLabel(diurnal) === "变天不买";
  const todayWeather = String(diurnal.weather_class || "");
  const yesterdayWeather = String(diurnal.yesterday_weather_class || "");
  const weatherReusable = Boolean(
    todayWeather
    && yesterdayWeather
    && todayWeather !== "unknown"
    && yesterdayWeather !== "unknown"
    && todayWeather === yesterdayWeather
  );
  const windows = [
    {
      kind: "min",
      label: "最低温",
      start: diurnal.learned_min_start_local,
      end: diurnal.learned_min_end_local,
      trigger: diurnal.learned_min_buy_local,
      valid: diurnal.learned_min_valid === true && weatherReusable,
      reference: `${clock(diurnal.min_window_start_local)}–${clock(diurnal.min_window_end_local)}`,
    },
    {
      kind: "max",
      label: "最高温",
      start: diurnal.learned_max_start_local,
      end: diurnal.learned_max_end_local,
      trigger: diurnal.learned_max_buy_local,
      valid: diurnal.learned_max_valid === true && weatherReusable,
      reference: `${clock(diurnal.max_window_start_local)}–${clock(diurnal.max_window_end_local)}`,
    },
  ];
  const activeWindows = windows.filter((item) => item.valid && item.start && item.end);
  const values = seriesPoints(detail).slice(-96);
  const page = wrhPageHref(meta);
  const hourlyNote = hourly
    ? "（WRH Show Hourly Data，只列出计入结算的小时点）"
    : /wunderground\.com|weather\.com/i.test(page)
      ? "（Wunderground Daily Observations 全部观测点，含 :30）"
      : "（同一 WRH 页全部观测点）";
  const caption = `<p class="obs-caption">结算序列：<b>${escapeHtml(sampleSetLabel(meta))}</b>${hourlyNote}${
    page ? `<br><span class="muted">${escapeHtml(page)}</span>` : ""
  }</p>`;
  const legend = `<p class="obs-legend">${windows.map((item) => {
    const learned = item.valid && item.start
      ? `${escapeHtml(clock(item.start))}–${escapeHtml(clock(item.end))} · 买入 ${escapeHtml(clock(item.trigger))}`
      : "不可买";
    return `<span>${diurnalTag(item.valid ? item.kind : "off", item.label)} ${learned}<small> 几何 ${escapeHtml(item.reference)}</small></span>`;
  }).join("")}${suppressed ? diurnalTag("off", "变天不买") : ""}</p>`;
  if (!values.length) {
    return caption + legend + `<div class="empty">${escapeHtml(observationEmptyMessage(detail))}</div>`;
  }
  const triggers = activeWindows
    .filter((item) => item.trigger)
    .map((item) => ({
      at: clockStamp(item.trigger),
      kind: item.kind,
      text: item.label + "触发" + (suppressed ? " · 不买" : ""),
    }))
    .sort((a, b) => a.at.localeCompare(b.at));
  const marked = [];
  let triggerIndex = 0;
  values.forEach((item) => {
    const when = clockStamp(item.local_time || fmtStationTime(item.timestamp, tz));
    const onRow = [];
    while (triggerIndex < triggers.length && triggers[triggerIndex].at <= when) {
      if (triggers[triggerIndex].at === when) onRow.push(triggers[triggerIndex]);
      else marked.push({ marker: triggers[triggerIndex] });
      triggerIndex += 1;
    }
    marked.push({ item, when, onRow });
  });
  while (triggerIndex < triggers.length) {
    marked.push({ marker: triggers[triggerIndex] });
    triggerIndex += 1;
  }
  const unit = meta.rule.unit || meta.source.unit || "";
  return `${caption}${legend}<table class="compact"><thead><tr><th>站点当地时间${tz ? "（" + escapeHtml(tz) + "）" : ""}</th><th>温度</th></tr></thead><tbody>${
    marked.map((entry) => {
      if (entry.marker) {
        const marker = entry.marker;
        return `<tr class="is-diurnal-mark is-diurnal-${marker.kind}"><td><span class="obs-when">${escapeHtml(marker.at)} ${diurnalTag(marker.kind, marker.text)}</span></td><td class="obs-temp"></td></tr>`;
      }
      const when = entry.when;
      const hits = activeWindows.filter((item) => inPredictedWindow(when, item.start, item.end, hourly));
      const klass = hits.map((item) => "is-diurnal-" + item.kind).join(" ");
      const tags = hits.map((item) => diurnalTag(item.kind, item.label)).join("");
      const triggerTags = (entry.onRow || []).map((item) => diurnalTag(item.kind, item.text)).join("");
      const temp = fmtTemp(entry.item.temp ?? entry.item.value);
      return `<tr class="${klass}"><td><span class="obs-when">${escapeHtml(when)} ${tags}${triggerTags}</span></td><td class="obs-temp">${escapeHtml(temp)}${unit ? " " + escapeHtml(unit) : ""}</td></tr>`;
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
      <div class="facts"><span>station <b>${escapeHtml(meta.station)}</b></span><span>metric <b>${escapeHtml(meta.metric)}</b></span><span>unit <b>${escapeHtml(meta.rule.unit || meta.source.unit || "—")}</b></span><span>sample_set <b>${escapeHtml(sampleSetLabel(meta))}</b></span><span>盘中 <b>${escapeHtml(runningText)}</b></span><span>bucket <b>${escapeHtml(detail.matched_bucket?.outcome || "—")}</b></span>${diurnalFacts(detail)}</div>
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


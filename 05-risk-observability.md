# 05. 风险控制、审计与可观测性

## 5.1 风险矩阵

| 风险 | 典型原因 | 自动控制 | 触发结果 |
|---|---|---|---|
| 站点错误 | 城市名映射到错误机场/PWS | station_id 白名单 + 规则 hash | 全事件 review |
| 时区错误 | 本地日被按 UTC 聚合 | IANA timezone + 边界测试 | 禁止候选 |
| 观测未最终 | 使用 latest/forecast/preliminary | following-date/final flag gate | 只保留 provisional |
| 源修订 | QC 调整历史值 | evidence version + revision cutoff | 冻结 adapter |
| 源不可用 | NOAA 429/5xx/缺测 | 重试、限流、规则允许时 fallback | 延迟或 review |
| 桶解析错 | “26°C”与 26–27 区间混淆 | 11 兄弟桶完备性校验 | 不下单 |
| 市场已关闭 | endDate 过期/结算竞态 | 下单前刷新 lifecycle | 丢弃候选 |
| 盘口陈旧 | best ask 已被吃掉 | book TTL + 二次读取 | 重算/放弃 |
| 深度不足 | 总成交量大、目标桶无深度 | VWAP walking + min size | 部分/不成交 |
| 费用低估 | hardcode 体育 fee | 每市场 fee schedule | 经济门失败 |
| 规则争议 | UMA challenge / source mismatch | dispute watcher + freeze | 人工接管 |
| 重复下单 | 多次 poll / 重启 | candidate/evidence 幂等键 | 拒绝重复 |
| 密钥误用 | dry-run 进程拿到私钥 | 进程/凭证分离 | 进程启动失败 |

## 5.2 风控层级

### 全局硬门

- live_orders=false 是默认且安全启动值；
- 仅允许已批准的 rule_version；
- source_status=final、rule_status=matched、market.active=true、closed=false、accepting_orders=true；
- neg_risk=false、事件属于白名单、没有 dispute/freeze；
- CLOB book 未超过 TTL，目标 token 可查到 asks；
- 实际费率、tick、min order size 均已获取；
- 单事件、全局、每日资金上限均未超限；
- 同一 candidate/evidence version 没有已成交订单。

### 经济门

~~~text
net_edge = 1 - vwap - taker_fee(vwap) - slippage_buffer - rule_error_buffer
~~~

若 net_edge < min_net_edge、可成交 shares 小于 min order size、或最差价超过 max ask，直接 NO_TRADE。所有拒绝写入 reason code，不以“没有机会”吞掉。

### 额度建议（仅 POC 参数）

~~~text
单事件：$10–$50
全局天气仓：$500
每天累计新开仓：$500–$1,000
订单 TTL：3–5s
默认 max ask：0.995
建议 min_net_edge：0.005–0.010
~~~

上线前应通过回放估计实际滑点、失败率和等待时间，再调整这些参数；它们不是收益或风险保证。

## 5.3 幂等与一致性

候选幂等键：

~~~text
idempotency_key = hash(event_group_id, rule_version, evidence_id, target_token_id)
~~~

- 数据库唯一约束拒绝同一 key 的第二个 live order；
- 订单状态按 CREATED → SUBMITTED → ACKED → FILLED/PARTIAL/EXPIRED/CANCELLED 推进；
- 网络超时后先查询 exchange_order_id/client_order_id，再决定重试；
- 进程重启先 reconcile 未完成订单和 fills，再开始新扫描；
- 内存队列只是加速层，账本和原始 evidence 才是事实源。

## 5.4 熔断条件

任一条件持续超过一个扫描周期，设置 weather_global_circuit=OPEN：

- NWS 和 fallback 同时不可用；
- 同一站点连续出现时间戳倒退、重复冲突或单位冲突；
- source final 与后续证据不一致；
- 过去 20 个订单的成交确认缺失率超过 5%；
- 盘口/市场 API 错误率超过 20%，或 book stale 超过 5 分钟；
- 结算结果与 bucket mapping 出现一次未解释偏差；
- 账户余额、allowance、签名或 redeem 连续失败。

熔断期间只允许采集 evidence 和生成 review，不允许新订单；恢复需要人工确认或显式 CLI 解锁，并记录 operator、原因和时间。

## 5.5 审计日志

每次候选至少记录：

~~~json
{
  "decision_id": "dec-...",
  "event_group_id": "...",
  "rule_version": "...",
  "evidence_id": "...",
  "market_snapshot_hash": "sha256:...",
  "book_snapshot_hash": "sha256:...",
  "source_finality": "final",
  "matched_bucket": "26",
  "vwap": 0.991,
  "fee_rate": 0.05,
  "net_edge": 0.008554,
  "risk_gates": {"lifecycle": true, "depth": true, "limits": true},
  "decision": "DRY_RUN",
  "reason": "source_final_rule_match_book_ok",
  "created_at": "2026-09-07T00:05:03Z"
}
~~~

禁止记录私钥、签名原文或完整账户敏感信息。raw payload 可加密保存，日报仅保留 hash 和脱敏摘要。

## 5.6 指标

### 数据源

- weather_source_poll_total{provider,station,status}
- weather_source_latency_seconds
- weather_observation_age_seconds
- weather_finality_delay_seconds：window end → final confirmation
- weather_fallback_total{from,to,reason}
- weather_revision_total{station,rule_version}

### 市场与执行

- weather_market_discovered_total{status}
- weather_rule_review_total{reason}
- weather_candidate_total{decision,reason}
- weather_book_age_seconds
- weather_vwap_minus_best_ask
- weather_net_edge_histogram
- weather_order_total{status}、weather_fill_ratio
- weather_ack_to_fill_seconds
- weather_realized_fee_total

### 结算

- weather_settlement_match_total{match}
- weather_redeem_pending_total
- weather_dispute_total
- weather_pnl_by_event_group

## 5.7 告警分级

- **P0**：结算与 source bucket 不一致、疑似错误下单、私钥/签名泄露；立即熔断和人工接管。
- **P1**：源切换、连续 stale、订单 ack/fill 不一致、UMA dispute；停止新单，保留已有仓位等待处理。
- **P2**：单站点延迟升高、盘口深度下降、fallback 次数异常；继续 dry-run，通知值班人。
- **P3**：规则 review 比例、候选净边际、API 轻微错误率变化；日报汇总。

## 5.8 监控面板最小布局

~~~text
顶部：全局 circuit、live_orders、今日风险金额、未 redeem 仓位
左侧：按站点的 source freshness / finality delay / fallback
中部：event group 时间线（window -> provisional -> final -> order -> resolve）
右侧：候选净边际、book age、VWAP、fill ratio
底部：review / reject reason、异常 evidence、人工审批记录
~~~


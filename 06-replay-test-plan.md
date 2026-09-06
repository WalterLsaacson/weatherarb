# 06. 回放、测试与验收方案

## 6.1 验证目标

上线前必须分别证明四件事：

1. 规则解析不会把城市、站点、时区、单位或桶边界弄错；
2. source finality 在时间上晚于观测窗口、早于或重叠 PM 结算窗口；
3. 最终桶确定后，CLOB 仍有可成交折价的概率和深度；
4. 订单、成交、结算和 redeem 的账务可重放、可对账、可停止。

不要只统计页面显示的 total volume；需要按目标 token 记录 ask depth、VWAP、book age 和 actual fills。

## 6.2 Fixtures 设计

每个事件保存以下文件：

~~~text
events/{event_group_id}/
  gamma_markets.json
  rules_text.html
  rule_version.json
  source/
    2026-09-06T23-00Z.json
    2026-09-07T00-05Z.json
    2026-09-07T00-20Z.json
  books/
    2026-09-07T00-05-03Z.json
    2026-09-07T00-05-04Z.json
  fills.json
  settlement.json
  expected.json
~~~

expected.json 至少包含：最终聚合值、最终桶、final confirmation timestamp、是否应生成 candidate、理论 fee、理论 net edge、应否下单。

## 6.3 历史回放流程

~~~text
1. 从 Gamma 记录事件的所有兄弟 market 和 rules 原文
2. 下载/保存 PM 规则指定的 NWS/WU 原始观测
3. 用真实时间戳重建 source polling 视图
4. 对每个时间点运行 finality state machine
5. final 后加载当时 book snapshot，按实际 tick/min size walking
6. 与最终 PM outcome、成交和 redeem 结果对账
7. 输出延迟、边际、深度、失败和错误分类
~~~

建议第一批至少 100 个已结算事件，覆盖：不同城市、°C/°F、夏令时、极端温度、缺测、fallback、低流动性和规则修订。若历史源无法完整获取，先用 30 个事件验证解析，再补齐 100 个事件的统计集。

## 6.4 测试分层

### 单元测试

- ISO timestamp、IANA timezone 和 DST 转换；
- daily_max、daily_min、daily_sum、latest；
- 重复/乱序/迟到观测；
- °C/°F 转换、整数边界、首尾无限桶；
- sibling bucket 无重叠/无空洞/唯一覆盖；
- first-following-date finality、显式 final flag、fallback deadline；
- fee 公式、tick snap、VWAP、min order size；
- 幂等 key 和状态机非法跃迁。

### 合同测试

- Gamma 字段映射：active、closed、acceptingOrders、enableOrderBook、negRisk、tick、min size、fee；
- CLOB /books 多 token 响应和空书/过期书；
- NWS features/properties payload 解析、HTTP 429/5xx；
- WU fallback 的站点和时间字段；
- 规则原文 hash 变化检测。

### 集成 / 端到端（mock）

- source final → candidate → dry-run JSONL；
- candidate → FAK mock → ack → fill → ledger；
- 网络超时后 reconcile，不重复下单；
- 下单前 market closed/acceptingOrders=false 的竞态；
- partial fill 后净边际变差，撤余量；
- resolved/dispute/revision 触发熔断。

### 故障注入

~~~text
NWS 延迟 20/60/180 分钟
NWS 缺测一整天
NOAA 与 WU 值冲突
首个次日数据点被撤回/修订
Gamma 规则文本改变
CLOB best ask 在刷新间隔内消失
fee schedule 临时变化
进程在 ACK 前重启
账户余额不足 / allowance 过期
~~~

任何故障注入都必须得到“停止或 review”的结果，不能静默产生 live candidate。

## 6.5 统计输出

每次回放生成：

~~~json
{
  "run_id": "replay-2026-09-05",
  "events": 100,
  "rule_parse_pass_rate": 0.98,
  "bucket_match_accuracy": 1.0,
  "finality_false_positive": 0,
  "median_finality_delay_s": 420,
  "p95_finality_delay_s": 1800,
  "candidate_count": 37,
  "book_available_rate": 0.81,
  "vwap_under_max_ask_rate": 0.24,
  "median_net_edge": 0.0081,
  "p95_slippage": 0.0027,
  "simulated_fill_rate": 0.63,
  "settlement_mismatch": 0,
  "reconciliation_errors": 0
}
~~~

区分 gross edge、扣费后 net edge、考虑滑点后的 net edge；不要把页面概率或 last trade 当作可实现收益。

## 6.6 验收标准

### 规则 / 数据

- 100 事件最终桶准确率 100%，不能有误买；
- finality false positive = 0；
- 站点、时区、单位、桶映射均有可追溯 evidence；
- 所有解析失败进入 review 并带 reason code。

### 执行 / 账务

- mock/live shadow 下重复下单率 0；
- 订单 ack/fill 对账差异 0；
- book stale 或 lifecycle 竞态不会发送订单；
- 每笔候选能由 candidate_id → evidence → book → order/fill → redeem 串起。

### 稳定性

- dry-run 连续运行 7–14 天无未解释进程退出；
- API 错误、限流、断网可恢复且不会重复下单；
- 熔断、解锁和人工审批均有审计；
- 资源使用（CPU、内存、raw payload 存储）低于部署上限。

## 6.7 本地验证命令

现有 finality fixtures 可作为回归基线：

~~~bash
cd /home/guanyin/aosp/temp/dqdhook
python3 .cursor/skills/polymarket-finality/scripts/test_finality.py
~~~

当前基线为 22/22 通过。天气新实现必须在此基础上新增天气专用 fixtures，不得修改足球 fixtures 以“消除”失败。

建议新增：

~~~bash
python3 -m pytest weather/tests -q
python3 -m weather.replay --dataset data/weather-replay --report out/weather-replay.json
python3 -m weather.scan --dry-run --horizon-hours 48
~~~

命令名可按仓库实际实现调整，但 dry-run 与 live executor 的依赖和凭证必须在进程层隔离。


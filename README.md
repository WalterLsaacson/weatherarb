# Polymarket 天气终局延迟套利：技术方案

版本：v1.0（2026-09-05）  
状态：可进入 POC 评审；默认只读 / dry-run，不启用真实下单

独立实现已放在本目录的 `weather_runtime/` 与 `weather_board/`，不依赖或修改 `dqdhook`。启动方式见下方“独立 POC 快速启动”。

## 1. 目标与结论

本方案把现有足球机器人的“事实已经确定、Polymarket 仍可交易、买入赢家 token”思路迁移到天气市场。它不是天气预报模型，也不是预测温度，而是捕捉：

```text
气象站观测已经足以确定结算桶
        ↓
按 resolution rules 得出唯一 Yes/No
        ↓
Polymarket 尚未 closed/resolved，且仍 accepting orders
        ↓
用当前订单簿可成交价格买入已确定的结果
        ↓
等待结算、记录成交并 redeem
```

结论：**技术上可行，建议做一个独立的天气 POC；第一批只做“指定机场站点 + 自然日最高/最低温 + 离散温度桶”市场。** 不能把足球的进球触发、0.01 tick、体育费率和按比赛建账逻辑直接复制过来。

当前 Weather 市场页面可以看到按城市/日期的温度市场；例如伦敦事件是 11 个整摄氏度区间，从“22°C 或以下”到“32°C 或以上”。页面抓取时该事件显示总成交量约 `$31,549`，26°C 桶约 51% 的页面概率，说明有足够市场活动做盘口和延迟研究，但总成交量不等于目标 token 的可吃深度。[Polymarket Weather](https://polymarket.com/climate-science/weather) · [London event](https://polymarket.com/event/highest-temperature-in-london-on-september-6-2026)

旧金山事件还展示了一个与本策略直接相关的生命周期现象：页面显示 scheduled end date 已经过，但市场尚未 officially resolved，仍可能保持 open、允许买卖。它证明“事实窗口结束”和“平台停止交易”之间确实可能存在机会窗口，但不证明任何单笔交易必然盈利。[San Francisco event](https://polymarket.com/event/highest-temperature-in-san-francisco-on-september-5-2026)

## 2. 文档导航

| 文档 | 内容 | 读者 |
|---|---|---|
| [01-architecture.md](01-architecture.md) | 边界、组件、状态机、时序和与足球路径的隔离 | 架构 / 开发 |
| [02-market-finality.md](02-market-finality.md) | Polymarket 天气规则、NWS/WU 数据源、终局判定和规则注册 | 数据 / 策略 |
| [03-strategy-execution.md](03-strategy-execution.md) | 赢家 Yes、互补 No、完整集合、费用、盘口 walking、执行状态 | 交易 / 风控 |
| [04-data-model-interfaces.md](04-data-model-interfaces.md) | 市场注册表、规则与证据 JSON、数据库表、模块接口、配置 | 后端 |
| [05-risk-observability.md](05-risk-observability.md) | 风险清单、硬门、幂等、熔断、指标、告警和审计 | SRE / 风控 |
| [06-replay-test-plan.md](06-replay-test-plan.md) | 历史回放、fixtures、单测/集成/故障注入、验收标准 | QA / 研究 |
| [07-rollout-runbook.md](07-rollout-runbook.md) | 分阶段上线、运行手册、回滚、人工接管、值班检查表 | 运维 / 负责人 |
| [08-backlog.md](08-backlog.md) | 可拆分的开发任务、优先级、Definition of Done | 项目管理 |
| [references.md](references.md) | 官方资料和本方案使用的外部依据 | 全员 |

## 3. 与现有项目的关系

方案层面天气模块可以作为独立策略域接入共享的 Polymarket 市场目录、CLOB 只读客户端、订单执行器、持仓账本和监控；本目录的 POC 采用完全独立实现，不导入、不链接、不修改 `dqdhook`，未来若要整合也应通过明确的接口/复制方式进行。

现有项目已经具备可复用的部分：

- Gamma/CLOB 拉取、批量 `/books`、VWAP 深度 walking、FAK/GTC 封装；
- `WIN/LOSE/PENDING`、成交去重、持仓与 post-settlement 记录；
- finality scanner 的 source evidence hash、dry-run 输出和 fail-closed 原则；
- 基于环境变量的 live 开关和审计日志。

需要隔离或重写的部分：

- 足球 `match_id`、比分事件、pitch-gate、T+10 不能用于天气；
- 足球路径中的默认体育 taker fee 和 0.01 tick 不能推广到 Weather；
- 天气多为 11 个互斥桶，不是现有二元足球 token 的简单替换；
- 现有天气适配器是聚合骨架，自动规则发现目前无法可靠理解“恰好为 26°C / 26°C 桶”这种问题，需要新解析器和人工审批。

代码基线见：[足球流水线 README](/home/guanyin/aosp/temp/dqdhook/README.md:1)、[finality skill](/home/guanyin/aosp/temp/dqdhook/.cursor/skills/polymarket-finality/SKILL.md:1)、[天气适配器](/home/guanyin/aosp/temp/dqdhook/.cursor/skills/polymarket-finality/scripts/adapters.py:337)。现有 finality fixtures 测试为 22/22 通过；这只证明通用 dry-run 骨架，不代表实时天气数据或真实交易已验证。

## 4. 非目标与安全边界

- 不承诺无风险收益；规则、数据修订、盘口深度、交易失败和 UMA 争议都会产生损失。
- 第一版不做预测型交易、天气预报 alpha、飓风/龙卷风叙事、无明确站点的降雨/降雪市场。
- 不把 `endDate` 经过、页面赔率、新闻或单一最新观测当作终局证据。
- 不自动交易无法绑定 resolution source、无法解析聚合窗口、包含 negRisk/多腿依赖的市场。
- POC 阶段不加载私钥，不调用下单 API；真实交易必须在 dry-run、回放、故障注入和人工审批全部通过后单独打开。

## 5. 推荐的 Go / No-Go

### Go（进入 POC）

- 只允许一个城市/机场站点白名单，规则版本人工审批；
- 能保存原始 NWS/WU 响应、时间戳、单位、站点和 SHA-256 evidence hash；
- 能重放至少 100 个已结算事件，逐桶核对最终结果；
- dry-run 连续运行 7–14 天，能够量化“source final → 可下单 → market closed”的时间窗口；
- 盘口使用市场实际 `tick_size`、`min_order_size`、fee schedule，并按 VWAP 计算净边际；
- 任一不确定性都进入 `REVIEW_REQUIRED`，不发送订单。

### No-Go（保持只读）

- PM 规则只给城市名，没有明确站点、时区、聚合口径或 fallback；
- 只能抓到预报或 preliminary 数据，不能证明是首个次日数据点后的最终口径；
- 目标桶的可成交净边际低于配置阈值，或盘口在 TTL 内消失；
- 事件出现规则修订、站点缺测、源切换、UMA dispute 或订单状态无法确认；
- 账户、地区或平台条款不允许相关交易。

## 6. 独立 POC 快速启动

从本目录执行，Python 标准库即可运行：

~~~bash
cd /home/guanyin/aosp/temp/polymarket-weather-arb-plan
python3 run_weather.py --fixture fixtures --data-dir /tmp/polymarket-weather-poc-data
~~~

等价入口：`python3 -m weather_runtime --fixture fixtures`。

浏览器打开 `http://127.0.0.1:8793/`。该 fixture 会展示 11 个温度桶、source final、26°C 匹配、盘口深度和一个 dry-run 候选；不会访问真实市场，也不会发送订单。

只读在线 Gamma/NWS/CLOB 扫描（仍不下单）：

~~~bash
python3 run_weather.py --sync --data-dir /tmp/polymarket-weather-online
~~~

运行前请确认网络/代理和接口访问权限；在线模式会把市场、source 和 book 请求写入指定 data directory。

Live 下单模式依赖官方 `polymarket-client` SDK（Python 3.11+）：

```bash
python3 -m pip install -r requirements-live.txt
```

只跑 dry-run/read-only 时不需要安装它。测试命令：

~~~bash
python3 -m unittest discover -s tests -v
~~~

也可以只运行一次扫描，或生成待人工审核的规则：

~~~bash
python3 -m weather_runtime.cli scan --fixture fixtures --output /tmp/weather-scan.json
python3 -m weather_runtime.cli discover --sync \
  --rules-out data/pm-weather/rules.auto.json \
  --review-out data/pm-weather/rules.review.json
~~~

服务端提供 REST 快照和 SSE 增量流：

| 接口 | 用途 |
|---|---|
| `GET /api/overview` | 匹配率、source final、book-ready、候选等 KPI |
| `GET /api/events` | 事件组列表，支持 `status`、`q`、`limit` |
| `GET /api/events/{event_group_id}` | 事件详情、规则、证据、兄弟桶盘口 |
| `GET /api/source` | 当前扫描的全部 source observation |
| `GET /api/books` | 当前扫描的全部 CLOB book |
| `GET /api/candidates` | dry-run 候选及 VWAP/fee/net edge |
| `GET /api/stream` | SSE：`snapshot`、`source_update`、`candidate_update`、`health_update` |
| `POST /api/scan/once` | 手动触发一次只读扫描 |

盘口缺少抓取时间、超过 `book_ttl_s`、缺少实际 tick/min size/fee、市场生命周期不满足或规则未审批时，扫描明确返回 `no_trade`/`review` 原因；不会隐式下单。fixture 的无时间戳盘口仅在 fixture loader 读取时标记为回放抓取时间，生产文件不会这样处理。

独立实现文件：

- `weather_runtime/`：市场、规则、source、CLOB、扫描、持久化、REST/SSE 服务；
- `weather_board/`：8793 端口只读前端和 board server wrapper；
- `fixtures/`：可重复的 11 桶伦敦温度事件；
- `tests/`：规则、终局、费用门、扫描、持久化和 SSE 发布测试。
- `requirements.txt`：dry-run/read-only 空依赖清单，只使用 Python 标准库。
- `requirements-live.txt`：live 下单路径所需的 `polymarket-client`。

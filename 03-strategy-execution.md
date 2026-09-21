# 03. 套利策略与 CLOB 执行

## 3.1 机会定义

天气市场的“套利”应定义为**结算延迟窗口中的确定性折价**，而不是预测概率优势。候选必须同时满足：

```text
source_final = true
rule_match = 唯一 outcome
market_active = true
closed = false
accepting_orders = true
book_fresh = true
net_edge >= min_net_edge
```

Polymarket 结算由 resolution rules 指定来源和边界，并通过 UMA optimistic oracle 处理提案/挑战；任何人可提案，挑战期通常为 2 小时。因此“事实已知”不等于“平台已 resolved”，也不等于可跳过规则审计。[Resolution](https://docs.polymarket.com/concepts/resolution)

## 3.2 策略 A：买确定赢家 Yes（P0）

适用：11 个温度桶中已有一个桶由 final observation 唯一确定。

```text
目标 token = settlement_bucket.yes_token_id
动作       = 读取 asks，按 VWAP 吃到 max_ask / max_slippage 内
结算       = 每份赢家 token 价值 $1
```

优势：订单少、资金集中、部分成交后容易核算。  
缺点：目标桶可能已被其他机器人买到 0.999/1.00，剩余深度不足。

最低经济门：

```text
gross_edge = 1 - execution_vwap
fee        = execution_vwap × (1 - execution_vwap) × actual_fee_rate
net_edge   = gross_edge - fee - slippage_buffer - rule_error_buffer
```

Polymarket 文档给出的费率为成交时计算；maker/taker 费率按市场计划，费率不能写死。公式为 `fee = C × feeRate × p × (1-p)`。[Fees](https://docs.polymarket.com/trading/fees)

以 `feeRate=0.05` 仅作计算示例：

| ask | 每份 fee | 扣费后毛边际 |
|---:|---:|---:|
| 0.990 | 0.000495 | 0.009505 |
| 0.995 | 0.000249 | 0.004751 |

生产环境先读市场实际 fee schedule，再叠加最小净边际，例如 POC 可从 `0.005–0.010 USDC/share` 做敏感性分析，不能将该区间当成收益保证。

## 3.3 策略 B：买其他桶的 No（P1，谨慎）

如果某桶确定为赢家，则其余 10 个桶的 No 理论上都为赢家。可以逐桶买 No：

```text
for token in losing_yes_tokens:
    buy(no_token(token)) if net_edge(token) >= threshold
```

使用场景：赢家 Yes 盘口已接近 1，但多个其他桶的 No 仍显著低于 1。  
代价：最多 10 个订单、更多 min-order 约束、更多 partial fill、资金占用更大；任何桶分组错误都会同时损失多腿。当前实现只在小额 live 白名单内自动 FAK 已锁定 No，仍按 P1 谨慎额度管理，赢家 Yes 不自动成交。

## 3.4 策略 C：完整集合 / Dutch book（P2）

同一事件 11 个 Yes 恰好一个结算为 1。理论条件：

```text
sum(execution_vwap_i + fee_i + slippage_i) < 1.0 - safety_margin
```

全部腿成交后，组合无论哪个桶赢都支付 1。现实限制：Polymarket CLOB 没有 11 腿原子成交；第一腿成交后价格会变化，组合可能只填部分。必须有：

- 每个 token 的最小下单量和 tick 校验；
- 全部腿的最大总风险和超时撤单；
- 组合净敞口、未完成腿和止损/放弃规则；
- 发生规则 review 或市场关闭时禁止继续补腿。

因此完整集合仅用于离线研究，不纳入 P0 live。

## 3.5 订单簿读取与价格计算

Polymarket CLOB 支持 `POST /books` 批量按 token ID 获取订单簿，响应包含 bids、asks、`min_order_size`、`tick_size`；单 token 可用 `/book`。[Order books](https://docs.polymarket.com/api-reference/market-data/get-order-books-request-body)

实现要求：

1. 在一次候选评估中批量读取目标 token 和（可选）兄弟桶 token；
2. 校验 book 的 `timestamp/age`，超过 TTL（建议 3–5 秒）重新拉取；
3. 按 ask 从低到高逐档 walking，计算可成交 shares、USDC、VWAP、最差价；
4. 将实际 `tick_size` 作为唯一报价精度；天气市场可能出现 0.001 级价格，不能套足球的 0.01 clamp；
5. 校验 `min_order_size`、余额、单事件上限和最大滑点；
6. 价格/深度只作为交易经济，不作为 settlement 证据。

VWAP 伪代码：

```python
def walk_asks(asks, max_usdc, max_price, max_slippage, tick):
    filled = 0.0
    cost = 0.0
    for level in sorted(asks, key=lambda x: x.price):
        price = snap_to_tick(level.price, tick)
        if price > max_price:
            break
        if price - best_ask > max_slippage:
            break
        qty = min(level.size, (max_usdc - cost) / price)
        if qty <= 0:
            break
        filled += qty
        cost += qty * price
    return filled, cost, cost / filled if filled else None
```

## 3.6 下单模式

### POC / P0：FAK

- 候选生成后再次刷新 Gamma lifecycle 和 CLOB book；
- 仅吃当前可见、满足净边际的 asks；
- 未成交部分立即失效，不挂长期单；
- 订单请求带 `decision_id` / `candidate_id`，以便幂等和审计；
- 收到 order ack 后必须查询 fills，不把 ack 当成交。

当前 `RuntimeService` 在 `LIVE_ORDERS=true` 时只自动 FAK 已锁定 No
（`intraday_impossible_no`、`provisional_loser_no`、`source_final_loser_no`）；
赢家 Yes 仍保持 dry-run，需单独审批后才可启用。

### 默认关闭：GTC/GTD rest

天气事实一旦 final，窗口可能在几秒内关闭；长期 rest 会在规则变化或市场关闭后意外成交。当前实现保留在 `LIMIT_ORDERS` 开关后，但默认必须为
`false`；除非后续回放证明有稳定未成交补单价值，否则不进入 live 审批。

## 3.7 交易前硬门

```python
def should_trade(candidate, market, book, account, cfg):
    return all([
        candidate.source_status in {"final", "intraday", "provisional"},
        candidate.lock_kind == "buy_no",
        candidate.rule_status == "matched",
        market.active and not market.closed,
        market.accepting_orders and market.enable_order_book,
        not market.neg_risk,
        book.is_fresh(cfg.book_ttl_s),
        book.best_ask is not None,
        book.vwap <= cfg.max_ask,
        candidate.rule_version == market.approved_rule_version,
        account.remaining_event_usdc >= cfg.event_cap_usdc,
        economics.net_edge >= cfg.min_net_edge,
        not ledger.has_open_order(candidate.id),
    ])
```

任一字段为 unknown 都按 false 处理。市场状态字段可从 Gamma/Markets API 获得，如 `active`、`closed`、`acceptingOrders`、`enableOrderBook`、tick/min size 和 fee schedule。[Markets API](https://docs.polymarket.com/api-reference/markets/list-markets)

## 3.8 成交后处理

- `FILLED/PARTIAL` 都写入 ledger，并保存实际均价、fee、source evidence hash；
- 订单与 event group 绑定，禁止同一 evidence version 重复扫单；
- 监控 Gamma resolved/closed 状态，进入可 redeem 状态后执行兑换；
- 若部分成交且剩余腿不再满足净边际，撤销未成交量，不能为“凑满”追价；
- 结算结果与 source bucket 不一致时，立即冻结该 adapter 和相关规则版本，保留原始证据，不自动卖出或加仓。

## 3.9 与足球执行器的适配边界

现有足球代码的 `flag_misprice` 默认使用体育费率并只针对 `WIN -> buy_win`；[misprice 判定](/home/guanyin/aosp/temp/dqdhook/.cursor/skills/polymarket-quote/scripts/quote_lib.py:1525)。天气应新增 `weather_economics.py` 或参数化通用函数，显式传入：

```text
fee_rate, fee_source, tick_size, min_order_size,
max_ask, max_slippage, max_usdc, min_net_edge
```

现有 rest ladder 还把足球 token 的 tick 约束为 0.01；[rest ladder](/home/guanyin/aosp/temp/dqdhook/.cursor/skills/polymarket-quote/scripts/rest_ladder.py:18)。天气路径必须绕开这一常量。

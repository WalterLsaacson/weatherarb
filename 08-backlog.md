# 08. 开发任务与 Definition of Done

## 8.1 P0（必须先做）

| ID | 任务 | 依赖 | 交付物 |
|---|---|---|---|
| W-001 | Gamma Weather market discovery | 现有 Gamma client | 分页同步、event grouping、raw snapshots |
| W-002 | sibling bucket validator | W-001 | 11 桶互斥/完备校验、失败 reason |
| W-003 | rules text fetch + hash | W-001 | rules 原文、hash、版本变更检测 |
| W-004 | station/timezone parser | W-003 | station whitelist、IANA timezone、人工 review JSON |
| W-005 | NWS observation adapter | W-004 | features 解析、重试、限流、evidence |
| W-006 | daily aggregation | W-005 | max/min/sum/latest、边界/单位测试 |
| W-007 | following-date finality | W-005 | provisional/final/revision 状态机 |
| W-008 | bucket mapping | W-002/W-006 | Rational 边界、极值桶、唯一覆盖 |
| W-009 | evidence store | W-005 | raw payload、SHA-256、压缩/保留策略 |
| W-010 | dry-run candidate stream | W-007/W-008 | candidate JSONL、幂等 key、拒绝原因 |

## 8.2 P1（验证经济与执行）

| ID | 任务 | 依赖 | 交付物 |
|---|---|---|---|
| W-101 | CLOB weather book reader | W-001 | batch books、TTL、tick/min size |
| W-102 | market fee schedule reader | W-101 | actual fee rate，禁止体育 hardcode |
| W-103 | VWAP/fee economics | W-101/W-102 | gross/net edge、slippage budget |
| W-104 | replay dataset builder | W-003/W-005 | 100+ settled event dataset |
| W-105 | replay engine | W-104 | as-of replay、统计报告、PM outcome 对账 |
| W-106 | fault injection suite | W-005/W-105 | source/book/lifecycle/网络故障用例 |
| W-107 | shadow dashboard | W-010/W-101 | finality delay、深度、候选、拒绝、健康 |
| W-108 | settlement/redeem monitor | 现有 ledger | closed/resolved、redeem、对账 |

## 8.3 P2（小额 live 前）

| ID | 任务 | 依赖 | 交付物 |
|---|---|---|---|
| W-201 | isolated weather executor | W-103/W-106 | 独立进程/凭证、FAK、无 GTC |
| W-202 | pre-submit lifecycle refresh | W-201 | 下单竞态硬门 |
| W-203 | order reconcile | W-201 | ack/fill/重启幂等 |
| W-204 | circuit breaker | W-106/W-107 | OPEN/CLOSED、人工解锁审计 |
| W-205 | operator review UI/CLI | W-004/W-204 | 审批、冻结、导出证据 |
| W-206 | tiny-cap live pilot | 全部 P0/P1/P2 | 单站点、单事件、逐笔复盘 |

## 8.4 Definition of Done

每个任务完成必须满足：

- 有单元测试和至少一个失败路径；
- 关键输出带 event_group_id、rule_version、evidence_id；
- 原始输入可通过 hash 定位；
- 失败为显式状态/原因，不返回空成功；
- dry-run 默认不触发交易；
- 对可能影响资金的代码有 replay fixture；
- 文档、配置默认值和运行命令同步更新。

## 8.5 Pull Request 检查清单

- [ ] 是否改变足球路径？若是，必须拆分或明确回归影响；
- [ ] 是否引入新的 source provider？是否有站点、时区、revision/fallback 证据；
- [ ] 是否使用了 endDate、latest、页面概率作为 finality？若是，拒绝合并；
- [ ] 是否硬编码 fee/tick/min size？若是，拒绝合并；
- [ ] 是否在所有错误分支保持 no-order；
- [ ] 是否补充了 event replay 和状态机测试；
- [ ] 是否在日志中泄露私钥、签名或账户敏感信息；
- [ ] 是否更新 rollback / runbook。


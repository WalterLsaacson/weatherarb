# 07. 分阶段上线与运行手册

## 7.1 阶段路线

| 阶段 | 目标 | 真实订单 | 退出条件 |
|---|---|---:|---|
| P0 规则与数据 | 建 registry、解析器、NWS/WU adapter、证据存储 | 否 | 30 个事件无歧义解析；源证据可重放 |
| P1 历史回放 | 100+ 已结算事件，测 bucket、finality、盘口、费用 | 否 | bucket 准确率 100%，无 false finality |
| P2 Shadow / dry-run | 在线发现和候选生成，记录假设成交 | 否 | 连续 7–14 天；source/lifecycle/book 指标稳定 |
| P3 小额 live | 只做赢家 Yes + FAK，白名单站点 | 是 | 逐周复盘；无 P0/P1 事故后才扩容 |

P3 初始建议：每事件 $10–$50、全局天气仓不超过 $500、只允许 max_ask=0.995 以内、订单 TTL 3–5 秒；这些是保护性起始参数，不是收益承诺。

## 7.2 P0：规则注册与人工审批

1. 同步 Gamma 未来 48 小时 Weather markets；
2. 按 question/description/resolution source 聚合同一 event 的兄弟桶；
3. 抓取并保存 rules 原文和 source contract；
4. 解析站点、日期、时区、单位、aggregation、fallback、revision cutoff；
5. 生成 rule review 页面，人工确认每一字段；
6. 审批后写入 rule_version，任何文本变化自动生成新版本并撤销旧审批；
7. 保持 live executor 进程没有私钥/下单权限。

人工审批最小问题：

~~~text
PM 问题对应哪个站点 ID？
自然日按哪个 IANA timezone？
指标是观测最高/最低，还是预报？
单位和舍入在什么时候发生？
首个次日数据点/最终标志如何定义？
NOAA 不可用时能否用 WU？截止时间是什么？
无数据、冲突、修订如何处理？
11 个桶是否互斥且完备？
~~~

## 7.3 P1：回放执行

~~~bash
cd /home/guanyin/aosp/temp/dqdhook
python3 -m weather.replay \
  --dataset /path/to/weather-replay \
  --as-of source_timestamp \
  --books historical \
  --output /path/to/reports/weather-replay.json
~~~

每次报告由负责人签字保存；失败事件必须能点击到原始 rules/source/book payload，而不是只有最终数字。

## 7.4 P2：Shadow 运行

~~~bash
cd /home/guanyin/aosp/temp/dqdhook
python3 -m weather.scan \
  --sync \
  --categories weather \
  --horizon-hours 48 \
  --dry-run \
  --no-orders \
  --output data/pm-weather/
~~~

运行要求：

- 只读 Gamma、CLOB、NWS/WU；HTTP 代理和超时策略与现有 finality scanner 一致；
- 输出 source poll、finality、candidate、reject、book snapshot、health JSONL；
- 每个候选保存“假设成交价”和下单时刻，次日与真实 PM result 对账；
- 每日检查 review reason 是否因规则变化突然升高；
- 任何人工修改规则后重跑受影响事件，不直接覆盖旧日志。

## 7.5 P3：小额 live 前检查表

- [ ] 账户地区、平台条款和相关合规要求已确认；
- [ ] 只启用白名单 station/rule_version；
- [ ] source finality 经过历史和在线 shadow 验证；
- [ ] Gamma lifecycle 和 CLOB fee/tick/min size 在下单前二次刷新；
- [ ] weather_executor 使用独立凭证/进程，默认资金上限已设置；
- [ ] FAK、无 GTC、无 negRisk、多腿；
- [ ] candidate/evidence 幂等和重启 reconcile 已验证；
- [ ] P0/P1 告警接收人和人工停机命令已测试；
- [ ] 先用单事件、单站点、单笔小额，完成一次结算/redeem 对账后再扩大范围。

## 7.6 Live 操作流程

~~~text
启动 -> 打印配置快照 -> 校验 live flag/凭证/余额
     -> registry sync -> source poll -> finality
     -> lifecycle refresh -> book refresh -> economics
     -> risk gate -> submit FAK
     -> reconcile ack/fill -> ledger -> monitor resolve/redeem
~~~

操作员每小时查看：circuit 状态、source freshness、未 redeem、未完成订单、近 20 单 fill ratio、今日净风险金额。

## 7.7 停机与回滚

### 软停机（首选）

将 live_orders=false 或关闭 Weather executor；继续运行 source/evidence/settlement，方便审计。已有订单不自动卖出，由 ledger/reconcile 继续跟踪。

### 紧急停机

1. 设置 global circuit OPEN；
2. 撤销未成交 Weather orders；
3. 导出最近 24h candidate/order/fill/evidence；
4. 锁定 rule_version 和 adapter 版本；
5. 通知值班人和负责人；
6. 只有完成根因分析、回放重现和人工批准后才解锁。

### 代码回滚

- 不删除 raw evidence、订单和账本；
- 回滚 adapter 时保留旧版本读路径，禁止用新版本覆盖旧结果；
- 将受影响事件标记为 FROZEN_REVIEW；
- 任何已成交仓位继续由 settlement monitor 追踪。

## 7.8 日报模板

~~~text
日期 / 运行版本 / rule_version:
扫描事件数 / final 事件数 / review 数 / 候选数:
假设成交数 / 实际成交数 / fill ratio:
VWAP、fee、net edge 分布:
source 延迟、fallback、修订:
market closed race / stale book / API errors:
未结算仓位 / 待 redeem / dispute:
今日 P0/P1/P2/P3 告警:
负责人结论：继续 / 降额 / 熔断 / 扩大白名单
~~~


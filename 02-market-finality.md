# 02. 天气市场、规则解析与终局判定

## 2.1 P0 市场范围

首批白名单只接受以下形态：

```text
指定机场/官方气象站 + 明确本地自然日 + daily_max 或 daily_min
                         + 离散、互斥、覆盖完整的温度桶
```

例如伦敦事件规则明确使用 NOAA London City Airport Station，整摄氏度，结果范围为 22°C 或以下至 32°C 或以上；NOAA 不可用时在次日 11:59 PM ET 后使用 Wunderground，且首次次日数据点发布后才结算。[London resolution rules](https://polymarket.com/event/highest-temperature-in-london-on-september-6-2026)

旧的西雅图事件也采用指定来源、全天最高温和次日首个数据点的模式，说明“日终观测 + 次日确认”是可抽象的合约族，而不是单个城市的特例。[Seattle resolution rules](https://polymarket.com/event/highest-temperature-in-seattle-on-july-17-2026)

暂不纳入：

- 只有城市名、没有站点的市场；
- 预报高温/低温（forecast）而非观测值（observed）；
- 降雨概率、降雪深度、风速等统计周期不明确的市场；
- “approximately”“to be determined”、规则依赖人工解释的市场；
- negRisk、跨市场组合、同一事件并非互斥完备的桶。

## 2.2 市场分组与兄弟桶

Gamma 返回的是单个 market；交易时必须把同一 event 的 11 个 outcome 组成 `event_group_id`，验证：

1. 站点、日期、单位、指标和 source URL 相同；
2. 桶区间无重叠、无空洞；
3. 恰好一个桶覆盖任何合法最终值（首尾桶分别是 `(-∞, lower]`、`[upper, +∞)`）；
4. 每个桶只有一个 Yes token，outcome 数量与 token 数量匹配；
5. 各兄弟 market 的 resolution rules 文本 hash 相同，或差异已人工解释并记录。

无法证明以上条件时，不能用“买一份 Yes 必赢”的假设；进入 `RULE_REVIEW`。

## 2.3 规则结构化模型

解析器不直接把自然语言变成交易指令，而是生成如下不可变 `rule_version`：

```yaml
rule_version: weather-london-2026-09-06-v1
event_group_id: highest-temperature-london-2026-09-06
market_ids: ["..."]
adapter: weather_observation
source:
  provider: NOAA
  station_id: EGLC
  fallback_provider: wunderground
  fallback_after: 2026-09-07T23:59:00-04:00
  url: https://www.weather.gov/wrh/timeseries?site=eglc
  api_url: https://api.weather.gov/stations/EGLC/observations
metric: daily_max
local_timezone: Europe/London
observation_local_date: 2026-09-06
observation_start: 2026-09-06T00:00:00+01:00
observation_end: 2026-09-06T23:59:59+01:00
unit: C
rounding: whole_degree_as_published
finality:
  requires_first_following_date_point: true
  revisions_allowed_until: first_following_date_point
  no_data_policy: review
buckets:
  - {outcome: "22 or below", lower: null, upper: 22, upper_inclusive: true}
  - {outcome: "23", lower: 22, lower_exclusive: true, upper: 23, upper_inclusive: true}
  # ...
  - {outcome: "32 or higher", lower: 32, lower_inclusive: true, upper: null}
manual_approval: true
```

注意：示例中的具体字段/阈值必须从每个市场的原文和 API 实际响应生成，不能把伦敦规则复制给其他城市。

## 2.4 数据源策略

### 主源：NWS / NOAA

NWS Web API 是公开的 `https://api.weather.gov`；站点 observations 可以提供带时间戳的原始观测。[NWS API documentation](https://www.weather.gov/documentation/services-web-api)

实现要求：

- 使用站点 ID，不用地理反查结果替代合约站点；
- 以 observation timestamp 转换到合约指定本地时区后过滤自然日；
- 读取完整 hourly/observation collection，计算 `daily_max`/`daily_min`，不要只读一条 latest；
- 保存 `temperature.value`、单位、时间戳、quality/finality 字段以及原始 JSON；
- 处理缺测、重复时间戳、非数值、HTTP 429/5xx 和源延迟；
- NWS 文档提示某些站点 24 小时最高/最低字段可能受 MADIS bug 影响，observations 也可能因质量控制延迟约 20 分钟，因此不能把 summary 字段直接当最终值。

NWS Time Series Viewer 明确提示数据是 preliminary，可能被质量控制调整；数据频率和可用时间因站点而异。[NWS Time Series Viewer](https://www.weather.gov/wrh/timeseries?site=eglc)

### 备用源：Wunderground

仅当 resolution rules 明确写出 Wunderground fallback 才使用。其机场 ASOS 观测通常按小时或更高频率更新，但站点/来源混合和近站选择规则不同。[Wunderground About Data](https://www.wunderground.com/about/data)

备用源适配器必须：

- 保存 WU 页面/接口原文、站点标识和抓取时刻；
- 证明站点和 PM 规则一致，不能用“离城市最近的 PWS”替代机场站；
- 记录触发 fallback 的证据（NOAA unavailable、截止时间已过）；
- 进入 `source_provider=wunderground` 后提高人工审计级别；
- NOAA 和 WU 给出不同值时禁止自动选择，进入 review。

## 2.5 Finality 判定算法

定义：

```text
window_end = 观测日最后一秒（按规则时区）
confirmation = 首个 following-date 数据点，或规则明确的 final 标记/截止时间
```

伪代码：

```python
def evaluate_finality(rule, observations, now):
    if now < rule.observation_end:
        return WAITING_WINDOW

    in_window = normalize_and_filter(observations, rule.observation_start,
                                     rule.observation_end, rule.timezone)
    if not in_window:
        return PROVISIONAL  # 等待迟到数据；超过 timeout 才 review

    aggregate = aggregate_metric(in_window, rule.metric)
    next_day = first_observation_after(observations, rule.observation_end,
                                       rule.timezone)
    final = has_explicit_final_flag(in_window, rule) or (
        rule.requires_first_following_date_point and next_day is not None
    )
    if not final:
        return PROVISIONAL(value=aggregate)

    bucket = map_to_exact_bucket(aggregate, rule.buckets, rule.rounding)
    if bucket is None or not siblings_are_complete(rule):
        return RULE_REVIEW
    return SOURCE_FINAL(value=aggregate, bucket=bucket,
                        confirmation_ts=next_day.timestamp)
```

关键语义：

- “窗口结束”只允许进入 `PROVISIONAL`，不能直接买；
- `latest` 观测不是 `daily_max` 的最终值；
- 首个次日数据点是 PM 规则中的结算确认边界，不能用本地午夜或 `endDate` 代替；
- 若 PM 规则允许 revisions until next-day point，则在 confirmation 前所有聚合值都不可交易；
- confirmation 后若源再次修订，冻结该事件并报警，不自动反向交易。

## 2.6 单位、时区与桶边界

统一用整数/有理数表示最终温度，不用二进制浮点直接做边界比较：

```text
raw_value -> unit conversion -> rule rounding -> rational integer -> bucket
```

必须测试：

- °C ↔ °F 转换后的舍入顺序；
- `22 or below`、`32 or higher` 的闭区间；
- 恰好阈值、负温度、夏令时切换日、跨 UTC 日；
- 观测 timestamp 带 `Z`、偏移、无时区三种异常；
- 站点当地 00:00–23:59 与 UTC 00:00–23:59 的差异。

## 2.7 现有代码需要补强的点

- 现有 [`WeatherObservationAdapter`](/home/guanyin/aosp/temp/dqdhook/.cursor/skills/polymarket-finality/scripts/adapters.py:337) 已支持 `daily_max`、`daily_min`、`daily_sum`、`latest` 和 observation bounds，但需要补充“following-date confirmation”和 11 桶映射。
- 当前自动规则发现的阈值提取主要识别 `above/below`；“highest temperature … 26°C”会进入 `threshold_or_comparison_ambiguous`，不能直接自动启用。[threshold parser](/home/guanyin/aosp/temp/dqdhook/.cursor/skills/polymarket-finality/scripts/rule_discovery.py:245)
- 规则自动启用要求 machine-readable source contract（URL、value path、timestamp path、finality）；HTML/chart URL 不能单独作为自动批准理由。[source contract](/home/guanyin/aosp/temp/dqdhook/.cursor/skills/polymarket-finality/scripts/rule_discovery.py:317)

## 2.8 PM 页面与 API 的等价性审计

PM resolution rules 可能链接到 HTML Timeseries Viewer，而实现为了稳定聚合会调用 NWS JSON API。两者不能默认等价，必须在 rule_version 中记录审计结论：

- HTML 页面使用的站点 ID、时间范围、单位和字段，与 JSON API 请求完全一致；
- 页面中的“最高温”是否来自 hourly observations、日摘要或人工发布值；
- JSON API 的质量标记、修订和发布时间是否覆盖页面语义；
- 若无法证明等价，直接使用规则原始页面作为只读证据，并把自动交易状态设为 REVIEW_REQUIRED。

这一审计是天气适配器进入 P3 的前置条件，不能用“两个页面都显示同一个数字”替代。

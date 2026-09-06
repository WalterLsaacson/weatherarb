# 04. 数据模型、接口与配置

## 4.1 数据分层

~~~text
raw_*       不可变原始响应（source / Gamma / CLOB）
normalized  规范化对象（market / observation / book）
decision    finality、bucket、economics、risk gate 结果
execution   order / fill / position / redeem
~~~

任何层都不能覆盖历史版本；更新通过 observed_at、received_at、version 和 hash 追加。

## 4.2 核心实体

### weather_event_group

~~~json
{
  "event_group_id": "highest-temperature-london-2026-09-06",
  "metric": "daily_max",
  "station_id": "EGLC",
  "local_date": "2026-09-06",
  "timezone": "Europe/London",
  "unit": "C",
  "market_ids": ["market-a", "market-b"],
  "sibling_count": 11,
  "bucket_set_hash": "sha256:...",
  "rule_version": "weather-london-2026-09-06-v1",
  "status": "PROVISIONAL"
}
~~~

### weather_market

~~~json
{
  "market_id": "market-a",
  "condition_id": "condition-a",
  "event_group_id": "highest-temperature-london-2026-09-06",
  "question": "Will the highest temperature ... be 26°C?",
  "slug": "highest-temperature-in-london-on-september-6-2026",
  "outcome": "26",
  "yes_token_id": "123...",
  "no_token_id": "456...",
  "resolution_source": "https://www.weather.gov/wrh/timeseries?site=eglc",
  "end_date": "2026-09-07T00:00:00Z",
  "active": true,
  "closed": false,
  "accepting_orders": true,
  "enable_order_book": true,
  "neg_risk": false,
  "tick_size": "0.001",
  "min_order_size": "1",
  "fee_schedule_id": "weather-current",
  "gamma_raw_hash": "sha256:...",
  "updated_at": "2026-09-07T00:02:00Z"
}
~~~

### weather_rule_version

~~~json
{
  "rule_version": "weather-london-2026-09-06-v1",
  "event_group_id": "highest-temperature-london-2026-09-06",
  "source": {
    "provider": "NOAA",
    "station_id": "EGLC",
    "url": "https://api.weather.gov/stations/EGLC/observations",
    "fallback_provider": "WU",
    "fallback_url": "https://www.wunderground.com/history/daily/...",
    "value_path": "properties.temperature.value",
    "timestamp_path": "properties.timestamp",
    "aggregation": "daily_max"
  },
  "observation_start": "2026-09-06T00:00:00+01:00",
  "observation_end": "2026-09-06T23:59:59+01:00",
  "finality": {
    "mode": "first_following_date_point",
    "fallback_deadline": "2026-09-07T23:59:00-04:00",
    "revision_cutoff": "confirmation_timestamp"
  },
  "buckets": [
    {"outcome": "22 or below", "upper": 22, "upper_inclusive": true},
    {"outcome": "23", "lower": 22, "lower_exclusive": true,
     "upper": 23, "upper_inclusive": true}
  ],
  "manual_approval": true,
  "approved_by": "operator-id",
  "approved_at": "2026-09-06T23:10:00Z",
  "rules_text_hash": "sha256:..."
}
~~~

### weather_observation_evidence

~~~json
{
  "evidence_id": "ev-...",
  "event_group_id": "...",
  "provider": "NOAA",
  "station_id": "EGLC",
  "request_url": "https://api.weather.gov/stations/EGLC/observations",
  "requested_at": "2026-09-07T00:05:00Z",
  "received_at": "2026-09-07T00:05:01Z",
  "source_timestamp_min": "2026-09-06T00:03:00+01:00",
  "source_timestamp_max": "2026-09-07T00:02:00+01:00",
  "aggregation": "daily_max",
  "aggregate_value": 26,
  "unit": "C",
  "finality_status": "final",
  "confirmation_timestamp": "2026-09-07T00:02:00+01:00",
  "raw_payload_uri": "raw/noaa/sha256-....json.gz",
  "payload_sha256": "sha256:...",
  "adapter_version": "weather-noaa-0.1.0"
}
~~~

### weather_candidate

~~~json
{
  "candidate_id": "cand-...",
  "event_group_id": "...",
  "rule_version": "...-v1",
  "evidence_id": "ev-...",
  "target_market_id": "market-a",
  "settlement_side": "YES",
  "target_token_id": "123...",
  "source_status": "final",
  "rule_status": "matched",
  "market_status_snapshot": {
    "active": true,
    "closed": false,
    "accepting_orders": true
  },
  "book_snapshot_id": "book-...",
  "economics": {
    "vwap": 0.991,
    "shares": 100,
    "gross_edge": 0.009,
    "fee_rate": 0.05,
    "fee_per_share": 0.000446,
    "net_edge": 0.008554
  },
  "decision": "DRY_RUN",
  "decision_reason": "source_final_rule_match_book_ok",
  "created_at": "2026-09-07T00:05:03Z"
}
~~~

## 4.3 数据库表（POC 最小集合）

~~~sql
weather_event_group(
  event_group_id TEXT PRIMARY KEY,
  local_date DATE, timezone TEXT, station_id TEXT, metric TEXT, unit TEXT,
  rule_version TEXT, status TEXT, bucket_set_hash TEXT,
  created_at TIMESTAMP, updated_at TIMESTAMP
);

weather_market(
  market_id TEXT PRIMARY KEY, event_group_id TEXT, condition_id TEXT,
  outcome TEXT, yes_token_id TEXT, no_token_id TEXT,
  active BOOLEAN, closed BOOLEAN, accepting_orders BOOLEAN,
  enable_order_book BOOLEAN, neg_risk BOOLEAN, tick_size TEXT,
  min_order_size TEXT, fee_schedule_id TEXT, raw_hash TEXT, updated_at TIMESTAMP
);

weather_rule_version(
  rule_version TEXT PRIMARY KEY, event_group_id TEXT, rule_json JSON,
  rules_text_hash TEXT, manual_approval BOOLEAN, approved_by TEXT,
  approved_at TIMESTAMP, created_at TIMESTAMP
);

weather_evidence(
  evidence_id TEXT PRIMARY KEY, event_group_id TEXT, provider TEXT,
  station_id TEXT, aggregate_value NUMERIC, unit TEXT, finality_status TEXT,
  confirmation_ts TIMESTAMP, payload_sha256 TEXT, raw_uri TEXT,
  adapter_version TEXT, received_at TIMESTAMP
);

weather_candidate(
  candidate_id TEXT PRIMARY KEY, event_group_id TEXT, evidence_id TEXT,
  target_market_id TEXT, target_token_id TEXT, decision TEXT,
  net_edge NUMERIC, reason TEXT, created_at TIMESTAMP
);

weather_order(
  client_order_id TEXT PRIMARY KEY, candidate_id TEXT, token_id TEXT,
  side TEXT, order_type TEXT, limit_price NUMERIC, size NUMERIC,
  status TEXT, exchange_order_id TEXT, submitted_at TIMESTAMP,
  cancelled_at TIMESTAMP, raw_ack_hash TEXT
);

weather_fill(
  fill_id TEXT PRIMARY KEY, client_order_id TEXT, price NUMERIC,
  size NUMERIC, fee NUMERIC, matched_at TIMESTAMP, raw_fill_hash TEXT
);
~~~

## 4.4 模块接口

~~~python
class WeatherMarketRegistry:
    def sync(self, *, horizon_hours: int) -> list[WeatherEventGroup]: ...
    def refresh_lifecycle(self, market_ids: list[str]) -> dict[str, MarketState]: ...

class WeatherRuleParser:
    def parse(self, markets: list[WeatherMarket]) -> RuleParseResult: ...
    def approve(self, rule_version: str, operator: str) -> None: ...

class WeatherSourceAdapter:
    def poll(self, rule: RuleVersion, now: datetime) -> ObservationEvidence: ...
    def evaluate_finality(self, rule: RuleVersion, evidence: ObservationEvidence) -> Finality: ...

class WeatherBucketMapper:
    def map(self, value: Rational, buckets: list[Bucket]) -> BucketMatch: ...

class WeatherEconomics:
    def quote(self, token_id: str, book: OrderBook, fee: FeeSchedule,
              limits: Limits) -> Economics: ...

class WeatherExecutor:
    def submit_fak(self, candidate: Candidate, economics: Economics) -> OrderResult: ...
    def reconcile(self, client_order_id: str) -> list[Fill]: ...
~~~

## 4.5 HTTP / 文件接口

| 接口 | 用途 | 约束 |
|---|---|---|
| Gamma /markets | 发现和刷新市场 | 只读；分页、缓存、保存 raw hash |
| CLOB /books | 批量盘口 | 目标 token 批量读；记录 tick/min size |
| CLOB /book | 单 token 二次确认 | 下单前刷新 |
| NWS /stations/{id}/observations | 主源观测 | User-Agent、重试、原文留存 |
| WU contract URL | 规则允许时 fallback | 不可泛化为任意 PWS |
| scan.jsonl | 所有状态/拒绝 | append-only |
| evidence.jsonl | 证据 hash/时间 | append-only |
| candidates.jsonl | dry-run/交易候选 | candidate_id 幂等 |

## 4.6 配置示例

~~~yaml
weather:
  enabled: true
  live_orders: false
  categories: [weather]
  horizon_hours: 48
  poll_interval_s: 60
  post_window_poll_interval_s: 20
  source_timeout_s: 15
  source_max_age_s: 120
  book_ttl_s: 3
  max_ask: 0.995
  max_slippage: 0.003
  min_net_edge: 0.0075
  event_cap_usdc: 50
  global_cap_usdc: 500
  order_ttl_s: 3
  use_fak: true
  use_gtc: false
  allow_fallback_source: true
  require_manual_rule_approval: true
  allow_neg_risk: false
  allow_multileg: false
~~~

所有 live 配置要有启动时打印的脱敏快照和变更审计；默认值必须安全（live_orders=false、use_gtc=false）。


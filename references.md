# 参考资料（官方优先）

本方案使用的市场规则和技术事实均应在实现时重新抓取并保存快照。页面价格、成交量、费率和可交易状态会变化，以下链接是入口，不是永久快照。

## Polymarket

1. [Weather / Climate & Science markets](https://polymarket.com/climate-science/weather)  
   用于确认天气市场类别、城市/日期事件形态。页面统计是动态的，文档只记录抓取时观察。

2. [Highest temperature in London on September 6, 2026](https://polymarket.com/event/highest-temperature-in-london-on-september-6-2026)  
   11 个整摄氏度桶；规则指定 NOAA London City Airport Station，次日首个数据点确认，NOAA 不可用时有 Wunderground fallback。

3. [Highest temperature in San Francisco on September 5, 2026](https://polymarket.com/event/highest-temperature-in-san-francisco-on-september-5-2026)  
   规则指定 NOAA SFO hourly data，整华氏度，并展示了 scheduled end date 经过但市场仍可能保持 open 的生命周期竞态。

4. [Highest temperature in Seattle on July 17, 2026](https://polymarket.com/event/highest-temperature-in-seattle-on-july-17-2026)  
   用于对照 Wunderground 指定来源、全天最高温和次日数据点语义。

5. [Polymarket resolution concepts](https://docs.polymarket.com/concepts/resolution)  
   resolution rules、来源、end date、UMA optimistic oracle、提案/挑战期。

6. [Polymarket trading fees](https://docs.polymarket.com/trading/fees)  
   maker/taker 费率和 fee = C × feeRate × p × (1-p)；生产实现应读实际市场费率。

7. [CLOB get order books](https://docs.polymarket.com/api-reference/market-data/get-order-books-request-body)  
   批量 /books、asks/bids、tick_size、min_order_size。

8. [Markets API](https://docs.polymarket.com/api-reference/markets/list-markets)  
   Gamma /markets 的市场状态、resolution source、outcomes、active/closed、acceptingOrders、order book 和费用字段。

## 官方气象源

9. [NWS Web API documentation](https://www.weather.gov/documentation/services-web-api)  
   api.weather.gov、站点 observations、质量控制和延迟说明。

10. [NWS Time Series Viewer](https://www.weather.gov/wrh/timeseries?site=eglc)  
    页面提示数据 preliminary、可能被质量控制调整；实现需要保存原始时间序列。

11. [Wunderground About Data](https://www.wunderground.com/about/data)  
    机场 ASOS 和其他来源的更新频率/来源说明；只在 PM 规则明确允许时作 fallback。

## 本地项目基线

- [足球机器人 README](/home/guanyin/aosp/temp/dqdhook/README.md:1)
- [通用 finality skill](/home/guanyin/aosp/temp/dqdhook/.cursor/skills/polymarket-finality/SKILL.md:1)
- [WeatherObservationAdapter](/home/guanyin/aosp/temp/dqdhook/.cursor/skills/polymarket-finality/scripts/adapters.py:337)
- [自动规则阈值解析](/home/guanyin/aosp/temp/dqdhook/.cursor/skills/polymarket-finality/scripts/rule_discovery.py:245)
- [source contract 校验](/home/guanyin/aosp/temp/dqdhook/.cursor/skills/polymarket-finality/scripts/rule_discovery.py:317)
- [足球费率/赢家判定](/home/guanyin/aosp/temp/dqdhook/.cursor/skills/polymarket-quote/scripts/quote_lib.py:1525)
- [足球 rest tick 约束](/home/guanyin/aosp/temp/dqdhook/.cursor/skills/polymarket-quote/scripts/rest_ladder.py:18)

## 引用和快照规范

每次研究/回放保存：

~~~text
抓取时间（UTC）
请求 URL 和 HTTP 状态
rules 原文及 hash
source 原文及 hash
Gamma market snapshot 及 hash
CLOB book snapshot 及 hash
代码版本 / adapter 版本
~~~

报告中的动态价格、成交量、市场状态必须带抓取时间；不得把页面当前值写成长期不变的配置。


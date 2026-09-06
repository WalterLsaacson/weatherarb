from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from weather_runtime.markets import (
    catalog_event_groups,
    filter_events_in_horizon,
    flatten_event_markets,
    group_weather_markets,
    load_market_rows,
    merge_event_groups,
    observation_in_horizon,
    weather_markets,
)
from weather_runtime.discovery import discover_rules
from weather_runtime.rules import (
    ROUNDING_NONE,
    RuleError,
    apply_rounding,
    bucket_for_value,
    bucket_impossible_while_open,
    buckets_from_outcomes,
    extract_event_group_id,
    load_rules,
    normalize_market,
    parse_bucket,
    parse_rule,
    validate_buckets,
    validate_event_group_siblings,
)
from weather_runtime.scanner import WeatherScanner, WeatherScannerConfig
from weather_runtime.service import RuntimeService, slim_board_groups
from weather_runtime.sources import WeatherSourceAdapter
from weather_runtime.storage import append_jsonl_dedup, load_json


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "fixtures"


class WeatherRuntimeTests(unittest.TestCase):
    def test_fixture_rules_cover_buckets(self) -> None:
        rules = load_rules(FIXTURES / "rules.json")
        self.assertEqual(len(rules), 11)
        self.assertEqual(bucket_for_value(26, rules[0].buckets).outcome, "26")
        self.assertEqual(bucket_for_value(22, rules[0].buckets).outcome, "22 or below")
        self.assertEqual(bucket_for_value(40, rules[0].buckets).outcome, "32 or higher")

    def test_bucket_validation_rejects_gap(self) -> None:
        with self.assertRaises(RuleError):
            validate_buckets(
                [
                    parse_bucket({"outcome": "low", "upper": 1}),
                    parse_bucket({"outcome": "high", "lower": 2}),
                ]
            )

    def test_bucket_validation_rejects_closed_boundary_overlap(self) -> None:
        with self.assertRaises(RuleError):
            validate_buckets(
                [
                    parse_bucket({"outcome": "low", "upper": 1}),
                    parse_bucket({"outcome": "high", "lower": 1}),
                ]
            )

    def test_source_explicit_final(self) -> None:
        rule = load_rules(FIXTURES / "rules.json")[0]
        observation = WeatherSourceAdapter().poll(
            rule,
            now=datetime(2026, 9, 5, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(observation.status, "final")
        self.assertEqual(observation.value, 26)
        self.assertTrue(observation.evidence_hash)

    def test_source_intraday_running_extremum_during_open_window(self) -> None:
        rule = load_rules(FIXTURES / "rules.json")[0]
        observation = WeatherSourceAdapter().poll(
            rule,
            now=datetime(2026, 9, 4, 12, tzinfo=timezone.utc),
        )
        self.assertEqual(observation.status, "intraday")
        self.assertEqual(observation.value, 26)
        self.assertEqual(observation.reason, "running_extremum")

    def test_source_does_not_fetch_before_observation_start(self) -> None:
        class CountingAdapter(WeatherSourceAdapter):
            def __init__(self) -> None:
                super().__init__()
                self.payload_calls = 0

            def _payload(self, rule):  # type: ignore[override]
                self.payload_calls += 1
                return super()._payload(rule)

        rule = load_rules(FIXTURES / "rules.json")[0]
        adapter = CountingAdapter()
        before = adapter.poll(rule, now=datetime(2026, 9, 3, 12, tzinfo=timezone.utc))
        self.assertEqual(before.status, "waiting_window")
        self.assertEqual(before.reason, "observation_window_not_started")
        self.assertEqual(adapter.payload_calls, 0)
        during = adapter.poll(rule, now=datetime(2026, 9, 4, 12, tzinfo=timezone.utc))
        self.assertEqual(during.status, "intraday")
        self.assertEqual(adapter.payload_calls, 1)

    def test_source_intraday_ignores_points_after_now(self) -> None:
        raw = {
            "market_id": "m-intraday-future",
            "event_group_id": "e-intraday-future",
            "source": {
                "static": {
                    "features": [
                        {"value": 22, "timestamp": "2026-09-06T02:00:00Z"},
                        {"value": 19, "timestamp": "2026-09-06T06:00:00Z"},
                        {"value": 15, "timestamp": "2026-09-06T18:00:00Z"},
                    ]
                },
                "url": "fixture://weather/intraday-future",
                "resolution_source": "fixture://weather/intraday-future",
                "value_path": "value",
                "timestamp_path": "timestamp",
            },
            "timezone": "UTC",
            "metric": "daily_min",
            "observation_start": "2026-09-06T00:00:00Z",
            "observation_end": "2026-09-06T23:59:59Z",
            "buckets": [
                {"outcome": "low", "upper": 20},
                {"outcome": "high", "lower": 20, "lower_inclusive": False},
            ],
        }
        observation = WeatherSourceAdapter().poll(
            parse_rule(raw),
            now=datetime(2026, 9, 6, 12, tzinfo=timezone.utc),
        )
        self.assertEqual(observation.status, "intraday")
        self.assertEqual(observation.value, 19)

    def test_source_waits_for_following_point(self) -> None:
        raw = {
            "market_id": "m",
            "event_group_id": "e",
            "source": {
                "static": {
                    "features": [{"value": 25, "timestamp": "2026-09-04T12:00:00Z"}]
                },
                "url": "fixture://weather/e",
                "resolution_source": "fixture://weather/e",
                "value_path": "value",
                "timestamp_path": "timestamp",
                "finality_mode": "first_following_date_point",
            },
            "timezone": "UTC",
            "metric": "daily_max",
            "observation_start": "2026-09-04T00:00:00Z",
            "observation_end": "2026-09-04T23:59:59Z",
            "buckets": [
                {"outcome": "low", "upper": 30},
                {"outcome": "high", "lower": 30, "lower_inclusive": False},
            ],
        }
        rule = parse_rule(raw)
        observation = WeatherSourceAdapter().poll(
            rule,
            now=datetime(2026, 9, 5, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(observation.status, "provisional")

    def test_source_unavailable_is_review_not_waiting(self) -> None:
        raw = {
            "market_id": "m-unavailable",
            "event_group_id": "e-unavailable",
            "source": {
                "static": {"features": []},
                "url": "fixture://weather/unavailable",
                "resolution_source": "fixture://weather/unavailable",
                "value_path": "value",
                "timestamp_path": "timestamp",
                "final_path": "final",
            },
            "timezone": "UTC",
            "metric": "daily_max",
            "observation_start": "2026-09-04T00:00:00Z",
            "observation_end": "2026-09-04T23:59:59Z",
            "buckets": [
                {"outcome": "low", "upper": 30},
                {"outcome": "high", "lower": 30, "lower_inclusive": False},
            ],
        }
        rule = parse_rule(raw)
        observation = WeatherSourceAdapter().poll(
            rule,
            now=datetime(2026, 9, 5, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(observation.status, "unavailable")

    def test_fixture_scan_has_one_candidate(self) -> None:
        markets = weather_markets(load_market_rows(str(FIXTURES / "markets.json")))
        rules = load_rules(FIXTURES / "rules.json")
        books = load_json(FIXTURES / "books.json", {})
        stamp = datetime(2026, 9, 5, 1, tzinfo=timezone.utc).isoformat()
        books = {token: {**book, "fetched_at": stamp} for token, book in books.items()}
        result = WeatherScanner(config=WeatherScannerConfig()).scan(
            markets,
            rules,
            books=books,
            fetch_books=False,
            now=datetime(2026, 9, 5, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(result["summary"]["event_groups"], 1)
        self.assertEqual(result["summary"]["market_match_rate"], 1.0)
        self.assertEqual(result["summary"]["bucket_match_rate"], 1.0)
        self.assertEqual(result["summary"]["opportunities"], 1)
        self.assertEqual(sum(1 for row in result["rows"] if row.get("book")), 11)
        self.assertTrue(all("raw" not in (row.get("market") or {}) for row in result["rows"]))
        self.assertTrue(all("raw" not in (row.get("observation") or {}) for row in result["rows"]))
        candidate = next(row for row in result["rows"] if row["status"] == "opportunity")
        self.assertEqual(candidate["matched_outcome"], "26")
        self.assertEqual(candidate["trade_side"], "YES")
        self.assertAlmostEqual(candidate["economics"]["net_edge"], 0.009505, places=6)
        losers = [row for row in result["rows"] if row.get("trade_side") == "NO"]
        self.assertEqual(len(losers), 10)
        self.assertTrue(all(row["status"] == "no_trade" for row in losers))
        self.assertTrue(all(row["reason"] == "ask_above_limit" for row in losers))

    def test_neg_risk_flag_does_not_block_winner_yes(self) -> None:
        raw_markets = load_market_rows(str(FIXTURES / "markets.json"))
        for row in raw_markets:
            row["negRisk"] = True
        markets = weather_markets(raw_markets)
        self.assertTrue(all(item.neg_risk and item.tradable for item in markets))
        rules = load_rules(FIXTURES / "rules.json")
        books = load_json(FIXTURES / "books.json", {})
        stamp = datetime(2026, 9, 5, 1, tzinfo=timezone.utc).isoformat()
        books = {token: {**book, "fetched_at": stamp} for token, book in books.items()}
        result = WeatherScanner(config=WeatherScannerConfig()).scan(
            markets,
            rules,
            books=books,
            fetch_books=False,
            now=datetime(2026, 9, 5, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(result["summary"]["opportunities"], 1)
        candidate = next(row for row in result["rows"] if row["status"] == "opportunity")
        self.assertEqual(candidate["matched_outcome"], "26")

    def test_reject_neg_risk_opt_in_blocks_winner_yes(self) -> None:
        raw_markets = load_market_rows(str(FIXTURES / "markets.json"))
        for row in raw_markets:
            row["negRisk"] = True
        markets = weather_markets(raw_markets)
        rules = load_rules(FIXTURES / "rules.json")
        books = load_json(FIXTURES / "books.json", {})
        stamp = datetime(2026, 9, 5, 1, tzinfo=timezone.utc).isoformat()
        books = {token: {**book, "fetched_at": stamp} for token, book in books.items()}
        result = WeatherScanner(
            config=WeatherScannerConfig(reject_neg_risk=True)
        ).scan(
            markets,
            rules,
            books=books,
            fetch_books=False,
            now=datetime(2026, 9, 5, 1, tzinfo=timezone.utc),
        )
        target = next(row for row in result["rows"] if row["target_outcome"] == "26")
        self.assertEqual(target["status"], "no_trade")
        self.assertEqual(target["reason"], "neg_risk_rejected")
        self.assertEqual(result["summary"]["opportunities"], 0)

    def _stamped_books(self, extra: Optional[dict] = None) -> dict:
        books = load_json(FIXTURES / "books.json", {})
        if extra:
            books = {**books, **extra}
        stamp = datetime(2026, 9, 5, 1, tzinfo=timezone.utc).isoformat()
        return {token: {**dict(book), "fetched_at": stamp} for token, book in books.items()}

    def test_loser_no_is_independent_candidate(self) -> None:
        markets = weather_markets(load_market_rows(str(FIXTURES / "markets.json")))
        rules = load_rules(FIXTURES / "rules.json")
        books = self._stamped_books(
            {
                "wx-london-26-yes": {
                    "token_id": "wx-london-26-yes",
                    "asks": [{"price": 0.999, "size": 20}],
                    "tick_size": 0.001,
                    "min_order_size": 1,
                },
                "wx-london-23-no": {
                    "token_id": "wx-london-23-no",
                    "asks": [{"price": 0.990, "size": 20}],
                    "tick_size": 0.001,
                    "min_order_size": 1,
                },
            }
        )
        result = WeatherScanner(config=WeatherScannerConfig()).scan(
            markets,
            rules,
            books=books,
            fetch_books=False,
            now=datetime(2026, 9, 5, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(result["summary"]["opportunities"], 1)
        candidate = next(row for row in result["rows"] if row["status"] == "opportunity")
        self.assertEqual(candidate["trade_side"], "NO")
        self.assertEqual(candidate["target_outcome"], "23")
        self.assertEqual(candidate["matched_outcome"], "26")
        winner = next(row for row in result["rows"] if row["target_outcome"] == "26")
        self.assertEqual(winner["status"], "no_trade")
        self.assertEqual(winner["reason"], "ask_above_limit")

    def test_winner_yes_and_loser_no_can_both_be_candidates(self) -> None:
        markets = weather_markets(load_market_rows(str(FIXTURES / "markets.json")))
        rules = load_rules(FIXTURES / "rules.json")
        books = self._stamped_books(
            {
                "wx-london-23-no": {
                    "token_id": "wx-london-23-no",
                    "asks": [{"price": 0.990, "size": 20}],
                    "tick_size": 0.001,
                    "min_order_size": 1,
                }
            }
        )
        result = WeatherScanner(config=WeatherScannerConfig()).scan(
            markets,
            rules,
            books=books,
            fetch_books=False,
            now=datetime(2026, 9, 5, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(result["summary"]["opportunities"], 2)
        sides = {
            (row["trade_side"], row["target_outcome"])
            for row in result["rows"]
            if row["status"] == "opportunity"
        }
        self.assertEqual(sides, {("YES", "26"), ("NO", "23")})

    def test_missing_no_token_is_no_trade(self) -> None:
        raw_markets = load_market_rows(str(FIXTURES / "markets.json"))
        for row in raw_markets:
            if row.get("outcome") == "23":
                row["clobTokenIds"] = [row["clobTokenIds"][0]]
        markets = weather_markets(raw_markets)
        rules = load_rules(FIXTURES / "rules.json")
        result = WeatherScanner(config=WeatherScannerConfig()).scan(
            markets,
            rules,
            books=self._stamped_books(),
            fetch_books=False,
            now=datetime(2026, 9, 5, 1, tzinfo=timezone.utc),
        )
        loser = next(row for row in result["rows"] if row["target_outcome"] == "23")
        self.assertEqual(loser["status"], "no_trade")
        self.assertEqual(loser["reason"], "missing_no_token")
        self.assertEqual(result["summary"]["opportunities"], 1)

    def test_waiting_window_does_not_mark_book_missing(self) -> None:
        markets = weather_markets(load_market_rows(str(FIXTURES / "markets.json")))
        rules = load_rules(FIXTURES / "rules.json")
        result = WeatherScanner(config=WeatherScannerConfig()).scan(
            markets,
            rules,
            fetch_books=False,
            now=datetime(2026, 9, 3, 12, tzinfo=timezone.utc),
        )
        self.assertTrue(result["rows"])
        self.assertTrue(all(row.get("status") == "waiting" for row in result["rows"]))
        self.assertTrue(all(not (row.get("book") or {}).get("book_missing") for row in result["rows"]))
        self.assertTrue(
            all(
                (row.get("observation") or {}).get("status") == "waiting_window"
                for row in result["rows"]
            )
        )

    def _event_with_static(
        self,
        *,
        event_id: str,
        metric: str,
        outcomes: list,
        buckets: list,
        features: list,
        extra_source: Optional[dict] = None,
        start: str = "2026-09-06T00:00:00Z",
        end: str = "2026-09-06T23:59:59Z",
    ):
        source_url = "fixture://weather/{}".format(event_id)
        source = {
            "static": {"features": features},
            "url": source_url,
            "resolution_source": source_url,
            "value_path": "value",
            "timestamp_path": "timestamp",
        }
        if extra_source:
            source.update(extra_source)
        markets_raw = []
        rules_raw = []
        for index, outcome in enumerate(outcomes):
            market_id = "{}-{}".format(event_id, index)
            markets_raw.append(
                {
                    "id": market_id,
                    "event_group_id": event_id,
                    "question": outcome,
                    "slug": event_id,
                    "category": "weather",
                    "outcome": outcome,
                    "outcomes": ["Yes", "No"],
                    "clobTokenIds": [
                        "{}-yes".format(market_id),
                        "{}-no".format(market_id),
                    ],
                    "resolutionSource": source_url,
                    "endDate": "2026-09-07T00:00:00Z",
                    "active": True,
                    "closed": False,
                    "acceptingOrders": True,
                    "enableOrderBook": True,
                    "negRisk": False,
                    "orderPriceMinTickSize": 0.001,
                    "orderMinSize": 1,
                    "feeSchedule": {"rate": 0.05},
                }
            )
            rules_raw.append(
                {
                    "market_id": market_id,
                    "event_group_id": event_id,
                    "source": source,
                    "timezone": "UTC",
                    "metric": metric,
                    "observation_start": start,
                    "observation_end": end,
                    "unit": "C",
                    "buckets": buckets,
                    "target_outcome": outcome,
                    "manual_approval": True,
                }
            )
        return weather_markets(markets_raw), [parse_rule(row) for row in rules_raw]

    def _priced_books(self, markets, now: datetime, cheap_no_outcomes: set) -> dict:
        stamp = now.isoformat()
        books: dict = {}
        cheap = {str(item) for item in cheap_no_outcomes}
        for market in markets:
            no_token = market.no_token_id or (
                market.token_ids[1] if len(market.token_ids) > 1 else ""
            )
            yes_token = market.yes_token_id or (
                market.token_ids[0] if market.token_ids else ""
            )
            no_price = 0.990 if str(market.outcome) in cheap else 0.999
            if no_token:
                books[no_token] = {
                    "token_id": no_token,
                    "asks": [{"price": no_price, "size": 20}],
                    "tick_size": 0.001,
                    "min_order_size": 1,
                    "fetched_at": stamp,
                }
            if yes_token:
                books[yes_token] = {
                    "token_id": yes_token,
                    "asks": [{"price": 0.999, "size": 20}],
                    "tick_size": 0.001,
                    "min_order_size": 1,
                    "fetched_at": stamp,
                }
        return books

    def test_intraday_min_impossible_no_is_candidate(self) -> None:
        buckets = [
            {"outcome": "18 or below", "upper": 18, "upper_inclusive": True},
            {
                "outcome": "19",
                "lower": 18,
                "lower_inclusive": False,
                "upper": 19,
                "upper_inclusive": True,
            },
            {
                "outcome": "20",
                "lower": 19,
                "lower_inclusive": False,
                "upper": 20,
                "upper_inclusive": True,
            },
            {
                "outcome": "21",
                "lower": 20,
                "lower_inclusive": False,
                "upper": 21,
                "upper_inclusive": True,
            },
            {
                "outcome": "22",
                "lower": 21,
                "lower_inclusive": False,
                "upper": 22,
                "upper_inclusive": True,
            },
            {"outcome": "23", "lower": 22, "lower_inclusive": False, "upper": 23, "upper_inclusive": True},
            {"outcome": "24 or higher", "lower": 23, "lower_inclusive": False},
        ]
        parsed = validate_buckets(parse_bucket(item) for item in buckets)
        self.assertTrue(bucket_impossible_while_open("daily_min", 19, parsed[5]))
        self.assertFalse(bucket_impossible_while_open("daily_min", 19, parsed[0]))
        self.assertFalse(bucket_impossible_while_open("daily_min", 19, parsed[1]))
        markets, rules = self._event_with_static(
            event_id="intraday-min-19",
            metric="daily_min",
            outcomes=[item["outcome"] for item in buckets],
            buckets=buckets,
            features=[
                {"value": 22, "timestamp": "2026-09-06T02:00:00Z"},
                {"value": 19, "timestamp": "2026-09-06T06:00:00Z"},
                {"value": 24, "timestamp": "2026-09-06T10:00:00Z"},
            ],
        )
        now = datetime(2026, 9, 6, 12, tzinfo=timezone.utc)
        result = WeatherScanner(config=WeatherScannerConfig()).scan(
            markets,
            rules,
            books=self._priced_books(markets, now, {"23"}),
            fetch_books=False,
            now=now,
        )
        by_outcome = {row["target_outcome"]: row for row in result["rows"]}
        self.assertEqual(by_outcome["23"]["status"], "opportunity")
        self.assertEqual(by_outcome["23"]["trade_side"], "NO")
        self.assertEqual(by_outcome["23"]["reason"], "intraday_impossible_no")
        self.assertTrue(by_outcome["23"].get("dry_run"))
        self.assertEqual(by_outcome["18 or below"]["status"], "waiting")
        self.assertEqual(by_outcome["19"]["status"], "waiting")
        self.assertNotEqual(by_outcome["19"].get("trade_side"), "YES")
        self.assertFalse(
            any(
                row.get("trade_side") == "YES" and row.get("target_outcome") == "19"
                for row in result["rows"]
            )
        )
        self.assertEqual((by_outcome["19"].get("observation") or {}).get("status"), "intraday")
        self.assertEqual((by_outcome["19"].get("observation") or {}).get("value"), 19)

    def test_intraday_max_impossible_no_is_candidate(self) -> None:
        buckets = [
            {"outcome": "30", "upper": 30, "upper_inclusive": True},
            {
                "outcome": "31",
                "lower": 30,
                "lower_inclusive": False,
                "upper": 31,
                "upper_inclusive": True,
            },
            {
                "outcome": "32",
                "lower": 31,
                "lower_inclusive": False,
                "upper": 32,
                "upper_inclusive": True,
            },
            {
                "outcome": "33",
                "lower": 32,
                "lower_inclusive": False,
                "upper": 33,
                "upper_inclusive": True,
            },
            {"outcome": "34", "lower": 33, "lower_inclusive": False},
        ]
        markets, rules = self._event_with_static(
            event_id="intraday-max-33",
            metric="daily_max",
            outcomes=[item["outcome"] for item in buckets],
            buckets=buckets,
            features=[
                {"value": 28, "timestamp": "2026-09-06T02:00:00Z"},
                {"value": 33, "timestamp": "2026-09-06T08:00:00Z"},
                {"value": 31, "timestamp": "2026-09-06T10:00:00Z"},
            ],
        )
        now = datetime(2026, 9, 6, 12, tzinfo=timezone.utc)
        result = WeatherScanner(config=WeatherScannerConfig()).scan(
            markets,
            rules,
            books=self._priced_books(markets, now, {"30"}),
            fetch_books=False,
            now=now,
        )
        by_outcome = {row["target_outcome"]: row for row in result["rows"]}
        self.assertEqual(by_outcome["30"]["status"], "opportunity")
        self.assertEqual(by_outcome["30"]["trade_side"], "NO")
        self.assertEqual(by_outcome["30"]["reason"], "intraday_impossible_no")
        self.assertEqual(by_outcome["33"]["status"], "waiting")
        self.assertEqual(by_outcome["34"]["status"], "waiting")
        self.assertFalse(any(row.get("trade_side") == "YES" for row in result["rows"]))

    def test_intraday_before_local_midnight_does_not_fetch(self) -> None:
        class CountingAdapter(WeatherSourceAdapter):
            def __init__(self) -> None:
                super().__init__()
                self.payload_calls = 0

            def _payload(self, rule):  # type: ignore[override]
                self.payload_calls += 1
                return super()._payload(rule)

        buckets = [
            {"outcome": "18 or below", "upper": 18, "upper_inclusive": True},
            {
                "outcome": "19",
                "lower": 18,
                "lower_inclusive": False,
                "upper": 19,
                "upper_inclusive": True,
            },
            {"outcome": "20 or higher", "lower": 19, "lower_inclusive": False},
        ]
        markets, rules = self._event_with_static(
            event_id="intraday-before-start",
            metric="daily_min",
            outcomes=[item["outcome"] for item in buckets],
            buckets=buckets,
            features=[{"value": 19, "timestamp": "2026-09-06T06:00:00Z"}],
        )
        adapter = CountingAdapter()
        result = WeatherScanner(
            config=WeatherScannerConfig(), source_adapter=adapter
        ).scan(
            markets,
            rules,
            fetch_books=False,
            now=datetime(2026, 9, 5, 23, tzinfo=timezone.utc),
        )
        self.assertEqual(adapter.payload_calls, 0)
        self.assertTrue(all(row.get("status") == "waiting" for row in result["rows"]))
        self.assertTrue(all(not (row.get("book") or {}).get("book_missing") for row in result["rows"]))

    def test_provisional_loser_no_waits_on_winner_yes(self) -> None:
        buckets = [
            {"outcome": "18 or below", "upper": 18, "upper_inclusive": True},
            {
                "outcome": "19",
                "lower": 18,
                "lower_inclusive": False,
                "upper": 19,
                "upper_inclusive": True,
            },
            {"outcome": "23", "lower": 19, "lower_inclusive": False, "upper": 23, "upper_inclusive": True},
            {"outcome": "24 or higher", "lower": 23, "lower_inclusive": False},
        ]
        markets, rules = self._event_with_static(
            event_id="provisional-min-19",
            metric="daily_min",
            outcomes=[item["outcome"] for item in buckets],
            buckets=buckets,
            features=[{"value": 19, "timestamp": "2026-09-06T06:00:00Z"}],
            extra_source={"finality_mode": "first_following_date_point"},
        )
        now = datetime(2026, 9, 7, 1, tzinfo=timezone.utc)
        result = WeatherScanner(config=WeatherScannerConfig()).scan(
            markets,
            rules,
            books=self._priced_books(markets, now, {"18 or below", "23", "24 or higher"}),
            fetch_books=False,
            now=now,
        )
        by_outcome = {row["target_outcome"]: row for row in result["rows"]}
        self.assertEqual((by_outcome["19"].get("observation") or {}).get("status"), "provisional")
        self.assertEqual(by_outcome["19"]["status"], "waiting")
        self.assertNotEqual(by_outcome["19"].get("trade_side"), "YES")
        self.assertEqual(by_outcome["23"]["status"], "opportunity")
        self.assertEqual(by_outcome["23"]["trade_side"], "NO")
        self.assertEqual(by_outcome["23"]["reason"], "provisional_loser_no")
        self.assertEqual(by_outcome["18 or below"]["status"], "opportunity")

    def test_category_fee_is_fail_closed(self) -> None:
        raw_markets = load_market_rows(str(FIXTURES / "markets.json"))
        for row in raw_markets:
            row.pop("feeSchedule", None)
            row.pop("feeRate", None)
        markets = weather_markets(raw_markets)
        rules = load_rules(FIXTURES / "rules.json")
        books = load_json(FIXTURES / "books.json", {})
        result = WeatherScanner(config=WeatherScannerConfig()).scan(
            markets,
            rules,
            books=books,
            fetch_books=False,
            now=datetime(2026, 9, 5, 1, tzinfo=timezone.utc),
        )
        target = next(row for row in result["rows"] if row["target_outcome"] == "26")
        self.assertEqual(target["status"], "no_trade")
        self.assertEqual(target["reason"], "fee_not_explicit")

    def test_book_without_capture_time_is_fail_closed(self) -> None:
        markets = weather_markets(load_market_rows(str(FIXTURES / "markets.json")))
        rules = load_rules(FIXTURES / "rules.json")
        books = load_json(FIXTURES / "books.json", {})
        result = WeatherScanner(config=WeatherScannerConfig()).scan(
            markets,
            rules,
            books=books,
            fetch_books=False,
            now=datetime.now(timezone.utc),
        )
        target = next(row for row in result["rows"] if row["target_outcome"] == "26")
        self.assertEqual(target["status"], "no_trade")
        self.assertEqual(target["reason"], "book_timestamp_missing")

    def test_discovery_without_machine_contract_requires_review(self) -> None:
        markets = weather_markets(load_market_rows(str(FIXTURES / "markets.json")))
        result = discover_rules(markets)
        self.assertEqual(result["summary"]["generated_rules"], 0)
        self.assertEqual(result["summary"]["review_event_groups"], 1)

    def test_service_persists_snapshot_and_publishes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = RuntimeService(
                root=ROOT,
                fixture=FIXTURES,
                data_dir=Path(temp) / "data",
                interval_s=60,
            )
            ident, channel = service.subscribe()
            result = service.scan_once()
            self.assertEqual(result["summary"]["opportunities"], 1)
            self.assertTrue((Path(temp) / "data" / "latest.json").is_file())
            event = channel.get(timeout=1)
            self.assertEqual(event["type"], "snapshot")
            service.unsubscribe(ident)
            latest = json.loads((Path(temp) / "data" / "latest.json").read_text())
            self.assertEqual(latest["summary"]["bucket_match_rate"], 1.0)
            self.assertNotIn("source_evidence", latest)
            published = service.board_snapshot()
            self.assertIn("event_groups", published)
            self.assertNotIn("rows", published)
            board = slim_board_groups(result["event_groups"])
            for group in board:
                for row in group.get("rows") or []:
                    self.assertNotIn("raw", row.get("observation") or {})

    def test_jsonl_dedup_keeps_first_key_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "scan.jsonl"
            append_jsonl_dedup(
                path,
                [{"id": "a", "n": 1}, {"id": "a", "n": 2}, {"id": "b", "n": 3}],
                key_fn=lambda row: str(row["id"]),
            )
            append_jsonl_dedup(
                path,
                [{"id": "a", "n": 4}, {"id": "c", "n": 5}],
                key_fn=lambda row: str(row["id"]),
            )
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual([row["id"] for row in rows], ["a", "b", "c"])
            self.assertEqual(rows[0]["n"], 1)


    def test_whole_degree_rounding_maps_26_4_to_26(self) -> None:
        rules = load_rules(FIXTURES / "rules.json")
        self.assertEqual(apply_rounding(26.4), 26)
        self.assertEqual(apply_rounding(26.5), 27)
        self.assertEqual(apply_rounding(-1.5), -2)
        self.assertEqual(bucket_for_value(26.4, rules[0].buckets).outcome, "26")
        self.assertEqual(
            bucket_for_value(26.4, rules[0].buckets, rounding=ROUNDING_NONE).outcome,
            "27",
        )

    def test_source_rounds_daily_max_before_bucket(self) -> None:
        raw = {
            "market_id": "wx-london-26",
            "event_group_id": "fixture-london-2026-09-04",
            "timezone": "Europe/London",
            "rounding": "whole_degree_as_published",
            "source": {
                "static": {
                    "features": [
                        {"value": 26.4, "timestamp": "2026-09-04T12:00:00Z"},
                        {"value": 25.1, "timestamp": "2026-09-04T18:00:00Z"},
                    ],
                    "final": True,
                },
                "url": "fixture://weather/london",
                "resolution_source": "fixture://weather/london",
                "value_path": "value",
                "timestamp_path": "timestamp",
                "final_path": "final",
            },
            "metric": "daily_max",
            "observation_start": "2026-09-04T00:00:00",
            "observation_end": "2026-09-04T23:59:59",
            "unit": "C",
            "buckets": load_rules(FIXTURES / "rules.json")[0].to_dict()["buckets"],
            "target_outcome": "26",
            "manual_approval": True,
        }
        rule = parse_rule(raw)
        observation = WeatherSourceAdapter().poll(
            rule,
            now=datetime(2026, 9, 5, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(observation.status, "final")
        self.assertEqual(observation.raw_aggregate, 26.4)
        self.assertEqual(observation.value, 26)
        self.assertEqual(bucket_for_value(observation.value, rule.buckets).outcome, "26")

    def test_london_window_excludes_utc_late_evening(self) -> None:
        raw = {
            "market_id": "m-tz",
            "event_group_id": "e-tz",
            "timezone": "Europe/London",
            "source": {
                "static": {
                    "features": [{"value": 26.0, "timestamp": "2026-09-04T23:30:00Z"}],
                    "final": True,
                },
                "url": "fixture://weather/tz",
                "resolution_source": "fixture://weather/tz",
                "value_path": "value",
                "timestamp_path": "timestamp",
                "final_path": "final",
            },
            "metric": "daily_max",
            "observation_start": "2026-09-04T00:00:00",
            "observation_end": "2026-09-04T23:59:59",
            "buckets": [
                {"outcome": "low", "upper": 30},
                {"outcome": "high", "lower": 30, "lower_inclusive": False},
            ],
        }
        rule = parse_rule(raw)
        outside = WeatherSourceAdapter().poll(
            rule,
            now=datetime(2026, 9, 5, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(outside.status, "unavailable")
        self.assertEqual(outside.reason, "no_observations_in_window")

        raw["source"]["static"]["features"] = [
            {"value": 26.0, "timestamp": "2026-09-04T22:30:00Z"}
        ]
        inside = WeatherSourceAdapter().poll(
            parse_rule(raw),
            now=datetime(2026, 9, 5, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(inside.status, "final")
        self.assertEqual(inside.value, 26)

    def test_dst_fallback_keeps_bst_hour_in_local_day(self) -> None:
        raw = {
            "market_id": "m-dst",
            "event_group_id": "e-dst",
            "timezone": "Europe/London",
            "source": {
                "static": {
                    "features": [{"value": 12.0, "timestamp": "2026-10-24T23:30:00Z"}],
                    "final": True,
                },
                "url": "fixture://weather/dst",
                "resolution_source": "fixture://weather/dst",
                "value_path": "value",
                "timestamp_path": "timestamp",
                "final_path": "final",
            },
            "metric": "daily_max",
            "observation_start": "2026-10-25T00:00:00",
            "observation_end": "2026-10-25T23:59:59",
            "buckets": [
                {"outcome": "low", "upper": 30},
                {"outcome": "high", "lower": 30, "lower_inclusive": False},
            ],
        }
        observation = WeatherSourceAdapter().poll(
            parse_rule(raw),
            now=datetime(2026, 10, 26, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(observation.status, "final")
        self.assertEqual(observation.value, 12)

    def test_naive_local_times_use_rule_timezone(self) -> None:
        raw = {
            "market_id": "m-naive",
            "event_group_id": "e-naive",
            "timezone": "Europe/London",
            "source": {
                "static": {
                    "features": [
                        {"value": 21, "timestamp": "2026-09-04T12:00:00"},
                    ],
                    "final": True,
                },
                "url": "fixture://weather/naive",
                "resolution_source": "fixture://weather/naive",
                "value_path": "value",
                "timestamp_path": "timestamp",
                "final_path": "final",
            },
            "metric": "daily_max",
            "observation_start": "2026-09-04T00:00:00",
            "observation_end": "2026-09-04T23:59:59",
            "buckets": [
                {"outcome": "low", "upper": 30},
                {"outcome": "high", "lower": 30, "lower_inclusive": False},
            ],
        }
        observation = WeatherSourceAdapter().poll(
            parse_rule(raw),
            now=datetime(2026, 9, 5, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(observation.status, "final")
        self.assertEqual(observation.value, 21)

    def test_rule_requires_iana_timezone(self) -> None:
        with self.assertRaises(RuleError):
            parse_rule(
                {
                    "market_id": "m",
                    "event_group_id": "e",
                    "source": {
                        "static": {"features": []},
                        "url": "fixture://x",
                        "resolution_source": "fixture://x",
                    },
                    "metric": "daily_max",
                    "observation_start": "2026-09-04T00:00:00Z",
                    "observation_end": "2026-09-04T23:59:59Z",
                    "buckets": [
                        {"outcome": "low", "upper": 30},
                        {"outcome": "high", "lower": 30, "lower_inclusive": False},
                    ],
                }
            )

    def test_gamma_events_slug_groups_distinct_market_slugs(self) -> None:
        rows = [
            {
                "id": "m-26",
                "slug": "highest-temperature-in-london-on-september-6-2026-26c",
                "question": "Will the highest temperature in London on September 6, 2026 be 26°C?",
                "category": "weather",
                "groupItemTitle": "26°C",
                "events": [{"slug": "highest-temperature-in-london-on-september-6-2026"}],
                "clobTokenIds": ["yes-26", "no-26"],
                "resolutionSource": "https://www.weather.gov/wrh/timeseries?site=eglc",
                "active": True,
                "closed": False,
                "acceptingOrders": True,
                "enableOrderBook": True,
            },
            {
                "id": "m-27",
                "slug": "highest-temperature-in-london-on-september-6-2026-27c",
                "question": "Will the highest temperature in London on September 6, 2026 be 27°C?",
                "category": "weather",
                "groupItemTitle": "27°C",
                "events": [{"slug": "highest-temperature-in-london-on-september-6-2026"}],
                "clobTokenIds": ["yes-27", "no-27"],
                "resolutionSource": "https://www.weather.gov/wrh/timeseries?site=eglc",
                "active": True,
                "closed": False,
                "acceptingOrders": True,
                "enableOrderBook": True,
            },
        ]
        markets = [normalize_market(row) for row in rows]
        self.assertEqual(
            extract_event_group_id(rows[0]),
            "highest-temperature-in-london-on-september-6-2026",
        )
        self.assertEqual(markets[0].event_group_id, markets[1].event_group_id)
        groups = group_weather_markets(markets)
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(next(iter(groups.values()))), 2)

    def test_unique_market_slugs_do_not_form_an_event_group(self) -> None:
        rows = [
            {
                "id": "m-26",
                "slug": "highest-temperature-in-london-on-september-6-2026-26c",
                "question": "Will the highest temperature in London be 26°C?",
                "category": "weather",
                "groupItemTitle": "26°C",
                "clobTokenIds": ["yes-26", "no-26"],
                "active": True,
                "closed": False,
            },
            {
                "id": "m-27",
                "slug": "highest-temperature-in-london-on-september-6-2026-27c",
                "question": "Will the highest temperature in London be 27°C?",
                "category": "weather",
                "groupItemTitle": "27°C",
                "clobTokenIds": ["yes-27", "no-27"],
                "active": True,
                "closed": False,
            },
        ]
        markets = [normalize_market(row) for row in rows]
        self.assertEqual(markets[0].event_group_id, "")
        groups = group_weather_markets(markets)
        self.assertEqual(len(groups), 2)

    def test_incomplete_siblings_are_review_not_opportunity(self) -> None:
        markets = weather_markets(load_market_rows(str(FIXTURES / "markets.json")))
        rules = [item for item in load_rules(FIXTURES / "rules.json") if item.target_outcome == "26"]
        books = load_json(FIXTURES / "books.json", {})
        stamp = datetime(2026, 9, 5, 1, tzinfo=timezone.utc).isoformat()
        books = {token: {**book, "fetched_at": stamp} for token, book in books.items()}
        result = WeatherScanner(config=WeatherScannerConfig()).scan(
            markets,
            rules,
            books=books,
            fetch_books=False,
            now=datetime(2026, 9, 5, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(len(result["rows"]), 1)
        self.assertEqual(result["rows"][0]["status"], "review")
        self.assertEqual(result["rows"][0]["reason"], "sibling_count_mismatch")
        self.assertEqual(result["summary"]["opportunities"], 0)

    def test_sibling_validator_accepts_complete_fixture_group(self) -> None:
        markets = weather_markets(load_market_rows(str(FIXTURES / "markets.json")))
        rules = load_rules(FIXTURES / "rules.json")
        self.assertIsNone(validate_event_group_siblings(markets, rules[0].buckets))

    def test_catalog_flattens_gamma_events_without_approving_rules(self) -> None:
        events = [
            {
                "id": "1",
                "slug": "highest-temperature-in-london-on-september-6-2026",
                "title": "Highest temperature in London on September 6?",
                "endDate": "2026-09-07T00:00:00Z",
                "negRisk": True,
                "markets": [
                    {"id": "a", "groupItemTitle": "26"},
                    {"id": "b", "groupItemTitle": "27"},
                ],
            },
            {
                "id": "2",
                "slug": "will-it-rain-in-nyc-on-september-6-2026",
                "title": "Will it rain in NYC on September 6?",
                "markets": [{"id": "c", "groupItemTitle": "Yes"}],
            },
        ]
        rows = flatten_event_markets(events)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["event_group_id"], "highest-temperature-in-london-on-september-6-2026")
        self.assertEqual(rows[0]["category"], "weather")
        groups = catalog_event_groups(events)
        self.assertEqual(groups[0]["metric"], "daily_max")
        self.assertEqual(groups[0]["local_date"], "2026-09-06")
        self.assertEqual(groups[0]["reason"], "rule_not_approved")
        self.assertEqual(groups[0]["station_id"], "")
        self.assertEqual(groups[1]["metric"], "precip")
        scanned = [
            {
                "event_group_id": "highest-temperature-in-london-on-september-6-2026",
                "status": "waiting",
                "source_status": "waiting_window",
            }
        ]
        merged = merge_event_groups(groups, scanned)
        self.assertEqual(len(merged), 2)
        self.assertEqual(merged[0]["status"], "waiting")
        self.assertTrue(merged[1]["catalog_only"])

    def test_service_merges_disk_catalog_without_guessing_station(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            data = Path(temp) / "data"
            data.mkdir()
            (data / "weather_catalog.json").write_text(
                json.dumps(
                    {
                        "event_groups": catalog_event_groups(
                            [
                                {
                                    "slug": "highest-temperature-in-paris-on-september-6-2026",
                                    "title": "Highest temperature in Paris on September 6?",
                                    "markets": [{"id": "p1", "groupItemTitle": "24"}],
                                }
                            ]
                        )
                    }
                ),
                encoding="utf-8",
            )
            service = RuntimeService(
                root=ROOT,
                fixture=FIXTURES,
                data_dir=data,
                interval_s=60,
            )
            service.fixture = None
            service.horizon_hours = 168
            service.markets_file = FIXTURES / "markets.json"
            service.rules_file = FIXTURES / "rules.json"
            service.books_file = FIXTURES / "books.json"
            result = service.scan_once()
            extra = next(
                group
                for group in result["event_groups"]
                if "paris" in str(group.get("event_group_id") or "")
            )
            self.assertEqual(extra["reason"], "rule_not_approved")
            self.assertEqual(extra["station_id"], "")
            self.assertGreaterEqual(result["summary"]["catalog_events"], 1)
            self.assertGreaterEqual(result["summary"]["event_groups"], 2)

    def test_horizon_keeps_local_day_overlapping_now(self) -> None:
        now = datetime(2026, 9, 6, 9, 34, tzinfo=timezone.utc)
        self.assertTrue(
            observation_in_horizon("2026-09-06", "Europe/London", now=now, past_hours=24, future_hours=24)
        )
        self.assertTrue(
            observation_in_horizon("2026-09-05", "America/Los_Angeles", now=now, past_hours=24, future_hours=24)
        )
        self.assertFalse(
            observation_in_horizon("2026-09-08", "Europe/London", now=now, past_hours=24, future_hours=24)
        )
        self.assertFalse(
            observation_in_horizon("2026-05-20", "Asia/Shanghai", now=now, past_hours=24, future_hours=24)
        )
        events = [
            {
                "slug": "highest-temperature-in-london-on-september-6-2026",
                "title": "Highest temperature in London on September 6?",
                "endDate": "2026-09-07T00:00:00Z",
                "markets": [
                    {"resolutionSource": "https://www.weather.gov/wrh/timeseries?site=eglc"},
                ],
            },
            {
                "slug": "highest-temperature-in-london-on-september-8-2026",
                "title": "Highest temperature in London on September 8?",
                "markets": [
                    {"resolutionSource": "https://www.weather.gov/wrh/timeseries?site=eglc"},
                ],
            },
            {
                "slug": "highest-temperature-in-jinan-on-may-20-2026",
                "title": "Highest temperature in Jinan on May 20?",
                "markets": [{"resolutionSource": "https://www.wunderground.com/history/daily/cn/jinan/ZSJN"}],
            },
        ]
        kept = filter_events_in_horizon(events, now=now, past_hours=24, future_hours=24)
        slugs = [item["slug"] for item in kept]
        self.assertEqual(slugs, ["highest-temperature-in-london-on-september-6-2026"])


    def test_buckets_from_celsius_and_fahrenheit_titles(self) -> None:
        celsius = buckets_from_outcomes(
            [
                "22°C or below",
                "23°C",
                "24°C",
                "25°C",
                "26°C",
                "27°C",
                "28°C",
                "29°C",
                "30°C",
                "31°C",
                "32°C or higher",
            ]
        )
        self.assertEqual(bucket_for_value(26, celsius).outcome, "26°C")
        self.assertEqual(bucket_for_value(22, celsius).outcome, "22°C or below")
        self.assertEqual(bucket_for_value(32, celsius).outcome, "32°C or higher")
        fahrenheit = buckets_from_outcomes(
            [
                "71°F or below",
                "72-73°F",
                "74-75°F",
                "76-77°F",
                "78-79°F",
                "80-81°F",
                "82-83°F",
                "84-85°F",
                "86-87°F",
                "88-89°F",
                "90°F or higher",
            ]
        )
        self.assertEqual(bucket_for_value(80, fahrenheit).outcome, "80-81°F")
        self.assertEqual(bucket_for_value(71, fahrenheit).outcome, "71°F or below")
        self.assertEqual(bucket_for_value(90, fahrenheit).outcome, "90°F or higher")

    def _temp_markets(
        self,
        *,
        slug: str,
        source: str,
        titles: list[str],
        metric_word: str = "highest",
        description: str = "",
    ) -> list:
        rows = []
        for index, title in enumerate(titles, start=1):
            row = {
                "id": "{}-{}".format(slug, index),
                "event_group_id": slug,
                "question": "{} temperature?".format(metric_word.capitalize()),
                "slug": slug,
                "category": "weather",
                "groupItemTitle": title,
                "resolutionSource": source,
                "endDate": "2026-09-07T00:00:00Z",
                "active": True,
                "closed": False,
                "acceptingOrders": True,
                "enableOrderBook": True,
                "negRisk": True,
                "clobTokenIds": ["yes-{}".format(index), "no-{}".format(index)],
                "feeSchedule": {"rate": 0.02},
            }
            if description:
                row["description"] = description
            rows.append(row)
        return weather_markets(rows)

    def test_discover_noaa_timeseries_enables_dry_run_rules(self) -> None:
        markets = self._temp_markets(
            slug="highest-temperature-in-seoul-on-september-6-2026",
            source="https://www.weather.gov/wrh/timeseries?site=rksi",
            titles=[
                "23°C or below",
                "24°C",
                "25°C",
                "26°C",
                "27°C",
                "28°C",
                "29°C",
                "30°C",
                "31°C",
                "32°C",
                "33°C or higher",
            ],
        )
        result = discover_rules(markets)
        self.assertEqual(result["summary"]["generated_rules"], 11)
        self.assertEqual(result["summary"]["auto_approved"], 1)
        rule = parse_rule(result["generated_rules"][0])
        self.assertEqual(rule.timezone, "Asia/Seoul")
        self.assertEqual(rule.source["station_id"], "RKSI")
        self.assertEqual(rule.source["provider"], "NOAA")
        self.assertEqual(rule.source["url"], "https://api.synopticdata.com/v2/stations/timeseries")
        self.assertNotIn("api.weather.gov", rule.source["url"])
        self.assertEqual(
            rule.source["resolution_source"],
            "https://www.weather.gov/wrh/timeseries?site=rksi",
        )
        self.assertEqual(rule.source["value_path"], "temp")
        self.assertEqual(rule.source["timestamp_path"], "timestamp")
        self.assertEqual(rule.source["params"]["STID"], "RKSI")
        self.assertEqual(rule.source["params"]["recent"], 4320)
        self.assertNotIn("start", rule.source.get("params") or {})
        self.assertNotIn("end", rule.source.get("params") or {})
        self.assertEqual(rule.source["params"]["obtimezone"], "local")
        self.assertEqual(rule.source["params"]["units"], "temp|F,speed|mph,english")
        self.assertNotIn("token", rule.source.get("params") or {})
        self.assertTrue(rule.enabled)
        self.assertTrue(rule.manual_approval)
        self.assertEqual(rule.observation_start, "2026-09-06T00:00:00")

    def test_discover_wunderground_history_enables_dry_run_rules(self) -> None:
        titles = [
            "12°C or below",
            "13°C",
            "14°C",
            "15°C",
            "16°C",
            "17°C",
            "18°C",
            "19°C",
            "20°C",
            "21°C",
            "22°C or higher",
        ]
        markets = self._temp_markets(
            slug="lowest-temperature-in-jinan-on-september-7-2026",
            source="https://www.wunderground.com/history/daily/cn/jinan/ZSJN",
            titles=titles,
            metric_word="lowest",
        )
        result = discover_rules(markets)
        self.assertEqual(result["summary"]["generated_rules"], 11)
        rule = parse_rule(result["generated_rules"][0])
        self.assertEqual(rule.timezone, "Asia/Shanghai")
        self.assertEqual(rule.source["station_id"], "ZSJN")
        self.assertEqual(rule.source["provider"], "Wunderground")
        self.assertIn("ZSJN:9:CN", rule.source["url"])
        self.assertNotIn("apiKey", rule.source.get("params") or {})
        self.assertEqual(rule.metric, "daily_min")
        self.assertTrue(rule.enabled)
        for slug, source, station, zone, loc in (
            (
                "lowest-temperature-in-taipei-on-september-7-2026",
                "https://www.wunderground.com/history/daily/tw/taipei/RCSS",
                "RCSS",
                "Asia/Taipei",
                "RCSS:9:TW",
            ),
            (
                "lowest-temperature-in-zhengzhou-on-september-7-2026",
                "https://www.wunderground.com/history/daily/cn/zhengzhou/ZHCC",
                "ZHCC",
                "Asia/Shanghai",
                "ZHCC:9:CN",
            ),
        ):
            sibling = self._temp_markets(slug=slug, source=source, titles=titles, metric_word="lowest")
            sibling_rule = parse_rule(discover_rules(sibling)["generated_rules"][0])
            self.assertEqual(sibling_rule.timezone, zone)
            self.assertEqual(sibling_rule.source["station_id"], station)
            self.assertIn(loc, sibling_rule.source["url"])
            self.assertNotIn("apiKey", sibling_rule.source.get("params") or {})

    def test_discover_pws_dashboard_stays_review(self) -> None:
        markets = self._temp_markets(
            slug="highest-temperature-in-loveland-on-september-6-2026",
            source="https://www.wunderground.com/dashboard/pws/KNVLOVEL7",
            titles=[
                "71°F or below",
                "72-73°F",
                "74-75°F",
                "76-77°F",
                "78-79°F",
                "80-81°F",
                "82-83°F",
                "84-85°F",
                "86-87°F",
                "88-89°F",
                "90°F or higher",
            ],
        )
        result = discover_rules(markets)
        self.assertEqual(result["summary"]["generated_rules"], 0)
        self.assertTrue(result["review"])
        self.assertEqual(result["review"][0]["reasons"][0], "resolution_source_unsupported")

    def test_wunderground_observations_finalize_daily_min(self) -> None:
        markets = self._temp_markets(
            slug="lowest-temperature-in-jinan-on-september-7-2026",
            source="https://www.wunderground.com/history/daily/cn/jinan/ZSJN",
            titles=[
                "12°C or below",
                "13°C",
                "14°C",
                "15°C",
                "16°C",
                "17°C",
                "18°C",
                "19°C",
                "20°C",
                "21°C",
                "22°C or higher",
            ],
            metric_word="lowest",
        )
        rule = parse_rule(discover_rules(markets)["generated_rules"][0])
        from weather_runtime.rules import with_source_contract

        rule = with_source_contract(
            rule,
            source={
                **rule.source,
                "static": {
                    "observations": [
                        {
                            "temp": 18,
                            "valid_time_gmt": int(datetime(2026, 9, 6, 18, tzinfo=timezone.utc).timestamp()),
                        },
                        {
                            "temp": 14,
                            "valid_time_gmt": int(datetime(2026, 9, 7, 7, tzinfo=timezone.utc).timestamp()),
                        },
                        {
                            "temp": 16,
                            "valid_time_gmt": int(datetime(2026, 9, 7, 16, 5, tzinfo=timezone.utc).timestamp()),
                        },
                    ]
                },
            },
        )
        observation = WeatherSourceAdapter().poll(
            rule,
            now=datetime(2026, 9, 8, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(observation.status, "final")
        self.assertEqual(observation.value, 14)
        self.assertEqual(observation.station_id, "ZSJN")

    def test_synoptic_observations_finalize_daily_max(self) -> None:
        markets = self._temp_markets(
            slug="highest-temperature-in-seoul-on-september-6-2026",
            source="https://www.weather.gov/wrh/timeseries?site=rksi",
            titles=[
                "23°C or below",
                "24°C",
                "25°C",
                "26°C",
                "27°C",
                "28°C",
                "29°C",
                "30°C",
                "31°C",
                "32°C",
                "33°C or higher",
            ],
        )
        rule = parse_rule(discover_rules(markets)["generated_rules"][0])
        from weather_runtime.rules import with_source_contract

        rule = with_source_contract(
            rule,
            source={
                **rule.source,
                "static": {
                    "STATION": [
                        {
                            "STID": "RKSI",
                            "OBSERVATIONS": {
                                "date_time": [
                                    "2026-09-06T03:00:00Z",
                                    "2026-09-06T06:00:00Z",
                                    "2026-09-06T12:00:00Z",
                                    "2026-09-06T15:30:00Z",
                                ],
                                "air_temp_set_1": [28.0, 32.2, 24.0, 22.0],
                            },
                        }
                    ]
                },
            },
        )
        observation = WeatherSourceAdapter().poll(
            rule,
            now=datetime(2026, 9, 6, 16, tzinfo=timezone.utc),
        )
        self.assertEqual(observation.status, "final")
        self.assertEqual(observation.value, 32)
        self.assertEqual(observation.station_id, "RKSI")
        self.assertEqual(observation.reason, "final_confirmation")

    def test_synoptic_empty_station_is_unavailable(self) -> None:
        markets = self._temp_markets(
            slug="highest-temperature-in-seoul-on-september-6-2026",
            source="https://www.weather.gov/wrh/timeseries?site=rksi",
            titles=[
                "23°C or below",
                "24°C",
                "25°C",
                "26°C",
                "27°C",
                "28°C",
                "29°C",
                "30°C",
                "31°C",
                "32°C",
                "33°C or higher",
            ],
        )
        rule = parse_rule(discover_rules(markets)["generated_rules"][0])
        from weather_runtime.rules import with_source_contract

        rule = with_source_contract(
            rule,
            source={
                **rule.source,
                "static": {"STATION": []},
            },
        )
        observation = WeatherSourceAdapter().poll(
            rule,
            now=datetime(2026, 9, 6, 16, tzinfo=timezone.utc),
        )
        self.assertEqual(observation.status, "unavailable")
        self.assertEqual(observation.reason, "no_observations_in_window")

    def test_synoptic_fahrenheit_payload_converts_to_celsius(self) -> None:
        markets = self._temp_markets(
            slug="highest-temperature-in-seoul-on-september-6-2026",
            source="https://www.weather.gov/wrh/timeseries?site=rksi",
            titles=[
                "23°C or below",
                "24°C",
                "25°C",
                "26°C",
                "27°C",
                "28°C",
                "29°C",
                "30°C",
                "31°C",
                "32°C",
                "33°C or higher",
            ],
        )
        rule = parse_rule(discover_rules(markets)["generated_rules"][0])
        from weather_runtime.rules import with_source_contract

        rule = with_source_contract(
            rule,
            source={
                **rule.source,
                "static": {
                    "UNITS": {"air_temp": "Fahrenheit"},
                    "STATION": [
                        {
                            "STID": "RKSI",
                            "UNITS": {"air_temp": "Fahrenheit"},
                            "OBSERVATIONS": {
                                "date_time": [
                                    "2026-09-06T03:00:00Z",
                                    "2026-09-06T06:00:00Z",
                                    "2026-09-06T12:00:00Z",
                                    "2026-09-06T15:30:00Z",
                                ],
                                "air_temp_set_1": [82.4, 89.6, 75.2, 71.6],
                            },
                        }
                    ],
                },
            },
        )
        observation = WeatherSourceAdapter().poll(
            rule,
            now=datetime(2026, 9, 6, 16, tzinfo=timezone.utc),
        )
        self.assertEqual(observation.status, "final")
        self.assertEqual(observation.value, 32)

    def test_synoptic_request_matches_timeseries_page(self) -> None:
        markets = self._temp_markets(
            slug="lowest-temperature-in-zhengzhou-on-september-6-2026",
            source="https://www.weather.gov/wrh/timeseries?site=zhcc",
            titles=[
                "12°C or below",
                "13°C",
                "14°C",
                "15°C",
                "16°C",
                "17°C",
                "18°C",
                "19°C",
                "20°C",
                "21°C",
                "22°C or higher",
            ],
            metric_word="lowest",
        )
        rule = parse_rule(discover_rules(markets)["generated_rules"][0])

        class Recorder:
            def __init__(self) -> None:
                self.params = None
                self.headers = None

            def get_json(self, url, params=None, headers=None):
                self.params = params
                self.headers = headers
                return {
                    "UNITS": {"air_temp": "Fahrenheit"},
                    "STATION": [
                        {
                            "STID": "ZHCC",
                            "UNITS": {"air_temp": "Fahrenheit"},
                            "OBSERVATIONS": {
                                "date_time": [
                                    "2026-09-06T12:00:00",
                                    "2026-09-07T00:30:00",
                                ],
                                "air_temp_set_1": [68.0, 64.4],
                            },
                        }
                    ],
                }

            def get_text(self, url, headers=None):
                return "var mesoToken='test-token';"

        http = Recorder()
        observation = WeatherSourceAdapter(http=http, http_cache_ttl_s=0).poll(
            rule,
            now=datetime(2026, 9, 6, 16, 5, tzinfo=timezone.utc),
        )
        self.assertEqual(http.params["units"], "temp|F,speed|mph,english")
        self.assertEqual(http.params["recent"], 4320)
        self.assertNotIn("start", http.params or {})
        self.assertNotIn("end", http.params or {})
        self.assertEqual(http.params["token"], "test-token")
        self.assertEqual(http.headers["Origin"], "https://www.weather.gov")
        self.assertEqual(
            http.headers["Referer"],
            "https://www.weather.gov/wrh/timeseries?site=zhcc",
        )
        self.assertIn("Mozilla/5.0", http.headers["User-Agent"])
        self.assertEqual(observation.status, "final")
        self.assertEqual(observation.value, 20)
        self.assertEqual(observation.station_id, "ZHCC")

    def test_expired_window_skips_source_fetch(self) -> None:
        rule = load_rules(FIXTURES / "rules.json")[0]
        observation = WeatherSourceAdapter().poll(
            rule,
            now=datetime(2026, 9, 10, tzinfo=timezone.utc),
        )
        self.assertEqual(observation.status, "expired")
        self.assertEqual(observation.reason, "observation_window_expired")

    def test_discover_noaa_from_description_when_gamma_source_blank(self) -> None:
        titles = [
            "16°C or below",
            "17°C",
            "18°C",
            "19°C",
            "20°C",
            "21°C",
            "22°C",
            "23°C",
            "24°C",
            "25°C",
            "26°C or higher",
        ]
        for slug, site, zone, metric_word in (
            (
                "lowest-temperature-in-istanbul-on-september-6-2026",
                "LTFM",
                "Europe/Istanbul",
                "lowest",
            ),
            (
                "lowest-temperature-in-moscow-on-september-6-2026",
                "UUWW",
                "Europe/Moscow",
                "lowest",
            ),
            (
                "highest-temperature-in-tel-aviv-on-september-6-2026",
                "LLBG",
                "Asia/Jerusalem",
                "highest",
            ),
        ):
            markets = self._temp_markets(
                slug=slug,
                source="",
                titles=titles,
                metric_word=metric_word,
                description=(
                    "NOAA Temp column for all times on this day, available here: "
                    "https://www.weather.gov/wrh/timeseries?site={}".format(site)
                ),
            )
            result = discover_rules(markets)
            self.assertEqual(result["summary"]["generated_rules"], 11, slug)
            rule = parse_rule(result["generated_rules"][0])
            self.assertEqual(rule.timezone, zone)
            self.assertEqual(rule.source["station_id"], site)
            self.assertEqual(rule.source["provider"], "NOAA")
            self.assertEqual(rule.source["url"], "https://api.synopticdata.com/v2/stations/timeseries")
            self.assertNotIn("api.weather.gov", rule.source["url"])
            self.assertIn(
                "wrh/timeseries?site={}".format(site.lower()),
                str(rule.source.get("resolution_source") or "").lower(),
            )
            self.assertEqual(rule.source["value_path"], "temp")
            self.assertNotIn("token", rule.source.get("params") or {})
            self.assertTrue(rule.enabled)

    def test_discover_hko_daily_extract_enables_dry_run_rules(self) -> None:
        markets = self._temp_markets(
            slug="lowest-temperature-in-hong-kong-on-september-5-2026",
            source="",
            titles=[
                "20°C or below",
                "21°C",
                "22°C",
                "23°C",
                "24°C",
                "25°C",
                "26°C",
                "27°C",
                "28°C",
                "29°C",
                "30°C or higher",
            ],
            metric_word="lowest",
            description=(
                'Hong Kong Observatory Absolute Daily Min (deg. C) in the Daily Extract, '
                "available here: https://www.weather.gov.hk/en/cis/climat.htm"
            ),
        )
        result = discover_rules(markets)
        self.assertEqual(result["summary"]["generated_rules"], 11)
        rule = parse_rule(result["generated_rules"][0])
        self.assertEqual(rule.timezone, "Asia/Hong_Kong")
        self.assertEqual(rule.source["station_id"], "HKO")
        self.assertEqual(rule.source["provider"], "HKO")
        self.assertEqual(rule.source["params"]["dataType"], "CLMMINT")
        self.assertEqual(rule.observation_start, "2026-09-05T00:00:00")
        self.assertTrue(rule.enabled)

    def test_hko_daily_extract_finalizes_published_row(self) -> None:
        markets = self._temp_markets(
            slug="lowest-temperature-in-hong-kong-on-september-5-2026",
            source="",
            titles=[
                "20°C or below",
                "21°C",
                "22°C",
                "23°C",
                "24°C",
                "25°C",
                "26°C",
                "27°C",
                "28°C",
                "29°C",
                "30°C or higher",
            ],
            metric_word="lowest",
            description="available here: https://www.weather.gov.hk/en/cis/climat.htm",
        )
        rule = parse_rule(discover_rules(markets)["generated_rules"][0])
        from weather_runtime.rules import with_source_contract

        rule = with_source_contract(
            rule,
            source={
                **rule.source,
                "static": {
                    "fields": ["Year", "Month", "Day", "Value", "data Completeness"],
                    "data": [
                        ["2026", "9", "5", "25.4", "C"],
                        ["2026", "9", "6", "26.1", "C"],
                    ],
                },
            },
        )
        observation = WeatherSourceAdapter().poll(
            rule,
            now=datetime(2026, 9, 6, 10, tzinfo=timezone.utc),
        )
        self.assertEqual(observation.status, "final")
        self.assertEqual(observation.value, 25)
        self.assertEqual(observation.station_id, "HKO")


if __name__ == "__main__":
    unittest.main()

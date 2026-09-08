from __future__ import annotations

import json
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from unittest.mock import patch

from weather_runtime.markets import (
    GammaError,
    catalog_event_groups,
    filter_events_in_horizon,
    flatten_event_markets,
    group_weather_markets,
    load_market_rows,
    merge_event_groups,
    observation_in_horizon,
    snapshot_payload,
    weather_markets,
)
from weather_runtime.discovery import (
    discover_rules,
    hourly_window_for_timezone,
    sample_set_from_description,
    wrh_hourly_page_url,
)
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
from weather_runtime.cli import serve_argv
from weather_runtime.scanner import WeatherScanner, WeatherScannerConfig
from weather_runtime.service import RuntimeService, slim_board_groups
from weather_runtime.server import build_parser as build_board_parser, resolved_service_options
from weather_runtime.sources import SourceError, WeatherSourceAdapter, series_from_raw
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

    def test_cli_without_subcommand_starts_board_and_scanner(self) -> None:
        self.assertEqual(serve_argv([]), [])
        self.assertEqual(serve_argv(["--sync", "--interval", "30"]), ["--sync", "--interval", "30"])
        self.assertEqual(serve_argv(["serve", "--no-open"]), ["--no-open"])
        self.assertIsNone(serve_argv(["scan", "--fixture", "fixtures"]))
        self.assertIsNone(serve_argv(["discover", "--sync"]))
        self.assertIsNone(serve_argv(["take", "--event", "x"]))

    def test_board_defaults_enable_live_dry_run_scan(self) -> None:
        root = ROOT
        live = resolved_service_options(build_board_parser().parse_args([]), root=root)
        self.assertTrue(live["sync"])
        self.assertEqual(live["data_dir"], (root / "data" / "pm-weather-live").resolve())
        self.assertIsNone(live["fixture"])
        fixture = resolved_service_options(
            build_board_parser().parse_args(["--fixture", str(FIXTURES)]),
            root=root,
        )
        self.assertFalse(fixture["sync"])
        self.assertEqual(fixture["fixture"], FIXTURES.resolve())
        off = resolved_service_options(build_board_parser().parse_args(["--no-sync"]), root=root)
        self.assertFalse(off["sync"])
        args = build_board_parser().parse_args([])
        self.assertEqual(args.interval, 30.0)
        self.assertFalse(args.no_open)
        self.assertFalse(args.no_auto_start)

    def test_candidate_desk_page_is_wired(self) -> None:
        page = ROOT / "weather_board" / "public" / "candidates.html"
        script = ROOT / "weather_board" / "src" / "candidates.js"
        self.assertTrue(page.is_file())
        self.assertTrue(script.is_file())
        html = page.read_text(encoding="utf-8")
        self.assertIn("/src/candidates.js", html)
        self.assertIn("candidateRows", html)
        self.assertIn("desk-table", html)
        self.assertIn('href="/candidates"', html)
        js = script.read_text(encoding="utf-8")
        self.assertIn("fetchCandidates", js)
        self.assertIn("net_edge", js)
        module = json.loads((ROOT / "weather_board" / "module.json").read_text(encoding="utf-8"))
        self.assertIn("GET /candidates", module["api"]["routes"])
        self.assertIn("GET /api/candidates", module["api"]["routes"])

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
        self.assertGreaterEqual(len(observation.series), 3)

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

            def _payload(self, rule, **kwargs):  # type: ignore[override]
                self.payload_calls += 1
                return super()._payload(rule, **kwargs)

        rule = load_rules(FIXTURES / "rules.json")[0]
        adapter = CountingAdapter()
        before = adapter.poll(rule, now=datetime(2026, 9, 3, 12, tzinfo=timezone.utc))
        self.assertEqual(before.status, "waiting_window")
        self.assertEqual(before.reason, "observation_window_not_started")
        self.assertEqual(adapter.payload_calls, 0)
        during = adapter.poll(rule, now=datetime(2026, 9, 4, 12, tzinfo=timezone.utc))
        self.assertEqual(during.status, "intraday")
        self.assertEqual(adapter.payload_calls, 1)

    def test_synoptic_token_failure_is_cached(self) -> None:
        class BoomHttp:
            def __init__(self) -> None:
                self.calls = 0

            def get_text(self, url, **kwargs):  # noqa: ARG002
                self.calls += 1
                raise SourceError("timed out after 8s from {}".format(url))

            def get_json(self, *args, **kwargs):  # noqa: ARG002
                raise AssertionError("timeseries should not run after token failure")

        http = BoomHttp()
        adapter = WeatherSourceAdapter(http=http)
        with self.assertRaises(SourceError):
            adapter._synoptic_token()
        with self.assertRaises(SourceError):
            adapter._synoptic_token()
        self.assertEqual(http.calls, 1)

    def _live_source_rule(self, *, url: str, station_id: str = "TEST"):
        rule = load_rules(FIXTURES / "rules.json")[0]
        live = parse_rule(
            {
                **rule.to_dict(),
                "source": {
                    **dict(rule.source),
                    "static": None,
                    "url": url,
                    "station_id": station_id,
                    "provider": "NOAA",
                },
            }
        )
        live.source.pop("static", None)
        return live

    def test_prefetch_real_timeout_is_cached(self) -> None:
        class CountingHttp:
            def __init__(self) -> None:
                self.calls = 0

            def get_json(self, url, **kwargs):  # noqa: ARG002
                self.calls += 1
                raise SourceError("timed out after 8s from {}".format(url))

        live = self._live_source_rule(url="https://example.invalid/weather")
        http = CountingHttp()
        adapter = WeatherSourceAdapter(http=http, http_cache_ttl_s=60.0)
        adapter.prefetch([live], now=datetime(2026, 9, 4, 12, tzinfo=timezone.utc), deadline_s=1.0)
        first_calls = http.calls
        self.assertGreaterEqual(first_calls, 1)
        observation = adapter.poll(live, now=datetime(2026, 9, 4, 12, tzinfo=timezone.utc))
        self.assertEqual(observation.status, "unavailable")
        self.assertIn("recently timed out", observation.reason)
        self.assertEqual(http.calls, first_calls)

    def test_prefetch_miss_does_not_mark_unfetched_timed_out(self) -> None:
        class GatedHttp:
            def __init__(self) -> None:
                self.calls: list[str] = []
                self.block = threading.Event()

            def get_json(self, url, **kwargs):  # noqa: ARG002
                self.calls.append(str(url))
                if "slow" in str(url):
                    self.block.wait(2.0)
                    raise SourceError("timed out after 8s from {}".format(url))
                return {"observations": [{"timestamp": "2026-09-04T12:00:00Z", "value": 26}]}

        slow = self._live_source_rule(url="https://example.invalid/slow", station_id="SLOW")
        later = self._live_source_rule(url="https://example.invalid/later", station_id="LATER")
        http = GatedHttp()
        adapter = WeatherSourceAdapter(http=http, http_cache_ttl_s=60.0)
        adapter.prefetch([slow, later], now=datetime(2026, 9, 4, 12, tzinfo=timezone.utc), deadline_s=0.05)
        http.block.set()
        observation = adapter.poll(later, now=datetime(2026, 9, 4, 12, tzinfo=timezone.utc))
        self.assertNotIn("recently timed out", observation.reason)
        self.assertIn("https://example.invalid/later", http.calls)

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
        self.assertEqual(sum(1 for row in result["rows"] if row.get("book")), 21)
        self.assertTrue(all("raw" not in (row.get("market") or {}) for row in result["rows"]))
        self.assertTrue(all("raw" not in (row.get("observation") or {}) for row in result["rows"]))
        self.assertTrue(all("series" not in (row.get("observation") or {}) for row in result["rows"]))
        group = result["event_groups"][0]
        self.assertGreaterEqual(len(group.get("source_series") or []), 3)
        self.assertEqual(group["source_series"][0]["temp"], 21)
        candidate = next(row for row in result["rows"] if row["status"] == "opportunity")
        self.assertEqual(candidate["matched_outcome"], "26")
        self.assertEqual(candidate["trade_side"], "YES")
        self.assertEqual(str(candidate.get("order_side") or "BUY"), "BUY")
        self.assertAlmostEqual(candidate["economics"]["net_edge"], 0.009505, places=6)
        losers = [row for row in result["rows"] if row.get("trade_side") == "NO"]
        self.assertEqual(len(losers), 10)
        self.assertTrue(all(row["status"] == "no_trade" for row in losers))
        self.assertTrue(all(row["reason"] == "ask_above_limit" for row in losers))
        sell_yes = [
            row
            for row in result["rows"]
            if str(row.get("order_side") or "") == "SELL"
        ]
        self.assertEqual(len(sell_yes), 10)
        self.assertTrue(all(row["status"] == "no_trade" for row in sell_yes))

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
        timezone_name: str = "UTC",
        unit: str = "C",
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
                    "timezone": timezone_name,
                    "metric": metric,
                    "observation_start": start,
                    "observation_end": end,
                    "unit": unit,
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

    def _buy_rows_by_outcome(self, rows):
        by_outcome = {}
        for row in rows:
            if str(row.get("order_side") or "BUY").upper() == "SELL":
                continue
            by_outcome[row["target_outcome"]] = row
        return by_outcome

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
        by_outcome = self._buy_rows_by_outcome(result["rows"])
        self.assertEqual(by_outcome["23"]["status"], "opportunity")
        self.assertEqual(by_outcome["23"]["trade_side"], "NO")
        self.assertEqual(by_outcome["23"]["reason"], "intraday_impossible_no")
        self.assertTrue(by_outcome["23"].get("dry_run"))
        self.assertEqual(by_outcome["18 or below"]["status"], "waiting")
        self.assertEqual(by_outcome["19"]["status"], "waiting")
        self.assertNotEqual(by_outcome["19"].get("trade_side"), "YES")
        self.assertFalse(
            any(
                row.get("trade_side") == "YES"
                and str(row.get("order_side") or "BUY") == "BUY"
                and row.get("target_outcome") == "19"
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
        by_outcome = self._buy_rows_by_outcome(result["rows"])
        self.assertEqual(by_outcome["30"]["status"], "opportunity")
        self.assertEqual(by_outcome["30"]["trade_side"], "NO")
        self.assertEqual(by_outcome["30"]["reason"], "intraday_impossible_no")
        self.assertEqual(by_outcome["33"]["status"], "waiting")
        self.assertEqual(by_outcome["34"]["status"], "waiting")
        self.assertFalse(
            any(
                row.get("trade_side") == "YES" and str(row.get("order_side") or "BUY") == "BUY"
                for row in result["rows"]
            )
        )

    def test_intraday_impossible_sell_yes_when_dead_yes_has_bids(self) -> None:
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
            {"outcome": "33", "lower": 32, "lower_inclusive": False},
        ]
        markets, rules = self._event_with_static(
            event_id="intraday-sell-yes-31",
            metric="daily_max",
            outcomes=[item["outcome"] for item in buckets],
            buckets=buckets,
            features=[
                {"value": 28, "timestamp": "2026-09-06T02:00:00Z"},
                {"value": 32, "timestamp": "2026-09-06T08:00:00Z"},
            ],
        )
        now = datetime(2026, 9, 6, 12, tzinfo=timezone.utc)
        stamp = now.isoformat()
        books = {}
        for market in markets:
            no_token = market.no_token_id or market.token_ids[1]
            yes_token = market.yes_token_id or market.token_ids[0]
            # No asks gone (common live case); dead 31 Yes still has a bid.
            books[no_token] = {
                "token_id": no_token,
                "asks": [],
                "bids": [{"price": 0.999, "size": 50}],
                "tick_size": 0.001,
                "min_order_size": 1,
                "fetched_at": stamp,
            }
            if str(market.outcome) == "31":
                books[yes_token] = {
                    "token_id": yes_token,
                    "asks": [{"price": 0.05, "size": 10}],
                    "bids": [{"price": 0.04, "size": 20}],
                    "tick_size": 0.001,
                    "min_order_size": 1,
                    "fetched_at": stamp,
                }
            else:
                books[yes_token] = {
                    "token_id": yes_token,
                    "asks": [{"price": 0.99, "size": 10}],
                    "tick_size": 0.001,
                    "min_order_size": 1,
                    "fetched_at": stamp,
                }
        result = WeatherScanner(config=WeatherScannerConfig()).scan(
            markets,
            rules,
            books=books,
            fetch_books=False,
            now=now,
        )
        sell_rows = [
            row
            for row in result["rows"]
            if row.get("target_outcome") == "31" and str(row.get("order_side") or "") == "SELL"
        ]
        self.assertEqual(len(sell_rows), 1)
        sell = sell_rows[0]
        self.assertEqual(sell["status"], "opportunity")
        self.assertEqual(sell["trade_side"], "YES")
        self.assertEqual(sell["reason"], "intraday_impossible_sell_yes")
        self.assertGreaterEqual(float((sell.get("economics") or {}).get("net_edge") or 0), 0.0075)
        no_rows = [
            row
            for row in result["rows"]
            if row.get("target_outcome") == "31" and str(row.get("order_side") or "BUY") == "BUY"
        ]
        self.assertEqual(no_rows[0]["status"], "no_trade")
        self.assertEqual(no_rows[0]["reason"], "book_missing")

    def test_sell_yes_skipped_when_buy_no_already_opportunity(self) -> None:
        buckets = [
            {"outcome": "30", "upper": 30, "upper_inclusive": True},
            {
                "outcome": "31",
                "lower": 30,
                "lower_inclusive": False,
                "upper": 31,
                "upper_inclusive": True,
            },
            {"outcome": "32", "lower": 31, "lower_inclusive": False},
        ]
        markets, rules = self._event_with_static(
            event_id="prefer-buy-no",
            metric="daily_max",
            outcomes=[item["outcome"] for item in buckets],
            buckets=buckets,
            features=[{"value": 32, "timestamp": "2026-09-06T08:00:00Z"}],
        )
        now = datetime(2026, 9, 6, 12, tzinfo=timezone.utc)
        stamp = now.isoformat()
        books = self._priced_books(markets, now, {"30", "31"})
        for market in markets:
            yes_token = market.yes_token_id or market.token_ids[0]
            books[yes_token] = {
                "token_id": yes_token,
                "asks": [{"price": 0.05, "size": 10}],
                "bids": [{"price": 0.04, "size": 20}],
                "tick_size": 0.001,
                "min_order_size": 1,
                "fetched_at": stamp,
            }
        result = WeatherScanner(config=WeatherScannerConfig()).scan(
            markets, rules, books=books, fetch_books=False, now=now
        )
        sell_30 = next(
            row
            for row in result["rows"]
            if row.get("target_outcome") == "30" and str(row.get("order_side") or "") == "SELL"
        )
        self.assertEqual(sell_30["status"], "no_trade")
        self.assertEqual(sell_30["reason"], "buy_no_preferred")
        buy_30 = next(
            row
            for row in result["rows"]
            if row.get("target_outcome") == "30" and str(row.get("order_side") or "BUY") == "BUY"
        )
        self.assertEqual(buy_30["status"], "opportunity")

    def test_live_take_refuses_sell_yes_without_inventory_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = RuntimeService(root=ROOT, data_dir=Path(temp) / "data", sync=False)
            service.last_result = {
                "summary": {},
                "rows": [
                    {
                        "event_group_id": "dead-bucket-event",
                        "target_outcome": "30",
                        "trade_side": "YES",
                        "order_side": "SELL",
                        "status": "opportunity",
                        "reason": "intraday_impossible_sell_yes",
                        "book_token_id": "yes-token",
                        "market_id": "m1",
                        "market": {"id": "m1"},
                    }
                ],
            }
            result = service.take_opportunity(
                event_group_id="dead-bucket-event",
                target_outcome="30",
                live=True,
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["error"], "sell_yes_requires_inventory")

    def test_auto_take_skips_sell_yes_rows(self) -> None:
        from unittest.mock import patch

        sell_row = {
            "event_group_id": "dead-bucket-event",
            "target_outcome": "30",
            "trade_side": "YES",
            "order_side": "SELL",
            "status": "opportunity",
            "reason": "intraday_impossible_sell_yes",
            "book_token_id": "yes-token",
            "market_id": "m1",
            "market": {"id": "m1"},
        }
        with tempfile.TemporaryDirectory() as temp:
            service = RuntimeService(root=ROOT, data_dir=Path(temp) / "data", sync=False)
            service.last_result = {"summary": {}, "rows": [sell_row]}
            with patch.dict("os.environ", {"LIVE_ORDERS": "true"}, clear=False):
                with patch.object(service, "take_opportunity") as take:
                    takes = service._auto_take_opportunities(service.last_result)
            self.assertEqual(takes, [])
            take.assert_not_called()

    def test_walk_bids_consumes_highest_levels_first(self) -> None:
        from weather_runtime.books import normalize_book, walk_bids

        book = normalize_book(
            "tok",
            {
                "bids": [
                    {"price": "0.03", "size": "10"},
                    {"price": "0.05", "size": "3"},
                    {"price": "0.04", "size": "4"},
                ],
                "fetched_at": "2026-09-07T00:00:00+00:00",
            },
        )
        self.assertEqual([level["price"] for level in book.bids], [0.05, 0.04, 0.03])
        fill = walk_bids(book, min_price=0.035, target_shares=5)
        self.assertTrue(fill["complete"])
        self.assertAlmostEqual(fill["filled_shares"], 5)
        self.assertAlmostEqual(fill["worst_price"], 0.04)
        self.assertAlmostEqual(fill["proceeds"], 3 * 0.05 + 2 * 0.04)

    def test_intraday_before_local_midnight_does_not_fetch(self) -> None:
        class CountingAdapter(WeatherSourceAdapter):
            def __init__(self) -> None:
                super().__init__()
                self.payload_calls = 0

            def _payload(self, rule, **kwargs):  # type: ignore[override]
                self.payload_calls += 1
                return super()._payload(rule, **kwargs)

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
        by_outcome = self._buy_rows_by_outcome(result["rows"])
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
            now=datetime(2026, 9, 5, 1, tzinfo=timezone.utc),
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
            cands = service.candidates()
            self.assertEqual(len(cands), 1)
            self.assertEqual(cands[0]["matched_outcome"], "26")
            self.assertEqual(cands[0]["trade_side"], "YES")
            self.assertIn("lock", cands[0])
            self.assertIn("net_edge", cands[0].get("economics") or {})
            self.assertIn("question", (cands[0].get("market") or {}))
            self.assertTrue((cands[0].get("book") or {}).get("asks"))
            published = service.board_snapshot()
            self.assertIn("event_groups", published)
            self.assertNotIn("rows", published)
            board = slim_board_groups(result["event_groups"])
            for group in board:
                self.assertNotIn("source_series", group)
                for row in group.get("rows") or []:
                    self.assertNotIn("raw", row.get("observation") or {})
                    self.assertNotIn("asks", row.get("book") or {})

    def test_service_hydrates_board_from_latest_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            data = Path(temp) / "data"
            data.mkdir()
            (data / "latest.json").write_text(
                json.dumps(
                    {
                        "summary": {"scanned_at": "2026-09-06T00:00:00+00:00", "event_groups": 1},
                        "event_groups": [
                            {
                                "event_group_id": "hydrated-event",
                                "status": "waiting",
                                "rows": [],
                                "markets": [],
                            }
                        ],
                        "rows": [],
                    }
                ),
                encoding="utf-8",
            )
            service = RuntimeService(root=ROOT, data_dir=data, sync=True)
            events = service.events()
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["event_group_id"], "hydrated-event")
            self.assertEqual(service.last_scan_at, "2026-09-06T00:00:00+00:00")

    def test_gamma_timeout_falls_back_to_disk_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            data = Path(temp) / "data"
            data.mkdir()
            rows = load_market_rows(str(FIXTURES / "markets.json"))
            (data / "market_snapshot.json").write_text(
                json.dumps(snapshot_payload(rows)),
                encoding="utf-8",
            )
            (data / "weather_catalog.json").write_text(
                json.dumps(
                    {
                        "event_groups": catalog_event_groups(
                            [
                                {
                                    "slug": "highest-temperature-in-london-on-september-4-2026",
                                    "title": "Highest temperature in London on September 4?",
                                    "markets": [{"id": "c", "groupItemTitle": "26"}],
                                }
                            ]
                        )
                    }
                ),
                encoding="utf-8",
            )
            service = RuntimeService(
                root=ROOT,
                data_dir=data,
                rules_file=FIXTURES / "rules.json",
                books_file=FIXTURES / "books.json",
                sync=True,
                horizon_hours=24.0,
            )
            service._horizon_now = lambda: datetime(2026, 9, 5, 1, tzinfo=timezone.utc)  # type: ignore[method-assign]
            with patch("weather_runtime.service.GammaClient") as client_cls:
                loaded = service._load_markets()
                client_cls.assert_not_called()
            self.assertTrue(loaded)
            service._catalog_at = 1.0
            with patch("weather_runtime.service.GammaClient") as client_cls:
                client_cls.return_value.list_events.side_effect = GammaError(
                    "network error from gamma: timed out"
                )
                loaded = service._load_markets()
            self.assertTrue(loaded)
            self.assertIn("timed out", service._gamma_sync_error)

    def test_event_detail_backfills_series_from_source_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            data = Path(temp) / "data"
            data.mkdir()
            event_id = "highest-temperature-in-zhengzhou-on-september-6-2026"
            evidence_hash = "313953c9f6a574d1deadbeef"
            (data / "latest.json").write_text(
                json.dumps(
                    {
                        "summary": {"scanned_at": "2026-09-06T16:50:21+00:00"},
                        "event_groups": [
                            {
                                "event_group_id": event_id,
                                "source_status": "final",
                                "rows": [
                                    {
                                        "observation": {
                                            "status": "final",
                                            "value": 33,
                                            "evidence_hash": evidence_hash,
                                            "station_id": "ZHCC",
                                        }
                                    }
                                ],
                                "markets": [],
                            }
                        ],
                        "rows": [],
                    }
                ),
                encoding="utf-8",
            )
            (data / "source_observations.jsonl").write_text(
                json.dumps(
                    {
                        "event_group_id": event_id,
                        "evidence_hash": evidence_hash,
                        "raw": {
                            "observations": [
                                {"timestamp": "2026-09-06T00:00:00+00:00", "temp": 22},
                                {"timestamp": "2026-09-06T06:00:00+00:00", "temp": 19},
                                {"timestamp": "2026-09-06T10:00:00+00:00", "temp": 33},
                            ]
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            service = RuntimeService(root=ROOT, data_dir=data, sync=True)
            detail = service.event_detail(event_id)
            self.assertIsNotNone(detail)
            series = (detail or {}).get("source_series") or []
            self.assertEqual(len(series), 3)
            self.assertEqual(series[-1]["temp"], 33)

    def test_event_detail_keeps_empty_source_series(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            data = Path(temp) / "data"
            data.mkdir()
            event_id = "highest-temperature-in-dallas-on-september-7-2026"
            (data / "latest.json").write_text(
                json.dumps(
                    {
                        "summary": {"scanned_at": "2026-09-06T17:34:58+00:00"},
                        "event_groups": [
                            {
                                "event_group_id": event_id,
                                "source_status": "waiting_window",
                                "source_series": [],
                                "rows": [
                                    {
                                        "observation": {
                                            "status": "waiting_window",
                                            "reason": "observation_window_not_started",
                                            "station_id": "KDAL",
                                        },
                                        "rule": {
                                            "timezone": "America/Chicago",
                                            "observation_start": "2026-09-07T00:00:00",
                                            "observation_end": "2026-09-07T23:59:59",
                                        },
                                    }
                                ],
                                "markets": [],
                            }
                        ],
                        "rows": [],
                    }
                ),
                encoding="utf-8",
            )
            (data / "source_observations.jsonl").write_text(
                json.dumps(
                    {
                        "event_group_id": event_id,
                        "raw": {
                            "observations": [
                                {"timestamp": "2026-09-06T00:00:00+00:00", "temp": 88},
                                {"timestamp": "2026-09-06T12:00:00+00:00", "temp": 94},
                            ]
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            service = RuntimeService(root=ROOT, data_dir=data, sync=True)
            detail = service.event_detail(event_id)
            self.assertEqual((detail or {}).get("source_series"), [])

    def test_scan_fetches_yes_books_for_waiting_intraday_buckets(self) -> None:
        from weather_runtime.models import Book, utc_now

        buckets = [
            {"outcome": "30", "upper": 30, "upper_inclusive": True},
            {
                "outcome": "33",
                "lower": 30,
                "lower_inclusive": False,
                "upper": 33,
                "upper_inclusive": True,
            },
            {"outcome": "34", "lower": 33, "lower_inclusive": False},
        ]
        markets, rules = self._event_with_static(
            event_id="intraday-max-display-books",
            metric="daily_max",
            outcomes=[item["outcome"] for item in buckets],
            buckets=buckets,
            features=[
                {"value": 28, "timestamp": "2026-09-06T02:00:00Z"},
                {"value": 33, "timestamp": "2026-09-06T08:00:00Z"},
            ],
        )
        now = datetime(2026, 9, 6, 12, tzinfo=timezone.utc)
        recorded: list[str] = []
        calls: list[list[str]] = []

        class FakeClob:
            def fetch_books(self, token_ids):
                batch = [str(item) for item in token_ids]
                calls.append(batch)
                recorded.extend(batch)
                stamp = utc_now().isoformat()
                return {
                    str(token_id): Book(
                        token_id=str(token_id),
                        best_ask=0.01 if str(token_id).endswith("-yes") else 0.99,
                        asks=[{"price": 0.01 if str(token_id).endswith("-yes") else 0.99, "size": 10}],
                        tick_size=0.001,
                        min_order_size=1,
                        fetched_at=stamp,
                    )
                    for token_id in token_ids
                }

        result = WeatherScanner(
            config=WeatherScannerConfig(),
            clob_client=FakeClob(),
        ).scan(markets, rules, fetch_books=True, now=now)
        by_outcome = self._buy_rows_by_outcome(result["rows"])
        waiting_yes = next(item.yes_token_id for item in markets if item.outcome == "33")
        impossible_no = next(item.no_token_id for item in markets if item.outcome == "30")
        self.assertTrue(calls)
        self.assertIn(impossible_no, calls[0])
        self.assertNotIn(waiting_yes, calls[0])
        self.assertIn(waiting_yes, recorded)
        self.assertIn(impossible_no, recorded)
        self.assertTrue(any(waiting_yes in batch for batch in calls[1:]))
        self.assertEqual(by_outcome["30"]["status"], "opportunity")
        self.assertEqual(by_outcome["30"]["trade_side"], "NO")
        self.assertEqual((by_outcome["33"].get("book") or {}).get("best_ask"), 0.01)
        self.assertEqual((by_outcome["33"].get("book") or {}).get("asks")[0]["size"], 10)
        group = result["event_groups"][0]
        waiting_view = next(item for item in group["markets"] if item.get("outcome") == "33")
        self.assertEqual((waiting_view.get("book") or {}).get("best_ask"), 0.01)

    def test_new_extremum_no_tokens_fetched_before_prior_nos(self) -> None:
        from weather_runtime.models import Book, utc_now

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
            event_id="intraday-max-priority",
            metric="daily_max",
            outcomes=[item["outcome"] for item in buckets],
            buckets=buckets,
            features=[{"value": 33, "timestamp": "2026-09-06T08:00:00Z"}],
        )
        now = datetime(2026, 9, 6, 12, tzinfo=timezone.utc)
        calls: list[list[str]] = []

        class FakeClob:
            def fetch_books(self, token_ids):
                calls.append([str(item) for item in token_ids])
                stamp = utc_now().isoformat()
                return {
                    str(token_id): Book(
                        token_id=str(token_id),
                        best_ask=0.99,
                        asks=[{"price": 0.99, "size": 10}],
                        tick_size=0.001,
                        min_order_size=1,
                        fetched_at=stamp,
                    )
                    for token_id in token_ids
                }

        WeatherScanner(config=WeatherScannerConfig(), clob_client=FakeClob()).scan(
            markets,
            rules,
            fetch_books=True,
            now=now,
            previous_extrema={
                "intraday-max-priority": {"value": 31.0, "status": "intraday"}
            },
        )
        no_30 = next(item.no_token_id for item in markets if item.outcome == "30")
        no_31 = next(item.no_token_id for item in markets if item.outcome == "31")
        no_32 = next(item.no_token_id for item in markets if item.outcome == "32")
        waiting_yes = next(item.yes_token_id for item in markets if item.outcome == "33")
        self.assertGreaterEqual(len(calls), 2)
        self.assertIn(no_31, calls[0])
        self.assertIn(no_32, calls[0])
        self.assertNotIn(no_30, calls[0])
        self.assertNotIn(waiting_yes, calls[0])
        self.assertTrue(any(no_30 in batch for batch in calls[1:]))
        self.assertIn(waiting_yes, calls[-1])
        self.assertNotIn(no_30, calls[-1])

    def test_eval_clock_accepts_books_fetched_after_scan_start(self) -> None:
        from weather_runtime.models import Book, utc_now

        buckets = [
            {"outcome": "30", "upper": 30, "upper_inclusive": True},
            {
                "outcome": "33",
                "lower": 30,
                "lower_inclusive": False,
                "upper": 33,
                "upper_inclusive": True,
            },
            {"outcome": "34", "lower": 33, "lower_inclusive": False},
        ]
        markets, rules = self._event_with_static(
            event_id="intraday-max-ttl-eval",
            metric="daily_max",
            outcomes=[item["outcome"] for item in buckets],
            buckets=buckets,
            features=[{"value": 33, "timestamp": "2026-09-07T08:00:00Z"}],
            start="2026-09-07T00:00:00Z",
            end="2026-09-07T23:59:59Z",
        )

        class FakeClob:
            def fetch_books(self, token_ids):
                stamp = utc_now().isoformat()
                return {
                    str(token_id): Book(
                        token_id=str(token_id),
                        best_ask=0.99,
                        asks=[{"price": 0.99, "size": 10}],
                        tick_size=0.001,
                        min_order_size=1,
                        fetched_at=stamp,
                    )
                    for token_id in token_ids
                }

        result = WeatherScanner(
            config=WeatherScannerConfig(),
            clob_client=FakeClob(),
        ).scan(markets, rules, fetch_books=True)
        by_outcome = self._buy_rows_by_outcome(result["rows"])
        self.assertEqual(by_outcome["30"]["status"], "opportunity")
        self.assertNotEqual(by_outcome["30"].get("reason"), "book_stale")

    def test_event_detail_uses_snapshot_books_without_clob(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            data = Path(temp) / "data"
            data.mkdir()
            event_id = "highest-temperature-in-guangzhou-on-september-7-2026"
            (data / "latest.json").write_text(
                json.dumps(
                    {
                        "summary": {"scanned_at": "2026-09-06T17:40:24+00:00"},
                        "event_groups": [
                            {
                                "event_group_id": event_id,
                                "source_status": "intraday",
                                "markets": [
                                    {
                                        "market_id": "4239390",
                                        "outcome": "29°C or below",
                                        "status": "waiting",
                                        "book": {
                                            "best_ask": 0.001,
                                            "asks": [{"price": 0.001, "size": 185.98}],
                                        },
                                    }
                                ],
                                "rows": [],
                            }
                        ],
                        "rows": [],
                    }
                ),
                encoding="utf-8",
            )
            service = RuntimeService(root=ROOT, data_dir=data, sync=True)

            class BoomClob:
                def fetch_books(self, token_ids):
                    raise AssertionError("event detail must not fetch CLOB")

            service.scanner.clob_client = BoomClob()
            detail = service.event_detail(event_id)
            book = ((detail or {}).get("markets") or [{}])[0].get("book") or {}
            self.assertEqual(book.get("best_ask"), 0.001)

    def test_intraday_series_uses_station_local_date(self) -> None:
        raw = {
            "market_id": "m-zhcc-local",
            "event_group_id": "highest-temperature-in-zhengzhou-on-september-7-2026",
            "source": {
                "static": {
                    "features": [
                        {"value": 21.999999999999996, "timestamp": "2026-09-06T16:00:00Z"},
                        {"value": 21.999999999999996, "timestamp": "2026-09-06T17:00:00Z"},
                    ]
                },
                "url": "fixture://weather/zhcc-local",
                "resolution_source": "fixture://weather/zhcc-local",
                "value_path": "value",
                "timestamp_path": "timestamp",
            },
            "timezone": "Asia/Shanghai",
            "metric": "daily_max",
            "observation_start": "2026-09-07T00:00:00",
            "observation_end": "2026-09-07T23:59:59",
            "unit": "C",
            "buckets": [
                {"outcome": "22 or below", "upper": 22, "upper_inclusive": True},
                {"outcome": "23 or higher", "lower": 22, "lower_inclusive": False},
            ],
        }
        observation = WeatherSourceAdapter().poll(
            parse_rule(raw),
            now=datetime(2026, 9, 6, 17, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(observation.status, "intraday")
        self.assertEqual(observation.series[0]["local_time"], "2026-09-07 00:00")
        self.assertEqual(observation.series[1]["local_time"], "2026-09-07 01:00")
        self.assertEqual(observation.series[0]["timezone"], "Asia/Shanghai")
        self.assertEqual(observation.series[0]["temp"], 22.0)

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
        self.assertEqual(rule.source.get("sample_set"), "all")
        self.assertNotIn("hourly_window", rule.source)
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

    def test_wunderground_half_hour_after_close_is_daily_min(self) -> None:
        markets = self._temp_markets(
            slug="lowest-temperature-in-taipei-on-september-7-2026",
            source="https://www.wunderground.com/history/daily/tw/taipei/RCSS",
            titles=[
                "19°C or below",
                "20°C",
                "21°C",
                "22°C",
                "23°C",
                "24°C",
                "25°C",
                "26°C",
                "27°C",
                "28°C",
                "29°C or higher",
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
                            "temp": 23,
                            "valid_time_gmt": int(datetime(2026, 9, 7, 15, tzinfo=timezone.utc).timestamp()),
                        },
                        {
                            "temp": 22,
                            "valid_time_gmt": int(datetime(2026, 9, 7, 15, 30, tzinfo=timezone.utc).timestamp()),
                        },
                        {
                            "temp": 23,
                            "valid_time_gmt": int(datetime(2026, 9, 7, 16, tzinfo=timezone.utc).timestamp()),
                        },
                    ]
                },
            },
        )
        observation = WeatherSourceAdapter().poll(
            rule,
            now=datetime(2026, 9, 7, 16, 10, tzinfo=timezone.utc),
        )
        self.assertEqual(observation.status, "final")
        self.assertEqual(observation.value, 22)
        self.assertEqual(observation.source_timestamp, "2026-09-07T15:30:00+00:00")
        by_local = {point["local_time"]: point for point in observation.series}
        self.assertEqual(by_local["2026-09-07 23:30"]["temp"], 22.0)
        self.assertTrue(by_local["2026-09-07 23:30"]["counts_for_resolution"])
        raw_points = series_from_raw(rule.source["static"])
        self.assertEqual(raw_points[-2]["temp"], 22)
        self.assertIn("2026-09-07T15:30:00", raw_points[-2]["timestamp"])

    def test_wunderground_post_close_refreshes_late_half_hour(self) -> None:
        markets = self._temp_markets(
            slug="lowest-temperature-in-taipei-on-september-7-2026",
            source="https://www.wunderground.com/history/daily/tw/taipei/RCSS",
            titles=[
                "19°C or below",
                "20°C",
                "21°C",
                "22°C",
                "23°C",
                "24°C",
                "25°C",
                "26°C",
                "27°C",
                "28°C",
                "29°C or higher",
            ],
            metric_word="lowest",
        )
        rule = parse_rule(discover_rules(markets)["generated_rules"][0])
        early = {
            "observations": [
                {
                    "temp": 23,
                    "valid_time_gmt": int(datetime(2026, 9, 7, 15, tzinfo=timezone.utc).timestamp()),
                },
                {
                    "temp": 23,
                    "valid_time_gmt": int(datetime(2026, 9, 7, 16, tzinfo=timezone.utc).timestamp()),
                },
            ]
        }
        late = {
            "observations": [
                {
                    "temp": 23,
                    "valid_time_gmt": int(datetime(2026, 9, 7, 15, tzinfo=timezone.utc).timestamp()),
                },
                {
                    "temp": 22,
                    "valid_time_gmt": int(datetime(2026, 9, 7, 15, 30, tzinfo=timezone.utc).timestamp()),
                },
                {
                    "temp": 23,
                    "valid_time_gmt": int(datetime(2026, 9, 7, 16, tzinfo=timezone.utc).timestamp()),
                },
            ]
        }

        class SequencingHttp:
            def __init__(self) -> None:
                self.payloads = [early, late]
                self.calls = 0

            def get_json(self, url, **kwargs):  # noqa: ARG002
                index = min(self.calls, len(self.payloads) - 1)
                self.calls += 1
                return self.payloads[index]

        http = SequencingHttp()
        adapter = WeatherSourceAdapter(http=http, http_cache_ttl_s=60.0)
        now = datetime(2026, 9, 7, 16, 10, tzinfo=timezone.utc)
        first = adapter.poll(rule, now=now)
        self.assertEqual(first.value, 23)
        with adapter._http_lock:
            for key, (stamp, payload) in list(adapter._http_cache.items()):
                adapter._http_cache[key] = (stamp - 20.0, payload)
        second = adapter.poll(rule, now=now)
        self.assertEqual(second.value, 22)
        self.assertEqual(http.calls, 2)

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
        self.assertEqual(rule.source.get("sample_set"), "all")
        self.assertNotIn("hourly_window", rule.source)

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
        self.assertEqual(http.params["complete"], 1)
        self.assertEqual(http.params["vars"], "air_temp,sea_level_pressure,metar")
        self.assertNotIn("start", http.params or {})
        self.assertNotIn("end", http.params or {})
        self.assertEqual(http.params["token"], "test-token")
        self.assertEqual(http.headers["Origin"], "https://www.weather.gov")
        self.assertEqual(
            http.headers["Referer"],
            "https://www.weather.gov/wrh/timeseries?site=zhcc",
        )
        self.assertIn("Mozilla/5.0", http.headers["User-Agent"])
        self.assertEqual(WeatherSourceAdapter().http.timeout, 20.0)
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

    def test_walk_asks_consumes_cheapest_levels_first(self) -> None:
        from weather_runtime.books import normalize_book, walk_asks

        book = normalize_book(
            "tok",
            {
                "asks": [
                    {"price": "0.97", "size": "10"},
                    {"price": "0.94", "size": "3"},
                    {"price": "0.95", "size": "4"},
                ],
                "fetched_at": "2026-09-07T00:00:00+00:00",
            },
        )
        self.assertEqual([level["price"] for level in book.asks], [0.94, 0.95, 0.97])
        fill = walk_asks(book, max_price=0.96, target_shares=5)
        self.assertTrue(fill["complete"])
        self.assertAlmostEqual(fill["filled_shares"], 5)
        self.assertAlmostEqual(fill["worst_price"], 0.95)
        self.assertAlmostEqual(fill["cost"], 3 * 0.94 + 2 * 0.95)

    def test_quantize_buy_makes_two_decimal_usdc(self) -> None:
        from decimal import Decimal

        from weather_runtime.orders import quantize_buy

        price, size = quantize_buy(0.58, 8.620689655172415, 5.0)
        maker = Decimal(str(price)) * Decimal(str(size))
        self.assertLessEqual(maker, Decimal("5.0"))
        self.assertEqual(maker, maker.quantize(Decimal("0.01")))

    def test_live_take_skips_already_filled_token(self) -> None:
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as temp:
            data = Path(temp) / "data"
            data.mkdir()
            (data / "orders.jsonl").write_text(
                json.dumps(
                    {
                        "dry_run": False,
                        "status": "matched",
                        "token_id": "already-filled",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            service = RuntimeService(root=ROOT, data_dir=data, sync=False)
            self.assertIn("already-filled", service._taken_tokens)
            service.last_result = {
                "summary": {},
                "rows": [
                    {
                        "event_group_id": "lowest-temperature-in-test-on-september-7-2026",
                        "target_outcome": "58-59°F",
                        "trade_side": "NO",
                        "status": "opportunity",
                        "book_token_id": "already-filled",
                        "market": {"id": "m1"},
                    }
                ],
            }
            with patch("weather_runtime.orders.submit_fak_buy") as submit:
                result = service.take_opportunity(
                    event_group_id="lowest-temperature-in-test-on-september-7-2026",
                    target_outcome="58-59°F",
                    live=True,
                )
            self.assertFalse(result["ok"])
            self.assertEqual(result["error"], "already_taken")
            submit.assert_not_called()

    def test_auto_take_is_disabled_on_fixture_even_if_live_env(self) -> None:
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as temp:
            service = RuntimeService(
                root=ROOT,
                fixture=FIXTURES,
                data_dir=Path(temp) / "data",
                interval_s=60,
            )
            with patch.dict("os.environ", {"LIVE_ORDERS": "true"}, clear=False):
                with patch("weather_runtime.orders.submit_fak_buy") as submit:
                    service.scan_once()
            submit.assert_not_called()

    def test_auto_take_submits_new_locked_token_once(self) -> None:
        from unittest.mock import patch

        row = {
            "event_group_id": "lowest-temperature-in-austin-on-september-7-2026",
            "target_outcome": "74-75°F",
            "trade_side": "NO",
            "status": "opportunity",
            "reason": "intraday_impossible_no",
            "book_token_id": "new-token",
            "market_id": "m-aus",
            "market": {"id": "m-aus", "market_id": "m-aus"},
        }
        with tempfile.TemporaryDirectory() as temp:
            service = RuntimeService(root=ROOT, data_dir=Path(temp) / "data", sync=False)
            service.last_result = {"summary": {}, "rows": [row, dict(row)]}
            with patch.dict("os.environ", {"LIVE_ORDERS": "true", "MAX_ORDER_USDC": "5"}, clear=False):
                with patch.object(service, "take_opportunity", return_value={"ok": True}) as take:
                    takes = service._auto_take_opportunities(service.last_result)
            self.assertEqual(len(takes), 1)
            take.assert_called_once()
            kwargs = take.call_args.kwargs
            self.assertTrue(kwargs["live"])
            self.assertEqual(kwargs["target_outcome"], "74-75°F")

    def test_sample_set_from_description_detects_hourly_button(self) -> None:
        self.assertEqual(
            sample_set_from_description(
                'This market will resolve off of the Hourly Data provided using the "Show Hourly Data" button.'
            ),
            "hourly",
        )
        self.assertEqual(
            sample_set_from_description(
                "NOAA Temp column for all times on this day, available here: "
                "https://www.weather.gov/wrh/timeseries?site=zhcc"
            ),
            "all",
        )
        self.assertEqual(hourly_window_for_timezone("America/Los_Angeles"), "nws_faa")
        self.assertEqual(hourly_window_for_timezone("Pacific/Honolulu"), "nws_faa")
        self.assertEqual(hourly_window_for_timezone("Asia/Shanghai"), "other")

    def test_discover_ksfo_hourly_vs_zhengzhou_all_data(self) -> None:
        titles_f = [
            "53°F or below",
            "54-55°F",
            "56-57°F",
            "58-59°F",
            "60°F or higher",
        ]
        titles_c = [
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
        ksfo = self._temp_markets(
            slug="lowest-temperature-in-san-francisco-on-september-7-2026",
            source="https://www.weather.gov/wrh/timeseries?site=ksfo",
            titles=titles_f,
            metric_word="lowest",
            description=(
                'This market will resolve off of the Hourly Data provided using the '
                '"Show Hourly Data" button. '
                "https://www.weather.gov/wrh/timeseries?site=ksfo"
            ),
        )
        ksfo_rule = parse_rule(discover_rules(ksfo)["generated_rules"][0])
        self.assertEqual(ksfo_rule.source["sample_set"], "hourly")
        self.assertEqual(ksfo_rule.source["hourly_window"], "nws_faa")
        self.assertEqual(ksfo_rule.source["station_id"], "KSFO")
        self.assertEqual(
            ksfo_rule.source["url"],
            "https://api.synopticdata.com/v2/stations/timeseries",
        )
        self.assertEqual(
            ksfo_rule.source["resolution_source"],
            "https://www.weather.gov/wrh/timeseries?site=ksfo",
        )
        self.assertEqual(
            wrh_hourly_page_url("https://www.weather.gov/wrh/timeseries?site=ksfo"),
            "https://www.weather.gov/wrh/timeseries?site=ksfo&hourly=true",
        )
        zhcc = self._temp_markets(
            slug="lowest-temperature-in-zhengzhou-on-september-7-2026",
            source="https://www.weather.gov/wrh/timeseries?site=zhcc",
            titles=titles_c,
            metric_word="lowest",
            description=(
                "This market will resolve based on the NOAA timeseries page: "
                "https://www.weather.gov/wrh/timeseries?site=zhcc"
            ),
        )
        zhcc_rule = parse_rule(discover_rules(zhcc)["generated_rules"][0])
        self.assertEqual(zhcc_rule.source["sample_set"], "all")
        self.assertNotIn("hourly_window", zhcc_rule.source)
        self.assertEqual(
            zhcc_rule.source["resolution_source"],
            "https://www.weather.gov/wrh/timeseries?site=zhcc",
        )
        self.assertEqual(
            zhcc_rule.source["url"],
            "https://api.synopticdata.com/v2/stations/timeseries",
        )

    def _ksfo_min_features(self) -> list:
        return [
            {
                "value": 57.2,
                "timestamp": "2026-09-07T09:53:00Z",
                "slp": 1013.2,
                "metar": "KSFO 071256Z AUTO 28008KT",
                "network": "ASOS/AWOS",
            },
            {
                "value": 55.4,
                "timestamp": "2026-09-07T12:45:00Z",
                "slp": None,
                "metar": None,
                "network": "ASOS/AWOS",
            },
        ]

    def _ksfo_min_buckets(self) -> list:
        return [
            bucket.to_dict()
            for bucket in buckets_from_outcomes(
                [
                    "53°F or below",
                    "54-55°F",
                    "56-57°F",
                    "58-59°F",
                    "60°F or higher",
                ]
            )
        ]

    def _ksfo_min_rule(self, sample_set: str, features: list, hourly_window: str = "nws_faa"):
        source = {
            "static": {"features": features},
            "url": "fixture://weather/ksfo",
            "resolution_source": "https://www.weather.gov/wrh/timeseries?site=ksfo",
            "value_path": "value",
            "timestamp_path": "timestamp",
            "station_id": "KSFO",
            "provider": "NOAA",
            "sample_set": sample_set,
            "value_unit": "F",
        }
        if sample_set == "hourly":
            source["hourly_window"] = hourly_window
        return parse_rule(
            {
                "market_id": "ksfo-56-57",
                "event_group_id": "lowest-temperature-in-san-francisco-on-september-7-2026",
                "timezone": "America/Los_Angeles",
                "rounding": "whole_degree_as_published",
                "source": source,
                "metric": "daily_min",
                "observation_start": "2026-09-07T00:00:00",
                "observation_end": "2026-09-07T23:59:59",
                "unit": "F",
                "buckets": self._ksfo_min_buckets(),
                "target_outcome": "56-57°F",
                "manual_approval": True,
            }
        )

    def test_hourly_min_ignores_ksfo_0545_all_data_includes_it(self) -> None:
        now = datetime(2026, 9, 7, 18, tzinfo=timezone.utc)
        hourly = WeatherSourceAdapter().poll(
            self._ksfo_min_rule("hourly", self._ksfo_min_features()),
            now=now,
        )
        self.assertEqual(hourly.status, "intraday")
        self.assertEqual(hourly.raw_aggregate, 57.2)
        self.assertEqual(hourly.value, 57)
        by_local = {point["local_time"]: point for point in hourly.series}
        self.assertTrue(by_local["2026-09-07 02:53"]["counts_for_resolution"])
        self.assertNotIn("2026-09-07 05:45", by_local)

        all_data = WeatherSourceAdapter().poll(
            self._ksfo_min_rule("all", self._ksfo_min_features()),
            now=now,
        )
        self.assertEqual(all_data.status, "intraday")
        self.assertEqual(all_data.raw_aggregate, 55.4)
        self.assertEqual(all_data.value, 55)
        self.assertTrue(all(point["counts_for_resolution"] for point in all_data.series))

    def test_hourly_filter_empty_does_not_fall_back_to_all_data(self) -> None:
        observation = WeatherSourceAdapter().poll(
            self._ksfo_min_rule(
                "hourly",
                [{"value": 55.4, "timestamp": "2026-09-07T12:45:00Z"}],
            ),
            now=datetime(2026, 9, 7, 18, tzinfo=timezone.utc),
        )
        self.assertEqual(observation.status, "unavailable")
        self.assertEqual(observation.reason, "hourly_filter_empty")
        self.assertIsNone(observation.value)
        self.assertEqual(observation.series, [])

    def test_hourly_ksfo_still_requests_same_synoptic_url(self) -> None:
        markets = self._temp_markets(
            slug="lowest-temperature-in-san-francisco-on-september-7-2026",
            source="https://www.weather.gov/wrh/timeseries?site=ksfo",
            titles=[
                "53°F or below",
                "54-55°F",
                "56-57°F",
                "58-59°F",
                "60°F or higher",
            ],
            metric_word="lowest",
            description=(
                'This market will resolve off of the Hourly Data provided using the '
                '"Show Hourly Data" button.'
            ),
        )
        rule = parse_rule(discover_rules(markets)["generated_rules"][0])
        self.assertEqual(rule.source["url"], "https://api.synopticdata.com/v2/stations/timeseries")
        self.assertEqual(
            rule.source["resolution_source"],
            "https://www.weather.gov/wrh/timeseries?site=ksfo",
        )
        self.assertEqual(rule.source.get("sample_set"), "hourly")

        class Recorder:
            def __init__(self) -> None:
                self.url = None
                self.params = None
                self.headers = None

            def get_json(self, url, params=None, headers=None):
                self.url = url
                self.params = params
                self.headers = headers
                return {
                    "UNITS": {"air_temp": "Fahrenheit"},
                    "STATION": [
                        {
                            "STID": "KSFO",
                            "SHORTNAME": "ASOS/AWOS",
                            "UNITS": {"air_temp": "Fahrenheit"},
                            "OBSERVATIONS": {
                                "date_time": [
                                    "2026-09-07T02:53:00",
                                    "2026-09-07T05:45:00",
                                ],
                                "air_temp_set_1": [57.2, 55.4],
                                "sea_level_pressure_set_1": [1013.2, None],
                                "metar_set_1": ["KSFO 071256Z AUTO", None],
                            },
                        }
                    ],
                }

            def get_text(self, url, headers=None):
                return "var mesoToken='test-token';"

        http = Recorder()
        observation = WeatherSourceAdapter(http=http, http_cache_ttl_s=0).poll(
            rule,
            now=datetime(2026, 9, 7, 18, tzinfo=timezone.utc),
        )
        self.assertEqual(http.url, "https://api.synopticdata.com/v2/stations/timeseries")
        self.assertEqual(http.params["STID"], "KSFO")
        self.assertNotIn("hourly", http.params or {})
        self.assertIn("timeseries?site=ksfo", str(http.headers.get("Referer") or "").lower())
        self.assertEqual(observation.status, "intraday")
        self.assertEqual(observation.value, 57)
        by_local = {point["local_time"]: point for point in observation.series}
        self.assertTrue(by_local["2026-09-07 02:53"]["counts_for_resolution"])
        self.assertNotIn("2026-09-07 05:45", by_local)

    def test_asos_hourly_keeps_speci_not_five_minute_at_53(self) -> None:
        observation = WeatherSourceAdapter().poll(
            self._ksfo_min_rule(
                "hourly",
                [
                    {
                        "value": 57.2,
                        "timestamp": "2026-09-07T09:12:00Z",
                        "slp": None,
                        "metar": "KSFO 071212Z AUTO 28008KT",
                        "network": "ASOS/AWOS",
                    },
                    {
                        "value": 55.4,
                        "timestamp": "2026-09-07T12:53:00Z",
                        "slp": None,
                        "metar": None,
                        "network": "ASOS/AWOS",
                    },
                ],
            ),
            now=datetime(2026, 9, 7, 18, tzinfo=timezone.utc),
        )
        self.assertEqual(observation.status, "intraday")
        self.assertEqual(observation.raw_aggregate, 57.2)
        self.assertEqual(observation.value, 57)
        by_local = {point["local_time"]: point for point in observation.series}
        self.assertTrue(by_local["2026-09-07 02:12"]["counts_for_resolution"])
        self.assertNotIn("2026-09-07 05:53", by_local)

    def test_scanner_hourly_does_not_lock_ksfo_56_57_no(self) -> None:
        outcomes = [
            "53°F or below",
            "54-55°F",
            "56-57°F",
            "58-59°F",
            "60°F or higher",
        ]
        now = datetime(2026, 9, 7, 18, tzinfo=timezone.utc)
        hourly_markets, hourly_rules = self._event_with_static(
            event_id="lowest-temperature-in-san-francisco-on-september-7-2026",
            metric="daily_min",
            outcomes=outcomes,
            buckets=self._ksfo_min_buckets(),
            features=self._ksfo_min_features(),
            extra_source={"sample_set": "hourly", "hourly_window": "nws_faa", "station_id": "KSFO", "value_unit": "F"},
            start="2026-09-07T00:00:00",
            end="2026-09-07T23:59:59",
            timezone_name="America/Los_Angeles",
            unit="F",
        )
        hourly_result = WeatherScanner(config=WeatherScannerConfig()).scan(
            hourly_markets,
            hourly_rules,
            books=self._priced_books(hourly_markets, now, {"56-57°F", "58-59°F"}),
            fetch_books=False,
            now=now,
        )
        hourly_by = self._buy_rows_by_outcome(hourly_result["rows"])
        self.assertEqual((hourly_by["56-57°F"].get("observation") or {}).get("value"), 57)
        self.assertNotEqual(hourly_by["56-57°F"].get("reason"), "intraday_impossible_no")
        self.assertNotEqual(hourly_by["56-57°F"].get("trade_side"), "NO")
        self.assertEqual(hourly_by["58-59°F"]["status"], "opportunity")
        self.assertEqual(hourly_by["58-59°F"]["trade_side"], "NO")
        self.assertEqual(hourly_by["58-59°F"]["reason"], "intraday_impossible_no")

        all_markets, all_rules = self._event_with_static(
            event_id="lowest-temperature-in-san-francisco-all-data",
            metric="daily_min",
            outcomes=outcomes,
            buckets=self._ksfo_min_buckets(),
            features=self._ksfo_min_features(),
            extra_source={"sample_set": "all", "station_id": "KSFO", "value_unit": "F"},
            start="2026-09-07T00:00:00",
            end="2026-09-07T23:59:59",
            timezone_name="America/Los_Angeles",
            unit="F",
        )
        all_result = WeatherScanner(config=WeatherScannerConfig()).scan(
            all_markets,
            all_rules,
            books=self._priced_books(all_markets, now, {"56-57°F", "58-59°F"}),
            fetch_books=False,
            now=now,
        )
        all_by = self._buy_rows_by_outcome(all_result["rows"])
        self.assertEqual((all_by["56-57°F"].get("observation") or {}).get("value"), 55)
        self.assertEqual(all_by["56-57°F"]["status"], "opportunity")
        self.assertEqual(all_by["56-57°F"]["reason"], "intraday_impossible_no")


class SynopticPushAndWrhArchiveTests(unittest.TestCase):
    def test_parse_synoptic_push_date(self) -> None:
        from weather_runtime.synoptic_push import parse_synoptic_push_date

        self.assertEqual(parse_synoptic_push_date(201801132220), "2018-01-13T22:20:00+00:00")
        self.assertIsNone(parse_synoptic_push_date("bad"))

    def test_synoptic_watch_from_rules_collects_active_stations(self) -> None:
        from weather_runtime.models import Bucket, WeatherRule
        from weather_runtime.synoptic_push import synoptic_watch_from_rules

        now = datetime(2026, 9, 8, 12, tzinfo=timezone.utc)
        rules = [
            WeatherRule(
                market_id="m1",
                event_group_id="highest-temperature-in-san-francisco-on-september-8-2026",
                adapter="weather_observation",
                metric="daily_max",
                unit="F",
                timezone="America/Los_Angeles",
                observation_start="2026-09-08T00:00:00",
                observation_end="2026-09-08T23:59:59",
                buckets=[Bucket(outcome="70+", lower=70, lower_inclusive=True)],
                source={
                    "provider": "noaa",
                    "station_id": "KSFO",
                    "url": "https://api.synopticdata.com/v2/stations/timeseries",
                    "sample_set": "hourly",
                },
            ),
            WeatherRule(
                market_id="m2",
                event_group_id="highest-temperature-in-austin-on-september-1-2026",
                adapter="weather_observation",
                metric="daily_max",
                unit="F",
                timezone="America/Chicago",
                observation_start="2026-09-01T00:00:00",
                observation_end="2026-09-01T23:59:59",
                buckets=[Bucket(outcome="90+", lower=90, lower_inclusive=True)],
                source={
                    "provider": "noaa",
                    "station_id": "KAUS",
                    "url": "https://api.synopticdata.com/v2/stations/timeseries",
                },
            ),
        ]
        watches = synoptic_watch_from_rules(rules, now=now)
        self.assertIn("KSFO", watches)
        self.assertNotIn("KAUS", watches)
        self.assertEqual(
            watches["KSFO"]["event_group_ids"],
            ["highest-temperature-in-san-francisco-on-september-8-2026"],
        )

    def test_push_recorder_writes_data_rows_with_received_at(self) -> None:
        from weather_runtime.storage import read_jsonl
        from weather_runtime.synoptic_push import SynopticPushRecorder

        with tempfile.TemporaryDirectory() as temp:
            recorder = SynopticPushRecorder(
                Path(temp),
                token_fn=lambda: "token",
                enabled=False,
            )
            watches = {
                "C4824": {
                    "station_id": "C4824",
                    "event_group_ids": ["evt-1"],
                    "sample_sets": ["all"],
                }
            }
            recorder.set_watch(watches)
            recorder._handle_message(
                json.dumps(
                    {
                        "type": "metadata",
                        "units": [{"sensor": "air_temp", "unit": "Fahrenheit"}],
                        "stations": [{"stid": "C4824"}],
                    }
                ),
                watches,
                track_session=True,
            )
            recorder._handle_message(
                json.dumps(
                    {
                        "type": "data",
                        "data": [
                            {
                                "set": 1,
                                "stid": "C4824",
                                "value": 72.5,
                                "qc": [],
                                "date": 202609081200,
                                "sensor": "air_temp",
                            }
                        ],
                    }
                ),
                watches,
                track_session=True,
            )
            rows = read_jsonl(Path(temp) / "synoptic_push.jsonl")
            data_rows = [row for row in rows if row.get("msg_type") == "data"]
            self.assertEqual(len(data_rows), 1)
            self.assertEqual(data_rows[0]["station_id"], "C4824")
            self.assertAlmostEqual(data_rows[0]["value"], (72.5 - 32.0) * 5.0 / 9.0, places=4)
            self.assertEqual(data_rows[0]["unit"], "C")
            self.assertEqual(data_rows[0]["value_raw"], 72.5)
            self.assertEqual(data_rows[0]["unit_raw"], "Fahrenheit")
            self.assertEqual(data_rows[0]["obs_timestamp"], "2026-09-08T12:00:00+00:00")
            self.assertTrue(data_rows[0].get("received_at"))
            self.assertEqual(data_rows[0]["event_group_ids"], ["evt-1"])

    def test_normalize_push_air_temp_to_celsius(self) -> None:
        from weather_runtime.synoptic_push import normalize_push_value

        value, unit, raw, unit_raw = normalize_push_value("air_temp", 32.0, "Fahrenheit")
        self.assertEqual(value, 0.0)
        self.assertEqual(unit, "C")
        self.assertEqual(raw, 32.0)
        self.assertEqual(unit_raw, "Fahrenheit")

    def test_wrh_points_keep_first_seen_timestamp(self) -> None:
        from weather_runtime.observation_archive import persist_wrh_observation_points
        from weather_runtime.storage import read_jsonl

        evidence = [
            {
                "event_group_id": "evt-1",
                "station_id": "KSFO",
                "provider": "noaa",
                "sample_set": "hourly",
                "evidence_hash": "abc",
                "source_timestamp": "2026-09-08T12:00:00+00:00",
                "series": [
                    {
                        "timestamp": "2026-09-08T19:00:00+00:00",
                        "local_time": "2026-09-08 12:00",
                        "timezone": "America/Los_Angeles",
                        "temp": 72.0,
                        "counts_for_resolution": True,
                    }
                ],
            }
        ]
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "wrh_observation_points.jsonl"
            first = persist_wrh_observation_points(path, evidence, polled_at="2026-09-08T12:05:00+00:00")
            second = persist_wrh_observation_points(path, evidence, polled_at="2026-09-08T12:10:00+00:00")
            self.assertEqual(first, 1)
            self.assertEqual(second, 0)
            rows = read_jsonl(path)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["first_seen_at"], "2026-09-08T12:05:00+00:00")
            self.assertEqual(rows[0]["obs_timestamp"], "2026-09-08T19:00:00+00:00")


if __name__ == "__main__":
    unittest.main()

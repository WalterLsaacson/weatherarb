from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

from weather_runtime.models import Book
from weather_runtime.rules import load_rules, normalize_market
from weather_runtime.scanner import (
    SWEEP_TAIL_LOSER_NO,
    SWEEP_TAIL_WINNER_YES,
    WeatherScanner,
    WeatherScannerConfig,
)
from weather_runtime.service import RuntimeService
from weather_runtime.storage import read_jsonl, write_json_atomic


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "fixtures"
SCAN_NOW = datetime(2026, 9, 5, 1, tzinfo=timezone.utc)


def _market_payloads(
    *,
    leader_yes: float = 0.995,
    loser_yes: float = 0.005,
    missing_prices: bool = False,
    duplicate_leader: bool = False,
) -> list[dict[str, Any]]:
    payload = json.loads((FIXTURES / "markets.json").read_text(encoding="utf-8"))
    for market in payload["markets"]:
        if missing_prices and market["id"] == "wx-london-23":
            market.pop("outcomePrices", None)
            continue
        is_leader = market["outcome"] == "26" or (
            duplicate_leader and market["outcome"] == "25"
        )
        yes = leader_yes if is_leader else loser_yes
        market["outcomePrices"] = [str(yes), str(1.0 - yes)]
    return payload["markets"]


def _sweep_tail_group_payloads(
    markets: list[dict[str, Any]], rules: list[Any]
) -> tuple[list[Any], dict[str, Any], list[Any]]:
    normalized = [normalize_market(row) for row in markets]
    market_map = {market.market_id: market for market in normalized}
    group_markets = list(normalized)
    return rules, market_map, group_markets


def _books(ask: float = 0.995) -> dict[str, Book]:
    payload = json.loads((FIXTURES / "books.json").read_text(encoding="utf-8"))
    books: dict[str, Book] = {}
    for token_id, raw in payload.items():
        books[token_id] = Book(
            token_id=token_id,
            best_ask=ask,
            best_bid=0.001,
            asks=[{"price": ask, "size": 50}],
            bids=[{"price": 0.001, "size": 10}],
            tick_size=0.001,
            min_order_size=1,
            fetched_at=SCAN_NOW.isoformat(),
        )
    return books


def _scanner() -> WeatherScanner:
    return WeatherScanner(
        config=WeatherScannerConfig(
            sweep_tail_enabled=True,
            sweep_tail_order_usdc=5.0,
            max_ask=0.995,
            min_net_edge=0.0075,
            target_shares=100.0,
        )
    )


class SweepTailTests(unittest.TestCase):
    def test_outcome_prices_string_is_parsed(self) -> None:
        market = normalize_market(_market_payloads()[0])
        self.assertEqual(market.yes_probability, 0.005)
        self.assertEqual(market.no_probability, 0.995)

    def test_probability_payload_must_have_exactly_two_valid_prices(self) -> None:
        payload = dict(_market_payloads()[0])
        for value in (["0.5"], ["0.5", "0.4", "0.1"], "0.5", []):
            payload["outcomePrices"] = value
            market = normalize_market(payload)
            self.assertIsNone(market.yes_probability)
            self.assertIsNone(market.no_probability)

    def test_unique_99_1_group_queues_leader_yes_and_loser_no(self) -> None:
        scanner = _scanner()
        rules = load_rules(FIXTURES / "rules.json")
        markets = [normalize_market(row) for row in _market_payloads()]
        result = scanner.scan(
            markets,
            rules,
            books=_books(),
            now=SCAN_NOW,
            fetch_books=False,
        )
        rows = result["rows"]
        leader = [row for row in rows if row["reason"] == SWEEP_TAIL_WINNER_YES]
        losers = [row for row in rows if row["reason"] == SWEEP_TAIL_LOSER_NO]
        self.assertEqual(len(leader), 1)
        self.assertEqual(len(losers), 10)
        self.assertEqual(leader[0]["trade_side"], "YES")
        self.assertEqual({row["trade_side"] for row in losers}, {"NO"})
        self.assertTrue(all(row["status"] == "opportunity" for row in rows))
        self.assertEqual(result["summary"]["sweep_tail_opportunities"], 11)
        for row in rows:
            self.assertLessEqual(row["economics"]["best_ask"], 0.995)
            self.assertIsNotNone(row["economics"]["best_bid"])

    def test_ask_above_limit_does_not_become_an_order_opportunity(self) -> None:
        result = _scanner().scan(
            [normalize_market(row) for row in _market_payloads()],
            load_rules(FIXTURES / "rules.json"),
            books=_books(ask=0.996),
            now=SCAN_NOW,
            fetch_books=False,
        )
        self.assertEqual(result["summary"]["sweep_tail_opportunities"], 0)
        self.assertTrue(all(row["status"] == "no_trade" for row in result["rows"]))
        self.assertTrue(all(row["reason"] == "ask_above_limit" for row in result["rows"]))

    def test_group_rejects_duplicate_leader_high_loser_and_missing_probability(self) -> None:
        scanner = _scanner()
        rules = load_rules(FIXTURES / "rules.json")
        variants = (
            _market_payloads(duplicate_leader=True),
            _market_payloads(loser_yes=0.02),
            _market_payloads(missing_prices=True),
        )
        for markets in variants:
            _, market_map, group_markets = _sweep_tail_group_payloads(markets, rules)
            self.assertIsNone(scanner._sweep_tail_group(rules, market_map, group_markets))

        markets = _market_payloads()
        _, market_map, group_markets = _sweep_tail_group_payloads(markets, rules)
        self.assertIsNone(scanner._sweep_tail_group(rules[:2], market_map, group_markets))

    def test_dry_run_persists_opportunity_and_resolution_records(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Path(temp) / "fixture"
            data_dir = Path(temp) / "data"
            shutil.copytree(FIXTURES, fixture)
            payload = json.loads((fixture / "markets.json").read_text(encoding="utf-8"))
            for market in payload["markets"]:
                yes = 0.995 if market["outcome"] == "26" else 0.005
                market["outcomePrices"] = [str(yes), str(1.0 - yes)]
            write_json_atomic(fixture / "markets.json", payload)
            books = json.loads((fixture / "books.json").read_text(encoding="utf-8"))
            for book in books.values():
                book["asks"] = [{"price": 0.995, "size": 50}]
            write_json_atomic(fixture / "books.json", books)

            service = RuntimeService(
                root=fixture,
                data_dir=data_dir,
                fixture=fixture,
                sync=False,
                scanner_config=WeatherScannerConfig(
                    sweep_tail_enabled=True,
                    sweep_tail_order_usdc=5.0,
                    max_ask=0.995,
                    min_net_edge=0.0075,
                    target_shares=100.0,
                ),
            )
            result = service.scan_once()
            self.assertEqual(result["summary"]["sweep_tail_opportunities"], 11)

            records = read_jsonl(data_dir / "sweep_tail_opportunities.jsonl")
            opportunities = [
                row for row in records if row.get("record_type") == "opportunity"
            ]
            resolutions = [
                row for row in records if row.get("record_type") == "resolution"
            ]
            self.assertEqual(len(opportunities), 1)
            self.assertEqual(len(resolutions), 1)
            self.assertEqual(opportunities[0]["leader_outcome"], "26")
            self.assertEqual(opportunities[0]["leg_count"], 11)
            self.assertEqual(len(opportunities[0]["intended_orders"]), 11)
            self.assertTrue(opportunities[0]["dry_run"])
            self.assertEqual(resolutions[0]["final_bucket_outcome"], "26")
            self.assertFalse((data_dir / "orders.jsonl").exists())

    def test_live_sweep_tail_does_not_require_locked_no(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Path(temp) / "fixture"
            data_dir = Path(temp) / "data"
            shutil.copytree(FIXTURES, fixture)
            service = RuntimeService(
                root=fixture,
                data_dir=data_dir,
                fixture=None,
                sync=False,
                scanner_config=WeatherScannerConfig(sweep_tail_enabled=True),
            )
            row = {
                "status": "opportunity",
                "order_side": "BUY",
                "book_token_id": "sweep-token",
                "event_group_id": "sweep-event",
                "target_outcome": "26",
                "trade_side": "NO",
                "reason": SWEEP_TAIL_LOSER_NO,
            }
            with patch.dict(
                os.environ,
                {"LIVE_ORDERS": "true", "SWEEP_TAIL_ORDERS": "false"},
            ):
                with patch.object(service, "take_opportunity") as take:
                    self.assertEqual(service._auto_take_opportunities({"rows": [row]}), [])
                    take.assert_not_called()

            with patch.dict(
                os.environ,
                {"LIVE_ORDERS": "true", "SWEEP_TAIL_ORDERS": "true"},
            ):
                with patch.object(service, "take_opportunity", return_value={"ok": True}) as take:
                    self.assertEqual(
                        service._auto_take_opportunities({"rows": [row]}), [{"ok": True}]
                    )
                    take.assert_called_once_with(
                        event_group_id="sweep-event",
                        target_outcome="26",
                        live=True,
                        require_locked_no=False,
                        token_ledger="sweep_tail",
                    )

    def test_manual_take_infers_sweep_tail_ledger_and_skips_lock(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = Path(temp) / "fixture"
            data_dir = Path(temp) / "data"
            shutil.copytree(FIXTURES, fixture)
            service = RuntimeService(
                root=fixture,
                data_dir=data_dir,
                fixture=None,
                sync=False,
                scanner_config=WeatherScannerConfig(sweep_tail_enabled=True),
            )
            service.last_result["rows"] = [
                {
                    "status": "opportunity",
                    "order_side": "BUY",
                    "book_token_id": "sweep-token",
                    "event_group_id": "sweep-event",
                    "target_outcome": "26",
                    "trade_side": "NO",
                    "reason": SWEEP_TAIL_LOSER_NO,
                }
            ]

            class FakeClob:
                def fetch_books(self, token_ids):
                    return {}

            service.scanner.clob_client = FakeClob()
            result = service.take_opportunity(
                event_group_id="sweep-event",
                target_outcome="26",
                live=False,
                require_locked_no=True,
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["error"], "market_not_found")


if __name__ == "__main__":
    unittest.main()

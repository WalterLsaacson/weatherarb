from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from weather_runtime.dynamic_exit import DynamicExitManager


def _config() -> dict:
    return {
        "live_orders": True,
        "dynamic_exit": True,
        "exit_price_001": 0.999,
        "exit_min_size": 5,
        "exit_interval_s": 0,
        "exit_max_orders_per_cycle": 10,
        "exit_unavailable_retry_s": 3600,
    }


def _position(size: str = "10") -> dict:
    return {
        "asset_id": "weather-token",
        "condition_id": "condition",
        "title": "Will the highest temperature in New York be 20°C?",
        "slug": "weather/temperature-in-new-york",
        "current_size": size,
    }


class DynamicExitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temp.name)
        self.manager = DynamicExitManager(data_dir=self.data_dir, config=_config())

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_throttles_dynamic_exit_runs(self) -> None:
        manager = DynamicExitManager(
            data_dir=self.data_dir,
            config={**_config(), "exit_interval_s": 1800},
        )

        with patch(
            "weather_runtime.dynamic_exit.fetch_positions", return_value=[]
        ) as positions, patch(
            "weather_runtime.dynamic_exit.fetch_open_orders", return_value=[]
        ):
            first = manager.run_once()
            second = manager.run_once()

        self.assertEqual(first, [])
        self.assertEqual(second, [])
        self.assertEqual(positions.call_count, 1)
        self.assertNotEqual(manager.snapshot()["next_run_at"], "")

    def test_places_999_sell_and_reserves_all_open_sells(self) -> None:
        existing = {
            "id": "existing-sell",
            "asset_id": "weather-token",
            "side": "SELL",
            "original_size": "3",
            "size_matched": "1",
        }
        submitted = {"id": "new-sell", "status": "LIVE"}
        open_orders = [existing]

        with patch(
            "weather_runtime.dynamic_exit.fetch_positions", return_value=[_position()]
        ), patch(
            "weather_runtime.dynamic_exit.fetch_open_orders", return_value=open_orders
        ), patch(
            "weather_runtime.dynamic_exit.fetch_order_book",
            return_value={"tick_size": "0.001", "min_order_size": "5"},
        ), patch(
            "weather_runtime.dynamic_exit.submit_gtc_sell", return_value=submitted
        ) as sell:
            first = self.manager.run_once()
            open_orders.append(
                {
                    "id": "new-sell",
                    "asset_id": "weather-token",
                    "side": "SELL",
                    "original_size": "8",
                    "size_matched": "0",
                }
            )
            second = self.manager.run_once()

        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])
        self.assertEqual(sell.call_count, 1)
        self.assertEqual(sell.call_args.kwargs["price"], 0.999)
        self.assertEqual(sell.call_args.kwargs["size"], 8.0)

    def test_01_tick_book_is_skipped(self) -> None:
        book = {"tick_size": "0.01", "min_order_size": "5"}

        with patch(
            "weather_runtime.dynamic_exit.fetch_positions", return_value=[_position("5")]
        ), patch(
            "weather_runtime.dynamic_exit.fetch_open_orders", return_value=[]
        ), patch(
            "weather_runtime.dynamic_exit.fetch_order_book", return_value=book
        ), patch(
            "weather_runtime.dynamic_exit.submit_gtc_sell"
        ) as sell:
            submitted = self.manager.run_once()

        self.assertEqual(submitted, [])
        sell.assert_not_called()

    def test_cancels_existing_below_floor_sells(self) -> None:
        self.manager.state["orders"] = {
            "old-sell": {
                "order_id": "old-sell",
                "token_id": "weather-token",
                "price": "0.99",
                "original_size": "5",
                "size_matched": "0",
                "status": "LIVE",
            }
        }
        open_order = {
            "id": "old-sell",
            "asset_id": "weather-token",
            "side": "SELL",
            "status": "LIVE",
            "price": "0.99",
            "original_size": "5",
            "size_matched": "0",
        }

        with patch(
            "weather_runtime.dynamic_exit.fetch_positions", return_value=[]
        ), patch(
            "weather_runtime.dynamic_exit.fetch_open_orders", return_value=[open_order]
        ), patch(
            "weather_runtime.dynamic_exit.cancel_order", return_value={"ok": True}
        ) as cancel:
            self.manager.run_once()

        cancel.assert_called_once_with(order_id="old-sell", config=self.manager.config)
        record = self.manager.state["orders"]["old-sell"]
        self.assertEqual(record["status"], "CANCELLED")
        self.assertTrue(record["closed_logged"])
        self.assertEqual(self.manager.snapshot()["open_exchange_sell_orders"], 0)

    def test_ignores_non_weather_positions(self) -> None:
        position = _position()
        position["title"] = "Will an unrelated market resolve Yes?"
        position["slug"] = "unrelated-market"

        with patch(
            "weather_runtime.dynamic_exit.fetch_positions", return_value=[position]
        ), patch(
            "weather_runtime.dynamic_exit.fetch_open_orders", return_value=[]
        ), patch(
            "weather_runtime.dynamic_exit.fetch_order_book"
        ) as book, patch(
            "weather_runtime.dynamic_exit.submit_gtc_sell"
        ) as sell:
            submitted = self.manager.run_once()

        self.assertEqual(submitted, [])
        book.assert_not_called()
        sell.assert_not_called()

    def test_adopts_existing_exchange_sells_after_restart(self) -> None:
        existing = {
            "id": "existing-sell",
            "asset_id": "weather-token",
            "side": "SELL",
            "status": "LIVE",
            "price": "0.999",
            "original_size": "5",
            "size_matched": "1",
        }

        with patch(
            "weather_runtime.dynamic_exit.fetch_positions", return_value=[_position("5")]
        ), patch(
            "weather_runtime.dynamic_exit.fetch_open_orders", return_value=[existing]
        ), patch(
            "weather_runtime.dynamic_exit.submit_gtc_sell"
        ) as sell:
            submitted = self.manager.run_once()

        self.assertEqual(submitted, [])
        sell.assert_not_called()
        snapshot = self.manager.snapshot()
        self.assertEqual(snapshot["open_exit_orders"], 1)
        self.assertEqual(snapshot["open_exchange_sell_orders"], 1)
        self.assertTrue(snapshot["exit_orders"][0]["adopted"])

    def test_caches_tokens_without_order_books(self) -> None:
        from weather_runtime.orders import LiveOrderError

        with patch(
            "weather_runtime.dynamic_exit.fetch_positions", return_value=[_position()]
        ), patch(
            "weather_runtime.dynamic_exit.fetch_open_orders", return_value=[]
        ), patch(
            "weather_runtime.dynamic_exit.fetch_order_book",
            side_effect=LiveOrderError("No orderbook exists for the requested token id"),
        ) as book:
            self.manager.run_once()
            self.manager.run_once()

        self.assertEqual(book.call_count, 1)
        self.assertEqual(self.manager.snapshot()["unavailable_tokens"], 1)

    def test_submit_match_waits_for_exchange_reconciliation(self) -> None:
        submitted = {"id": "new-sell", "status": "matched"}

        with patch(
            "weather_runtime.dynamic_exit.fetch_positions", return_value=[_position()]
        ), patch(
            "weather_runtime.dynamic_exit.fetch_open_orders", return_value=[]
        ), patch(
            "weather_runtime.dynamic_exit.fetch_order_book",
            return_value={"tick_size": "0.001", "min_order_size": "5"},
        ), patch(
            "weather_runtime.dynamic_exit.submit_gtc_sell", return_value=submitted
        ):
            self.manager.run_once()

        record = self.manager.state["orders"]["new-sell"]
        self.assertEqual(record["status"], "SUBMITTED")
        self.assertEqual(record["submit_status"], "MATCHED")

    def test_reconciles_full_fill_from_account_trades_once(self) -> None:
        self.manager.state["orders"] = {
            "exit": {
                "order_id": "exit",
                "token_id": "weather-token",
                "original_size": "5",
                "size_matched": "0",
                "status": "LIVE",
            }
        }
        trade = {
            "maker_orders": [
                {"order_id": "exit", "matched_amount": "5"},
            ]
        }

        with patch(
            "weather_runtime.dynamic_exit.fetch_positions", return_value=[]
        ), patch(
            "weather_runtime.dynamic_exit.fetch_open_orders", return_value=[]
        ), patch(
            "weather_runtime.dynamic_exit.fetch_account_trades", return_value=[trade]
        ) as trades:
            self.manager.run_once()
            self.manager.run_once()

        record = self.manager.state["orders"]["exit"]
        self.assertEqual(record["status"], "MATCHED")
        self.assertEqual(record["size_matched"], "5")
        self.assertEqual(trades.call_count, 1)
        events = [
            json.loads(line)
            for line in (self.data_dir / "exit_orders.jsonl").read_text().splitlines()
        ]
        self.assertEqual(
            [event["event"] for event in events],
            ["EXIT_ORDER_CLOSED"],
        )

    def test_logs_each_new_partial_fill(self) -> None:
        self.manager.state["orders"] = {
            "exit": {
                "order_id": "exit",
                "token_id": "weather-token",
                "original_size": "10",
                "size_matched": "0",
                "status": "LIVE",
            }
        }
        open_order = {
            "id": "exit",
            "asset_id": "weather-token",
            "side": "SELL",
            "status": "LIVE",
            "original_size": "10",
            "size_matched": "3",
        }

        with patch(
            "weather_runtime.dynamic_exit.fetch_positions", return_value=[]
        ), patch(
            "weather_runtime.dynamic_exit.fetch_open_orders", return_value=[open_order]
        ):
            self.manager.run_once()
            open_order["size_matched"] = "6"
            self.manager.run_once()

        events = [
            json.loads(line)
            for line in (self.data_dir / "exit_orders.jsonl").read_text().splitlines()
        ]
        self.assertEqual(
            [event["event"] for event in events],
            ["EXIT_PARTIAL_FILL", "EXIT_PARTIAL_FILL"],
        )
        self.assertEqual(self.manager.state["orders"]["exit"]["size_matched"], "6")


if __name__ == "__main__":
    unittest.main()

"""Dynamic inventory exits for weather positions.

The manager treats the exchange as the source of truth. Every cycle it compares
open weather positions with resting sell orders, then places a GTC sell for any
unreserved shares. A GTC sell accepts partial fills and never sells below its
limit price.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Callable, Optional

from .orders import (
    LiveOrderError,
    cancel_order,
    fetch_account_trades,
    fetch_open_orders,
    fetch_order_book,
    fetch_positions,
    jsonable,
    submit_gtc_sell,
)
from .storage import append_jsonl, load_json, write_json_atomic


_TERMINAL_ORDER_STATUSES = {"MATCHED", "FILLED", "CANCELLED", "CANCELED", "EXPIRED", "CLOSED"}
_MISSING_ORDER_CLOSE_LIMIT = 2
_UNAVAILABLE_ORDER_ERRORS = ("no orderbook exists", "invalid token id")


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _number(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _order_id(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    for key in ("id", "order_id", "orderID"):
        found = value.get(key)
        if found:
            return str(found)
    nested = value.get("order")
    if isinstance(nested, dict):
        return _order_id(nested)
    return ""


def _is_weather_position(position: dict[str, Any]) -> bool:
    slug = str(position.get("slug") or "").lower()
    title = str(position.get("title") or "").lower()
    return "temperature-in-" in slug or ("will the" in title and "temperature" in title)


def _remaining_size(order: dict[str, Any]) -> Decimal:
    original = _decimal(order.get("original_size") or order.get("size"))
    matched = _decimal(order.get("size_matched"))
    return max(Decimal("0"), original - matched)


class DynamicExitManager:
    """Place and monitor GTC sells for unreserved weather inventory."""

    def __init__(
        self,
        *,
        data_dir: Path,
        config: dict[str, Any],
        clock: Callable[[], float] = time.monotonic,
    ):
        self.data_dir = Path(data_dir).resolve()
        self.config = dict(config or {})
        self.clock = clock
        self.state_path = self.data_dir / "dynamic_exit.json"
        self.log_path = self.data_dir / "exit_orders.jsonl"
        self.state = self._load_state()
        self.last_run_at = str(self.state.get("last_run_at") or "")
        self.last_error = ""

    @property
    def enabled(self) -> bool:
        return bool(self.config.get("dynamic_exit")) and bool(self.config.get("live_orders"))

    def _load_state(self) -> dict[str, Any]:
        state = load_json(self.state_path, default={})
        if not isinstance(state, dict):
            state = {}
        state.setdefault("orders", {})
        state.setdefault("created_at", datetime.now(timezone.utc).isoformat())
        return state

    def _save_state(self) -> None:
        state = dict(self.state)
        state["updated_at"] = datetime.now(timezone.utc).isoformat()
        write_json_atomic(self.state_path, state)

    def _log(self, event: str, payload: dict[str, Any]) -> None:
        record = dict(payload)
        record["event"] = event
        record["at"] = datetime.now(timezone.utc).isoformat()
        append_jsonl(self.log_path, [record])

    def snapshot(self) -> dict[str, Any]:
        orders = list((self.state.get("orders") or {}).values())
        active = [order for order in orders if str(order.get("status") or "") not in _TERMINAL_ORDER_STATUSES]
        exchange_orders = list(self.state.get("exchange_open_sell_orders") or [])
        return {
            "enabled": self.enabled,
            "exit_price_001": _number(self.config.get("exit_price_001")),
            "exit_min_size": _number(self.config.get("exit_min_size")),
            "exit_interval_s": self._interval_s(),
            "next_run_at": self._next_run_at(),
            "unavailable_tokens": len(self.state.get("unavailable_tokens") or {}),
            "open_exit_orders": len(active),
            "open_exchange_sell_orders": len(exchange_orders),
            "exit_orders": orders,
            "exchange_sell_orders": exchange_orders,
            "last_run_at": self.last_run_at,
            "last_error": self.last_error,
        }

    def _interval_s(self) -> float:
        raw = self.config.get("exit_interval_s")
        if raw is None or str(raw).strip() == "":
            return 1800.0
        interval = _number(raw)
        return interval if interval >= 0 else 1800.0

    def _last_run_datetime(self) -> Optional[datetime]:
        if not self.last_run_at:
            return None
        try:
            parsed = datetime.fromisoformat(self.last_run_at)
        except ValueError:
            return None
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed

    def _next_run_at(self) -> str:
        last_run = self._last_run_datetime()
        interval = self._interval_s()
        if last_run is None or interval <= 0:
            return ""
        return (last_run + timedelta(seconds=interval)).isoformat()

    def _due(self) -> bool:
        last_run = self._last_run_datetime()
        interval = self._interval_s()
        if last_run is None or interval <= 0:
            return True
        return datetime.now(timezone.utc) >= last_run + timedelta(seconds=interval)

    def _exit_price(self, book: dict[str, Any]) -> Optional[float]:
        tick = _number(book.get("tick_size")) or 0.01
        if abs(tick - 0.001) <= 1e-9:
            return _number(self.config.get("exit_price_001")) or 0.999
        return None

    def _min_size(self, book: dict[str, Any]) -> Decimal:
        configured = _decimal(self.config.get("exit_min_size") or "5")
        exchange = _decimal(book.get("min_order_size") or "5")
        return max(Decimal("0"), configured, exchange)

    def _is_unavailable(self, token_id: str) -> bool:
        unavailable = dict(self.state.get("unavailable_tokens") or {})
        marked_at = unavailable.get(token_id)
        if not marked_at:
            return False
        try:
            marked = datetime.fromisoformat(str(marked_at))
        except ValueError:
            unavailable.pop(token_id, None)
            self.state["unavailable_tokens"] = unavailable
            return False
        retry_s = max(60.0, _number(self.config.get("exit_unavailable_retry_s")) or 3600.0)
        if datetime.now(timezone.utc) >= marked + timedelta(seconds=retry_s):
            unavailable.pop(token_id, None)
            self.state["unavailable_tokens"] = unavailable
            return False
        return True

    def _mark_unavailable(self, token_id: str) -> None:
        unavailable = dict(self.state.get("unavailable_tokens") or {})
        unavailable[token_id] = datetime.now(timezone.utc).isoformat()
        self.state["unavailable_tokens"] = unavailable

    def _clear_unavailable(self, token_id: str) -> None:
        unavailable = dict(self.state.get("unavailable_tokens") or {})
        if token_id in unavailable:
            unavailable.pop(token_id, None)
            self.state["unavailable_tokens"] = unavailable

    def _sync_exchange_orders(self, open_orders: list[dict[str, Any]]) -> None:
        self.state["exchange_open_sell_orders"] = [
            {
                "order_id": _order_id(order),
                "token_id": str(order.get("asset_id") or order.get("token_id") or ""),
                "price": str(order.get("price") or ""),
                "remaining_size": str(_remaining_size(order)),
            }
            for order in open_orders
            if str(order.get("side") or "").upper() == "SELL"
        ]

    def _cancel_below_floor(self, open_orders: list[dict[str, Any]]) -> None:
        open_by_id = {_order_id(order): order for order in open_orders if _order_id(order)}
        floor = _decimal(self.config.get("exit_price_001") or "0.999")
        for order_id, record in list((self.state.get("orders") or {}).items()):
            if record.get("closed_logged") or order_id not in open_by_id:
                continue
            price = _decimal(record.get("price"))
            if price <= 0 or price >= floor:
                continue
            try:
                cancel_order(order_id=order_id, config=self.config)
            except LiveOrderError as exc:
                record["last_cancel_error"] = str(exc)
                self._log(
                    "EXIT_CANCEL_ERROR",
                    {
                        "order_id": order_id,
                        "token_id": str(record.get("token_id") or ""),
                        "error": str(exc),
                    },
                )
                continue
            record.pop("last_cancel_error", None)
            record["status"] = "CANCELLED"
            record["closed_logged"] = True
            self._log("EXIT_ORDER_CANCELLED", record)
            open_orders[:] = [
                order for order in open_orders if _order_id(order) != order_id
            ]
            self._sync_exchange_orders(open_orders)

    def _update_orders(self, open_orders: list[dict[str, Any]]) -> None:
        open_by_id = {_order_id(order): order for order in open_orders if _order_id(order)}
        orders = self.state.setdefault("orders", {})
        for order_id, record in list(orders.items()):
            if record.get("closed_logged"):
                continue
            current = open_by_id.get(order_id)
            if current is None:
                record["miss_count"] = int(record.get("miss_count") or 0) + 1
                if record["miss_count"] < _MISSING_ORDER_CLOSE_LIMIT:
                    record["status"] = "PENDING_OPEN"
                    continue
                try:
                    trades = fetch_account_trades(
                        token_id=str(record.get("token_id") or ""),
                        config=self.config,
                    )
                except LiveOrderError as exc:
                    record["status"] = "PENDING_CLOSE"
                    record["last_close_error"] = str(exc)
                    continue
                record.pop("last_close_error", None)
                matched = Decimal("0")
                for trade in trades:
                    for maker_order in trade.get("maker_orders") or []:
                        if _order_id(maker_order) == order_id:
                            matched += _decimal(maker_order.get("matched_amount"))
                previous_matched = _decimal(record.get("size_matched"))
                record["size_matched"] = str(max(previous_matched, matched))
                original = _decimal(record.get("original_size") or record.get("size"))
                if original > 0 and matched >= original:
                    record["status"] = "MATCHED"
                else:
                    record["status"] = "CLOSED"
                record["closed_logged"] = True
                self._log("EXIT_ORDER_CLOSED", record)
                continue
            status = str(current.get("status") or record.get("status") or "").upper()
            record["miss_count"] = 0
            matched = _decimal(current.get("size_matched"))
            original = _decimal(current.get("original_size") or record.get("size"))
            record["status"] = status
            record["size_matched"] = str(matched)
            record["original_size"] = str(original)
            if status in _TERMINAL_ORDER_STATUSES or (original > 0 and matched >= original):
                record["status"] = "MATCHED" if status not in {"CANCELLED", "CANCELED", "EXPIRED"} else status
                record["closed_logged"] = True
                self._log("EXIT_ORDER_CLOSED", record)
            elif matched > _decimal(record.get("last_logged_matched") or "0"):
                record["last_logged_matched"] = str(matched)
                self._log("EXIT_PARTIAL_FILL", record)

    def _adopt_open_orders(
        self,
        open_orders: list[dict[str, Any]],
        positions: list[dict[str, Any]],
    ) -> None:
        position_by_token = {
            str(position.get("asset_id") or ""): position for position in positions
        }
        orders = self.state.setdefault("orders", {})
        adopted: list[dict[str, Any]] = []
        for order in open_orders:
            if str(order.get("side") or "").upper() != "SELL":
                continue
            order_id = _order_id(order)
            token_id = str(order.get("asset_id") or order.get("token_id") or "")
            if not order_id or not token_id or order_id in orders:
                continue
            position = position_by_token.get(token_id)
            if position is None:
                continue
            record = {
                "order_id": order_id,
                "token_id": token_id,
                "condition_id": str(position.get("condition_id") or order.get("condition_id") or ""),
                "title": str(position.get("title") or ""),
                "slug": str(position.get("slug") or ""),
                "price": str(order.get("price") or ""),
                "size": str(_remaining_size(order)),
                "original_size": str(_decimal(order.get("original_size") or order.get("size"))),
                "size_matched": str(_decimal(order.get("size_matched"))),
                "last_logged_matched": str(_decimal(order.get("size_matched"))),
                "status": str(order.get("status") or "LIVE").upper(),
                "created_at": str(order.get("created_at") or datetime.now(timezone.utc).isoformat()),
                "adopted": True,
                "miss_count": 0,
            }
            orders[order_id] = record
            adopted.append(record)
            self._log("EXIT_ORDER_ADOPTED", record)
        self._sync_exchange_orders(open_orders)

    def _reserved_by_token(self, open_orders: list[dict[str, Any]]) -> dict[str, Decimal]:
        reserved: dict[str, Decimal] = {}
        open_sell_ids: set[str] = set()
        for order in open_orders:
            if str(order.get("side") or "").upper() != "SELL":
                continue
            order_id = _order_id(order)
            if order_id:
                open_sell_ids.add(order_id)
            token = str(order.get("asset_id") or order.get("token_id") or "")
            if token:
                reserved[token] = reserved.get(token, Decimal("0")) + _remaining_size(order)
        for order_id, record in (self.state.get("orders") or {}).items():
            if order_id in open_sell_ids or str(record.get("status") or "") in _TERMINAL_ORDER_STATUSES:
                continue
            token = str(record.get("token_id") or "")
            if not token:
                continue
            original = _decimal(record.get("original_size") or record.get("size"))
            matched = _decimal(record.get("size_matched"))
            remaining = max(Decimal("0"), original - matched)
            reserved[token] = reserved.get(token, Decimal("0")) + remaining
        return reserved

    def _submit_exit(
        self,
        *,
        position: dict[str, Any],
        size: Decimal,
        book: dict[str, Any],
        price: float,
    ) -> Optional[dict[str, Any]]:
        token_id = str(position.get("asset_id") or "")
        if not token_id:
            return None
        sell_size = size.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        if sell_size <= 0:
            return None
        response = submit_gtc_sell(
            token_id=token_id,
            price=price,
            size=float(sell_size),
            config=self.config,
        )
        payload = jsonable(response)
        order_id = _order_id(payload)
        if not order_id:
            self._log(
                "EXIT_SUBMIT_NO_ORDER_ID",
                {
                    "token_id": token_id,
                    "price": price,
                    "size": str(sell_size),
                    "response": payload,
                },
            )
            return None
        record = {
            "order_id": order_id,
            "token_id": token_id,
            "condition_id": str(position.get("condition_id") or ""),
            "title": str(position.get("title") or ""),
            "slug": str(position.get("slug") or ""),
            "price": str(price),
            "size": str(sell_size),
            "original_size": str(sell_size),
            "size_matched": "0",
            "status": "SUBMITTED",
            "submit_status": str(payload.get("status") or "").upper(),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "miss_count": 0,
        }
        self.state.setdefault("orders", {})[order_id] = record
        self._log("EXIT_SUBMITTED", record)
        print(
            "EXIT SELL {} {} @ {} x {}".format(
                record["slug"],
                token_id,
                price,
                sell_size,
            ),
            flush=True,
        )
        return record

    def run_once(self) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        if not self._due():
            return []
        started = time.monotonic()
        self.last_run_at = datetime.now(timezone.utc).isoformat()
        submitted: list[dict[str, Any]] = []
        position_errors: list[str] = []
        try:
            positions = fetch_positions(status="OPEN", config=self.config)
            open_orders = fetch_open_orders(self.config)
            weather_positions = [position for position in positions if _is_weather_position(position)]
            self._adopt_open_orders(open_orders, weather_positions)
            self._update_orders(open_orders)
            self._cancel_below_floor(open_orders)
            reserved = self._reserved_by_token(open_orders)
            max_orders = max(1, int(_number(self.config.get("exit_max_orders_per_cycle")) or 10))
            for position in weather_positions:
                if len(submitted) >= max_orders:
                    break
                token_id = str(position.get("asset_id") or "")
                if not token_id:
                    continue
                available = _decimal(position.get("current_size")) - reserved.get(token_id, Decimal("0"))
                if available <= 0:
                    continue
                if self._is_unavailable(token_id):
                    continue
                try:
                    book = fetch_order_book(token_id=token_id, config=self.config)
                    self._clear_unavailable(token_id)
                    price = self._exit_price(book)
                    if price is None:
                        continue
                    min_size = self._min_size(book)
                    if available < min_size:
                        continue
                    record = self._submit_exit(
                        position=position,
                        size=available,
                        book=book,
                        price=price,
                    )
                    if record is not None:
                        submitted.append(record)
                except Exception as exc:  # noqa: BLE001
                    position_errors.append("{}:{}".format(token_id, exc))
                    if any(text in str(exc).lower() for text in _UNAVAILABLE_ORDER_ERRORS):
                        self._mark_unavailable(token_id)
                    self._log(
                        "EXIT_POSITION_ERROR",
                        {
                            "token_id": token_id,
                            "slug": str(position.get("slug") or ""),
                            "error": str(exc),
                        },
                    )
            self.last_error = "; ".join(position_errors)
        except Exception as exc:
            self.last_error = str(exc)
            self._log("EXIT_ERROR", {"error": str(exc)})
        self.state["last_run_at"] = self.last_run_at
        self.state["last_error"] = self.last_error
        self.state["last_run_s"] = round(time.monotonic() - started, 3)
        self._save_state()
        return submitted

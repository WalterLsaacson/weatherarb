"""Weather finality scanner: source finality -> bucket -> book economics."""

from __future__ import annotations

import copy
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from .books import ClobClient, ask_depth, bid_depth, normalize_book, walk_asks, walk_bids
from .models import Book, WeatherMarket, WeatherRule, as_float, parse_time, utc_now
from .rules import (
    bucket_for_outcome,
    bucket_for_value,
    bucket_impossible_while_open,
    norm_outcome,
    normalize_market,
    source_matches,
    validate_event_group_siblings,
)
from .sources import WeatherSourceAdapter, sample_set_of
from .storage import sha256_json


def _iso(value: Optional[datetime] = None) -> str:
    return (value or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()


def _norm_outcome(value: Any) -> str:
    return norm_outcome(value)


def _rule_market_id(rule: WeatherRule, market: WeatherMarket) -> str:
    return str(rule.target_outcome or market.outcome or "").strip()


def _norm_event_id(value: Any) -> str:
    return str(value or "").strip().rstrip("/").lower()


def _without_raw(payload: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(payload)
    cleaned.pop("raw", None)
    return cleaned


def _yes_token(market: WeatherMarket) -> str:
    return str(market.yes_token_id or (market.token_ids[0] if market.token_ids else "") or "")


def _no_token(market: WeatherMarket) -> str:
    if market.no_token_id:
        return str(market.no_token_id)
    if len(market.token_ids) > 1:
        return str(market.token_ids[1])
    return ""


def _unique_ids(ids: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in ids:
        token = str(item or "")
        if not token or token in seen:
            continue
        seen.add(token)
        result.append(token)
    return result


_OPPORTUNITY_REASONS = {
    "intraday_impossible_no",
    "provisional_loser_no",
    "source_final_loser_no",
    "intraday_impossible_sell_yes",
    "provisional_loser_sell_yes",
    "source_final_loser_sell_yes",
    "source_final_winner_yes",
}


def extrema_from_rows(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    extrema: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        event_id = str(row.get("event_group_id") or "")
        observation = row.get("observation") if isinstance(row.get("observation"), dict) else {}
        value = as_float(observation.get("value"))
        if not event_id or value is None:
            continue
        extrema[event_id] = {
            "value": value,
            "status": str(observation.get("status") or ""),
        }
    return extrema


@dataclass
class WeatherScannerConfig:
    max_ask: float = 0.995
    min_net_edge: float = 0.0075
    min_order_shares: float = 1.0
    target_shares: float = 1.0
    max_usdc: float = 50.0
    max_slippage: float = 0.003
    book_ttl_s: float = 5.0
    display_book_ttl_s: float = 60.0
    weather_prefetch_deadline_s: float = 20.0
    require_manual_approval: bool = True
    require_explicit_fee: bool = True
    require_market_constraints: bool = True
    reject_neg_risk: bool = True


class WeatherScanner:
    def __init__(
        self,
        *,
        config: Optional[WeatherScannerConfig] = None,
        source_adapter: Optional[WeatherSourceAdapter] = None,
        clob_client: Optional[ClobClient] = None,
    ):
        self.config = config or WeatherScannerConfig()
        self.source_adapter = source_adapter or WeatherSourceAdapter()
        self.clob_client = clob_client or ClobClient(http=self.source_adapter.http)

    @staticmethod
    def _book(value: Any, token_id: str) -> Book:
        if isinstance(value, Book):
            return value
        if isinstance(value, dict):
            return normalize_book(token_id, value)
        return Book(token_id=token_id, book_missing=True, error="book_missing")

    def _queue_matched_trade(
        self,
        base: dict[str, Any],
        rows: list[dict[str, Any]],
        token_ids: list[str],
        market: WeatherMarket,
        *,
        bucket: Any,
        trade_side: str,
        is_target: bool,
        source_status: str,
        reason: str,
        history: list[str],
        priority_token_ids: Optional[list[str]] = None,
        priority: bool = False,
        order_side: str = "BUY",
        enqueue_token: bool = True,
    ) -> None:
        trade_token = _yes_token(market) if trade_side == "YES" else _no_token(market)
        base.update(
            {
                "matched_bucket": bucket.to_dict(),
                "matched_outcome": bucket.outcome,
                "rule_status": "matched" if is_target else "not_target",
                "source_status": source_status,
                "trade_side": trade_side,
                "order_side": str(order_side or "BUY").upper(),
                "winning_side": trade_side,
                "winning_token_id": trade_token,
                "book_token_id": trade_token,
                "lifecycle": {"state": "RULE_MATCHED", "history": list(history)},
            }
        )
        if not trade_token:
            base.update(
                {
                    "status": "no_trade",
                    "reason": "missing_yes_token" if is_target else "missing_no_token",
                }
            )
            rows.append(base)
            return
        if enqueue_token:
            token_ids.append(trade_token)
            if priority and priority_token_ids is not None:
                priority_token_ids.append(trade_token)
        if not market.tradable:
            base.update({"status": "no_trade", "reason": "market_not_tradable"})
            rows.append(base)
            return
        base["status"] = "rule_matched"
        base["reason"] = reason
        rows.append(base)

    def _queue_impossible_bucket_trades(
        self,
        base: dict[str, Any],
        rows: list[dict[str, Any]],
        token_ids: list[str],
        market: WeatherMarket,
        *,
        bucket: Any,
        source_status: str,
        no_reason: str,
        sell_reason: str,
        history: list[str],
        priority_token_ids: Optional[list[str]] = None,
        priority: bool = False,
    ) -> None:
        sell_base = copy.deepcopy(base)
        self._queue_matched_trade(
            base,
            rows,
            token_ids,
            market,
            bucket=bucket,
            trade_side="NO",
            is_target=False,
            source_status=source_status,
            reason=no_reason,
            history=list(history),
            priority_token_ids=priority_token_ids,
            priority=priority,
            order_side="BUY",
        )
        # Defer Yes-token CLOB fetch until buy-No fails; avoids doubling books when No is tradeable.
        self._queue_matched_trade(
            sell_base,
            rows,
            token_ids,
            market,
            bucket=bucket,
            trade_side="YES",
            is_target=False,
            source_status=source_status,
            reason=sell_reason,
            history=list(history) + (["SELL_YES"] if "SELL_YES" not in history else []),
            priority_token_ids=priority_token_ids,
            priority=False,
            order_side="SELL",
            enqueue_token=False,
        )

    def _buy_no_opportunity_markets(self, rows: list[dict[str, Any]]) -> set[str]:
        return {
            str(row.get("market_id") or "")
            for row in rows
            if row.get("status") == "opportunity"
            and str(row.get("trade_side") or "").upper() == "NO"
            and str(row.get("order_side") or "BUY").upper() == "BUY"
            and row.get("market_id")
        }

    def _finalize_deferred_sell_yes(
        self,
        rows: list[dict[str, Any]],
        books: dict[str, Any],
        market_map: dict[str, WeatherMarket],
        eval_at: datetime,
        *,
        fetch: bool,
    ) -> list[str]:
        buy_no_ok = self._buy_no_opportunity_markets(rows)
        pending_tokens: list[str] = []
        for row in rows:
            if row.get("status") != "rule_matched":
                continue
            if str(row.get("order_side") or "").upper() != "SELL":
                continue
            market_id = str(row.get("market_id") or "")
            if market_id in buy_no_ok:
                row.update({"status": "no_trade", "reason": "buy_no_preferred"})
                continue
            token_id = str(row.get("book_token_id") or "")
            if token_id:
                pending_tokens.append(token_id)
        pending = _unique_ids(pending_tokens)
        if fetch and pending:
            books.update(self.clob_client.fetch_books(pending))
        if pending:
            self._evaluate_tokens(rows, books, market_map, eval_at, pending)
        for row in rows:
            if row.get("status") != "rule_matched":
                continue
            if str(row.get("order_side") or "").upper() != "SELL":
                continue
            row.update({"status": "no_trade", "reason": "book_missing"})
        return pending

    def _trade_is_priority(
        self,
        *,
        observation_status: str,
        metric: str,
        previous: Any,
        market_bucket: Any,
        rounding: str,
        trade_side: str,
    ) -> bool:
        prev = previous if isinstance(previous, dict) else {}
        prev_status = str(prev.get("status") or "")
        if observation_status == "final":
            return prev_status != "final"
        if trade_side != "NO":
            return prev_status != observation_status
        prev_value = as_float(prev.get("value"))
        if prev_value is None:
            return True
        return not bucket_impossible_while_open(
            metric, prev_value, market_bucket, rounding=rounding
        )

    def _bind_row_book(
        self,
        row: dict[str, Any],
        books: dict[str, Any],
        *,
        overwrite: bool = False,
    ) -> str:
        if row.get("book") and not overwrite:
            return str((row.get("book") or {}).get("token_id") or "")
        token_id = str(
            row.get("book_token_id")
            or row.get("winning_token_id")
            or row.get("display_token_id")
            or ""
        )
        fetched = books.get(token_id) if token_id else None
        if fetched is not None:
            row["book"] = _without_raw(self._book(fetched, token_id).to_dict())
        return token_id

    def _evaluate_matched_row(
        self,
        row: dict[str, Any],
        books: dict[str, Any],
        market_map: dict[str, WeatherMarket],
        eval_at: datetime,
    ) -> None:
        if row.get("status") != "rule_matched":
            return
        market = market_map.get(str(row.get("market_id")))
        token_id = self._bind_row_book(row, books, overwrite=True)
        if self.config.reject_neg_risk and market is not None and market.neg_risk:
            row.update({"status": "no_trade", "reason": "neg_risk_rejected"})
            return
        book = self._book(books.get(token_id), token_id)
        row["book"] = _without_raw(book.to_dict())
        if market is None:
            row.update({"status": "no_trade", "reason": "market_not_found"})
            return
        if self.config.require_explicit_fee and market.fee_source == "category_default":
            row.update({"status": "no_trade", "reason": "fee_not_explicit"})
            return
        book_time = parse_time(book.fetched_at)
        if book_time is None:
            row.update({"status": "no_trade", "reason": "book_timestamp_missing"})
            return
        book_age = (eval_at.astimezone(timezone.utc) - book_time).total_seconds()
        if book_age < -1.0 or book_age > float(self.config.book_ttl_s):
            row.update(
                {
                    "status": "no_trade",
                    "reason": "book_stale",
                    "economics": {
                        "book_age_s": book_age,
                        "book_ttl_s": float(self.config.book_ttl_s),
                    },
                }
            )
            return
        order_side = str(row.get("order_side") or "BUY").upper()
        if order_side == "SELL":
            self._evaluate_sell_row(row, book, market, book_age)
            return
        self._evaluate_buy_row(row, book, market, book_age)

    def _mark_opportunity(self, row: dict[str, Any]) -> None:
        match_reason = str(row.get("reason") or "")
        opportunity_reason = (
            match_reason
            if match_reason in _OPPORTUNITY_REASONS
            else "dry_run_candidate_only"
        )
        history = list((row.get("lifecycle") or {}).get("history") or [])
        if not history:
            history = ["DISCOVERED", "RULE_MATCHED"]
        if history[-1] != "BOOK_READY":
            history = history + ["BOOK_READY"]
        row.update(
            {
                "status": "opportunity",
                "reason": opportunity_reason,
                "dry_run": True,
                "lifecycle": {"state": "BOOK_READY", "history": history},
            }
        )

    def _evaluate_buy_row(
        self,
        row: dict[str, Any],
        book: Book,
        market: WeatherMarket,
        book_age: float,
    ) -> None:
        if book.book_missing or book.best_ask is None:
            row.update({"status": "no_trade", "reason": "book_missing"})
            return
        best_ask = float(book.best_ask)
        if best_ask <= 0.0:
            row.update({"status": "no_trade", "reason": "invalid_ask"})
            return
        if best_ask > self.config.max_ask or best_ask >= 1.0:
            row.update({"status": "no_trade", "reason": "ask_above_limit"})
            return
        tick = book.tick_size or market.tick_size
        min_order_size = book.min_order_size or market.min_order_size
        if self.config.require_market_constraints and (
            tick is None or tick <= 0 or min_order_size is None or min_order_size <= 0
        ):
            row.update({"status": "no_trade", "reason": "market_constraints_missing"})
            return
        target = max(
            float(self.config.min_order_shares),
            float(min_order_size or 0.0),
            min(float(self.config.target_shares), self.config.max_usdc / best_ask),
        )
        if target * best_ask > float(self.config.max_usdc) + 1e-12:
            row.update(
                {
                    "status": "no_trade",
                    "reason": "max_usdc_below_min_order",
                    "economics": {
                        "best_ask": best_ask,
                        "min_order_size": min_order_size,
                        "max_usdc": float(self.config.max_usdc),
                        "tick_size": tick,
                    },
                }
            )
            return
        execution_limit = min(
            float(self.config.max_ask),
            best_ask + max(0.0, float(self.config.max_slippage)),
        )
        fill = walk_asks(book, max_price=execution_limit, target_shares=target)
        if not fill["complete"]:
            row.update(
                {
                    "status": "no_trade",
                    "reason": "insufficient_depth_within_slippage",
                    "economics": {
                        "best_ask": best_ask,
                        "execution_limit": execution_limit,
                        "ask_depth_at_limit": ask_depth(book, max_price=execution_limit),
                        "target_shares": target,
                        "filled_shares": fill["filled_shares"],
                        "tick_size": tick,
                    },
                }
            )
            return
        execution_price = float(fill["vwap"] or 0.0)
        fee_rate = market.fee_rate
        if fee_rate is None:
            row.update({"status": "no_trade", "reason": "fee_unknown"})
            return
        fee = float(fee_rate) * execution_price * (1.0 - execution_price)
        gross = 1.0 - execution_price
        edge = gross - fee
        row["economics"] = {
            "best_ask": best_ask,
            "best_bid": book.best_bid,
            "execution_price": execution_price,
            "execution_shares": fill["filled_shares"],
            "worst_price": fill["worst_price"],
            "slippage": max(0.0, execution_price - best_ask),
            "fee_rate": float(fee_rate),
            "fee_source": market.fee_source,
            "fee_per_share": fee,
            "gross_edge": gross,
            "net_edge": edge,
            "ask_depth_at_limit": ask_depth(book, max_price=execution_limit),
            "execution_limit": execution_limit,
            "max_ask": self.config.max_ask,
            "min_net_edge": self.config.min_net_edge,
            "tick_size": tick,
            "min_order_size": min_order_size,
            "book_age_s": book_age,
            "trade_side": row.get("trade_side") or "YES",
            "order_side": "BUY",
        }
        if edge < self.config.min_net_edge:
            row.update({"status": "no_trade", "reason": "edge_below_minimum"})
            return
        self._mark_opportunity(row)

    def _evaluate_sell_row(
        self,
        row: dict[str, Any],
        book: Book,
        market: WeatherMarket,
        book_age: float,
    ) -> None:
        # Dead Yes fair value is ~0; selling into remaining bids is +EV when bid clears fees.
        if book.book_missing or book.best_bid is None:
            row.update({"status": "no_trade", "reason": "book_missing"})
            return
        best_bid = float(book.best_bid)
        min_bid = float(self.config.min_net_edge)
        if best_bid <= 0.0:
            row.update({"status": "no_trade", "reason": "invalid_bid"})
            return
        if best_bid < min_bid:
            row.update({"status": "no_trade", "reason": "bid_below_minimum"})
            return
        tick = book.tick_size or market.tick_size
        min_order_size = book.min_order_size or market.min_order_size
        if self.config.require_market_constraints and (
            tick is None or tick <= 0 or min_order_size is None or min_order_size <= 0
        ):
            row.update({"status": "no_trade", "reason": "market_constraints_missing"})
            return
        # Cap sell notional by max_usdc (proceeds), same risk dial as buys.
        target = max(
            float(self.config.min_order_shares),
            float(min_order_size or 0.0),
            min(
                float(self.config.target_shares),
                self.config.max_usdc / max(best_bid, 1e-9),
            ),
        )
        if target * best_bid > float(self.config.max_usdc) + 1e-12:
            row.update(
                {
                    "status": "no_trade",
                    "reason": "max_usdc_below_min_order",
                    "economics": {
                        "best_bid": best_bid,
                        "min_order_size": min_order_size,
                        "max_usdc": float(self.config.max_usdc),
                        "tick_size": tick,
                        "order_side": "SELL",
                    },
                }
            )
            return
        execution_floor = max(
            min_bid,
            best_bid - max(0.0, float(self.config.max_slippage)),
        )
        fill = walk_bids(book, min_price=execution_floor, target_shares=target)
        if not fill["complete"]:
            row.update(
                {
                    "status": "no_trade",
                    "reason": "insufficient_bid_depth_within_slippage",
                    "economics": {
                        "best_bid": best_bid,
                        "execution_floor": execution_floor,
                        "bid_depth_at_floor": bid_depth(book, min_price=execution_floor),
                        "target_shares": target,
                        "filled_shares": fill["filled_shares"],
                        "tick_size": tick,
                        "order_side": "SELL",
                    },
                }
            )
            return
        execution_price = float(fill["vwap"] or 0.0)
        fee_rate = market.fee_rate
        if fee_rate is None:
            row.update({"status": "no_trade", "reason": "fee_unknown"})
            return
        fee = float(fee_rate) * execution_price * (1.0 - execution_price)
        # Worthless Yes: gross proceeds ≈ execution_price, net after fee.
        gross = execution_price
        edge = gross - fee
        row["economics"] = {
            "best_ask": book.best_ask,
            "best_bid": best_bid,
            "execution_price": execution_price,
            "execution_shares": fill["filled_shares"],
            "worst_price": fill["worst_price"],
            "slippage": max(0.0, best_bid - execution_price),
            "fee_rate": float(fee_rate),
            "fee_source": market.fee_source,
            "fee_per_share": fee,
            "gross_edge": gross,
            "net_edge": edge,
            "bid_depth_at_floor": bid_depth(book, min_price=execution_floor),
            "ask_depth_at_limit": bid_depth(book, min_price=execution_floor),
            "execution_floor": execution_floor,
            "min_bid": min_bid,
            "min_net_edge": self.config.min_net_edge,
            "tick_size": tick,
            "min_order_size": min_order_size,
            "book_age_s": book_age,
            "trade_side": "YES",
            "order_side": "SELL",
        }
        if edge < self.config.min_net_edge:
            row.update({"status": "no_trade", "reason": "edge_below_minimum"})
            return
        self._mark_opportunity(row)

    def _evaluate_tokens(
        self,
        rows: list[dict[str, Any]],
        books: dict[str, Any],
        market_map: dict[str, WeatherMarket],
        eval_at: datetime,
        token_ids: Iterable[str],
    ) -> None:
        wanted = {str(token_id) for token_id in token_ids if token_id}
        if not wanted:
            return
        for row in rows:
            if row.get("status") != "rule_matched":
                continue
            if str(row.get("book_token_id") or "") not in wanted:
                continue
            self._evaluate_matched_row(row, books, market_map, eval_at)

    def scan(
        self,
        markets: Iterable[WeatherMarket | dict[str, Any]],
        rules: Iterable[WeatherRule],
        *,
        books: Optional[dict[str, Book | dict[str, Any]]] = None,
        now: Optional[datetime] = None,
        fetch_books: bool = True,
        previous_extrema: Optional[dict[str, Any]] = None,
        display_books: Optional[dict[str, Book | dict[str, Any]]] = None,
    ) -> dict[str, Any]:
        scan_started = time.monotonic()
        current = now or datetime.now(timezone.utc)
        market_list = [
            item if isinstance(item, WeatherMarket) else normalize_market(item)
            for item in markets
        ]
        market_map = {item.market_id: item for item in market_list if item.market_id}
        rule_list = [item for item in rules if isinstance(item, WeatherRule)]
        group_targets: dict[str, dict[str, int]] = {}
        rules_by_group: dict[str, list[WeatherRule]] = {}
        for rule in rule_list:
            group_id = _norm_event_id(rule.event_group_id)
            rules_by_group.setdefault(group_id, []).append(rule)
            target = _norm_outcome(rule.target_outcome)
            if target:
                targets = group_targets.setdefault(str(rule.event_group_id), {})
                targets[target] = targets.get(target, 0) + 1
        group_block: dict[str, str] = {}
        for group_id, group_rules in rules_by_group.items():
            bucket_sets = {
                json.dumps([bucket.to_dict() for bucket in item.buckets], sort_keys=True)
                for item in group_rules
            }
            if len(bucket_sets) > 1:
                group_block[group_id] = "bucket_set_mismatch"
                continue
            group_markets = []
            seen_ids: set[str] = set()
            for item in group_rules:
                market = market_map.get(item.market_id)
                if market is None or market.market_id in seen_ids:
                    continue
                seen_ids.add(market.market_id)
                group_markets.append(market)
            sibling_reason = validate_event_group_siblings(group_markets, group_rules[0].buckets)
            if sibling_reason:
                group_block[group_id] = sibling_reason
        rows: list[dict[str, Any]] = []
        token_ids: list[str] = []
        priority_token_ids: list[str] = []
        prior_extrema = previous_extrema if isinstance(previous_extrema, dict) else {}
        source_cache: dict[str, Any] = {}
        source_evidence: dict[str, dict[str, Any]] = {}
        self.source_adapter.prefetch(
            rule_list,
            now=current,
            deadline_s=float(self.config.weather_prefetch_deadline_s),
        )
        weather_prefetch_done = time.monotonic()

        for rule in rule_list:
            market = market_map.get(rule.market_id)
            base: dict[str, Any] = {
                "market_id": rule.market_id,
                "event_group_id": rule.event_group_id,
                "rule_version": rule.rule_version,
                "target_outcome": rule.target_outcome,
                "status": "review",
                "reason": "",
                "market": _without_raw(market.to_dict()) if market else {},
                "rule": _without_raw(rule.to_dict()),
                "created_at": _iso(current),
            }
            if market is None:
                base.update({"status": "no_trade", "reason": "market_not_found"})
                rows.append(base)
                continue
            if market.event_group_id and _norm_event_id(market.event_group_id) != _norm_event_id(
                rule.event_group_id
            ):
                base.update({"status": "review", "reason": "event_group_mismatch"})
                rows.append(base)
                continue
            sibling_reason = group_block.get(_norm_event_id(rule.event_group_id))
            if sibling_reason:
                base.update({"status": "review", "reason": sibling_reason})
                rows.append(base)
                continue
            target_key = _norm_outcome(_rule_market_id(rule, market))
            if target_key and group_targets.get(str(rule.event_group_id), {}).get(target_key, 0) > 1:
                base.update({"status": "review", "reason": "duplicate_target_outcome"})
                rows.append(base)
                continue
            if not rule.enabled:
                base.update({"status": "no_trade", "reason": "rule_disabled"})
                rows.append(base)
                continue
            if self.config.require_manual_approval and not rule.manual_approval:
                base.update({"status": "review", "reason": "rule_not_approved"})
                rows.append(base)
                continue
            if not market.resolution_source:
                base.update({"status": "review", "reason": "resolution_source_missing"})
                rows.append(base)
                continue
            if not source_matches(market, rule):
                base.update({"status": "review", "reason": "resolution_source_mismatch"})
                rows.append(base)
                continue

            base["market_match_status"] = "matched"

            cache_key = sha256_json(
                {
                    "event_group_id": rule.event_group_id,
                    "source": rule.source,
                    "start": rule.observation_start,
                    "end": rule.observation_end,
                    "metric": rule.metric,
                    "timezone": rule.timezone,
                    "rounding": rule.rounding,
                }
            )
            observation = source_cache.get(cache_key)
            if observation is None:
                observation = self.source_adapter.poll(rule, now=current)
                source_cache[cache_key] = observation
            evidence = observation.to_dict()
            if evidence.get("raw") is not None and evidence.get("evidence_hash"):
                source_evidence.setdefault(
                    str(evidence["evidence_hash"]),
                    {
                        "event_group_id": rule.event_group_id,
                        "evidence_hash": evidence.get("evidence_hash"),
                        "status": evidence.get("status"),
                        "source_timestamp": evidence.get("source_timestamp"),
                        "observed_at": evidence.get("observed_at"),
                        "station_id": evidence.get("station_id")
                        or rule.source.get("station_id"),
                        "provider": evidence.get("provider")
                        or rule.source.get("provider"),
                        "sample_set": sample_set_of(rule),
                        "series": evidence.get("series") or [],
                        "raw": evidence.get("raw"),
                    },
                )
            base["observation"] = _without_raw(evidence)
            if observation.status == "waiting_window":
                base.update(
                    {
                        "status": "waiting",
                        "reason": observation.reason or observation.status,
                    }
                )
                base["lifecycle"] = {
                    "state": "WAITING_WINDOW",
                    "history": ["DISCOVERED", "WAITING_WINDOW"],
                }
                rows.append(base)
                continue
            if observation.status == "expired":
                base.update(
                    {
                        "status": "no_trade",
                        "reason": observation.reason or observation.status,
                    }
                )
                base["lifecycle"] = {
                    "state": "WINDOW_EXPIRED",
                    "history": ["DISCOVERED", "WINDOW_EXPIRED"],
                }
                rows.append(base)
                continue
            if observation.status not in {"final", "intraday", "provisional"}:
                base.update(
                    {
                        "status": "review",
                        "reason": observation.reason or observation.status,
                    }
                )
                base["lifecycle"] = {
                    "state": "SOURCE_UNAVAILABLE",
                    "history": ["DISCOVERED", "SOURCE_UNAVAILABLE"],
                }
                rows.append(base)
                continue

            running_bucket = bucket_for_value(
                observation.value, rule.buckets, rounding=rule.rounding
            )
            if running_bucket is None:
                if observation.status == "intraday":
                    history_prefix = ["DISCOVERED", "INTRADAY", "RULE_REVIEW"]
                elif observation.status == "final":
                    history_prefix = ["DISCOVERED", "WINDOW_CLOSED", "SOURCE_FINAL", "RULE_REVIEW"]
                else:
                    history_prefix = ["DISCOVERED", "PROVISIONAL", "RULE_REVIEW"]
                base.update({"status": "review", "reason": "value_outside_bucket_set"})
                base["lifecycle"] = {
                    "state": "RULE_REVIEW",
                    "history": history_prefix,
                }
                rows.append(base)
                continue

            market_bucket = bucket_for_outcome(_rule_market_id(rule, market), rule.buckets)
            if market_bucket is None:
                base.update({"status": "review", "reason": "outcome_not_in_bucket_set"})
                base["lifecycle"] = {
                    "state": "RULE_REVIEW",
                    "history": ["DISCOVERED", "RULE_REVIEW"],
                }
                rows.append(base)
                continue
            is_running = _norm_outcome(market_bucket.outcome) == _norm_outcome(
                running_bucket.outcome
            )

            if observation.status == "intraday":
                if not bucket_impossible_while_open(
                    rule.metric,
                    observation.value,
                    market_bucket,
                    rounding=rule.rounding,
                ):
                    base.update(
                        {
                            "status": "waiting",
                            "reason": observation.reason or observation.status,
                            "matched_bucket": running_bucket.to_dict(),
                            "matched_outcome": running_bucket.outcome,
                            "source_status": "intraday",
                        }
                    )
                    base["lifecycle"] = {
                        "state": "INTRADAY",
                        "history": ["DISCOVERED", "INTRADAY"],
                    }
                    rows.append(base)
                    continue
                self._queue_impossible_bucket_trades(
                    base,
                    rows,
                    token_ids,
                    market,
                    bucket=running_bucket,
                    source_status="intraday",
                    no_reason="intraday_impossible_no",
                    sell_reason="intraday_impossible_sell_yes",
                    history=["DISCOVERED", "INTRADAY", "RULE_MATCHED"],
                    priority_token_ids=priority_token_ids,
                    priority=self._trade_is_priority(
                        observation_status="intraday",
                        metric=rule.metric,
                        previous=prior_extrema.get(str(rule.event_group_id)),
                        market_bucket=market_bucket,
                        rounding=rule.rounding,
                        trade_side="NO",
                    ),
                )
                continue

            if observation.status == "provisional":
                if is_running:
                    base.update(
                        {
                            "status": "waiting",
                            "reason": observation.reason or observation.status,
                            "matched_bucket": running_bucket.to_dict(),
                            "matched_outcome": running_bucket.outcome,
                            "source_status": "provisional",
                        }
                    )
                    base["lifecycle"] = {
                        "state": "PROVISIONAL",
                        "history": ["DISCOVERED", "PROVISIONAL"],
                    }
                    rows.append(base)
                    continue
                self._queue_impossible_bucket_trades(
                    base,
                    rows,
                    token_ids,
                    market,
                    bucket=running_bucket,
                    source_status="provisional",
                    no_reason="provisional_loser_no",
                    sell_reason="provisional_loser_sell_yes",
                    history=["DISCOVERED", "PROVISIONAL", "RULE_MATCHED"],
                    priority_token_ids=priority_token_ids,
                    priority=self._trade_is_priority(
                        observation_status="provisional",
                        metric=rule.metric,
                        previous=prior_extrema.get(str(rule.event_group_id)),
                        market_bucket=market_bucket,
                        rounding=rule.rounding,
                        trade_side="NO",
                    ),
                )
                continue

            final_side = "YES" if is_running else "NO"
            if is_running:
                self._queue_matched_trade(
                    base,
                    rows,
                    token_ids,
                    market,
                    bucket=running_bucket,
                    trade_side="YES",
                    is_target=True,
                    source_status="final",
                    reason="source_final_winner_yes",
                    history=["DISCOVERED", "WINDOW_CLOSED", "SOURCE_FINAL", "RULE_MATCHED"],
                    priority_token_ids=priority_token_ids,
                    priority=self._trade_is_priority(
                        observation_status="final",
                        metric=rule.metric,
                        previous=prior_extrema.get(str(rule.event_group_id)),
                        market_bucket=market_bucket,
                        rounding=rule.rounding,
                        trade_side="YES",
                    ),
                )
            else:
                self._queue_impossible_bucket_trades(
                    base,
                    rows,
                    token_ids,
                    market,
                    bucket=running_bucket,
                    source_status="final",
                    no_reason="source_final_loser_no",
                    sell_reason="source_final_loser_sell_yes",
                    history=["DISCOVERED", "WINDOW_CLOSED", "SOURCE_FINAL", "RULE_MATCHED"],
                    priority_token_ids=priority_token_ids,
                    priority=self._trade_is_priority(
                        observation_status="final",
                        metric=rule.metric,
                        previous=prior_extrema.get(str(rule.event_group_id)),
                        market_bucket=market_bucket,
                        rounding=rule.rounding,
                        trade_side="NO",
                    ),
                )

        weather_process_done = time.monotonic()
        display_token_ids: list[str] = []
        for row in rows:
            if row.get("book_token_id") and row.get("status") == "rule_matched":
                continue
            status = str((row.get("observation") or {}).get("status") or "")
            if status not in {"intraday", "provisional", "final"}:
                continue
            market = market_map.get(str(row.get("market_id")))
            yes_token = _yes_token(market) if market else ""
            if not yes_token:
                continue
            row["display_token_id"] = yes_token
            display_token_ids.append(yes_token)

        fetch_priority = _unique_ids(priority_token_ids)
        fetch_trade = _unique_ids(
            token_id for token_id in token_ids if str(token_id) not in set(fetch_priority)
        )
        fetch_display = _unique_ids(
            token_id
            for token_id in display_token_ids
            if token_id and str(token_id) not in set(fetch_priority) and str(token_id) not in set(fetch_trade)
        )
        display_cache_hits = 0
        cached_display: dict[str, Any] = {}
        live_fetch = books is None and fetch_books
        if live_fetch and display_books:
            previous_books = display_books if isinstance(display_books, dict) else {}
            for token_id in fetch_display:
                value = previous_books.get(str(token_id))
                if value is None:
                    continue
                book = self._book(value, str(token_id))
                if book.book_missing or book.best_ask is None:
                    continue
                book_time = parse_time(book.fetched_at)
                if book_time is None:
                    continue
                age_s = (current.astimezone(timezone.utc) - book_time).total_seconds()
                if 0.0 <= age_s <= float(self.config.display_book_ttl_s):
                    cached_display[str(token_id)] = value
        display_cache_hits = len(cached_display)
        fetch_display = _unique_ids(
            token_id for token_id in fetch_display if str(token_id) not in cached_display
        )
        book_phase_started = time.monotonic()
        book_fetch_elapsed = 0.0
        if live_fetch:
            books = {}

            def _pull(token_ids_batch: list[str]) -> None:
                nonlocal book_fetch_elapsed
                if token_ids_batch:
                    started = time.monotonic()
                    books.update(self.clob_client.fetch_books(token_ids_batch))
                    book_fetch_elapsed += time.monotonic() - started

            def _eval_at() -> datetime:
                return utc_now()

            _pull(fetch_priority)
            self._evaluate_tokens(rows, books, market_map, _eval_at(), fetch_priority)
            _pull(fetch_trade)
            self._evaluate_tokens(rows, books, market_map, _eval_at(), fetch_trade)
            self._finalize_deferred_sell_yes(
                rows, books, market_map, _eval_at(), fetch=True
            )
            remaining_matched = [
                str(row.get("book_token_id") or "")
                for row in rows
                if row.get("status") == "rule_matched"
            ]
            if remaining_matched:
                self._evaluate_tokens(rows, books, market_map, _eval_at(), remaining_matched)
            books.update(cached_display)
            _pull(fetch_display)
            for row in rows:
                self._bind_row_book(row, books)
        else:
            books = books or {}
            eval_at = now if now is not None else utc_now()
            for row in rows:
                if row.get("status") != "rule_matched":
                    self._bind_row_book(row, books)
                    continue
                if str(row.get("order_side") or "BUY").upper() == "SELL":
                    continue
                self._evaluate_matched_row(row, books, market_map, eval_at)
            self._finalize_deferred_sell_yes(
                rows, books, market_map, eval_at, fetch=False
            )
            for row in rows:
                if row.get("status") == "rule_matched":
                    self._evaluate_matched_row(row, books, market_map, eval_at)
                elif not row.get("book"):
                    self._bind_row_book(row, books)

        book_phase_s = time.monotonic() - book_phase_started
        postprocess_started = time.monotonic()

        event_groups: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            event_groups.setdefault(str(row.get("event_group_id") or "unknown"), []).append(row)
        group_views: list[dict[str, Any]] = []
        for event_group_id, group_rows in sorted(event_groups.items()):
            first = group_rows[0]
            observations = [row.get("observation") or {} for row in group_rows if row.get("observation")]
            matched_values = {
                json.dumps(row.get("matched_bucket"), sort_keys=True)
                for row in group_rows
                if row.get("matched_bucket")
            }
            bucket_match_consistent = len(matched_values) <= 1
            if not bucket_match_consistent:
                for row in group_rows:
                    if row.get("status") in {"rule_matched", "opportunity"}:
                        row.update(
                            {
                                "status": "review",
                                "reason": "bucket_match_conflict",
                                "rule_status": "conflict",
                            }
                        )
            target_rows = [row for row in group_rows if row.get("rule_status") == "matched"]
            observation_statuses = {
                str((row.get("observation") or {}).get("status") or "")
                for row in group_rows
                if row.get("observation")
            }
            if not observation_statuses:
                group_source_status = "not_checked"
            elif observation_statuses <= {"final"}:
                group_source_status = "final"
            elif observation_statuses & {"unavailable", "error"}:
                group_source_status = "unavailable"
            elif observation_statuses & {"waiting_window"}:
                group_source_status = "waiting_window"
            elif observation_statuses & {"intraday"}:
                group_source_status = "intraday"
            else:
                group_source_status = "provisional"
            source_series = []
            for row in group_rows:
                obs = row.get("observation") or {}
                points = obs.get("series")
                if isinstance(points, list) and points:
                    source_series = points
                    break
            for row in group_rows:
                obs = row.get("observation")
                if isinstance(obs, dict) and "series" in obs:
                    cleaned = dict(obs)
                    cleaned.pop("series", None)
                    row["observation"] = cleaned
            group_views.append(
                {
                    "event_group_id": event_group_id,
                    "question": (first.get("market") or {}).get("question", ""),
                    "station_id": (observations[0] if observations else {}).get("station_id", ""),
                    "unit": (observations[0] if observations else {}).get("unit", ""),
                    "metric": (observations[0] if observations else {}).get("aggregation", ""),
                    "source_status": group_source_status,
                    "source_series": source_series,
                    "bucket_match_consistent": bucket_match_consistent,
                    "matched_bucket": (
                        (target_rows[0].get("matched_bucket") if target_rows else None)
                    ),
                    "market_count": len(group_rows),
                    "matched_market_count": len(target_rows),
                    "candidate_count": sum(1 for row in group_rows if row.get("status") == "opportunity"),
                    "status_counts": _status_counts(group_rows),
                    "markets": [
                        {
                            "market_id": row.get("market_id"),
                            "outcome": row.get("target_outcome")
                            or (row.get("market") or {}).get("outcome"),
                            "trade_side": row.get("trade_side") or "",
                            "status": row.get("status"),
                            "reason": row.get("reason"),
                            "economics": row.get("economics") or {},
                            "book": row.get("book") or {},
                        }
                        for row in group_rows
                    ],
                    "rows": group_rows,
                }
            )

        postprocess_s = time.monotonic() - postprocess_started
        eligible = len(rows)
        paired = sum(1 for row in rows if row.get("market_match_status") == "matched")
        matched = sum(1 for row in rows if row.get("rule_status") == "matched")
        actionable = sum(1 for row in rows if row.get("trade_side"))
        final_groups = sum(1 for group in group_views if group.get("source_status") == "final")
        mapped_groups = sum(
            1
            for group in group_views
            if group.get("matched_bucket") and group.get("bucket_match_consistent", False)
        )
        summary = {
            "scanned_at": _iso(current),
            "markets": len(market_list),
            "rules": len(rule_list),
            "event_groups": len(group_views),
            "eligible_markets": eligible,
            "matched_markets": paired,
            "market_match_rate": (paired / eligible) if eligible else 0.0,
            "winner_bucket_markets": matched,
            "winner_bucket_rate": (matched / eligible) if eligible else 0.0,
            # Event-level KPI for the board; keep market-level count for
            # audit consumers that need the per-sibling poll cardinality.
            "source_final": final_groups,
            "source_final_markets": sum(
                1 for row in rows if (row.get("observation") or {}).get("status") == "final"
            ),
            "final_event_groups": final_groups,
            "mapped_event_groups": mapped_groups,
            "bucket_match_rate": (mapped_groups / final_groups) if final_groups else 0.0,
            "book_ready": sum(1 for row in rows if row.get("status") == "opportunity"),
            "book_ready_rate": (
                sum(1 for row in rows if row.get("status") == "opportunity") / actionable
                if actionable
                else 0.0
            ),
            "opportunities": sum(1 for row in rows if row.get("status") == "opportunity"),
            "waiting": sum(1 for row in rows if row.get("status") == "waiting"),
            "review": sum(1 for row in rows if row.get("status") == "review"),
            "no_trade": sum(1 for row in rows if row.get("status") == "no_trade"),
            "book_fetch_priority": len(fetch_priority),
            "book_fetch_trade": len(fetch_trade),
            "book_fetch_display": len(fetch_display),
            "display_book_cache_hits": display_cache_hits,
            "dry_run": True,
            "live_orders_submitted": 0,
            "timings": {
                "total_s": round(time.monotonic() - scan_started, 3),
                "weather_prefetch_s": round(
                    weather_prefetch_done - scan_started, 3
                ),
                "weather_process_s": round(
                    weather_process_done - weather_prefetch_done, 3
                ),
                "book_fetch_s": round(book_fetch_elapsed, 3),
                "book_evaluate_and_resolve_s": round(
                    max(0.0, book_phase_s - book_fetch_elapsed), 3
                ),
                "postprocess_s": round(postprocess_s, 3),
            },
        }
        return {
            "summary": summary,
            "rows": rows,
            "event_groups": group_views,
            "source_evidence": list(source_evidence.values()),
            "books": {
                token_id: _without_raw(self._book(value, token_id).to_dict())
                for token_id, value in books.items()
            },
        }


def _status_counts(rows: Iterable[dict[str, Any]]) -> dict[str, int]:
    result: dict[str, int] = {}
    for row in rows:
        key = str(row.get("status") or "unknown")
        result[key] = result.get(key, 0) + 1
    return result

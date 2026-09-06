"""Weather finality scanner: source finality -> bucket -> book economics."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from .books import ClobClient, ask_depth, normalize_book, walk_asks
from .models import Book, WeatherMarket, WeatherRule, parse_time
from .rules import bucket_for_value, norm_outcome, normalize_market, source_matches, validate_event_group_siblings
from .sources import WeatherSourceAdapter
from .storage import sha256_json


def _iso(value: Optional[datetime] = None) -> str:
    return (value or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()


def _norm_outcome(value: Any) -> str:
    return norm_outcome(value)


def _rule_market_id(rule: WeatherRule, market: WeatherMarket) -> str:
    return str(rule.target_outcome or market.outcome or "").strip()


def _norm_event_id(value: Any) -> str:
    return str(value or "").strip().rstrip("/").lower()


def _yes_token(market: WeatherMarket) -> str:
    return str(market.yes_token_id or (market.token_ids[0] if market.token_ids else "") or "")


def _no_token(market: WeatherMarket) -> str:
    if market.no_token_id:
        return str(market.no_token_id)
    if len(market.token_ids) > 1:
        return str(market.token_ids[1])
    return ""


@dataclass
class WeatherScannerConfig:
    max_ask: float = 0.995
    min_net_edge: float = 0.0075
    min_order_shares: float = 1.0
    target_shares: float = 1.0
    max_usdc: float = 50.0
    max_slippage: float = 0.003
    book_ttl_s: float = 5.0
    require_manual_approval: bool = True
    require_explicit_fee: bool = True
    require_market_constraints: bool = True
    reject_neg_risk: bool = False


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

    def scan(
        self,
        markets: Iterable[WeatherMarket | dict[str, Any]],
        rules: Iterable[WeatherRule],
        *,
        books: Optional[dict[str, Book | dict[str, Any]]] = None,
        now: Optional[datetime] = None,
        fetch_books: bool = True,
    ) -> dict[str, Any]:
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
        source_cache: dict[str, Any] = {}

        for rule in rule_list:
            market = market_map.get(rule.market_id)
            base: dict[str, Any] = {
                "market_id": rule.market_id,
                "event_group_id": rule.event_group_id,
                "rule_version": rule.rule_version,
                "target_outcome": rule.target_outcome,
                "status": "review",
                "reason": "",
                "market": market.to_dict() if market else {},
                "rule": rule.to_dict(),
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
            base["observation"] = observation.to_dict()
            if observation.status != "final":
                if observation.status == "waiting_window":
                    state = "WAITING_WINDOW"
                    row_status = "waiting"
                elif observation.status == "provisional":
                    state = "PROVISIONAL"
                    row_status = "waiting"
                elif observation.status == "expired":
                    state = "WINDOW_EXPIRED"
                    row_status = "no_trade"
                else:
                    state = "SOURCE_UNAVAILABLE"
                    row_status = "review"
                base.update({"status": row_status, "reason": observation.reason or observation.status})
                base["lifecycle"] = {"state": state, "history": ["DISCOVERED", state]}
                rows.append(base)
                continue

            bucket = bucket_for_value(observation.value, rule.buckets, rounding=rule.rounding)
            if bucket is None:
                base.update({"status": "review", "reason": "value_outside_bucket_set"})
                base["lifecycle"] = {"state": "RULE_REVIEW", "history": ["SOURCE_FINAL", "RULE_REVIEW"]}
                rows.append(base)
                continue
            target_outcome = _rule_market_id(rule, market)
            is_target = _norm_outcome(target_outcome) == _norm_outcome(bucket.outcome)
            trade_side = "YES" if is_target else "NO"
            trade_token = _yes_token(market) if is_target else _no_token(market)
            base.update(
                {
                    "matched_bucket": bucket.to_dict(),
                    "matched_outcome": bucket.outcome,
                    "rule_status": "matched" if is_target else "not_target",
                    "source_status": "final",
                    "trade_side": trade_side,
                    "winning_side": trade_side,
                    "winning_token_id": trade_token,
                    "book_token_id": trade_token,
                }
            )
            base["lifecycle"] = {
                "state": "RULE_MATCHED",
                "history": ["DISCOVERED", "WINDOW_CLOSED", "SOURCE_FINAL", "RULE_MATCHED"],
            }
            if not trade_token:
                base.update(
                    {
                        "status": "no_trade",
                        "reason": "missing_yes_token" if is_target else "missing_no_token",
                    }
                )
                rows.append(base)
                continue
            token_ids.append(trade_token)
            if not market.tradable:
                base.update({"status": "no_trade", "reason": "market_not_tradable"})
                rows.append(base)
                continue
            base["status"] = "rule_matched"
            base["reason"] = (
                "source_final_winner_yes" if is_target else "source_final_loser_no"
            )
            rows.append(base)

        if books is None and fetch_books and token_ids:
            fetched = self.clob_client.fetch_books(sorted(set(token_ids)))
            books = {token_id: book for token_id, book in fetched.items()}
        books = books or {}

        for row in rows:
            market = market_map.get(str(row.get("market_id")))
            token_id = str(row.get("book_token_id") or row.get("winning_token_id") or "")
            fetched = books.get(token_id) if token_id else None
            if fetched is not None:
                row["book"] = self._book(fetched, token_id).to_dict()
            if row.get("status") != "rule_matched":
                continue
            if self.config.reject_neg_risk and market is not None and market.neg_risk:
                # Opt-in only. Weather bucket events are negRisk by design;
                # a source-final winner Yes or loser No is still a single CLOB take.
                row.update({"status": "no_trade", "reason": "neg_risk_rejected"})
                continue
            book = self._book(books.get(token_id), token_id)
            row["book"] = book.to_dict()
            if book.book_missing or book.best_ask is None:
                row.update({"status": "no_trade", "reason": "book_missing"})
                continue
            if market is None:
                row.update({"status": "no_trade", "reason": "market_not_found"})
                continue
            if self.config.require_explicit_fee and market.fee_source == "category_default":
                row.update({"status": "no_trade", "reason": "fee_not_explicit"})
                continue
            book_time = parse_time(book.fetched_at)
            if book_time is None:
                row.update({"status": "no_trade", "reason": "book_timestamp_missing"})
                continue
            book_age = (current.astimezone(timezone.utc) - book_time).total_seconds()
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
                continue
            best_ask = float(book.best_ask)
            if best_ask <= 0.0:
                row.update({"status": "no_trade", "reason": "invalid_ask"})
                continue
            if best_ask > self.config.max_ask or best_ask >= 1.0:
                row.update({"status": "no_trade", "reason": "ask_above_limit"})
                continue
            tick = book.tick_size or market.tick_size
            min_order_size = book.min_order_size or market.min_order_size
            if self.config.require_market_constraints and (
                tick is None or tick <= 0 or min_order_size is None or min_order_size <= 0
            ):
                row.update({"status": "no_trade", "reason": "market_constraints_missing"})
                continue
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
                continue
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
                continue
            execution_price = float(fill["vwap"] or 0.0)
            fee_rate = market.fee_rate
            if fee_rate is None:
                row.update({"status": "no_trade", "reason": "fee_unknown"})
                continue
            fee = float(fee_rate) * execution_price * (1.0 - execution_price)
            gross = 1.0 - execution_price
            edge = gross - fee
            row["economics"] = {
                "best_ask": best_ask,
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
            }
            if edge < self.config.min_net_edge:
                row.update({"status": "no_trade", "reason": "edge_below_minimum"})
                continue
            row.update(
                {
                    "status": "opportunity",
                    "reason": "dry_run_candidate_only",
                    "dry_run": True,
                    "lifecycle": {
                        "state": "BOOK_READY",
                        "history": [
                            "DISCOVERED",
                            "WINDOW_CLOSED",
                            "SOURCE_FINAL",
                            "RULE_MATCHED",
                            "BOOK_READY",
                        ],
                    },
                }
            )

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
            else:
                group_source_status = "provisional"
            group_views.append(
                {
                    "event_group_id": event_group_id,
                    "question": (first.get("market") or {}).get("question", ""),
                    "station_id": (observations[0] if observations else {}).get("station_id", ""),
                    "unit": (observations[0] if observations else {}).get("unit", ""),
                    "metric": (observations[0] if observations else {}).get("aggregation", ""),
                    "source_status": group_source_status,
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
            "dry_run": True,
            "live_orders_submitted": 0,
        }
        return {
            "summary": summary,
            "rows": rows,
            "event_groups": group_views,
            "books": {token_id: self._book(value, token_id).to_dict() for token_id, value in books.items()},
        }


def _status_counts(rows: Iterable[dict[str, Any]]) -> dict[str, int]:
    result: dict[str, int] = {}
    for row in rows:
        key = str(row.get("status") or "unknown")
        result[key] = result.get(key, 0) + 1
    return result

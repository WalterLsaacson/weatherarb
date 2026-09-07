"""Stateful standalone weather scanner service used by the 8793 board."""

from __future__ import annotations

import copy
import json
import queue
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .books import ClobClient
from .markets import (
    GammaClient,
    catalog_event_groups,
    filter_catalog_groups_in_horizon,
    filter_market_rows_in_horizon,
    flatten_event_markets,
    load_market_rows,
    merge_event_groups,
    observation_in_horizon,
    snapshot_payload,
    weather_markets,
)
from .discovery import discover_rules
from .models import WeatherMarket, parse_time
from .rules import (
    RuleError,
    bucket_for_outcome,
    bucket_impossible_while_open,
    load_rules,
    load_timezone,
    norm_outcome,
    normalize_market,
    parse_bucket,
    parse_rule,
)
from .scanner import WeatherScanner, WeatherScannerConfig, extrema_from_rows
from .sources import JsonHttp, series_from_raw
from .storage import append_jsonl, append_jsonl_dedup, load_json, write_json_atomic


def slim_board_groups(groups: Any) -> list[dict[str, Any]]:
    """Compact groups for the event list and SSE. Detail still reads last_result."""

    slim: list[dict[str, Any]] = []
    if not isinstance(groups, list):
        return slim
    for group in groups:
        if not isinstance(group, dict):
            continue
        slim.append(
            {
                "event_group_id": group.get("event_group_id"),
                "question": group.get("question"),
                "station_id": group.get("station_id"),
                "unit": group.get("unit"),
                "metric": group.get("metric"),
                "source_status": group.get("source_status"),
                "bucket_match_consistent": group.get("bucket_match_consistent"),
                "matched_bucket": group.get("matched_bucket"),
                "market_count": group.get("market_count"),
                "matched_market_count": group.get("matched_market_count"),
                "candidate_count": group.get("candidate_count"),
                "status_counts": group.get("status_counts"),
                "rows": _list_rows_for_board(group),
                "markets": [],
            }
        )
    return slim


def _list_rows_for_board(group: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [row for row in group.get("rows") or [] if isinstance(row, dict)]
    if not rows:
        return []
    chosen = next((row for row in rows if row.get("status") == "opportunity"), rows[0])
    return [_list_row(chosen)]


def _compact_book(book: Any) -> dict[str, Any]:
    if not isinstance(book, dict):
        return {}
    return {
        "best_ask": book.get("best_ask"),
        "best_bid": book.get("best_bid"),
        "fetched_at": book.get("fetched_at"),
        "book_missing": book.get("book_missing"),
        "error": book.get("error"),
    }


def _list_row(row: dict[str, Any]) -> dict[str, Any]:
    observation = row.get("observation") if isinstance(row.get("observation"), dict) else {}
    market = row.get("market") if isinstance(row.get("market"), dict) else {}
    rule = row.get("rule") if isinstance(row.get("rule"), dict) else {}
    source = rule.get("source") if isinstance(rule.get("source"), dict) else {}
    economics = row.get("economics") if isinstance(row.get("economics"), dict) else {}
    return {
        "market_id": row.get("market_id"),
        "event_group_id": row.get("event_group_id"),
        "status": row.get("status"),
        "reason": row.get("reason"),
        "trade_side": row.get("trade_side"),
        "target_outcome": row.get("target_outcome"),
        "matched_outcome": row.get("matched_outcome"),
        "lifecycle": row.get("lifecycle"),
        "economics": {
            "execution_price": economics.get("execution_price"),
            "net_edge": economics.get("net_edge"),
        },
        "book": _compact_book(row.get("book")),
        "observation": {
            "status": observation.get("status"),
            "provider": observation.get("provider"),
            "station_id": observation.get("station_id"),
            "value": observation.get("value"),
            "unit": observation.get("unit"),
            "aggregation": observation.get("aggregation"),
            "reason": observation.get("reason"),
            "observed_at": observation.get("observed_at"),
            "source_timestamp": observation.get("source_timestamp"),
        },
        "market": {
            "question": market.get("question"),
            "outcome": market.get("outcome"),
            "market_id": market.get("market_id"),
        },
        "rule": {
            "timezone": rule.get("timezone"),
            "observation_start": rule.get("observation_start"),
            "metric": rule.get("metric"),
            "unit": rule.get("unit"),
            "manual_approval": rule.get("manual_approval"),
            "source": {"station_id": source.get("station_id")},
        },
    }


def _list_market(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "market_id": row.get("market_id"),
        "outcome": row.get("outcome"),
        "trade_side": row.get("trade_side"),
        "status": row.get("status"),
        "reason": row.get("reason"),
        "book": _compact_book(row.get("book")),
    }


def _take_matches(
    rows: list[dict[str, Any]],
    event_group_id: str,
    target_outcome: str,
) -> list[dict[str, Any]]:
    wanted_event = event_group_id.strip().lower().rstrip("/")
    wanted_outcome = norm_outcome(target_outcome) if target_outcome else ""
    found: list[dict[str, Any]] = []
    for row in rows:
        gid = str(row.get("event_group_id") or "").strip().lower().rstrip("/")
        if gid != wanted_event and wanted_event not in gid:
            continue
        if wanted_outcome:
            actual = norm_outcome(row.get("target_outcome") or row.get("matched_outcome"))
            if actual != wanted_outcome:
                continue
        found.append(row)
    if wanted_outcome or len(found) <= 1:
        return found
    opportunities = [row for row in found if row.get("status") == "opportunity"]
    return opportunities or found


_LOCKED_NO_REASONS = {
    "intraday_impossible_no",
    "provisional_loser_no",
    "source_final_loser_no",
}


def _lock_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    observation = row.get("observation") if isinstance(row.get("observation"), dict) else {}
    rule = row.get("rule") if isinstance(row.get("rule"), dict) else {}
    buckets = []
    for item in rule.get("buckets") or []:
        if isinstance(item, dict):
            try:
                buckets.append(parse_bucket(item))
            except Exception:  # noqa: BLE001
                continue
    target = bucket_for_outcome(row.get("target_outcome") or row.get("matched_outcome"), buckets)
    metric = str(rule.get("metric") or "")
    rounding = str(rule.get("rounding") or "whole_degree_as_published")
    running = observation.get("value")
    impossible = bool(
        target is not None
        and metric in {"daily_min", "daily_max"}
        and bucket_impossible_while_open(metric, running, target, rounding=rounding)
    )
    reason = str(row.get("reason") or "")
    side = str(row.get("trade_side") or "").upper()
    return {
        "locked": bool(
            side == "NO" and reason in _LOCKED_NO_REASONS and impossible
        ),
        "trade_side": side,
        "reason": reason,
        "metric": metric,
        "running_value": running,
        "raw_aggregate": observation.get("raw_aggregate"),
        "observation_status": observation.get("status"),
        "station_id": observation.get("station_id"),
        "target_outcome": row.get("target_outcome"),
        "target_bucket": target.to_dict() if target is not None else None,
        "bucket_impossible": impossible,
        "provider": observation.get("provider"),
    }


_FILL_STATUSES = {"submitted", "matched", "live"}


def _fill_keys(record: Any) -> set[str]:
    if not isinstance(record, dict) or record.get("dry_run"):
        return set()
    status = str(record.get("status") or "")
    exchange = record.get("exchange") if isinstance(record.get("exchange"), dict) else {}
    filled = status in _FILL_STATUSES or bool(exchange.get("ok")) or str(exchange.get("status") or "") in _FILL_STATUSES
    if not filled:
        return set()
    keys: set[str] = set()
    token_id = str(record.get("token_id") or "")
    if token_id:
        keys.add(token_id)
    event_id = str(record.get("event_group_id") or "")
    outcome = str(record.get("target_outcome") or "")
    if event_id and outcome:
        keys.add(event_id + "|" + outcome)
    return keys


def _instrument_keys(row: dict[str, Any], token_id: str = "") -> set[str]:
    keys: set[str] = set()
    token = str(token_id or row.get("book_token_id") or row.get("winning_token_id") or row.get("token_id") or "")
    if token:
        keys.add(token)
    event_id = str(row.get("event_group_id") or "")
    outcome = str(row.get("target_outcome") or "")
    if event_id and outcome:
        keys.add(event_id + "|" + outcome)
    return keys


def _slim_row(row: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    observation = item.get("observation")
    if isinstance(observation, dict) and "raw" in observation:
        observation = dict(observation)
        observation.pop("raw", None)
        item["observation"] = observation
    market = item.get("market")
    if isinstance(market, dict) and "raw" in market:
        market = dict(market)
        market.pop("raw", None)
        item["market"] = market
    return item


def _slim_market_view(row: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    book = item.get("book")
    if isinstance(book, dict) and "raw" in book:
        book = dict(book)
        book.pop("raw", None)
        item["book"] = book
    return item


def _series_in_group_window(group: dict[str, Any], points: list[dict[str, Any]]) -> list[dict[str, Any]]:
    row = next((item for item in group.get("rows") or [] if isinstance(item, dict)), {})
    rule = row.get("rule") if isinstance(row.get("rule"), dict) else {}
    tz_name = str(rule.get("timezone") or "UTC")
    try:
        zone = load_timezone(tz_name)
    except RuleError:
        zone = timezone.utc
    start = parse_time(rule.get("observation_start"), default_tz=zone)
    end = parse_time(rule.get("observation_end"), default_tz=zone)
    if start is None and end is None:
        return points
    filtered: list[dict[str, Any]] = []
    for point in points:
        if not isinstance(point, dict):
            continue
        stamp = parse_time(point.get("timestamp") or point.get("local_time"), default_tz=zone)
        if stamp is None:
            continue
        if start is not None and stamp < start:
            continue
        if end is not None and stamp > end:
            continue
        filtered.append(point)
    return filtered


class RuntimeService:
    """Owns periodic scans, last snapshot and bounded SSE subscriber queues."""

    def __init__(
        self,
        *,
        root: Path,
        data_dir: Optional[Path] = None,
        markets_file: Optional[Path] = None,
        rules_file: Optional[Path] = None,
        books_file: Optional[Path] = None,
        fixture: Optional[Path] = None,
        proxy: Optional[str] = None,
        interval_s: float = 60.0,
        dry_run: bool = True,
        sync: bool = False,
        scanner_config: Optional[WeatherScannerConfig] = None,
        horizon_hours: float = 24.0,
    ):
        self.root = Path(root).resolve()
        self.data_dir = (data_dir or self.root / "data").resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.fixture = Path(fixture).resolve() if fixture else None
        if self.fixture and self.fixture.is_dir():
            self.markets_file = self.fixture / "markets.json"
            self.rules_file = self.fixture / "rules.json"
            self.books_file = self.fixture / "books.json"
        else:
            self.markets_file = markets_file or self.data_dir / "market_snapshot.json"
            self.rules_file = rules_file or self.data_dir / "rules.json"
            self.books_file = books_file
        self.proxy = proxy
        self.interval_s = max(1.0, float(interval_s))
        self.dry_run = bool(dry_run)
        self.sync_enabled = bool(sync)
        self.horizon_hours = max(0.0, float(horizon_hours))
        self.catalog_ttl_s = 900.0
        self.catalog_groups: list[dict[str, Any]] = []
        self._all_catalog_groups: list[dict[str, Any]] = []
        self._catalog_at = 0.0
        self.http = JsonHttp(proxy=proxy)
        self.scanner = WeatherScanner(
            config=scanner_config,
            source_adapter=None,
            clob_client=None,
        )
        # Keep all source/CLOB calls on the explicitly configured HTTP client.
        from .sources import WeatherSourceAdapter

        self.scanner.source_adapter = WeatherSourceAdapter(http=self.http)
        self.scanner.clob_client = ClobClient(http=self.http)
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.running = False
        self.ticks = 0
        self.last_error = ""
        self.last_scan_at = ""
        self.last_result: dict[str, Any] = {
            "summary": {
                "scanned_at": "",
                "markets": 0,
                "rules": 0,
                "event_groups": 0,
                "market_match_rate": 0.0,
                "bucket_match_rate": 0.0,
                "book_ready_rate": 0.0,
                "opportunities": 0,
                "dry_run": self.dry_run,
                "live_orders_submitted": 0,
            },
            "rows": [],
            "event_groups": [],
            "books": {},
        }
        self._next_event_id = 0
        self._subscribers: dict[int, queue.Queue[dict[str, Any]]] = {}
        self._subscriber_id = 0
        self._history: deque[dict[str, Any]] = deque(maxlen=64)
        self._gamma_sync_error = ""
        self._series_index: Optional[dict[str, list[dict[str, Any]]]] = None
        self.scan_in_progress = False
        self._extrema: dict[str, dict[str, Any]] = {}
        self._taken_tokens: set[str] = set()
        self._hydrate_last_result()
        self._hydrate_taken()

    def _catalog_path(self) -> Path:
        return self.data_dir / "weather_catalog.json"

    def _hydrate_last_result(self) -> None:
        if self.fixture:
            return
        latest = load_json(self.data_dir / "latest.json", default={})
        if not isinstance(latest, dict):
            return
        groups = latest.get("event_groups")
        if not isinstance(groups, list) or not groups:
            return
        latest.pop("source_evidence", None)
        self.last_result = latest
        self.last_scan_at = str((latest.get("summary") or {}).get("scanned_at") or "")
        self._extrema = extrema_from_rows(latest.get("rows") or [])

    def _hydrate_taken(self) -> None:
        path = self.data_dir / "orders.jsonl"
        if not path.is_file():
            return
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    try:
                        record = json.loads(line)
                    except ValueError:
                        continue
                    keys = _fill_keys(record)
                    if keys:
                        self._taken_tokens.update(keys)
        except OSError:
            return

    def _disk_market_rows(self) -> list[dict[str, Any]]:
        if self.markets_file and Path(self.markets_file).is_file():
            return load_market_rows(str(self.markets_file))
        snapshot = self.data_dir / "market_snapshot.json"
        if snapshot.is_file():
            return load_market_rows(str(snapshot))
        return []

    def _ensure_catalog_from_disk(self) -> None:
        if not self._all_catalog_groups:
            self._all_catalog_groups = self._load_catalog_from_disk()

    def _horizon_now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _scan_now(self) -> datetime:
        if self.fixture:
            return datetime(2026, 9, 5, 1, tzinfo=timezone.utc)
        return datetime.now(timezone.utc)

    def _apply_horizon_to_catalog(self) -> list[dict[str, Any]]:
        self.catalog_groups = filter_catalog_groups_in_horizon(
            self._all_catalog_groups,
            now=self._horizon_now(),
            past_hours=self.horizon_hours,
            future_hours=self.horizon_hours,
        )
        return self.catalog_groups

    def _load_catalog_from_disk(self) -> list[dict[str, Any]]:
        payload = load_json(self._catalog_path(), default={})
        groups = payload.get("event_groups") if isinstance(payload, dict) else []
        return [item for item in groups if isinstance(item, dict)] if isinstance(groups, list) else []

    def _write_weather_catalog(
        self,
        events: list[dict[str, Any]],
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        groups = catalog_event_groups(events)
        counts: dict[str, int] = {}
        for group in groups:
            kind = str(group.get("metric") or "other")
            counts[kind] = counts.get(kind, 0) + 1
        write_json_atomic(
            self._catalog_path(),
            {
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "source": "gamma-events-weather",
                "event_count": len(events),
                "market_count": len(rows),
                "kind_counts": counts,
                "horizon_hours": self.horizon_hours,
                "event_groups": groups,
            },
        )
        return groups

    def _load_markets(self) -> list[dict[str, Any]]:
        now = time.time()
        catalog_stale = (now - self._catalog_at) >= self.catalog_ttl_s or not self._all_catalog_groups
        rows: list[dict[str, Any]] = []
        if self.sync_enabled and catalog_stale:
            self._ensure_catalog_from_disk()
            disk_rows = self._disk_market_rows()
            # First process start: use the on-disk snapshot immediately so the
            # board is not blocked on a Gamma timeout.
            if self._catalog_at <= 0.0 and (disk_rows or self._all_catalog_groups):
                rows = disk_rows
                self._catalog_at = now
            else:
                try:
                    events = GammaClient(http=self.http).list_events(tag_slug="weather", max_pages=20)
                    rows = flatten_event_markets(events)
                    self._all_catalog_groups = self._write_weather_catalog(events, rows)
                    self._catalog_at = now
                    write_json_atomic(self.data_dir / "market_snapshot.json", snapshot_payload(rows))
                    self._gamma_sync_error = ""
                except Exception as exc:  # noqa: BLE001
                    self._gamma_sync_error = str(exc)
                    self._ensure_catalog_from_disk()
                    rows = self._disk_market_rows()
                    if not rows and not self._all_catalog_groups:
                        raise
                    self._catalog_at = now
        else:
            self._ensure_catalog_from_disk()
            if self._all_catalog_groups and not self._catalog_at:
                self._catalog_at = now
            rows = self._disk_market_rows()
        if self.fixture:
            return rows
        self._apply_horizon_to_catalog()
        return filter_market_rows_in_horizon(
            rows,
            now=self._horizon_now(),
            past_hours=self.horizon_hours,
            future_hours=self.horizon_hours,
        )

    def _load_rules(self, markets: Optional[list] = None):
        operator = []
        if self.rules_file and self.rules_file.is_file():
            try:
                operator = load_rules(self.rules_file)
            except (OSError, ValueError, RuleError) as exc:
                raise RuleError("cannot load rules: {}".format(exc)) from exc
        if self.fixture:
            return operator
        discovered = discover_rules(markets or [])
        generated_rows = discovered.get("generated_rules") or []
        write_json_atomic(
            self.data_dir / "rules.auto.json",
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "summary": discovered.get("summary") or {},
                "review": discovered.get("review") or [],
                "rules": generated_rows,
            },
        )
        auto = []
        for row in generated_rows:
            try:
                auto.append(parse_rule(row))
            except RuleError:
                continue
        merged = {rule.market_id: rule for rule in auto}
        for rule in operator:
            merged[rule.market_id] = rule
        rules = list(merged.values())
        if self.fixture:
            return rules
        now = self._horizon_now()
        return [
            rule
            for rule in rules
            if observation_in_horizon(
                str(rule.observation_start or "")[:10],
                rule.timezone,
                now=now,
                past_hours=self.horizon_hours,
                future_hours=self.horizon_hours,
            )
        ]

    def _load_books(self) -> Optional[dict[str, Any]]:
        if not self.books_file or not self.books_file.is_file():
            return None
        payload = load_json(self.books_file, default={})
        if not isinstance(payload, dict):
            return {}
        if self.fixture:
            # Fixture books are replay inputs without an exchange capture
            # clock. Stamp them at load time so the normal book TTL gate is
            # still applied to every subsequent scan.
            stamp = self._scan_now().isoformat()
            return {
                str(token_id): (
                    {**book, "fetched_at": stamp}
                    if isinstance(book, dict) and not book.get("fetched_at")
                    else book
                )
                for token_id, book in payload.items()
            }
        return payload

    def _rotate_jsonl(self, path: Path, *, limit_bytes: int = 32 * 1024 * 1024) -> None:
        if path.is_file() and path.stat().st_size > limit_bytes:
            rotated = path.with_name(path.name + ".old")
            try:
                path.replace(rotated)
            except OSError:
                try:
                    path.unlink()
                except OSError:
                    pass

    def _persist(self, result: dict[str, Any]) -> None:
        stamp = (result.get("summary") or {}).get("scanned_at") or datetime.now(timezone.utc).isoformat()
        evidence = list(result.pop("source_evidence", []) or [])
        write_json_atomic(self.data_dir / "latest.json", result)
        write_json_atomic(
            self.data_dir / "health.json",
            {
                "ok": not bool(self.last_error),
                "running": self.running,
                "ticks": self.ticks,
                "last_scan_at": stamp,
                "last_error": self.last_error,
                "dry_run": self.dry_run,
            },
            indent=2,
        )
        rows = result.get("rows") or []
        scan_path = self.data_dir / "scan.jsonl"
        self._rotate_jsonl(scan_path)
        append_jsonl_dedup(
            scan_path,
            [
                {"scan_at": stamp, **row}
                for row in rows
                if row.get("status") not in {"waiting"}
            ],
            key_fn=lambda row: json.dumps(
                {
                    "market_id": row.get("market_id"),
                    "rule_version": row.get("rule_version"),
                    "status": row.get("status"),
                    "evidence": (row.get("observation") or {}).get("evidence_hash"),
                    "book": (row.get("book") or {}).get("fetched_at"),
                },
                sort_keys=True,
            ),
        )
        append_jsonl_dedup(
            self.data_dir / "candidates.jsonl",
            [
                {"scan_at": stamp, **row}
                for row in rows
                if row.get("status") == "opportunity"
            ],
            key_fn=lambda row: json.dumps(
                {
                    "market_id": row.get("market_id"),
                    "rule_version": row.get("rule_version"),
                    "evidence": (row.get("observation") or {}).get("evidence_hash"),
                    "execution_price": (row.get("economics") or {}).get("execution_price"),
                },
                sort_keys=True,
            ),
        )
        self._rotate_jsonl(self.data_dir / "source_observations.jsonl")
        append_jsonl_dedup(
            self.data_dir / "source_observations.jsonl",
            [{"scan_at": stamp, **item} for item in evidence],
            key_fn=lambda row: "{}|{}|{}".format(
                row.get("event_group_id"), row.get("evidence_hash"), row.get("source_timestamp")
            ),
        )
        self._series_index = None

    def scan_once(self) -> dict[str, Any]:
        self.scan_in_progress = True
        try:
            return self._scan_once()
        finally:
            self.scan_in_progress = False

    def _scan_once(self) -> dict[str, Any]:
        try:
            raw_markets = self._load_markets()
            markets = weather_markets(raw_markets)
        except Exception as exc:  # noqa: BLE001
            with self.lock:
                self.last_error = str(exc)
                self.last_scan_at = datetime.now(timezone.utc).isoformat()
            self._publish("health_update", self.status())
            raise
        try:
            rules = self._load_rules(markets)
            books = self._load_books()
        except Exception as exc:  # noqa: BLE001
            with self.lock:
                self.last_error = str(exc)
                self.last_scan_at = datetime.now(timezone.utc).isoformat()
            self._publish("health_update", self.status())
            raise
        with self.lock:
            previous_extrema = dict(self._extrema)
        scan_now = self._scan_now()
        result = self.scanner.scan(
            markets,
            rules,
            books=books,
            now=scan_now,
            fetch_books=books is None,
            previous_extrema=previous_extrema,
        )
        result["summary"]["proxy"] = self.http.proxy or "direct"
        result["summary"]["data_dir"] = str(self.data_dir)
        catalog = [] if self.fixture else list(self.catalog_groups)
        if catalog:
            scanned = result.get("event_groups") or []
            merged = merge_event_groups(catalog, scanned)
            catalog_only = max(0, len(merged) - len(scanned))
            result["event_groups"] = merged
            result["summary"]["event_groups"] = len(merged)
            result["summary"]["catalog_events"] = len(catalog)
            result["summary"]["catalog_markets"] = sum(
                int(group.get("market_count") or 0) for group in catalog
            )
            result["summary"]["catalog_only"] = catalog_only
            result["summary"]["review"] = int(result["summary"].get("review") or 0) + catalog_only
            result["summary"]["approved_event_groups"] = len(scanned)
            result["summary"]["horizon_hours"] = self.horizon_hours
            result["summary"]["catalog_events_unfiltered"] = len(self._all_catalog_groups)
        with self.lock:
            self.ticks += 1
            self.last_error = ""
            self.last_scan_at = str((result.get("summary") or {}).get("scanned_at") or "")
            self.last_result = result
            self._extrema = extrema_from_rows(result.get("rows") or [])
        self._persist(result)
        self._publish("snapshot", self.board_snapshot())
        self._publish(
            "source_update",
            {
                "scanned_at": self.last_scan_at,
                "event_groups": slim_board_groups(result.get("event_groups") or []),
            },
        )
        candidates = [
            _slim_row(row)
            for row in result.get("rows") or []
            if isinstance(row, dict) and row.get("status") == "opportunity"
        ]
        self._publish(
            "candidate_update",
            {
                "scanned_at": self.last_scan_at,
                "candidates": candidates,
            },
        )
        takes = self._auto_take_opportunities(result)
        if takes:
            self._publish("health_update", self.status())
        return result

    def _loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.scan_once()
            except Exception:
                self._publish("health_update", self.status())
            if self.stop_event.wait(self.interval_s):
                break
        with self.lock:
            self.running = False
        self._publish("health_update", self.status())

    def start(self, *, scan_now: bool = True) -> dict[str, Any]:
        with self.lock:
            if self.running:
                return self.status()
            self.stop_event.clear()
            self.running = True
            self.thread = threading.Thread(target=self._loop, name="weather-scan", daemon=True)
            self.thread.start()
        if scan_now:
            # The loop performs its own scan; waiting briefly here makes the
            # start endpoint useful for a local operator without blocking long.
            time.sleep(0.02)
        self._publish("health_update", self.status())
        return self.status()

    def stop(self) -> dict[str, Any]:
        self.stop_event.set()
        with self.lock:
            thread = self.thread
            self.running = False
        if thread and thread.is_alive():
            thread.join(timeout=2.0)
        self._publish("health_update", self.status())
        return self.status()

    def snapshot(self) -> dict[str, Any]:
        return self.board_snapshot()

    def current_books(self) -> dict[str, Any]:
        with self.lock:
            books = self.last_result.get("books") or {}
            return books if isinstance(books, dict) else {}

    def board_snapshot(self) -> dict[str, Any]:
        with self.lock:
            result = self.last_result
            return {
                "summary": dict(result.get("summary") or {}),
                "event_groups": slim_board_groups(result.get("event_groups") or []),
                "status": self.status(),
            }

    def overview(self) -> dict[str, Any]:
        with self.lock:
            return {
                "ok": not bool(self.last_error),
                "service": self.status(),
                "summary": dict(self.last_result.get("summary") or {}),
            }

    def events(self, *, status: str = "", query: str = "", limit: int = 200) -> list[dict[str, Any]]:
        with self.lock:
            groups = slim_board_groups(self.last_result.get("event_groups") or [])
        status = status.strip().lower()
        query = query.strip().lower()
        result = []
        for group in groups:
            if status and status not in json.dumps(group, ensure_ascii=False).lower():
                continue
            if query and query not in json.dumps(group, ensure_ascii=False).lower():
                continue
            result.append(group)
        return result[: max(1, min(int(limit), 1000))]

    def _source_series_index(self) -> dict[str, list[dict[str, Any]]]:
        if self._series_index is not None:
            return self._series_index
        index: dict[str, list[dict[str, Any]]] = {}
        path = self.data_dir / "source_observations.jsonl"
        if path.is_file():
            try:
                with path.open("r", encoding="utf-8") as handle:
                    for line in handle:
                        if not line.strip():
                            continue
                        try:
                            row = json.loads(line)
                        except ValueError:
                            continue
                        if not isinstance(row, dict):
                            continue
                        points = row.get("series")
                        if not isinstance(points, list) or not points:
                            points = series_from_raw(row.get("raw"))
                        if not points:
                            continue
                        event_id = str(row.get("event_group_id") or "")
                        if event_id:
                            index[event_id] = points
            except OSError:
                pass
        self._series_index = index
        return index

    def _lookup_source_series(self, group: dict[str, Any]) -> list[dict[str, Any]]:
        if "source_series" in group and isinstance(group.get("source_series"), list):
            return group["source_series"]
        index = self._source_series_index()
        event_id = str(group.get("event_group_id") or "")
        points = index.get(event_id) if event_id else None
        if not isinstance(points, list) or not points:
            return []
        return _series_in_group_window(group, points)

    def event_detail(self, event_group_id: str) -> Optional[dict[str, Any]]:
        wanted = str(event_group_id).strip()
        with self.lock:
            for group in self.last_result.get("event_groups") or []:
                if str(group.get("event_group_id")) != wanted:
                    continue
                item = dict(group)
                item["source_series"] = self._lookup_source_series(item)
                return item
        return None

    def candidates(self, *, limit: int = 200) -> list[dict[str, Any]]:
        with self.lock:
            rows = [
                _slim_row(row)
                for row in self.last_result.get("rows") or []
                if isinstance(row, dict) and row.get("status") == "opportunity"
            ]
        return rows[: max(1, min(int(limit), 1000))]

    def status(self) -> dict[str, Any]:
        with self.lock:
            summary = self.last_result.get("summary") or {}
            return {
                "module": "weather-board",
                "version": "0.1.0",
                "running": self.running,
                "dry_run": self.dry_run,
                "sync": self.sync_enabled,
                "horizon_hours": self.horizon_hours,
                "interval_s": self.interval_s,
                "ticks": self.ticks,
                "scan_in_progress": self.scan_in_progress,
                "last_scan_at": self.last_scan_at,
                "last_error": self.last_error,
                "data_dir": str(self.data_dir),
                "markets_file": str(self.markets_file) if self.markets_file else "",
                "rules_file": str(self.rules_file) if self.rules_file else "",
                "books_file": str(self.books_file) if self.books_file else "",
                "proxy": self.http.proxy or "direct",
                "circuit": "OPEN" if self.last_error else "CLOSED",
                "gamma_sync_error": self._gamma_sync_error,
                "summary": summary,
                "trading": self.trading_status(),
                "taken_tokens": len(self._taken_tokens),
            }

    def trading_status(self) -> dict[str, Any]:
        from .env import public_trading_status

        status = public_trading_status()
        status["taken_tokens"] = len(self._taken_tokens)
        return status

    def _auto_take_opportunities(self, result: dict[str, Any]) -> list[dict[str, Any]]:
        from .env import trading_config

        cfg = trading_config()
        if not cfg["live_orders"] or self.fixture:
            return []
        takes: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in result.get("rows") or []:
            if not isinstance(row, dict) or row.get("status") != "opportunity":
                continue
            token_id = str(row.get("book_token_id") or row.get("winning_token_id") or "")
            if not token_id:
                continue
            keys = _instrument_keys(row, token_id)
            if keys & self._taken_tokens or token_id in seen:
                continue
            seen.add(token_id)
            event_id = str(row.get("event_group_id") or "")
            outcome = str(row.get("target_outcome") or "")
            print(
                "LIVE auto-take {} {} {}".format(event_id, outcome, row.get("trade_side")),
                flush=True,
            )
            takes.append(
                self.take_opportunity(
                    event_group_id=event_id,
                    target_outcome=outcome,
                    live=True,
                    require_locked_no=True,
                )
            )
        return takes

    def take_opportunity(
        self,
        *,
        event_group_id: str,
        target_outcome: str = "",
        live: bool = False,
        require_locked_no: bool = True,
    ) -> dict[str, Any]:
        from .env import trading_config
        from .models import utc_now
        from .orders import LiveOrderError, quantize_buy, submit_fak_buy

        cfg = trading_config()
        wanted_event = str(event_group_id or "").strip()
        wanted_outcome = str(target_outcome or "").strip()
        if not wanted_event:
            return {"ok": False, "error": "event_required"}
        with self.lock:
            rows = [row for row in self.last_result.get("rows") or [] if isinstance(row, dict)]
        matches = _take_matches(rows, wanted_event, wanted_outcome)
        if not matches:
            return {
                "ok": False,
                "error": "row_not_found",
                "event_group_id": wanted_event,
                "target_outcome": wanted_outcome,
            }
        if len(matches) > 1:
            return {
                "ok": False,
                "error": "ambiguous_outcome",
                "matches": [
                    {
                        "event_group_id": row.get("event_group_id"),
                        "target_outcome": row.get("target_outcome"),
                        "status": row.get("status"),
                    }
                    for row in matches[:20]
                ],
            }
        row = copy.deepcopy(matches[0])
        token_id = str(row.get("book_token_id") or row.get("winning_token_id") or "")
        if not token_id:
            return {"ok": False, "error": "missing_token", "row": _slim_row(row)}
        if live and (_instrument_keys(row, token_id) & self._taken_tokens):
            return {
                "ok": False,
                "error": "already_taken",
                "token_id": token_id,
                "event_group_id": row.get("event_group_id"),
                "target_outcome": row.get("target_outcome"),
            }
        market_payload = row.get("market") if isinstance(row.get("market"), dict) else {}
        try:
            market = normalize_market(market_payload)
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": "invalid_market", "detail": str(exc)}
        books = self.scanner.clob_client.fetch_books([token_id])
        row["status"] = "rule_matched"
        original_max_usdc = float(self.scanner.config.max_usdc)
        original_target = float(self.scanner.config.target_shares)
        self.scanner.config.max_usdc = float(cfg["max_order_usdc"])
        self.scanner.config.target_shares = max(
            original_target, float(cfg["max_order_usdc"]) * 1000.0
        )
        try:
            self.scanner._evaluate_matched_row(row, books, {market.market_id: market}, utc_now())
        finally:
            self.scanner.config.max_usdc = original_max_usdc
            self.scanner.config.target_shares = original_target
        if row.get("status") != "opportunity":
            return {
                "ok": False,
                "error": str(row.get("reason") or "not_opportunity"),
                "row": _slim_row(row),
            }
        lock = _lock_snapshot(row)
        if require_locked_no and not lock.get("locked"):
            return {
                "ok": False,
                "error": "not_locked_no",
                "lock": lock,
                "row": _slim_row(row),
            }
        economics = row.get("economics") if isinstance(row.get("economics"), dict) else {}
        price = float(economics.get("worst_price") or economics.get("execution_price") or 0.0)
        size = float(economics.get("execution_shares") or 0.0)
        min_order = float(economics.get("min_order_size") or 0.0)
        if price <= 0.0 or size <= 0.0:
            return {"ok": False, "error": "invalid_size_or_price", "row": _slim_row(row)}
        max_shares = float(cfg["max_order_usdc"]) / price
        size = min(size, max_shares)
        try:
            price, size = quantize_buy(price, size, float(cfg["max_order_usdc"]))
        except LiveOrderError as exc:
            return {"ok": False, "error": str(exc), "price": price, "size": size}
        if min_order > 0 and size + 1e-12 < min_order:
            return {
                "ok": False,
                "error": "max_usdc_below_min_order",
                "max_order_usdc": cfg["max_order_usdc"],
                "min_order_size": min_order,
                "price": price,
            }
        record = {
            "client_order_id": str(uuid.uuid4()),
            "created_at": utc_now().isoformat(),
            "event_group_id": row.get("event_group_id"),
            "market_id": row.get("market_id"),
            "target_outcome": row.get("target_outcome"),
            "trade_side": row.get("trade_side"),
            "token_id": token_id,
            "side": "BUY",
            "order_type": "FAK",
            "price": price,
            "size": size,
            "notional_usdc": round(price * size, 6),
            "economics": economics,
            "reason": row.get("reason"),
            "lock": lock,
            "dry_run": not bool(live),
            "live_requested": bool(live),
        }
        if live:
            if not cfg["live_orders"]:
                record["status"] = "blocked"
                record["error"] = "live_orders_disabled"
                append_jsonl(self.data_dir / "orders.jsonl", [record])
                return {
                    "ok": False,
                    "error": "live_orders_disabled",
                    "hint": "Set LIVE_ORDERS=true in .env, then pass --live",
                    "order": record,
                    "trading": self.trading_status(),
                }
            with self.lock:
                fill_keys = _instrument_keys(row, token_id)
                if fill_keys & self._taken_tokens:
                    return {
                        "ok": False,
                        "error": "already_taken",
                        "token_id": token_id,
                        "event_group_id": row.get("event_group_id"),
                        "target_outcome": row.get("target_outcome"),
                    }
                self._taken_tokens.update(fill_keys)
            try:
                exchange = submit_fak_buy(
                    token_id=token_id,
                    price=price,
                    size=size,
                    config=cfg,
                )
                record["exchange"] = exchange
                if isinstance(exchange, dict):
                    record["status"] = str(exchange.get("status") or "submitted")
                    if exchange.get("order_id"):
                        record["client_order_id"] = str(exchange.get("order_id"))
                else:
                    record["status"] = "submitted"
            except LiveOrderError as exc:
                with self.lock:
                    self._taken_tokens.difference_update(fill_keys)
                record["status"] = "error"
                record["error"] = str(exc)
                append_jsonl(self.data_dir / "orders.jsonl", [record])
                return {"ok": False, "error": str(exc), "order": record}
            except Exception as exc:  # noqa: BLE001
                with self.lock:
                    self._taken_tokens.difference_update(fill_keys)
                record["status"] = "error"
                record["error"] = str(exc)
                append_jsonl(self.data_dir / "orders.jsonl", [record])
                return {"ok": False, "error": str(exc), "order": record}
            with self.lock:
                summary = self.last_result.setdefault("summary", {})
                summary["live_orders_submitted"] = int(summary.get("live_orders_submitted") or 0) + 1
        else:
            record["status"] = "simulated_fak"
        append_jsonl(self.data_dir / "orders.jsonl", [record])
        return {"ok": True, "order": record, "trading": self.trading_status()}

    def _publish(self, event_type: str, data: Any) -> None:
        with self.lock:
            self._next_event_id += 1
            event = {
                "id": str(self._next_event_id),
                "type": event_type,
                "data": data,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            self._history.append(event)
            subscribers = list(self._subscribers.values())
        for channel in subscribers:
            try:
                channel.put_nowait(event)
            except queue.Full:
                try:
                    channel.get_nowait()
                    channel.put_nowait(event)
                except (queue.Empty, queue.Full):
                    pass

    def subscribe(self, last_event_id: int = 0) -> tuple[int, queue.Queue[dict[str, Any]]]:
        channel: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=32)
        with self.lock:
            self._subscriber_id += 1
            ident = self._subscriber_id
            self._subscribers[ident] = channel
            pending = [event for event in self._history if int(event.get("id") or 0) > int(last_event_id or 0)]
        if last_event_id and pending:
            for event in pending[-31:]:
                channel.put(event)
        else:
            channel.put(
                {
                    "id": str(self._next_event_id),
                    "type": "snapshot",
                    "data": self.board_snapshot(),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        return ident, channel

    def unsubscribe(self, ident: int) -> None:
        with self.lock:
            self._subscribers.pop(ident, None)

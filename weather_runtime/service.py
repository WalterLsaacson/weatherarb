"""Stateful standalone weather scanner service used by the 8793 board."""

from __future__ import annotations

import json
import queue
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .books import Book
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
from .models import WeatherMarket
from .rules import RuleError, load_rules, parse_rule
from .scanner import WeatherScanner, WeatherScannerConfig
from .sources import JsonHttp
from .storage import append_jsonl_dedup, load_json, write_json_atomic


def slim_board_groups(groups: Any) -> list[dict[str, Any]]:
    """Drop bulky source/market payloads from the board JSON without copying them."""

    slim: list[dict[str, Any]] = []
    if not isinstance(groups, list):
        return slim
    for group in groups:
        if not isinstance(group, dict):
            continue
        item = dict(group)
        item["rows"] = [_slim_row(row) for row in group.get("rows") or [] if isinstance(row, dict)]
        item["markets"] = [
            _slim_market_view(row) for row in group.get("markets") or [] if isinstance(row, dict)
        ]
        slim.append(item)
    return slim


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
        from .books import ClobClient

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

    def _catalog_path(self) -> Path:
        return self.data_dir / "weather_catalog.json"

    def _horizon_now(self) -> datetime:
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
        if self.sync_enabled and catalog_stale:
            events = GammaClient(http=self.http).list_events(tag_slug="weather", max_pages=20)
            rows = flatten_event_markets(events)
            self._all_catalog_groups = self._write_weather_catalog(events, rows)
            self._catalog_at = now
            write_json_atomic(self.data_dir / "market_snapshot.json", snapshot_payload(rows))
        else:
            if not self._all_catalog_groups:
                self._all_catalog_groups = self._load_catalog_from_disk()
                if self._all_catalog_groups:
                    self._catalog_at = now
            if self.markets_file and self.markets_file.is_file():
                rows = load_market_rows(str(self.markets_file))
            elif self.sync_enabled and (self.data_dir / "market_snapshot.json").is_file():
                rows = load_market_rows(str(self.data_dir / "market_snapshot.json"))
            else:
                rows = []
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
            stamp = datetime.now(timezone.utc).isoformat()
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

    def scan_once(self) -> dict[str, Any]:
        try:
            raw_markets = self._load_markets()
            markets = weather_markets(raw_markets)
        except Exception as exc:  # noqa: BLE001
            with self.lock:
                self.last_error = str(exc)
                self.last_scan_at = datetime.now(timezone.utc).isoformat()
            self._publish("health_update", self.status())
            raise
        with self.lock:
            try:
                rules = self._load_rules(markets)
                books = self._load_books()
            except Exception as exc:  # noqa: BLE001
                self.last_error = str(exc)
                self.last_scan_at = datetime.now(timezone.utc).isoformat()
                self._publish("health_update", self.status())
                raise
        result = self.scanner.scan(
            markets,
            rules,
            books=books,
            now=datetime.now(timezone.utc),
            fetch_books=books is None,
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
            self._persist(result)
        self._publish("snapshot", self.board_snapshot())
        self._publish(
            "source_update",
            {
                "scanned_at": self.last_scan_at,
                "event_groups": slim_board_groups(result.get("event_groups") or []),
            },
        )
        self._publish(
            "candidate_update",
            {
                "scanned_at": self.last_scan_at,
                "candidates": [
                _slim_row(row)
                for row in result.get("rows") or []
                if isinstance(row, dict) and row.get("status") == "opportunity"
            ],
            },
        )
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

    def event_detail(self, event_group_id: str) -> Optional[dict[str, Any]]:
        wanted = str(event_group_id).strip()
        with self.lock:
            for group in self.last_result.get("event_groups") or []:
                if str(group.get("event_group_id")) == wanted:
                    return group
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
                "last_scan_at": self.last_scan_at,
                "last_error": self.last_error,
                "data_dir": str(self.data_dir),
                "markets_file": str(self.markets_file) if self.markets_file else "",
                "rules_file": str(self.rules_file) if self.rules_file else "",
                "books_file": str(self.books_file) if self.books_file else "",
                "proxy": self.http.proxy or "direct",
                "circuit": "OPEN" if self.last_error else "CLOSED",
                "summary": summary,
            }

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

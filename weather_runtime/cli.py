"""Command-line entry points for the independent weather POC."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .books import normalize_book
from .discovery import discover_rules, write_discovery
from .markets import (
    GammaClient,
    flatten_event_markets,
    load_market_rows,
    snapshot_payload,
    weather_markets,
)
from .rules import load_rules
from .scanner import WeatherScanner, WeatherScannerConfig
from .sources import JsonHttp
from .storage import load_json, write_json_atomic


ROOT = Path(__file__).resolve().parents[1]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Standalone weather finality POC. No subcommand starts the board UI and scan loop together."
    )
    sub = parser.add_subparsers(dest="command", required=True)
    discover = sub.add_parser("discover", help="discover conservative rules from Gamma metadata")
    discover.add_argument("--markets", type=Path)
    discover.add_argument("--sync", action="store_true")
    discover.add_argument("--proxy", default=None)
    discover.add_argument("--rules-out", type=Path, default=Path("data/pm-weather/rules.auto.json"))
    discover.add_argument("--review-out", type=Path, default=Path("data/pm-weather/rules.review.json"))
    scan = sub.add_parser("scan", help="run one read-only scan")
    scan.add_argument("--fixture", type=Path)
    scan.add_argument("--markets", type=Path)
    scan.add_argument("--rules", type=Path)
    scan.add_argument("--books", type=Path)
    scan.add_argument("--proxy", default=None)
    scan.add_argument("--output", type=Path, default=Path("data/pm-weather/latest.json"))
    scan.add_argument("--allow-category-fee", action="store_true")
    scan.add_argument("--min-net-edge", type=float, default=0.0075)
    scan.add_argument("--max-ask", type=float, default=0.995)
    take = sub.add_parser("take", help="refresh one candidate book and simulate or submit a FAK buy")
    take.add_argument("--data-dir", type=Path, default=Path("data/pm-weather-live"))
    take.add_argument("--event", required=True, help="event group id or unique substring")
    take.add_argument("--outcome", default="", help="bucket title, e.g. 58-59°F")
    take.add_argument("--live", action="store_true", help="POST a FAK buy; also requires LIVE_ORDERS=true")
    take.add_argument("--allow-unlocked", action="store_true", help="skip the locked-No gate")
    take.add_argument("--proxy", default=None)
    return parser


def discover_command(args: argparse.Namespace) -> int:
    http = JsonHttp(proxy=args.proxy)
    if args.markets:
        rows = load_market_rows(str(args.markets))
    else:
        events = GammaClient(http=http).list_events(tag_slug="weather", max_pages=20)
        rows = flatten_event_markets(events)
        if args.sync:
            snapshot_dir = ROOT / "data" / "pm-weather"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            write_json_atomic(snapshot_dir / "market_snapshot.json", snapshot_payload(rows))
    markets = weather_markets(rows)
    result = discover_rules(markets)
    write_discovery(result, rules_out=args.rules_out, review_out=args.review_out)
    print(json.dumps(result["summary"], ensure_ascii=False, sort_keys=True))
    return 0


def scan_command(args: argparse.Namespace) -> int:
    fixture = args.fixture.resolve() if args.fixture else None
    if fixture:
        markets_path = fixture / "markets.json"
        rules_path = fixture / "rules.json"
        books_path = fixture / "books.json"
    else:
        markets_path = args.markets or Path("data/pm-weather/market_snapshot.json")
        rules_path = args.rules or Path("data/pm-weather/rules.json")
        books_path = args.books
    rows = load_market_rows(str(markets_path))
    rules = load_rules(rules_path)
    books = load_json(books_path, {}) if books_path else None
    if fixture and isinstance(books, dict):
        # Fixture books have no exchange capture timestamp. Mark the replay
        # read time while keeping the production stale-book hard gate active.
        stamp = datetime.now(timezone.utc).isoformat()
        books = {
            str(token_id): (
                {**book, "fetched_at": stamp}
                if isinstance(book, dict) and not book.get("fetched_at")
                else book
            )
            for token_id, book in books.items()
        }
    scanner = WeatherScanner(
        config=WeatherScannerConfig(
            max_ask=args.max_ask,
            min_net_edge=args.min_net_edge,
            require_explicit_fee=not args.allow_category_fee,
        ),
        source_adapter=None,
        clob_client=None,
    )
    from .sources import WeatherSourceAdapter, _WEATHER_HTTP_TIMEOUT_S
    from .books import ClobClient
    scanner.source_adapter = WeatherSourceAdapter(
        http=JsonHttp(proxy=args.proxy, timeout=_WEATHER_HTTP_TIMEOUT_S)
    )
    scanner.clob_client = ClobClient(http=JsonHttp(proxy=args.proxy))
    result = scanner.scan(
        weather_markets(rows),
        rules,
        books=books,
        now=datetime.now(timezone.utc),
        fetch_books=books is None,
    )
    write_json_atomic(args.output, result)
    print(json.dumps(result["summary"], ensure_ascii=False, sort_keys=True))
    return 0


def take_command(args: argparse.Namespace) -> int:
    from .env import load_dotenv, public_trading_status, trading_config
    from .scanner import WeatherScannerConfig
    from .service import RuntimeService

    load_dotenv()
    trading = trading_config()
    service = RuntimeService(
        root=ROOT,
        data_dir=args.data_dir.resolve(),
        proxy=args.proxy,
        sync=False,
        dry_run=not bool(args.live),
        scanner_config=WeatherScannerConfig(
            max_ask=float(trading["max_ask"]),
            max_slippage=float(trading["max_slippage"]),
            min_net_edge=float(trading["min_net_edge"]),
            max_usdc=float(trading["max_order_usdc"]),
            target_shares=max(1.0, float(trading["max_order_usdc"]) * 1000.0),
        ),
    )
    result = service.take_opportunity(
        event_group_id=args.event,
        target_outcome=args.outcome,
        live=bool(args.live),
        require_locked_no=not bool(args.allow_unlocked),
    )
    result["trading"] = public_trading_status()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0 if result.get("ok") else 1


def serve_argv(argv: Optional[list[str]]) -> Optional[list[str]]:
    """Return leftover argv for the combined board+scanner, or None for one-shot commands."""

    raw = list(argv or [])
    if not raw or raw[0] not in {"discover", "scan", "take"}:
        if raw and raw[0] == "serve":
            return raw[1:]
        return raw
    return None


def main(argv: Optional[list[str]] = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    rest = serve_argv(raw)
    if rest is not None:
        from .server import main as serve_main

        return serve_main(rest)
    args = build_parser().parse_args(raw)
    if args.command == "discover":
        return discover_command(args)
    if args.command == "scan":
        return scan_command(args)
    if args.command == "take":
        return take_command(args)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

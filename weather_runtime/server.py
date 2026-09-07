"""HTTP REST + SSE server for the standalone weather board."""

from __future__ import annotations

import argparse
import json
import mimetypes
import sys
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, unquote, urlparse

from .service import RuntimeService


ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ROOT / "weather_board" / "public"
SRC = ROOT / "weather_board" / "src"
HOST = "127.0.0.1"
PORT = 8793
SERVICE: RuntimeService


_DISCONNECT = (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)


def json_response(handler: BaseHTTPRequestHandler, code: int, payload: Any) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    try:
        handler.send_response(code)
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        handler.send_header("Content-Length", str(len(body)))
        handler.send_header("Cache-Control", "no-store")
        handler.send_header("Access-Control-Allow-Origin", "*")
        handler.end_headers()
        handler.wfile.write(body)
    except _DISCONNECT:
        return


def _safe_file(root: Path, relative: str) -> Path | None:
    path = (root / relative).resolve()
    if root.resolve() not in path.parents or not path.is_file():
        return None
    return path


def serve_file(handler: BaseHTTPRequestHandler, path: Path) -> None:
    content_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
    if path.suffix == ".js":
        content_type = "text/javascript; charset=utf-8"
    elif path.suffix == ".css":
        content_type = "text/css; charset=utf-8"
    elif path.suffix == ".html":
        content_type = "text/html; charset=utf-8"
    data = path.read_bytes()
    handler.send_response(200)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(data)))
    handler.send_header("Cache-Control", "no-cache")
    handler.end_headers()
    handler.wfile.write(data)


def sse(handler: BaseHTTPRequestHandler) -> None:
    try:
        last_event_id = int(handler.headers.get("Last-Event-ID") or 0)
    except ValueError:
        last_event_id = 0
    ident, channel = SERVICE.subscribe(last_event_id=last_event_id)
    try:
        handler.send_response(200)
        handler.send_header("Content-Type", "text/event-stream; charset=utf-8")
        handler.send_header("Cache-Control", "no-cache")
        handler.send_header("Connection", "keep-alive")
        handler.send_header("X-Accel-Buffering", "no")
        handler.send_header("Access-Control-Allow-Origin", "*")
        handler.end_headers()
        while True:
            try:
                event = channel.get(timeout=15.0)
            except Exception:
                handler.wfile.write(b": heartbeat\n\n")
                handler.wfile.flush()
                continue
            handler.wfile.write(("id: {}\n".format(event["id"])).encode("utf-8"))
            handler.wfile.write(("event: {}\n".format(event["type"])).encode("utf-8"))
            payload = json.dumps(event["data"], ensure_ascii=False)
            handler.wfile.write(("data: {}\n\n".format(payload)).encode("utf-8"))
            handler.wfile.flush()
    except (BrokenPipeError, ConnectionResetError, OSError):
        pass
    finally:
        SERVICE.unsubscribe(ident)


class Handler(BaseHTTPRequestHandler):
    server_version = "PolymarketWeatherBoard/0.1"

    def handle(self) -> None:
        try:
            super().handle()
        except _DISCONNECT:
            return

    def log_error(self, fmt: str, *args: Any) -> None:
        message = fmt % args if args else str(fmt)
        if "Broken pipe" in message or "Connection reset" in message:
            return
        super().log_error(fmt, *args)

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Last-Event-ID")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        if path in {"/", "/index.html"}:
            file_path = PUBLIC / "index.html"
            if file_path.is_file():
                serve_file(self, file_path)
            else:
                self.send_error(404)
            return
        if path in {"/candidates", "/candidates.html"}:
            file_path = PUBLIC / "candidates.html"
            if file_path.is_file():
                serve_file(self, file_path)
            else:
                self.send_error(404)
            return
        if path.startswith("/src/"):
            file_path = _safe_file(SRC, path[len("/src/") :])
            if file_path:
                serve_file(self, file_path)
            else:
                self.send_error(404)
            return
        if path == "/api/health":
            json_response(
                self,
                200,
                {
                    "ok": not bool(SERVICE.last_error),
                    "module": "weather-board",
                    "version": "0.1.0",
                    "dry_run": SERVICE.dry_run,
                },
            )
            return
        if path == "/api/module":
            meta = ROOT / "weather_board" / "module.json"
            json_response(self, 200 if meta.is_file() else 404, json.loads(meta.read_text()) if meta.is_file() else {"error": "module.json missing"})
            return
        if path == "/api/status":
            json_response(self, 200, SERVICE.status())
            return
        if path == "/api/overview":
            json_response(self, 200, SERVICE.overview())
            return
        if path == "/api/metrics":
            json_response(self, 200, SERVICE.overview().get("summary") or {})
            return
        if path == "/api/source":
            observations = []
            for group in SERVICE.events(limit=1000):
                observations.extend(
                    row.get("observation") or {}
                    for row in group.get("rows") or []
                    if row.get("observation")
                )
            json_response(self, 200, {"fetched_at": SERVICE.last_scan_at, "observations": observations})
            return
        if path == "/api/books":
            json_response(self, 200, {"fetched_at": SERVICE.last_scan_at, "books": SERVICE.current_books()})
            return
        if path == "/api/events":
            status = str((query.get("status") or [""])[0])
            text = str((query.get("q") or [""])[0])
            try:
                limit = int((query.get("limit") or ["200"])[0])
            except ValueError:
                limit = 200
            json_response(
                self,
                200,
                {
                    "fetched_at": SERVICE.last_scan_at,
                    "count": len(SERVICE.events(status=status, query=text, limit=limit)),
                    "events": SERVICE.events(status=status, query=text, limit=limit),
                },
            )
            return
        if path.startswith("/api/events/"):
            tail = [unquote(part) for part in path.split("/") if part]
            if len(tail) < 3:
                self.send_error(404)
                return
            event_group_id = tail[2]
            detail = SERVICE.event_detail(event_group_id)
            if detail is None:
                json_response(self, 404, {"ok": False, "error": "event_not_found"})
                return
            if len(tail) == 3:
                json_response(self, 200, detail)
                return
            if tail[3] == "source":
                json_response(
                    self,
                    200,
                    {
                        "event_group_id": event_group_id,
                        "observations": [
                            row.get("observation") or {}
                            for row in detail.get("rows") or []
                            if row.get("observation")
                        ],
                    },
                )
                return
            if tail[3] == "books":
                json_response(
                    self,
                    200,
                    {
                        "event_group_id": event_group_id,
                        "books": [
                            {
                                "market_id": row.get("market_id"),
                                "outcome": row.get("target_outcome"),
                                "book": row.get("book") or {},
                            }
                            for row in detail.get("rows") or []
                        ],
                    },
                )
                return
            self.send_error(404)
            return
        if path == "/api/candidates":
            try:
                limit = int((query.get("limit") or ["200"])[0])
            except ValueError:
                limit = 200
            json_response(self, 200, {"candidates": SERVICE.candidates(limit=limit)})
            return
        if path == "/api/stream":
            sse(self)
            return
        self.send_error(404)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/scan/once":
            try:
                SERVICE.scan_once()
                json_response(self, 200, SERVICE.overview())
            except _DISCONNECT:
                return
            except Exception as exc:  # noqa: BLE001
                json_response(self, 502, {"ok": False, "error": str(exc)})
            return
        if parsed.path == "/api/start":
            json_response(self, 200, SERVICE.start())
            return
        if parsed.path == "/api/stop":
            json_response(self, 200, SERVICE.stop())
            return
        self.send_error(404)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Standalone Polymarket weather board")
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--markets", type=Path, default=None)
    parser.add_argument("--rules", type=Path, default=None)
    parser.add_argument("--books", type=Path, default=None)
    parser.add_argument("--fixture", type=Path, default=None)
    parser.add_argument("--proxy", default=None)
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument(
        "--sync",
        action="store_true",
        help="paginate Gamma /events?tag_slug=weather and merge the catalog onto the board",
    )
    parser.add_argument(
        "--no-sync",
        action="store_true",
        help="do not call Gamma; scan only local snapshots/rules",
    )
    parser.add_argument("--no-auto-start", action="store_true")
    parser.add_argument(
        "--no-open",
        action="store_true",
        help="do not open the board in a browser",
    )
    parser.add_argument("--min-net-edge", type=float, default=0.0075)
    parser.add_argument("--max-ask", type=float, default=0.995)
    parser.add_argument("--max-slippage", type=float, default=0.003)
    parser.add_argument("--allow-category-fee", action="store_true")
    parser.add_argument(
        "--horizon-hours",
        type=float,
        default=24.0,
        help="only scan/catalog events whose local observation day overlaps now±N hours",
    )
    return parser


def resolved_service_options(args: argparse.Namespace, *, root: Path) -> dict[str, Any]:
    """Pick scanner+board defaults so one process serves the UI and the scan loop."""

    fixture = args.fixture.resolve() if args.fixture else None
    if args.no_sync and args.sync:
        raise SystemExit("choose --sync or --no-sync, not both")
    if args.no_sync:
        sync = False
    elif args.sync:
        sync = True
    else:
        sync = fixture is None
    data_dir = args.data_dir.resolve() if args.data_dir else root / "data" / "pm-weather-live"
    return {
        "data_dir": data_dir,
        "fixture": fixture,
        "sync": sync,
        "markets": args.markets.resolve() if args.markets else None,
        "rules": args.rules.resolve() if args.rules else None,
        "books": args.books.resolve() if args.books else None,
    }


def main(argv: Optional[list[str]] = None) -> int:
    global SERVICE, PUBLIC, SRC
    from .env import load_dotenv, trading_config

    load_dotenv()
    args = build_parser().parse_args(argv)
    root = args.root.resolve()
    PUBLIC = root / "weather_board" / "public"
    SRC = root / "weather_board" / "src"
    options = resolved_service_options(args, root=root)
    from .scanner import WeatherScannerConfig

    trading = trading_config()
    config = WeatherScannerConfig(
        max_ask=args.max_ask,
        max_slippage=args.max_slippage,
        min_net_edge=args.min_net_edge,
        require_explicit_fee=not args.allow_category_fee,
        max_usdc=float(trading["max_order_usdc"]),
        target_shares=max(1.0, float(trading["max_order_usdc"]) * 1000.0),
    )
    live = bool(trading["live_orders"])
    SERVICE = RuntimeService(
        root=root,
        data_dir=options["data_dir"],
        markets_file=options["markets"],
        rules_file=options["rules"],
        books_file=options["books"],
        fixture=options["fixture"],
        proxy=args.proxy,
        interval_s=args.interval,
        dry_run=not live,
        sync=options["sync"],
        scanner_config=config,
        horizon_hours=args.horizon_hours,
    )
    server = ThreadingHTTPServer((args.host, int(args.port)), Handler)
    server.daemon_threads = True
    if not args.no_auto_start:
        SERVICE.start()
    board_url = "http://{}:{}/".format(args.host, args.port)
    print("Weather scanner + board → {}".format(board_url), flush=True)
    print("Data directory → {}".format(SERVICE.data_dir), flush=True)
    print(
        "Scan loop → every {}s · Gamma sync {} · auto-start {}".format(
            args.interval,
            "on" if options["sync"] else "off",
            "on" if not args.no_auto_start else "off",
        ),
        flush=True,
    )
    if live:
        print(
            "Mode → LIVE auto-take · max {} USDC · taken {}".format(
                trading["max_order_usdc"],
                len(SERVICE._taken_tokens),
            ),
            flush=True,
        )
    else:
        print("Mode → dry-run/read-only", flush=True)
    if not args.no_open:
        threading.Timer(0.3, lambda: webbrowser.open(board_url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        SERVICE.stop()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

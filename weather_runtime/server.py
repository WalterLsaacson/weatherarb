"""HTTP REST + SSE server for the standalone weather board."""

from __future__ import annotations

import argparse
import json
import mimetypes
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from .service import RuntimeService


ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ROOT / "weather_board" / "public"
SRC = ROOT / "weather_board" / "src"
HOST = "127.0.0.1"
PORT = 8793
SERVICE: RuntimeService


def json_response(handler: BaseHTTPRequestHandler, code: int, payload: Any) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.end_headers()
    handler.wfile.write(body)


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
            books = SERVICE.snapshot().get("books") or {}
            json_response(self, 200, {"fetched_at": SERVICE.last_scan_at, "books": books})
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
                json_response(self, 200, SERVICE.scan_once())
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
    parser.add_argument("--interval", type=float, default=60.0)
    parser.add_argument(
        "--sync",
        action="store_true",
        help="paginate Gamma /events?tag_slug=weather and merge the catalog onto the board",
    )
    parser.add_argument("--no-auto-start", action="store_true")
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


def main(argv: Optional[list[str]] = None) -> int:
    global SERVICE, PUBLIC, SRC
    args = build_parser().parse_args(argv)
    root = args.root.resolve()
    PUBLIC = root / "weather_board" / "public"
    SRC = root / "weather_board" / "src"
    data_dir = args.data_dir.resolve() if args.data_dir else root / "data" / "pm-weather"
    from .scanner import WeatherScannerConfig

    config = WeatherScannerConfig(
        max_ask=args.max_ask,
        max_slippage=args.max_slippage,
        min_net_edge=args.min_net_edge,
        require_explicit_fee=not args.allow_category_fee,
    )
    SERVICE = RuntimeService(
        root=root,
        data_dir=data_dir,
        markets_file=args.markets.resolve() if args.markets else None,
        rules_file=args.rules.resolve() if args.rules else None,
        books_file=args.books.resolve() if args.books else None,
        fixture=args.fixture.resolve() if args.fixture else None,
        proxy=args.proxy,
        interval_s=args.interval,
        dry_run=True,
        sync=args.sync,
        scanner_config=config,
        horizon_hours=args.horizon_hours,
    )
    server = ThreadingHTTPServer((args.host, int(args.port)), Handler)
    server.daemon_threads = True
    if not args.no_auto_start:
        SERVICE.start()
    print("Weather Board → http://{}:{}/".format(args.host, args.port), flush=True)
    print("Data directory → {}".format(SERVICE.data_dir), flush=True)
    print("Mode → dry-run/read-only", flush=True)
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

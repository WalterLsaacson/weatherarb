"""Synoptic Push Streaming recorder for scanned NOAA/WRH stations."""

from __future__ import annotations

import json
import os
import socket
import threading
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from .models import WeatherRule, now_iso, parse_time
from .rules import load_timezone
from .sources import _convert
from .storage import append_jsonl
from .ws_minimal import MinimalWebSocket, WebSocketError, _normalize_proxy

_PUSH_BASE = "wss://push.synopticdata.com/feed"
_DEFAULT_VARS = "air_temp,sea_level_pressure,metar"
_DEFAULT_UNITS = "temp|F,speed|mph,english"
_STID_CHUNK = 60
_REWIND_MINUTES = 30


def normalize_push_value(sensor: str, value: Any, unit: str) -> tuple[Any, str, Any, str]:
    """Return (value_c_or_raw, unit_for_compare, value_raw, unit_raw).

    WRH HTTP evidence stores air_temp in Celsius; push feed defaults to English/F.
    """

    unit_raw = str(unit or "").strip()
    if value is None:
        return None, unit_raw or "", None, unit_raw
    if str(sensor or "").startswith("air_temp"):
        try:
            number = float(value)
        except (TypeError, ValueError):
            return value, unit_raw or "", value, unit_raw
        lower = unit_raw.lower()
        if "fahrenheit" in lower or lower in {"f", "degf", "deg_f"}:
            return round(_convert(number, "F", "C"), 4), "C", number, unit_raw or "F"
        if "celsius" in lower or lower in {"c", "degc", "deg_c"}:
            return round(number, 4), "C", number, unit_raw or "C"
        # Feed requested english/temp|F; assume F when metadata has not arrived yet.
        return round(_convert(number, "F", "C"), 4), "C", number, unit_raw or "F"
    return value, unit_raw, value, unit_raw


def parse_synoptic_push_date(value: Any) -> Optional[str]:
    """Convert Synoptic push `date` (yyyymmddhhmm UTC) to ISO-8601."""

    raw = str(value or "").strip()
    if not raw.isdigit() or len(raw) != 12:
        return None
    try:
        stamp = datetime(
            int(raw[0:4]),
            int(raw[4:6]),
            int(raw[6:8]),
            int(raw[8:10]),
            int(raw[10:12]),
            tzinfo=timezone.utc,
        )
    except ValueError:
        return None
    return stamp.isoformat()


def synoptic_watch_from_rules(
    rules: Iterable[WeatherRule],
    *,
    now: Optional[datetime] = None,
) -> dict[str, dict[str, Any]]:
    """Unique Synoptic STIDs still inside the observation/grace window."""

    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    watches: dict[str, dict[str, Any]] = {}
    for rule in rules:
        if not isinstance(rule, WeatherRule):
            continue
        source = rule.source if isinstance(rule.source, dict) else {}
        url = str(source.get("url") or source.get("resolution_source") or "")
        provider = str(source.get("provider") or "").lower()
        is_synoptic = provider == "noaa" or "synopticdata.com" in url or "weather.gov/wrh" in url
        if not is_synoptic:
            continue
        params = source.get("params") if isinstance(source.get("params"), dict) else {}
        stid = str(params.get("STID") or source.get("station_id") or "").strip().upper()
        if not stid:
            continue
        zone = load_timezone(rule.timezone)
        start = parse_time(rule.observation_start, default_tz=zone)
        end = parse_time(rule.observation_end, default_tz=zone)
        if start is not None and current < start.astimezone(timezone.utc):
            continue
        if end is not None:
            grace_hours = float(source.get("poll_after_end_hours") or 48)
            if current > end.astimezone(timezone.utc) + timedelta(hours=max(0.0, grace_hours)):
                continue
        item = watches.setdefault(
            stid,
            {
                "station_id": stid,
                "event_group_ids": [],
                "sample_sets": [],
                "timezones": [],
            },
        )
        gid = str(rule.event_group_id or "")
        if gid and gid not in item["event_group_ids"]:
            item["event_group_ids"].append(gid)
        sample = str(source.get("sample_set") or "all")
        if sample not in item["sample_sets"]:
            item["sample_sets"].append(sample)
        tz_name = str(rule.timezone or "")
        if tz_name and tz_name not in item["timezones"]:
            item["timezones"].append(tz_name)
    return watches


class SynopticPushRecorder:
    """Background Synoptic push listener that appends every message to JSONL."""

    def __init__(
        self,
        data_dir: Path,
        *,
        token_fn: Callable[[], str],
        enabled: Optional[bool] = None,
        path: Optional[Path] = None,
        proxy: Optional[str] = None,
    ):
        self.data_dir = Path(data_dir)
        self.token_fn = token_fn
        env_flag = str(os.environ.get("SYNOPTIC_PUSH") or "1").strip().lower()
        self.enabled = (env_flag not in {"0", "false", "no", "off"}) if enabled is None else bool(enabled)
        self.path = path or (self.data_dir / "synoptic_push.jsonl")
        if proxy is None:
            proxy = os.environ.get("WEATHER_PROXY") or os.environ.get("PM_PROXY")
        self.proxy = _normalize_proxy(proxy)
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._watch: dict[str, dict[str, Any]] = {}
        self._watch_version = 0
        self._session = ""
        self._units: dict[str, str] = {}
        self._running = False
        self._connected = False
        self._last_error = ""
        self._last_message_at = ""
        self._messages = 0
        self._data_rows = 0
        self._reconnects = 0

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "enabled": self.enabled,
                "running": self._running,
                "connected": self._connected,
                "stations": len(self._watch),
                "station_ids": sorted(self._watch.keys()),
                "session": self._session,
                "messages": self._messages,
                "data_rows": self._data_rows,
                "reconnects": self._reconnects,
                "last_message_at": self._last_message_at,
                "last_error": self._last_error,
                "path": str(self.path),
                "proxy": self.proxy or "direct",
            }

    def set_watch(self, watches: dict[str, dict[str, Any]]) -> None:
        cleaned: dict[str, dict[str, Any]] = {}
        for stid, meta in (watches or {}).items():
            key = str(stid or "").strip().upper()
            if not key:
                continue
            payload = dict(meta or {})
            payload["station_id"] = key
            cleaned[key] = payload
        with self._lock:
            same_stations = set(cleaned.keys()) == set(self._watch.keys())
            same_events = same_stations and all(
                list(cleaned[key].get("event_group_ids") or [])
                == list(self._watch[key].get("event_group_ids") or [])
                for key in cleaned
            )
            self._watch = cleaned
            if same_events:
                return
            self._watch_version += 1
            self._session = ""

    def start(self) -> None:
        if not self.enabled:
            return
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            self._running = True
            self._thread = threading.Thread(target=self._loop, name="synoptic-push", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            thread = self._thread
            self._running = False
            self._connected = False
        if thread and thread.is_alive():
            thread.join(timeout=2.0)

    def _record(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        try:
            if self.path.is_file() and self.path.stat().st_size > 32 * 1024 * 1024:
                rotated = self.path.with_name(self.path.name + ".old")
                try:
                    self.path.replace(rotated)
                except OSError:
                    pass
        except OSError:
            pass
        append_jsonl(self.path, rows)

    def _handle_message(
        self,
        text: str,
        watches: dict[str, dict[str, Any]],
        *,
        track_session: bool,
    ) -> None:
        received_at = now_iso()
        try:
            payload = json.loads(text)
        except ValueError:
            self._record(
                [
                    {
                        "channel": "synoptic_push",
                        "msg_type": "parse_error",
                        "received_at": received_at,
                        "raw_text": text[:4000],
                    }
                ]
            )
            return
        if not isinstance(payload, dict):
            self._record(
                [
                    {
                        "channel": "synoptic_push",
                        "msg_type": "unknown",
                        "received_at": received_at,
                        "raw": payload,
                    }
                ]
            )
            return
        msg_type = str(payload.get("type") or "unknown")
        with self._lock:
            self._messages += 1
            self._last_message_at = received_at
        if msg_type == "auth":
            session = str(payload.get("session") or "")
            code = str(payload.get("code") or "")
            if track_session and session and code == "success":
                with self._lock:
                    self._session = session
            if code and code != "success":
                with self._lock:
                    self._last_error = "auth:{}".format(payload.get("messages") or code)
            self._record(
                [
                    {
                        "channel": "synoptic_push",
                        "msg_type": "auth",
                        "received_at": received_at,
                        "code": code,
                        "session": session,
                        "messages": payload.get("messages"),
                        "raw": payload,
                    }
                ]
            )
            return
        if msg_type == "metadata":
            units = payload.get("units")
            if isinstance(units, list):
                with self._lock:
                    for item in units:
                        if not isinstance(item, dict):
                            continue
                        sensor = str(item.get("sensor") or "").strip()
                        unit = str(item.get("unit") or "").strip()
                        if sensor and unit:
                            self._units[sensor] = unit
            self._record(
                [
                    {
                        "channel": "synoptic_push",
                        "msg_type": "metadata",
                        "received_at": received_at,
                        "station_count": len(payload.get("stations") or [])
                        if isinstance(payload.get("stations"), list)
                        else 0,
                        "units": payload.get("units"),
                        "stations": payload.get("stations"),
                        "raw": payload,
                    }
                ]
            )
            return
        if msg_type != "data":
            self._record(
                [
                    {
                        "channel": "synoptic_push",
                        "msg_type": msg_type,
                        "received_at": received_at,
                        "raw": payload,
                    }
                ]
            )
            return
        rows_out: list[dict[str, Any]] = []
        data_rows = payload.get("data")
        if not isinstance(data_rows, list):
            self._record(
                [
                    {
                        "channel": "synoptic_push",
                        "msg_type": "data",
                        "received_at": received_at,
                        "raw": payload,
                    }
                ]
            )
            return
        with self._lock:
            units = dict(self._units)
        for item in data_rows:
            if not isinstance(item, dict):
                continue
            stid = str(item.get("stid") or "").strip().upper()
            meta = watches.get(stid) or {}
            sensor = str(item.get("sensor") or "")
            value_norm, unit_norm, value_raw, unit_raw = normalize_push_value(
                sensor, item.get("value"), units.get(sensor) or ""
            )
            row = {
                "channel": "synoptic_push",
                "msg_type": "data",
                "received_at": received_at,
                "station_id": stid,
                "event_group_ids": list(meta.get("event_group_ids") or []),
                "sample_sets": list(meta.get("sample_sets") or []),
                "sensor": sensor,
                "set": item.get("set"),
                "value": value_norm,
                "unit": unit_norm,
                "value_raw": value_raw,
                "unit_raw": unit_raw,
                "temp": value_norm if sensor.startswith("air_temp") else None,
                "qc": item.get("qc") if isinstance(item.get("qc"), list) else [],
                "obs_date_raw": item.get("date"),
                "obs_timestamp": parse_synoptic_push_date(item.get("date")),
                "raw": item,
            }
            rows_out.append(row)
        if rows_out:
            with self._lock:
                self._data_rows += len(rows_out)
            self._record(rows_out)

    def _feed_urls(self, token: str, stids: list[str], *, session: str = "") -> list[str]:
        safe_token = urllib.parse.quote(token, safe="")
        if session and len(stids) <= _STID_CHUNK:
            return ["{}/{}/{}".format(_PUSH_BASE, safe_token, session)]
        urls: list[str] = []
        for index in range(0, len(stids), _STID_CHUNK):
            chunk = stids[index : index + _STID_CHUNK]
            query = urllib.parse.urlencode(
                {
                    "stid": ",".join(chunk),
                    "vars": _DEFAULT_VARS,
                    "units": _DEFAULT_UNITS,
                    "rewind": str(_REWIND_MINUTES),
                    "metadata": "1",
                }
            )
            urls.append("{}/{}/?{}".format(_PUSH_BASE, safe_token, query))
        return urls

    def _consume_url(
        self,
        url: str,
        watches: dict[str, dict[str, Any]],
        *,
        version: int,
        track_session: bool,
        stop_event: Optional[threading.Event] = None,
    ) -> None:
        local_stop = stop_event or self._stop
        ws = MinimalWebSocket.connect(
            url,
            timeout_s=30.0,
            origin="https://www.weather.gov",
            proxy=self.proxy,
            idle_timeout_s=30.0,
        )
        try:
            with self._lock:
                self._connected = True
                if not self._last_error.startswith("auth:"):
                    self._last_error = ""
            while not self._stop.is_set() and not local_stop.is_set():
                with self._lock:
                    if self._watch_version != version:
                        break
                try:
                    text = ws.recv_text()
                except (socket.timeout, TimeoutError):
                    continue
                if text is None:
                    break
                self._handle_message(text, watches, track_session=track_session)
        finally:
            try:
                ws.send_text(json.dumps({"feed_action": "stop"}))
            except Exception:
                pass
            ws.close()
            with self._lock:
                self._connected = False

    def _run_session(
        self,
        watches: dict[str, dict[str, Any]],
        *,
        version: int,
        resume_session: str = "",
    ) -> None:
        token = str(self.token_fn() or "").strip()
        if not token:
            raise WebSocketError("synoptic push token missing")
        stids = sorted(watches.keys())
        urls = self._feed_urls(token, stids, session=resume_session)
        if len(urls) == 1:
            self._consume_url(urls[0], watches, version=version, track_session=True)
            return
        errors: list[str] = []
        chunk_stop = threading.Event()

        def _worker(target_url: str) -> None:
            try:
                self._consume_url(
                    target_url,
                    watches,
                    version=version,
                    track_session=False,
                    stop_event=chunk_stop,
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))

        helpers = [
            threading.Thread(target=_worker, args=(url,), daemon=True, name="synoptic-push-chunk")
            for url in urls[1:]
        ]
        for worker in helpers:
            worker.start()
        try:
            self._consume_url(
                urls[0],
                watches,
                version=version,
                track_session=False,
                stop_event=chunk_stop,
            )
        finally:
            chunk_stop.set()
            for worker in helpers:
                worker.join(timeout=35.0)
        if errors:
            raise WebSocketError(errors[0])

    def _loop(self) -> None:
        backoff = 2.0
        while not self._stop.is_set():
            if not self.enabled:
                break
            with self._lock:
                watches = dict(self._watch)
                version = self._watch_version
                session = self._session
            if not watches:
                self._stop.wait(2.0)
                continue
            try:
                self._run_session(watches, version=version, resume_session=session)
                backoff = 2.0
            except Exception as exc:  # noqa: BLE001
                with self._lock:
                    self._last_error = str(exc)
                    self._connected = False
                    self._reconnects += 1
                self._record(
                    [
                        {
                            "channel": "synoptic_push",
                            "msg_type": "error",
                            "received_at": now_iso(),
                            "error": str(exc),
                            "stations": sorted(watches.keys()),
                        }
                    ]
                )
                self._stop.wait(min(60.0, backoff))
                backoff = min(60.0, backoff * 1.7)
            else:
                # Clean disconnect or watch change: brief pause then reconnect.
                self._stop.wait(1.0)

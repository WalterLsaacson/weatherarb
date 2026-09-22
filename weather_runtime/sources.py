"""NOAA WRH/METAR, Wunderground, and HKO read-only weather adapters."""

from __future__ import annotations

import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

from .models import ObservationEvidence, WeatherRule, now_iso, parse_time
from .rules import ROUNDING_WHOLE, apply_rounding, load_timezone
from .storage import sha256_json


class SourceError(RuntimeError):
    pass


# Weather Underground's public web client key. Override with WEATHER_COM_API_KEY.
_WU_WEB_API_KEY = "e1f10a1e78da46f5b10a1e78da96f525"
_SYNOPTIC_TOKEN_JS = "https://www.weather.gov/source/wrh/apiKey.js"
_SYNOPTIC_TOKEN_RE = re.compile(r"mesoToken\s*=\s*['\"]([^'\"]+)['\"]")
_SYNOPTIC_TOKEN_TTL_S = 3600.0
_SYNOPTIC_PAGE_UNITS = "temp|F,speed|mph,english"
_SYNOPTIC_PAGE_RECENT_MINUTES = 72 * 60
_SYNOPTIC_VARS = "air_temp,sea_level_pressure,metar"
_SYNOPTIC_TIMESERIES_URL = "https://api.synopticdata.com/v2/stations/timeseries"
# Keep per-station waits short so a slow Synoptic host cannot serialize the scan.
_WEATHER_HTTP_TIMEOUT_S = 10.0
_HTTP_FAIL_COOLDOWN_S = 120.0
_DEFAULT_HTTP_CACHE_TTL_S = 300.0
_DEFAULT_PREFETCH_WORKERS = 16
_WU_POST_CLOSE_REFRESH_HOURS = 3.0
_WU_POST_CLOSE_CACHE_S = 15.0
# Do not mark source-final if the last in-window sample is too far from local midnight.
_WINDOW_END_MAX_LAG = timedelta(hours=4)
_SYNOPTIC_PAGE_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


def sample_set_of(rule: WeatherRule) -> str:
    raw = str(rule.source.get("sample_set") or "all").strip().lower()
    return "hourly" if raw == "hourly" else "all"


def hourly_window_of(rule: WeatherRule) -> str:
    raw = str(rule.source.get("hourly_window") or "").strip().lower()
    if raw in {"nws_faa", "other"}:
        return raw
    name = str(rule.timezone or "").strip()
    if name.startswith("America/") or name in {"Pacific/Honolulu", "America/Anchorage"}:
        return "nws_faa"
    return "other"


def hourly_minute_counts(minute: int, window: str) -> bool:
    value = int(minute)
    if window == "other":
        return value >= 56 or value <= 4
    return 51 <= value <= 59


def _asos_network(value: Any) -> str:
    name = str(value or "").strip().upper().replace("_", "/")
    if name == "GLOBAL-METAR":
        return "ASOS/AWOS"
    return name


def wrh_show_hourly_counts(
    *,
    stamp: datetime,
    row: dict[str, Any],
    zone: Any,
    window: str,
    station_id: str,
) -> bool:
    """Client-side filter used by weather.gov after Show Hourly Data.

    The timeseries page still loads the same Synoptic URL; hourly=true only
    changes which rows are rendered (obs.js). ASOS/AWOS keeps official METAR
    rows that have sea-level pressure, plus SPECI whose METAR starts with the
    station id. Other networks keep minutes 56-04; non-fed ASOS without SLP
    keeps minutes 51-59.
    """
    network = _asos_network(row.get("network"))
    has_slp_field = "slp" in row or "sea_level_pressure" in row
    has_metar_field = "metar" in row
    slp = row.get("slp", row.get("sea_level_pressure"))
    metar = str(row.get("metar") or "").strip()
    site = str(station_id or "").strip().upper()
    if network == "ASOS/AWOS" or has_slp_field or has_metar_field:
        if has_slp_field:
            if slp not in {None, ""}:
                return True
            return bool(metar and site and metar.upper().startswith(site))
        if network != "ASOS/AWOS":
            local = stamp.astimezone(zone)
            return hourly_minute_counts(local.minute, window)
        local = stamp.astimezone(zone)
        return hourly_minute_counts(local.minute, "nws_faa")
    local = stamp.astimezone(zone)
    return hourly_minute_counts(local.minute, window)


def json_path(value: Any, path: str) -> Any:
    current = value
    for part in str(path or "").strip(".").split("."):
        if not part:
            continue
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
    return current


class JsonHttp:
    def __init__(
        self,
        *,
        proxy: Optional[str] = None,
        timeout: float = 8.0,
        retries: int = 1,
        backoff_s: float = 0.25,
    ):
        raw_proxy = proxy
        if raw_proxy is None:
            raw_proxy = os.environ.get("WEATHER_PROXY") or os.environ.get("PM_PROXY")
        if raw_proxy and str(raw_proxy).lower() not in {"none", "direct", "off", "0"}:
            value = str(raw_proxy)
            if "://" not in value:
                value = "http://" + value
            self.proxy = value
        else:
            self.proxy = None
        self.timeout = float(timeout)
        self.retries = max(0, int(retries))
        self.backoff_s = max(0.0, float(backoff_s))

    def _opener(self):
        if self.proxy:
            return urllib.request.build_opener(
                urllib.request.ProxyHandler({"http": self.proxy, "https": self.proxy})
            )
        return urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def _deadline(self, fn, *, label: str) -> Any:
        box: list[Any] = []
        err: list[BaseException] = []

        def worker() -> None:
            try:
                box.append(fn())
            except BaseException as exc:  # noqa: BLE001
                err.append(exc)

        worker_thread = threading.Thread(target=worker, daemon=True)
        worker_thread.start()
        worker_thread.join(self.timeout)
        if worker_thread.is_alive():
            raise SourceError("timed out after {}s from {}".format(self.timeout, label))
        if err:
            raise err[0]
        if not box:
            raise SourceError("empty response from {}".format(label))
        return box[0]

    def _read(self, request: urllib.request.Request) -> str:
        with self._opener().open(request, timeout=self.timeout) as response:
            return response.read().decode("utf-8")

    def get_json(
        self,
        url: str,
        *,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> Any:
        if params:
            query = urllib.parse.urlencode(
                {key: value for key, value in params.items() if value is not None},
                doseq=True,
            )
            url = url + ("&" if "?" in url else "?") + query
        request_headers = {
            "User-Agent": "polymarket-weather-arb/0.1 (+read-only-dry-run)",
            "Accept": "application/geo+json, application/json",
        }
        if headers:
            request_headers.update(headers)
        request = urllib.request.Request(url, headers=request_headers, method="GET")
        for attempt in range(self.retries + 1):
            try:
                raw = self._deadline(lambda: self._read(request), label=url)
                break
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")[:300]
                retryable = exc.code == 429 or 500 <= exc.code <= 599
                if retryable and attempt < self.retries:
                    time.sleep(self.backoff_s * (2 ** attempt))
                    continue
                raise SourceError("HTTP {} from {}: {}".format(exc.code, url, detail)) from exc
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                if attempt < self.retries:
                    time.sleep(self.backoff_s * (2 ** attempt))
                    continue
                raise SourceError("network error from {}: {}".format(url, exc)) from exc
        try:
            return json.loads(raw)
        except ValueError as exc:
            raise SourceError("invalid JSON from {}".format(url)) from exc

    def get_text(
        self,
        url: str,
        *,
        headers: Optional[dict[str, str]] = None,
    ) -> str:
        request_headers = {
            "User-Agent": "polymarket-weather-arb/0.1 (+read-only-dry-run)",
            "Accept": "text/plain, text/javascript, application/javascript, */*",
        }
        if headers:
            request_headers.update(headers)
        request = urllib.request.Request(url, headers=request_headers, method="GET")
        for attempt in range(self.retries + 1):
            try:
                return self._deadline(lambda: self._read(request), label=url)
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")[:300]
                retryable = exc.code == 429 or 500 <= exc.code <= 599
                if retryable and attempt < self.retries:
                    time.sleep(self.backoff_s * (2 ** attempt))
                    continue
                raise SourceError("HTTP {} from {}: {}".format(exc.code, url, detail)) from exc
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                if attempt < self.retries:
                    time.sleep(self.backoff_s * (2 ** attempt))
                    continue
                raise SourceError("network error from {}: {}".format(url, exc)) from exc
        raise SourceError("network error from {}".format(url))

    def post_json(
        self,
        url: str,
        body: Any,
        *,
        headers: Optional[dict[str, str]] = None,
    ) -> Any:
        request_headers = {
            "User-Agent": "polymarket-weather-arb/0.1 (+read-only-dry-run)",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if headers:
            request_headers.update(headers)
        request = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            headers=request_headers,
            method="POST",
        )
        for attempt in range(self.retries + 1):
            try:
                raw = self._deadline(lambda: self._read(request), label=url)
                break
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")[:300]
                retryable = exc.code == 429 or 500 <= exc.code <= 599
                if retryable and attempt < self.retries:
                    time.sleep(self.backoff_s * (2 ** attempt))
                    continue
                raise SourceError("HTTP {} from {}: {}".format(exc.code, url, detail)) from exc
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                if attempt < self.retries:
                    time.sleep(self.backoff_s * (2 ** attempt))
                    continue
                raise SourceError("network error from {}: {}".format(url, exc)) from exc
        try:
            return json.loads(raw)
        except ValueError as exc:
            raise SourceError("invalid JSON from {}".format(url)) from exc


def _number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def series_from_raw(payload: Any) -> list[dict[str, Any]]:
    """Compact hour points for the board; accepts Synoptic or NWS GeoJSON blobs."""

    if not isinstance(payload, dict):
        return []
    points: list[dict[str, Any]] = []
    rows = payload.get("observations")
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            timestamp = (
                row.get("timestamp")
                or row.get("date_time")
                or row.get("validTime")
                or row.get("valid_time_gmt")
            )
            temp = row.get("temp")
            if temp is None:
                temp = row.get("value")
            props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
            if timestamp is None:
                timestamp = props.get("timestamp")
            if temp is None:
                temperature = props.get("temperature")
                if isinstance(temperature, dict):
                    temp = temperature.get("value")
                else:
                    temp = temperature
            number = _number(temp)
            if timestamp in {None, ""} or number is None:
                continue
            parsed = parse_time(timestamp)
            points.append({"timestamp": parsed.isoformat() if parsed else str(timestamp), "temp": number})
        if points:
            return points
    features = payload.get("features")
    if isinstance(features, list):
        return series_from_raw({"observations": features})
    return points


def _boolean(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "final"}


def _convert(value: float, from_unit: str, to_unit: str) -> float:
    source = (from_unit or to_unit or "C").strip().upper().replace("°", "")
    target = (to_unit or source).strip().upper().replace("°", "")
    if source == target or not source or not target:
        return value
    if source in {"C", "CELSIUS"} and target in {"F", "FAHRENHEIT"}:
        return value * 9.0 / 5.0 + 32.0
    if source in {"F", "FAHRENHEIT"} and target in {"C", "CELSIUS"}:
        return (value - 32.0) * 5.0 / 9.0
    return value


def _synoptic_air_temp_unit(payload: Any, station: dict[str, Any]) -> str:
    for blob in (station, payload if isinstance(payload, dict) else {}):
        if not isinstance(blob, dict):
            continue
        units = blob.get("UNITS") or blob.get("units")
        if not isinstance(units, dict):
            continue
        raw = str(units.get("air_temp") or units.get("air_temp_set_1") or "").strip().lower()
        if "celsius" in raw or raw in {"c", "degc", "deg_c"}:
            return "C"
        if "fahrenheit" in raw or raw in {"f", "degf", "deg_f"}:
            return "F"
    return ""


class WeatherSourceAdapter:
    """Poll one approved rule and return immutable source evidence."""

    def __init__(
        self,
        *,
        http: Optional[JsonHttp] = None,
        http_cache_ttl_s: float = _DEFAULT_HTTP_CACHE_TTL_S,
    ):
        self.http = http or JsonHttp(timeout=_WEATHER_HTTP_TIMEOUT_S, retries=0)
        self.http_cache_ttl_s = max(0.0, float(http_cache_ttl_s))
        self._http_cache: dict[tuple[str, str], tuple[float, Any]] = {}
        self._http_fail: dict[tuple[str, str], float] = {}
        self._http_inflight: dict[tuple[str, str], threading.Event] = {}
        self._http_lock = threading.Lock()
        self._synoptic_token_cache: Optional[tuple[float, str]] = None
        self._synoptic_token_error_at = 0.0
        self._watch_seen: set[tuple[str, str]] = set()
        self._watch_pending: list[dict[str, Any]] = []

    def _zone(self, rule: WeatherRule):
        return load_timezone(rule.timezone)

    def _parse(self, rule: WeatherRule, value: Any):
        return parse_time(value, default_tz=self._zone(rule))

    def _window_open(self, rule: WeatherRule, current_utc: datetime) -> bool:
        start = self._parse(rule, rule.observation_start)
        end = self._parse(rule, rule.observation_end)
        if end is None:
            return False
        if start is not None and current_utc < start:
            return False
        return current_utc < end

    def _cache_key(self, url: str, params: dict[str, Any]) -> tuple[str, str]:
        return (url, json.dumps(params or {}, sort_keys=True, default=str))

    def _is_wunderground(self, rule: WeatherRule) -> bool:
        source = rule.source
        provider = str(source.get("provider") or "").lower()
        url = str(source.get("url") or source.get("resolution_source") or "")
        return provider == "wunderground" or "api.weather.com" in url or "wunderground.com" in url

    def _cache_ttl_s(self, rule: WeatherRule, current_utc: datetime) -> float:
        """WU often inserts :30 METARs after local midnight; do not keep a stale hour."""

        if not self.http_cache_ttl_s:
            return 0.0
        if not self._is_wunderground(rule):
            return self.http_cache_ttl_s
        end = self._parse(rule, rule.observation_end)
        if end is None or current_utc < end:
            return self.http_cache_ttl_s
        if current_utc > end + timedelta(hours=_WU_POST_CLOSE_REFRESH_HOURS):
            return self.http_cache_ttl_s
        return min(self.http_cache_ttl_s, _WU_POST_CLOSE_CACHE_S)

    def _request_parts(self, rule: WeatherRule) -> tuple[str, dict[str, Any], Optional[dict[str, str]]]:
        source = rule.source
        url = str(source.get("url") or "").strip()
        if not url:
            raise SourceError("weather source URL missing")
        params = dict(source.get("params") or {}) if isinstance(source.get("params"), dict) else {}
        if str(source.get("provider") or "").lower() == "wunderground" or "api.weather.com" in url:
            params.setdefault("units", "m")
            params.setdefault(
                "apiKey",
                os.environ.get("WEATHER_COM_API_KEY") or os.environ.get("WU_API_KEY") or _WU_WEB_API_KEY,
            )
        if "aviationweather.gov" in url or "synopticdata.com" in url:
            url = _SYNOPTIC_TIMESERIES_URL
        if "synopticdata.com" in url:
            params.setdefault("token", self._wrh_page_token())
            params["STID"] = params.get("STID") or source.get("station_id")
            params["showemptystations"] = 1
            params["units"] = _SYNOPTIC_PAGE_UNITS
            params["recent"] = _SYNOPTIC_PAGE_RECENT_MINUTES
            params["complete"] = 1
            params["vars"] = _SYNOPTIC_VARS
            params["obtimezone"] = "local"
            params.pop("start", None)
            params.pop("end", None)
        headers = dict(source.get("headers") or {}) if isinstance(source.get("headers"), dict) else {}
        if str(source.get("provider") or "").lower() == "wunderground" or "api.weather.com" in url:
            headers.setdefault("Accept", "application/json")
            headers.setdefault("Referer", "https://www.wunderground.com/")
        if "synopticdata.com" in url:
            headers.update(self._synoptic_headers(source, params))
        return url, params, headers or None

    def _payload(self, rule: WeatherRule, *, now: Optional[datetime] = None) -> tuple[Any, str]:
        source = rule.source
        if isinstance(source.get("static"), dict):
            payload = self._normalize_payload(rule, source["static"])
            return payload, str(source.get("url") or source.get("resolution_source") or "static://weather")
        url, params, headers = self._request_parts(rule)
        cache_key = self._cache_key(url, params)
        current_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        cache_ttl_s = self._cache_ttl_s(rule, current_utc)
        owner = False
        wait_event: Optional[threading.Event] = None
        if self.http_cache_ttl_s:
            while True:
                with self._http_lock:
                    hit = self._http_cache.get(cache_key)
                    failed_at = self._http_fail.get(cache_key)
                    if hit and cache_ttl_s and (time.time() - hit[0]) < cache_ttl_s:
                        return hit[1], url
                    if failed_at and (time.time() - failed_at) < _HTTP_FAIL_COOLDOWN_S:
                        raise SourceError("source recently timed out")
                    inflight = self._http_inflight.get(cache_key)
                    if inflight is None:
                        wait_event = threading.Event()
                        self._http_inflight[cache_key] = wait_event
                        owner = True
                        break
                    wait_event = inflight
                # Another thread is fetching this station; wait then re-check cache.
                wait_event.wait(timeout=max(1.0, float(getattr(self.http, "timeout", 10.0)) + 1.0))
        else:
            owner = True
        if not owner:
            with self._http_lock:
                hit = self._http_cache.get(cache_key)
                failed_at = self._http_fail.get(cache_key)
            if hit and cache_ttl_s and (time.time() - hit[0]) < cache_ttl_s:
                return hit[1], url
            if failed_at and (time.time() - failed_at) < _HTTP_FAIL_COOLDOWN_S:
                raise SourceError("source recently timed out")
            # Leader failed without publishing fail/cache; fall through and fetch.
            with self._http_lock:
                if cache_key not in self._http_inflight:
                    wait_event = threading.Event()
                    self._http_inflight[cache_key] = wait_event
                    owner = True
                else:
                    raise SourceError("source recently timed out")
        try:
            try:
                payload = self.http.get_json(url, params=params or None, headers=headers)
            except SourceError:
                with self._http_lock:
                    self._http_fail[cache_key] = time.time()
                raise
            except Exception:
                # Any unexpected leader failure must still free waiters.
                with self._http_lock:
                    self._http_fail[cache_key] = time.time()
                raise
            # WRH timeseries requests english/temp|F; if Synoptic omits UNITS, treat as F.
            if "synopticdata.com" in url and isinstance(payload, dict):
                units = payload.get("UNITS") or payload.get("units")
                has_air = isinstance(units, dict) and bool(
                    units.get("air_temp") or units.get("air_temp_set_1")
                )
                if not has_air:
                    payload = dict(payload)
                    merged = dict(units) if isinstance(units, dict) else {}
                    merged.setdefault("air_temp", "Fahrenheit")
                    payload["UNITS"] = merged
            payload = self._normalize_payload(rule, payload)
            self._note_synoptic_watch(rule, payload)
            if self.http_cache_ttl_s:
                with self._http_lock:
                    self._http_cache[cache_key] = (time.time(), payload)
                    self._http_fail.pop(cache_key, None)
            return payload, url
        finally:
            if owner and wait_event is not None:
                with self._http_lock:
                    if self._http_inflight.get(cache_key) is wait_event:
                        self._http_inflight.pop(cache_key, None)
                wait_event.set()

    def _unique_prefetch_rules(
        self,
        rules: Iterable[WeatherRule],
        *,
        now: Optional[datetime] = None,
    ) -> list[WeatherRule]:
        current = now or datetime.now(timezone.utc)
        current_utc = current.astimezone(timezone.utc)
        unique: list[WeatherRule] = []
        seen: set[tuple[str, str, str]] = set()
        for rule in rules:
            if not isinstance(rule, WeatherRule):
                continue
            if isinstance(rule.source.get("static"), dict):
                continue
            start = self._parse(rule, rule.observation_start)
            end = self._parse(rule, rule.observation_end)
            if start is not None and current_utc < start:
                continue
            if end is not None:
                grace_hours = float(rule.source.get("poll_after_end_hours") or 48)
                if current_utc > end + timedelta(hours=max(0.0, grace_hours)):
                    continue
            url = str(rule.source.get("url") or "")
            station = str(rule.source.get("station_id") or "")
            params = dict(rule.source.get("params") or {}) if isinstance(rule.source.get("params"), dict) else {}
            key = (url, station, json.dumps(params, sort_keys=True, default=str))
            if not url or key in seen:
                continue
            seen.add(key)
            unique.append(rule)
        unique.sort(key=lambda item: 0 if self._window_open(item, current_utc) else 1)
        return unique

    def _payload_cached_or_failed(self, rule: WeatherRule, *, now: Optional[datetime] = None) -> bool:
        if isinstance(rule.source.get("static"), dict):
            return True
        try:
            url, params, _headers = self._request_parts(rule)
        except Exception:
            return False
        cache_key = self._cache_key(url, params)
        current_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        cache_ttl_s = self._cache_ttl_s(rule, current_utc)
        with self._http_lock:
            hit = self._http_cache.get(cache_key)
            failed_at = self._http_fail.get(cache_key)
        if hit and cache_ttl_s and (time.time() - hit[0]) < cache_ttl_s:
            return True
        if failed_at and (time.time() - failed_at) < _HTTP_FAIL_COOLDOWN_S:
            return True
        return False

    def prefetch(
        self,
        rules: Iterable[WeatherRule],
        *,
        now: Optional[datetime] = None,
        deadline_s: float = 90.0,
        workers: int = _DEFAULT_PREFETCH_WORKERS,
    ) -> None:
        """Warm the HTTP cache for unique stations in parallel.

        Uses a wall-clock budget so a hung host cannot block the scan forever.
        Stations never reached before the deadline stay uncached so a later
        parallel wave (or poll) can still try them — they are not marked failed.
        """

        current = now or datetime.now(timezone.utc)
        unique = self._unique_prefetch_rules(rules, now=current)
        if not unique:
            return
        try:
            if any("synopticdata.com" in str(rule.source.get("url") or "") for rule in unique):
                self._wrh_page_token()
        except Exception:
            pass
        worker_count = max(1, int(workers))
        gate = threading.Semaphore(worker_count)
        deadline = time.time() + max(1.0, float(deadline_s))

        def _warm(item: WeatherRule) -> None:
            with gate:
                if time.time() > deadline:
                    return
                try:
                    self._payload(item, now=current)
                except Exception:
                    return

        # Two waves: first fills most stations; second retries only misses so a
        # short first-wave stall cannot force serial poll() afterwards.
        pending = list(unique)
        for _wave in range(2):
            if not pending or time.time() >= deadline:
                break
            threads = [
                threading.Thread(target=_warm, args=(rule,), daemon=True, name="wx-prefetch")
                for rule in pending
            ]
            for worker in threads:
                worker.start()
            for worker in threads:
                remain = deadline - time.time()
                worker.join(timeout=max(0.05, remain))
            pending = [
                rule
                for rule in pending
                if not self._payload_cached_or_failed(rule, now=current)
            ]

    def poll(self, rule: WeatherRule, *, now: Optional[datetime] = None) -> ObservationEvidence:
        current = now or datetime.now(timezone.utc)
        start = self._parse(rule, rule.observation_start)
        end = self._parse(rule, rule.observation_end)
        if end is None:
            return ObservationEvidence(
                status="error",
                observed_at=now_iso(current),
                reason="invalid_observation_end",
            )
        current_utc = current.astimezone(timezone.utc)
        if start is not None and current_utc < start:
            return ObservationEvidence(
                status="waiting_window",
                observed_at=now_iso(current),
                provider=str(rule.source.get("provider") or ""),
                station_id=str(rule.source.get("station_id") or ""),
                unit=rule.unit,
                aggregation=rule.metric,
                reason="observation_window_not_started",
            )
        window_open = current_utc < end
        if not window_open:
            grace_hours = float(rule.source.get("poll_after_end_hours") or 48)
            if current_utc > end + timedelta(hours=max(0.0, grace_hours)):
                return ObservationEvidence(
                    status="expired",
                    observed_at=now_iso(current),
                    provider=str(rule.source.get("provider") or ""),
                    station_id=str(rule.source.get("station_id") or ""),
                    unit=rule.unit,
                    aggregation=rule.metric,
                    reason="observation_window_expired",
                )
        try:
            payload, url = self._payload(rule, now=current)
        except SourceError as exc:
            return ObservationEvidence(
                status="unavailable",
                observed_at=now_iso(current),
                source_url=str(rule.source.get("url") or ""),
                provider=str(rule.source.get("provider") or ""),
                station_id=str(rule.source.get("station_id") or ""),
                unit=rule.unit,
                aggregation=rule.metric,
                reason=str(exc),
            )
        evidence_hash = sha256_json(payload)
        try:
            return self._evaluate(
                rule, payload, url, current, evidence_hash, window_open=window_open
            )
        except Exception as exc:  # noqa: BLE001
            return ObservationEvidence(
                status="error",
                observed_at=now_iso(current),
                source_url=url,
                provider=str(rule.source.get("provider") or ""),
                station_id=str(rule.source.get("station_id") or ""),
                unit=rule.unit,
                aggregation=rule.metric,
                reason="weather_payload_error:{}".format(exc),
                raw=payload,
                evidence_hash=evidence_hash,
            )

    def drain_synoptic_watch(self) -> list[dict[str, Any]]:
        """First-seen observation timestamps from WRH's Synoptic fetches."""

        with self._http_lock:
            rows = list(self._watch_pending)
            self._watch_pending.clear()
            return rows

    def _note_synoptic_watch(self, rule: WeatherRule, payload: Any) -> None:
        """Record when each Synoptic observation was first present in a WRH fetch.

        The timeseries page renders this same response, so a second request would
        not show an earlier arrival. Hourly settlement filtering happens later.
        """

        url = str(rule.source.get("url") or "")
        if "synopticdata.com" not in url and "aviationweather.gov" not in url:
            return
        if not isinstance(payload, dict):
            return
        rows = payload.get("observations")
        if not isinstance(rows, list):
            return
        station = str(rule.source.get("station_id") or "").strip().upper()
        fetched_at = datetime.now(timezone.utc).isoformat()
        fresh: list[dict[str, Any]] = []
        with self._http_lock:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                stamp = str(row.get("timestamp") or "").strip()
                if not stamp:
                    continue
                key = (station, stamp)
                if key in self._watch_seen:
                    continue
                self._watch_seen.add(key)
                fresh.append(
                    {
                        "fetched_at": fetched_at,
                        "station_id": station,
                        "feed": "wrh",
                        "obs_timestamp": stamp,
                        "temp_c": row.get("temp"),
                    }
                )
            self._watch_pending.extend(fresh)

    def _wrh_page_token(self) -> str:
        """Token embedded in the WRH timeseries page, not an account API token."""

        now = time.time()
        with self._http_lock:
            if self._synoptic_token_error_at and (now - self._synoptic_token_error_at) < 60.0:
                raise SourceError("synoptic token recently failed")
            cached = self._synoptic_token_cache
            if cached and (now - cached[0]) < _SYNOPTIC_TOKEN_TTL_S and cached[1]:
                return cached[1]
        try:
            raw = self.http.get_text(_SYNOPTIC_TOKEN_JS)
        except SourceError:
            with self._http_lock:
                self._synoptic_token_error_at = time.time()
            raise
        match = _SYNOPTIC_TOKEN_RE.search(raw)
        token = (match.group(1) if match else "").strip()
        if not token:
            with self._http_lock:
                self._synoptic_token_error_at = time.time()
            raise SourceError("synoptic token missing")
        with self._http_lock:
            self._synoptic_token_cache = (now, token)
            self._synoptic_token_error_at = 0.0
        return token

    def _synoptic_headers(self, source: dict[str, Any], params: dict[str, Any]) -> dict[str, str]:
        page = str(source.get("resolution_source") or "").strip()
        if "timeseries" not in page.lower():
            station = str(params.get("STID") or source.get("station_id") or "").strip().lower()
            page = "https://www.weather.gov/wrh/timeseries?site={}".format(station)
        return {
            "User-Agent": _SYNOPTIC_PAGE_UA,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Origin": "https://www.weather.gov",
            "Referer": page,
        }

    def _is_awc_source(self, source: dict[str, Any], url: str = "") -> bool:
        target = str(url or source.get("url") or "")
        return "aviationweather.gov" in target

    def _normalize_payload(self, rule: WeatherRule, payload: Any) -> Any:
        source = rule.source
        provider = str(source.get("provider") or "").lower()
        url = str(source.get("url") or source.get("resolution_source") or "")
        if provider == "hko" or "data.weather.gov.hk" in url or "weather.gov.hk" in url:
            return self._hko_observations(payload)
        if isinstance(payload, dict) and (
            isinstance(payload.get("STATION"), list) or isinstance(payload.get("station"), list)
        ):
            return self._synoptic_observations(payload)
        if isinstance(payload, list) or self._is_awc_source(source, url):
            return self._awc_observations(payload)
        if provider == "noaa" or "synopticdata.com" in url:
            return self._synoptic_observations(payload)
        return payload

    def _awc_observations(self, payload: Any) -> Any:
        if isinstance(payload, dict) and isinstance(payload.get("observations"), list):
            rows = payload.get("observations")
        elif isinstance(payload, list):
            rows = payload
        else:
            return payload
        observations: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            temp = _number(row.get("temp"))
            if temp is None:
                temp = _number(row.get("value"))
            timestamp = row.get("reportTime") or row.get("timestamp") or row.get("obsTime")
            if isinstance(timestamp, (int, float)) and timestamp > 10_000_000:
                timestamp = datetime.fromtimestamp(float(timestamp), tz=timezone.utc).isoformat()
            if temp is None or timestamp in {None, ""}:
                continue
            station = str(row.get("icaoId") or row.get("station_id") or "").strip().upper()
            metar = str(row.get("rawOb") or row.get("metar") or "").strip()
            item: dict[str, Any] = {"timestamp": timestamp, "temp": temp}
            if station:
                item["station_id"] = station
            if metar:
                item["metar"] = metar
            if "altim" in row:
                item["slp"] = row.get("altim")
            elif "slp" in row:
                item["slp"] = row.get("slp")
            if station.startswith("K") or str(row.get("metarType") or "").upper() in {"METAR", "SPECI"}:
                item["network"] = "ASOS/AWOS"
            elif station:
                item["network"] = "GLOBAL-METAR"
            observations.append(item)
        return {"observations": observations}

    def _synoptic_observations(self, payload: Any) -> Any:
        if not isinstance(payload, dict):
            return payload
        stations = payload.get("STATION")
        if stations is None:
            stations = payload.get("station")
        if not isinstance(stations, list):
            return payload
        observations: list[dict[str, Any]] = []
        for station in stations:
            if not isinstance(station, dict):
                continue
            obs = station.get("OBSERVATIONS") or station.get("observations") or {}
            if not isinstance(obs, dict):
                continue
            times = obs.get("date_time")
            if not isinstance(times, list):
                times = obs.get("date_time_set_1")
            temps = None
            for key in ("air_temp_set_1", "air_temp_set_1d"):
                value = obs.get(key)
                if isinstance(value, list):
                    temps = value
                    break
            if temps is None:
                for key, value in obs.items():
                    if str(key).lower().startswith("air_temp") and isinstance(value, list):
                        temps = value
                        break
            if not isinstance(times, list) or not isinstance(temps, list):
                continue
            source_unit = _synoptic_air_temp_unit(payload, station)
            slp_series = obs.get("sea_level_pressure_set_1")
            if not isinstance(slp_series, list):
                slp_series = obs.get("sea_level_pressure_set_1d")
            metar_series = obs.get("metar_set_1")
            network = _asos_network(station.get("SHORTNAME") or station.get("shortname"))
            for index, (timestamp_raw, temp_raw) in enumerate(zip(times, temps)):
                if timestamp_raw in {None, ""}:
                    continue
                number = _number(temp_raw)
                if number is None:
                    continue
                if source_unit == "F":
                    number = _convert(number, "F", "C")
                row: dict[str, Any] = {"timestamp": timestamp_raw, "temp": number}
                if network:
                    row["network"] = network
                if isinstance(slp_series, list):
                    row["slp"] = slp_series[index] if index < len(slp_series) else None
                if isinstance(metar_series, list):
                    row["metar"] = metar_series[index] if index < len(metar_series) else None
                observations.append(row)
        return {"observations": observations}

    def _hko_observations(self, payload: Any) -> Any:
        if not isinstance(payload, dict):
            return payload
        if isinstance(payload.get("observations"), list):
            return payload
        fields = payload.get("fields")
        rows = payload.get("data")
        if not isinstance(fields, list) or not isinstance(rows, list):
            return payload
        labels = [str(item or "").lower() for item in fields]
        def _index(*needles: str) -> int:
            for needle in needles:
                for index, label in enumerate(labels):
                    if needle in label:
                        return index
            return -1
        year_i = _index("year", "年")
        month_i = _index("month", "月")
        day_i = _index("day", "日")
        value_i = _index("value", "temperature", "數值")
        complete_i = _index("completeness", "完整性")
        if min(year_i, month_i, day_i, value_i) < 0:
            return payload
        observations: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, (list, tuple)):
                continue
            try:
                year = int(row[year_i])
                month = int(row[month_i])
                day = int(row[day_i])
            except (TypeError, ValueError, IndexError):
                continue
            raw_value = row[value_i] if value_i < len(row) else None
            if raw_value in {None, "", "***"}:
                continue
            number = _number(raw_value)
            if number is None:
                continue
            completeness = ""
            if complete_i >= 0 and complete_i < len(row):
                completeness = str(row[complete_i] or "").strip().upper()
            observations.append(
                {
                    "timestamp": "{:04d}-{:02d}-{:02d}T00:00:00".format(year, month, day),
                    "temp": number,
                    "final": completeness == "C",
                }
            )
        return {"observations": observations}

    def _payload_rows(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, dict):
            rows = payload.get("features")
            if isinstance(rows, list):
                return [item for item in rows if isinstance(item, dict)]
            rows = payload.get("observations")
            if isinstance(rows, list):
                return [item for item in rows if isinstance(item, dict)]
            return [payload]
        return []

    def _iter_values(self, rule: WeatherRule, payload: Any) -> list[tuple[datetime, float, dict[str, Any]]]:
        source = rule.source
        value_path = str(source.get("value_path") or "properties.temperature.value")
        timestamp_path = str(source.get("timestamp_path") or "properties.timestamp")
        source_unit = str(source.get("value_unit") or source.get("unit") or "C")
        rows = self._payload_rows(payload)
        start = self._parse(rule, rule.observation_start)
        end = self._parse(rule, rule.observation_end)
        values: list[tuple[datetime, float, dict[str, Any]]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            timestamp_raw = json_path(row, timestamp_path)
            if timestamp_raw is None:
                timestamp_raw = row.get("timestamp")
            timestamp = self._parse(rule, timestamp_raw)
            number = _number(json_path(row, value_path))
            if number is None and value_path != "value":
                number = _number(row.get("value"))
            if timestamp is None or number is None:
                continue
            converted = _convert(number, source_unit, rule.unit)
            if start is not None and timestamp < start:
                continue
            if end is not None and timestamp > end:
                continue
            values.append((timestamp, converted, row))
        return values

    def _counts_for_resolution(
        self, rule: WeatherRule, stamp: datetime, row: dict[str, Any]
    ) -> bool:
        if sample_set_of(rule) != "hourly":
            return True
        return wrh_show_hourly_counts(
            stamp=stamp,
            row=row if isinstance(row, dict) else {},
            zone=self._zone(rule),
            window=hourly_window_of(rule),
            station_id=str(rule.source.get("station_id") or ""),
        )

    def _resolution_values(
        self, rule: WeatherRule, values: list[tuple[datetime, float, dict[str, Any]]]
    ) -> list[tuple[datetime, float, dict[str, Any]]]:
        if sample_set_of(rule) != "hourly":
            return values
        return [item for item in values if self._counts_for_resolution(rule, item[0], item[2])]

    def _series_points(
        self,
        rule: WeatherRule,
        values: list[tuple[datetime, float, dict[str, Any]]],
        resolution_values: Optional[list[tuple[datetime, float, dict[str, Any]]]] = None,
    ) -> list[dict[str, Any]]:
        zone = self._zone(rule)
        zone_name = getattr(zone, "key", None) or str(rule.timezone or "UTC")
        source_values = values
        if sample_set_of(rule) == "hourly" and resolution_values is not None:
            source_values = resolution_values
        counted = {item[0] for item in source_values}
        points: list[dict[str, Any]] = []
        for stamp, temp, _row in sorted(source_values, key=lambda item: item[0]):
            utc = stamp.astimezone(timezone.utc)
            local = utc.astimezone(zone)
            points.append(
                {
                    "timestamp": utc.isoformat(),
                    "local_time": local.strftime("%Y-%m-%d %H:%M"),
                    "timezone": zone_name,
                    "temp": round(float(temp), 1),
                    "counts_for_resolution": stamp in counted,
                }
            )
        return points

    def _following_values(self, rule: WeatherRule, payload: Any) -> list[tuple[datetime, float]]:
        source = rule.source
        value_path = str(source.get("value_path") or "properties.temperature.value")
        timestamp_path = str(source.get("timestamp_path") or "properties.timestamp")
        source_unit = str(source.get("value_unit") or source.get("unit") or "C")
        end = self._parse(rule, rule.observation_end)
        if end is None:
            return []
        rows = self._payload_rows(payload)
        result: list[tuple[datetime, float]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            timestamp = self._parse(rule, json_path(row, timestamp_path) or row.get("timestamp"))
            number = _number(json_path(row, value_path))
            if timestamp is not None and number is not None and timestamp > end:
                if sample_set_of(rule) == "hourly" and not self._counts_for_resolution(
                    rule, timestamp, row
                ):
                    continue
                result.append((timestamp, _convert(number, source_unit, rule.unit)))
        return sorted(result, key=lambda item: item[0])

    def _evaluate(
        self,
        rule: WeatherRule,
        payload: Any,
        url: str,
        current: datetime,
        evidence_hash: str,
        *,
        window_open: bool = False,
    ) -> ObservationEvidence:
        values = self._iter_values(rule, payload)
        if window_open:
            current_utc = current.astimezone(timezone.utc)
            values = [item for item in values if item[0] <= current_utc]
        if not values:
            return ObservationEvidence(
                status="waiting_window" if window_open else "unavailable",
                observed_at=now_iso(current),
                source_url=url,
                provider=str(rule.source.get("provider") or ""),
                station_id=str(rule.source.get("station_id") or ""),
                unit=rule.unit,
                aggregation=rule.metric,
                reason="no_observations_in_window",
                raw=payload,
                evidence_hash=evidence_hash,
            )
        resolution_values = self._resolution_values(rule, values)
        series = self._series_points(rule, values, resolution_values)
        if sample_set_of(rule) == "hourly" and not resolution_values:
            return ObservationEvidence(
                status="unavailable",
                observed_at=now_iso(current),
                source_url=url,
                provider=str(rule.source.get("provider") or ""),
                station_id=str(rule.source.get("station_id") or ""),
                unit=rule.unit,
                aggregation=rule.metric,
                reason="hourly_filter_empty",
                raw=payload,
                evidence_hash=evidence_hash,
                series=series,
            )
        if rule.metric == "daily_max":
            value = max(item[1] for item in resolution_values)
        elif rule.metric == "daily_min":
            value = min(item[1] for item in resolution_values)
        elif rule.metric == "daily_sum":
            value = sum(item[1] for item in resolution_values)
        else:
            value = sorted(resolution_values, key=lambda item: item[0])[-1][1]
        raw_aggregate = float(value)
        settled = apply_rounding(raw_aggregate, rule.rounding or ROUNDING_WHOLE)
        if settled is None:
            return ObservationEvidence(
                status="error",
                observed_at=now_iso(current),
                source_url=url,
                provider=str(rule.source.get("provider") or ""),
                station_id=str(rule.source.get("station_id") or ""),
                unit=rule.unit,
                aggregation=rule.metric,
                reason="value_not_numeric",
                raw=payload,
                evidence_hash=evidence_hash,
                raw_aggregate=raw_aggregate,
                series=series,
            )
        value = settled
        latest_timestamp = max(item[0] for item in resolution_values)
        following = self._following_values(rule, payload)
        final_path = str(rule.source.get("final_path") or "").strip()
        explicit_final = _boolean(json_path(payload, final_path)) if final_path else False
        if not final_path and isinstance(payload, dict):
            explicit_final = _boolean(payload.get("final"))
        for _, _, row in resolution_values:
            if isinstance(row, dict) and _boolean(row.get("final")):
                explicit_final = True
        requires_following = bool(
            rule.source.get("requires_following_date_point")
            or rule.source.get("finality_mode") == "first_following_date_point"
        )
        if window_open:
            return ObservationEvidence(
                status="intraday",
                value=float(value),
                raw_aggregate=raw_aggregate,
                source_timestamp=latest_timestamp.isoformat(),
                observed_at=now_iso(current),
                source_url=url,
                provider=str(rule.source.get("provider") or ""),
                station_id=str(rule.source.get("station_id") or ""),
                unit=rule.unit,
                aggregation=rule.metric,
                reason="running_extremum",
                raw=payload,
                evidence_hash=evidence_hash,
                series=series,
            )
        coverage_ok = self._window_coverage_ok(rule, resolution_values)
        following_ok = requires_following and bool(following) and coverage_ok
        is_final = bool(explicit_final or following_ok)
        confirmation = ""
        if following:
            confirmation = following[0][0].isoformat()
        elif explicit_final:
            confirmation = str(
                rule.source.get("finalized_at")
                or rule.source.get("final_timestamp")
                or latest_timestamp.isoformat()
            )
        if is_final:
            reason = "final_confirmation"
        elif not coverage_ok:
            reason = "incomplete_observation_window"
        else:
            reason = "awaiting_final_confirmation"
        return ObservationEvidence(
            status="final" if is_final else "provisional",
            value=float(value),
            raw_aggregate=raw_aggregate,
            source_timestamp=latest_timestamp.isoformat(),
            confirmation_timestamp=confirmation,
            observed_at=now_iso(current),
            source_url=url,
            provider=str(rule.source.get("provider") or ""),
            station_id=str(rule.source.get("station_id") or ""),
            unit=rule.unit,
            aggregation=rule.metric,
            reason=reason,
            raw=payload,
            evidence_hash=evidence_hash,
            series=series,
        )

    def _window_coverage_ok(
        self,
        rule: WeatherRule,
        resolution_values: list[tuple[datetime, float, dict[str, Any]]],
    ) -> bool:
        """True when the last in-window sample is close enough to local day end."""

        end = self._parse(rule, rule.observation_end)
        if end is None or not resolution_values:
            return False
        last = max(item[0] for item in resolution_values).astimezone(timezone.utc)
        end_utc = end.astimezone(timezone.utc)
        if last > end_utc:
            return False
        return (end_utc - last) <= _WINDOW_END_MAX_LAG

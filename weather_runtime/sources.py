"""NWS/Wunderground-compatible read-only weather source adapters."""

from __future__ import annotations

import json
import os
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
        timeout: float = 15.0,
        retries: int = 2,
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
                with self._opener().open(request, timeout=self.timeout) as response:
                    raw = response.read().decode("utf-8")
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
                with self._opener().open(request, timeout=self.timeout) as response:
                    raw = response.read().decode("utf-8")
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


class WeatherSourceAdapter:
    """Poll one approved rule and return immutable source evidence."""

    def __init__(self, *, http: Optional[JsonHttp] = None, http_cache_ttl_s: float = 60.0):
        self.http = http or JsonHttp()
        self.http_cache_ttl_s = max(0.0, float(http_cache_ttl_s))
        self._http_cache: dict[tuple[str, str], tuple[float, Any]] = {}

    def _zone(self, rule: WeatherRule):
        return load_timezone(rule.timezone)

    def _parse(self, rule: WeatherRule, value: Any):
        return parse_time(value, default_tz=self._zone(rule))

    def _payload(self, rule: WeatherRule) -> tuple[Any, str]:
        source = rule.source
        if isinstance(source.get("static"), dict):
            payload = self._normalize_payload(rule, source["static"])
            return payload, str(source.get("url") or source.get("resolution_source") or "static://weather")
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
        headers = dict(source.get("headers") or {}) if isinstance(source.get("headers"), dict) else {}
        if str(source.get("provider") or "").lower() == "wunderground" or "api.weather.com" in url:
            headers.setdefault("Accept", "application/json")
            headers.setdefault("Referer", "https://www.wunderground.com/")
        headers = headers or None
        cache_key = (url, json.dumps(params or {}, sort_keys=True, default=str))
        if self.http_cache_ttl_s:
            hit = self._http_cache.get(cache_key)
            if hit and (time.time() - hit[0]) < self.http_cache_ttl_s:
                return hit[1], url
        payload = self.http.get_json(url, params=params or None, headers=headers)
        payload = self._normalize_payload(rule, payload)
        if self.http_cache_ttl_s:
            self._http_cache[cache_key] = (time.time(), payload)
        return payload, url

    def poll(self, rule: WeatherRule, *, now: Optional[datetime] = None) -> ObservationEvidence:
        current = now or datetime.now(timezone.utc)
        end = self._parse(rule, rule.observation_end)
        if end is None:
            return ObservationEvidence(
                status="error",
                observed_at=now_iso(current),
                reason="invalid_observation_end",
            )
        if current.astimezone(timezone.utc) < end:
            return ObservationEvidence(
                status="waiting_window",
                observed_at=now_iso(current),
                provider=str(rule.source.get("provider") or ""),
                station_id=str(rule.source.get("station_id") or ""),
                unit=rule.unit,
                aggregation=rule.metric,
                reason="observation_window_open",
            )
        grace_hours = float(rule.source.get("poll_after_end_hours") or 48)
        if current.astimezone(timezone.utc) > end + timedelta(hours=max(0.0, grace_hours)):
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
            payload, url = self._payload(rule)
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
            return self._evaluate(rule, payload, url, current, evidence_hash)
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

    def _normalize_payload(self, rule: WeatherRule, payload: Any) -> Any:
        source = rule.source
        provider = str(source.get("provider") or "").lower()
        url = str(source.get("url") or source.get("resolution_source") or "")
        if provider == "hko" or "data.weather.gov.hk" in url or "weather.gov.hk" in url:
            return self._hko_observations(payload)
        return payload

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
                result.append((timestamp, _convert(number, source_unit, rule.unit)))
        return sorted(result, key=lambda item: item[0])

    def _evaluate(
        self,
        rule: WeatherRule,
        payload: Any,
        url: str,
        current: datetime,
        evidence_hash: str,
    ) -> ObservationEvidence:
        values = self._iter_values(rule, payload)
        if not values:
            return ObservationEvidence(
                status="unavailable",
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
        if rule.metric == "daily_max":
            value = max(item[1] for item in values)
        elif rule.metric == "daily_min":
            value = min(item[1] for item in values)
        elif rule.metric == "daily_sum":
            value = sum(item[1] for item in values)
        else:
            value = sorted(values, key=lambda item: item[0])[-1][1]
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
            )
        value = settled
        latest_timestamp = max(item[0] for item in values)
        following = self._following_values(rule, payload)
        final_path = str(rule.source.get("final_path") or "").strip()
        explicit_final = _boolean(json_path(payload, final_path)) if final_path else False
        if not final_path and isinstance(payload, dict):
            explicit_final = _boolean(payload.get("final"))
        for _, _, row in values:
            if isinstance(row, dict) and _boolean(row.get("final")):
                explicit_final = True
        requires_following = bool(
            rule.source.get("requires_following_date_point")
            or rule.source.get("finality_mode") == "first_following_date_point"
        )
        is_final = explicit_final or (requires_following and bool(following))
        confirmation = ""
        if following:
            confirmation = following[0][0].isoformat()
        elif explicit_final:
            confirmation = str(
                rule.source.get("finalized_at")
                or rule.source.get("final_timestamp")
                or latest_timestamp.isoformat()
            )
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
            reason="final_confirmation" if is_final else "awaiting_final_confirmation",
            raw=payload,
            evidence_hash=evidence_hash,
        )

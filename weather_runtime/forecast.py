"""Daily forecast gate for the diurnal weather strategies.

Open-Meteo's daily endpoint is used instead of hourly forecasts. The gate
compares the event day with the previous day and blocks the whole event day
when the two whole-day weather classes differ.
"""

from __future__ import annotations

import threading
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from .sources import JsonHttp
from .station_tz import coordinates_for_station


OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
_DEFAULT_FORECAST_CACHE_TTL_S = 3600.0


def classify_wmo_daily(code: Any) -> str:
    """Map a WMO daily weather code to the diurnal weather classes."""

    try:
        value = int(code)
    except (TypeError, ValueError):
        return "unknown"
    if value in {0, 1}:
        return "clear"
    if value in {2, 3, 45, 48}:
        return "cloudy"
    if 51 <= value <= 67 or 71 <= value <= 77 or value in {80, 81, 82, 85, 86}:
        return "precip"
    if value in {95, 96, 99}:
        return "convective"
    return "unknown"


def _daily_value(daily: dict[str, Any], field: str, local_date: str) -> Any:
    values = daily.get(field)
    dates = daily.get("time")
    if not isinstance(values, list) or not isinstance(dates, list):
        return None
    for index, item in enumerate(dates):
        if str(item or "")[:10] == local_date and index < len(values):
            return values[index]
    return None


def _forecast_day(daily: dict[str, Any], local_date: str) -> dict[str, Any]:
    return {
        "date": local_date,
        "weather_code": _daily_value(daily, "weather_code", local_date),
        "weather_class": classify_wmo_daily(_daily_value(daily, "weather_code", local_date)),
        "temperature_max": _daily_value(daily, "temperature_2m_max", local_date),
        "temperature_min": _daily_value(daily, "temperature_2m_min", local_date),
        "precipitation_sum": _daily_value(daily, "precipitation_sum", local_date),
    }


class OpenMeteoForecastAdapter:
    """Cached Open-Meteo daily forecast reader used by the diurnal gate."""

    def __init__(
        self,
        *,
        http: Optional[JsonHttp] = None,
        cache_ttl_s: float = _DEFAULT_FORECAST_CACHE_TTL_S,
    ):
        self.http = http or JsonHttp(timeout=10.0, retries=2)
        self.cache_ttl_s = max(0.0, float(cache_ttl_s))
        self._cache: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}
        self._lock = threading.Lock()

    def daily_forecast(
        self,
        *,
        station_id: str,
        local_date: str,
        timezone_name: str,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        station = str(station_id or "").strip().upper()
        event_day_text = str(local_date or "")[:10]
        cache_key = (station, event_day_text)
        current_time = time.monotonic()
        with self._lock:
            cached = self._cache.get(cache_key)
            if cached is not None and current_time - cached[0] < self.cache_ttl_s:
                return cached[1]

        fetched_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
        try:
            event_day = date.fromisoformat(event_day_text)
            previous_day = event_day - timedelta(days=1)
            coordinates = coordinates_for_station(station)
            if coordinates is None:
                raise ValueError("unknown station")
            latitude, longitude = coordinates
            payload = self.http.get_json(
                OPEN_METEO_FORECAST_URL,
                params={
                    "latitude": latitude,
                    "longitude": longitude,
                    "daily": ",".join(
                        [
                            "weather_code",
                            "temperature_2m_max",
                            "temperature_2m_min",
                            "precipitation_sum",
                        ]
                    ),
                    "start_date": previous_day.isoformat(),
                    "end_date": event_day.isoformat(),
                    "timezone": str(timezone_name or "auto"),
                },
            )
            daily = payload.get("daily") if isinstance(payload, dict) else None
            if not isinstance(daily, dict):
                raise ValueError("daily payload missing")
            first_day = _forecast_day(daily, previous_day.isoformat())
            second_day = _forecast_day(daily, event_day.isoformat())
            if first_day["weather_class"] == "unknown" or second_day["weather_class"] == "unknown":
                status = "unknown"
            else:
                status = "ok"
            result = {
                "provider": "open-meteo",
                "station_id": station,
                "timezone": str(timezone_name or ""),
                "first_day": first_day,
                "second_day": second_day,
                "forecast_weather_class_first_day": first_day["weather_class"],
                "forecast_weather_class_second_day": second_day["weather_class"],
                "forecast_weather_match": (
                    status == "ok" and first_day["weather_class"] == second_day["weather_class"]
                ),
                "status": status,
                "error": "",
                "fetched_at": fetched_at,
            }
        except Exception as exc:  # noqa: BLE001
            result = {
                "provider": "open-meteo",
                "station_id": station,
                "timezone": str(timezone_name or ""),
                "first_day": None,
                "second_day": None,
                "forecast_weather_class_first_day": "unknown",
                "forecast_weather_class_second_day": "unknown",
                "forecast_weather_match": False,
                "status": "error",
                "error": str(exc),
                "fetched_at": fetched_at,
            }

        with self._lock:
            self._cache[cache_key] = (current_time, result)
        return result

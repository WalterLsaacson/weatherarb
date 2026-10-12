from __future__ import annotations

import unittest
from datetime import datetime
from typing import Any

from weather_runtime.diurnal import build_diurnal, clear_diurnal_state
from weather_runtime.forecast import OpenMeteoForecastAdapter, classify_wmo_daily
from weather_runtime.rules import load_timezone


def _point(local: str, temp: float, sky: str = "CLR") -> dict[str, Any]:
    return {
        "local_time": local,
        "temp": temp,
        "counts_for_resolution": True,
        "sky": sky,
    }


def _daily_payload(first_code: int, second_code: int) -> dict[str, Any]:
    return {
        "daily": {
            "time": ["2026-10-11", "2026-10-12"],
            "weather_code": [first_code, second_code],
            "temperature_2m_max": [20.0, 21.0],
            "temperature_2m_min": [10.0, 11.0],
            "precipitation_sum": [1.0, 0.0],
        }
    }


class FakeHttp:
    def __init__(self, payload: Any):
        self.payload = payload
        self.calls = 0

    def get_json(self, url: str, *, params: Any = None, headers: Any = None) -> Any:
        self.calls += 1
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class ForecastTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_diurnal_state()

    def test_wmo_daily_codes_map_to_diurnal_classes(self) -> None:
        self.assertEqual(classify_wmo_daily(0), "clear")
        self.assertEqual(classify_wmo_daily(3), "cloudy")
        self.assertEqual(classify_wmo_daily(63), "precip")
        self.assertEqual(classify_wmo_daily(95), "convective")
        self.assertEqual(classify_wmo_daily(None), "unknown")

    def test_adapter_compares_event_day_with_previous_day(self) -> None:
        http = FakeHttp(_daily_payload(63, 3))
        adapter = OpenMeteoForecastAdapter(http=http, cache_ttl_s=900.0)
        first = adapter.daily_forecast(
            station_id="KLGA",
            local_date="2026-10-12",
            timezone_name="America/New_York",
        )
        second = adapter.daily_forecast(
            station_id="KLGA",
            local_date="2026-10-12",
            timezone_name="America/New_York",
        )
        self.assertEqual(http.calls, 1)
        self.assertEqual(first["status"], "ok")
        self.assertFalse(first["forecast_weather_match"])
        self.assertEqual(first["forecast_weather_class_first_day"], "precip")
        self.assertEqual(first["forecast_weather_class_second_day"], "cloudy")
        self.assertIs(first, second)

    def test_adapter_marks_unknown_forecast_as_not_ok(self) -> None:
        adapter = OpenMeteoForecastAdapter(http=FakeHttp(_daily_payload(999, 3)))
        payload = adapter.daily_forecast(
            station_id="KLGA",
            local_date="2026-10-12",
            timezone_name="America/New_York",
        )
        self.assertEqual(payload["status"], "unknown")
        self.assertFalse(payload["forecast_weather_match"])

    def test_adapter_error_is_a_hard_block(self) -> None:
        adapter = OpenMeteoForecastAdapter(http=FakeHttp(RuntimeError("network down")))
        payload = adapter.daily_forecast(
            station_id="KLGA",
            local_date="2026-10-12",
            timezone_name="America/New_York",
        )
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["forecast_weather_class_first_day"], "unknown")
        self.assertEqual(payload["forecast_weather_class_second_day"], "unknown")
        self.assertFalse(payload["forecast_weather_match"])

    def _diurnal_payload(self, forecast: dict[str, Any]) -> dict[str, Any]:
        zone = load_timezone("America/New_York")
        yesterday = [
            _point("2026-10-11 08:00", 16),
            _point("2026-10-11 12:00", 24),
            _point("2026-10-11 16:00", 18),
        ]
        today = [
            _point("2026-10-12 08:00", 16),
            _point("2026-10-12 12:00", 24),
            _point("2026-10-12 16:00", 18),
        ]
        return build_diurnal(
            station_id="KLGA",
            timezone_name="America/New_York",
            local_date="2026-10-12",
            metric="daily_max",
            unit="C",
            series=today,
            yesterday_series=yesterday,
            now=datetime(2026, 10, 12, 20, 0, tzinfo=zone),
            running_value=24,
            running_bucket={"lower": 23.0, "upper": 24.0},
            observation_status="intraday",
            forecast=forecast,
        )

    def test_forecast_mismatch_blocks_the_whole_event_day(self) -> None:
        payload = self._diurnal_payload(
            {
                "status": "ok",
                "forecast_weather_class_first_day": "precip",
                "forecast_weather_class_second_day": "clear",
                "forecast_weather_match": False,
            }
        )
        self.assertFalse(payload["trigger"])
        self.assertEqual(payload["block_reason"], "forecast_weather_mismatch")
        self.assertEqual(payload["phase"], "forecast_weather_mismatch")
        self.assertFalse(payload["forecast_weather_match"])

    def test_matching_forecast_keeps_existing_diurnal_gates(self) -> None:
        payload = self._diurnal_payload(
            {
                "status": "ok",
                "forecast_weather_class_first_day": "clear",
                "forecast_weather_class_second_day": "clear",
                "forecast_weather_match": True,
            }
        )
        self.assertTrue(payload["trigger"])
        self.assertEqual(payload["block_reason"], "")
        self.assertEqual(payload["phase"], "ready")
        self.assertTrue(payload["forecast_weather_match"])

    def test_missing_forecast_is_a_hard_block(self) -> None:
        payload = self._diurnal_payload(
            {
                "status": "error",
                "forecast_weather_class_first_day": "unknown",
                "forecast_weather_class_second_day": "unknown",
                "forecast_weather_match": False,
            }
        )
        self.assertFalse(payload["trigger"])
        self.assertEqual(payload["block_reason"], "forecast_weather_unknown")
        self.assertEqual(payload["phase"], "forecast_weather_unknown")


if __name__ == "__main__":
    unittest.main()

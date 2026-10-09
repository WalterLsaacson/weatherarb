from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from weather_runtime.diurnal import (
    DIURNAL_MAX_LOSER_NO,
    DIURNAL_MAX_REASON,
    DIURNAL_MIN_LOSER_NO,
    DIURNAL_MIN_REASON,
    DIURNAL_REASONS,
    build_diurnal,
    classify_weather,
    clear_diurnal_state,
    max_window,
    parse_metar,
    solar_day,
)
from weather_runtime.rules import load_timezone
from weather_runtime.scanner import WeatherScanner, WeatherScannerConfig, _OPPORTUNITY_REASONS
from weather_runtime.service import (
    _LIMIT_ORDER_REASONS,
    _LOCKED_NO_REASONS,
    RuntimeService,
    _limit_lock_token,
    _list_rows_for_board,
    _lock_snapshot,
)
from weather_runtime.station_tz import STATION_COORDS, STATION_TIMEZONES


def _scanner_helper():
    from tests.test_weather_runtime import WeatherRuntimeTests

    return WeatherRuntimeTests()


def _yesterday_extreme(station: str, day: date, *, kind: str) -> list:
    """Previous-day extremum that turns inside today's clock window."""

    zone = load_timezone("America/Los_Angeles")
    solar = solar_day(station, day, zone)
    horizon = datetime(day.year, day.month, day.day, 23, tzinfo=zone)
    if kind == "max":
        start, end, _band = max_window(solar["solar_noon"], "clear", [], zone, horizon)
        extreme = 40.0
        other = 10.0
    else:
        start = solar["sunrise"] - timedelta(hours=1)
        end = solar["sunrise"] - timedelta(minutes=30)
        extreme = 0.0
        other = 20.0
    mid = start + (end - start) / 2
    prev = mid - timedelta(days=1)
    filler = prev.replace(hour=0, minute=10)
    after = prev + timedelta(hours=2)
    return [
        _point(filler.strftime("%Y-%m-%d %H:%M"), other),
        _point(prev.strftime("%Y-%m-%d %H:%M"), extreme),
        _point(after.strftime("%Y-%m-%d %H:%M"), other),
    ]


def _point(local: str, temp: float, sky: str = "CLR", wx: str = "") -> dict:
    zone = load_timezone("America/Los_Angeles")
    parsed = datetime.strptime(local, "%Y-%m-%d %H:%M").replace(tzinfo=zone)
    item = {
        "timestamp": parsed.astimezone(timezone.utc).isoformat(),
        "local_time": local,
        "temp": temp,
        "counts_for_resolution": True,
        "sky": sky,
    }
    if wx:
        item["wx"] = wx
    return item


class DiurnalTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_diurnal_state()

    def test_station_coords_cover_timezones(self) -> None:
        self.assertEqual(set(STATION_COORDS), set(STATION_TIMEZONES))

    def test_solar_noon_matches_noaa_within_two_minutes(self) -> None:
        # 105W is one hour west of the MDT meridian (90W). NOAA solar noon on
        # 2026-06-21 is 13:00 MDT plus the equation of time, about -1.6 minutes.
        zone = load_timezone("America/Denver")
        first = solar_day("KBKF", date(2026, 6, 21), zone)
        second = solar_day("KBKF", date(2026, 6, 21), zone)
        self.assertIs(first, second)
        noon = first["solar_noon"]
        self.assertEqual(noon.strftime("%Y-%m-%d %H:%M"), "2026-06-21 13:00")
        later = solar_day("KBKF", date(2026, 6, 22), zone)
        self.assertNotEqual(later["sunrise"], first["sunrise"])
        london = solar_day("EGLC", date(2026, 6, 21), load_timezone("Europe/London"))
        self.assertEqual(london["sunrise"].strftime("%H:%M"), "04:42")
        day_length = london["sunset"] - london["sunrise"]
        self.assertGreater(day_length, timedelta(hours=16))
        self.assertLess(day_length, timedelta(hours=17))
        self.assertLess(london["sunrise"], london["solar_noon"])
        self.assertLess(london["solar_noon"], london["sunset"])

    def test_metar_classes_and_max_window_shift(self) -> None:
        self.assertEqual(parse_metar("KSFO 071800Z 10SM CLR 22/12 RMK AO2"), ("CLR", ""))
        self.assertEqual(parse_metar("KSFO 071800Z 10SM BKN040 22/12"), ("BKN", ""))
        self.assertEqual(parse_metar("KSFO 071800Z 4SM -RA BKN020 18/16"), ("BKN", "precip"))
        self.assertEqual(parse_metar("KSFO 072000Z VCTS FEW040 24/17"), ("FEW", "thunder"))
        zone = load_timezone("America/Los_Angeles")
        now = datetime(2026, 9, 7, 23, tzinfo=timezone.utc)
        clear = [_point("2026-09-07 12:00", 24, "CLR")]
        cloudy = [_point("2026-09-07 12:00", 24, "BKN")]
        rainy = [_point("2026-09-07 12:00", 24, "OVC", "precip")]
        solar = solar_day("KSFO", date(2026, 9, 7), zone)
        storm_at = solar["solar_noon"] + timedelta(hours=2, minutes=20)
        storm = [_point(storm_at.strftime("%Y-%m-%d %H:%M"), 24, "FEW", "thunder")]
        self.assertEqual(classify_weather(clear, zone, now), "clear")
        self.assertEqual(classify_weather(cloudy, zone, now), "cloudy")
        self.assertEqual(classify_weather(rainy, zone, now), "precip")
        self.assertEqual(classify_weather(storm, zone, now), "convective")
        base_start, base_end, _uncertainty = max_window(solar["solar_noon"], "clear", clear, zone, now)
        cloudy_start, cloudy_end, _uncertainty = max_window(solar["solar_noon"], "cloudy", cloudy, zone, now)
        rainy_start, rainy_end, rainy_band = max_window(solar["solar_noon"], "precip", rainy, zone, now)
        storm_start, storm_end, storm_band = max_window(solar["solar_noon"], "convective", storm, zone, now)
        self.assertEqual(cloudy_start, base_start - timedelta(minutes=30))
        self.assertEqual(cloudy_end, base_end - timedelta(minutes=30))
        self.assertEqual(rainy_start, base_start - timedelta(minutes=60))
        self.assertEqual(rainy_end, base_end - timedelta(minutes=60))
        self.assertEqual(rainy_band, 2)
        self.assertEqual(storm_end, storm_at.replace(second=0, microsecond=0))
        self.assertEqual(storm_band, 2)
        self.assertEqual(storm_start, base_start)

    def test_two_day_delta_blocks_both_metrics_and_latches(self) -> None:
        zone = load_timezone("America/Los_Angeles")
        solar = solar_day("KSFO", date(2026, 9, 7), zone)
        noon = solar["solar_noon"]
        today = []
        yesterday = []
        for hour in range(0, 12):
            today.append(_point("2026-09-07 {:02d}:00".format(hour), 20.0))
            yesterday.append(_point("2026-09-06 {:02d}:00".format(hour), 16.5))
        now = noon + timedelta(hours=4)
        blocked = build_diurnal(
            station_id="KSFO",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_max",
            unit="C",
            series=today,
            yesterday_series=yesterday,
            now=now,
            running_value=20,
            running_bucket={"lower": 19, "upper": 21, "lower_inclusive": False, "upper_inclusive": True},
            observation_status="intraday",
        )
        self.assertEqual(blocked["continuity"]["status"], "regime_change")
        self.assertGreater(blocked["continuity"]["delta"], 2.0)
        self.assertFalse(blocked["trigger"])
        self.assertTrue(blocked["sunrise_local"])
        cooled = build_diurnal(
            station_id="KSFO",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_min",
            unit="C",
            series=[_point("2026-09-07 {:02d}:00".format(hour), 20.0) for hour in range(0, 12)],
            yesterday_series=[_point("2026-09-06 {:02d}:00".format(hour), 20.0) for hour in range(0, 12)],
            now=now,
            running_value=18,
            observation_status="intraday",
        )
        self.assertEqual(cooled["continuity"]["status"], "regime_change")
        self.assertFalse(cooled["trigger"])

    def test_missing_yesterday_does_not_learn_a_window(self) -> None:
        zone = load_timezone("America/Los_Angeles")
        solar = solar_day("KSFO", date(2026, 9, 7), zone)
        series = [
            _point("2026-09-07 08:00", 18),
            _point("2026-09-07 10:00", 21),
            _point("2026-09-07 12:00", 24),
            _point("2026-09-07 15:00", 27),
        ]
        now = solar["solar_noon"] + timedelta(hours=4)
        payload = build_diurnal(
            station_id="KATL",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_max",
            unit="F",
            series=series,
            yesterday_series=[],
            now=now,
            running_value=27,
            running_bucket={"lower": 26, "upper": 28},
            observation_status="intraday",
        )
        self.assertEqual(payload["continuity"]["status"], "no_baseline")
        self.assertEqual(payload["continuity"]["threshold"], 3.6)
        self.assertEqual(payload["city_class"], "A")
        self.assertEqual(payload["influences"], ["heat_island"])
        self.assertTrue(payload["max_window_start_local"])
        self.assertFalse(payload["learned_max_valid"])
        self.assertFalse(payload["learned_min_valid"])
        self.assertFalse(payload["trigger"])
        self.assertEqual(payload["block_reason"], "no_trend")
        self.assertEqual(payload["phase"], "trend_unlearned")
        self.assertFalse(payload["near_edge"])

    def test_min_waits_until_learned_buy_and_a_finished_fall(self) -> None:
        zone = load_timezone("America/Los_Angeles")
        yesterday = [
            _point("2026-09-06 02:00", 16),
            _point("2026-09-06 04:00", 8),
        ]
        early_series = [
            _point("2026-09-07 03:00", 15),
            _point("2026-09-07 04:00", 14),
            _point("2026-09-07 04:30", 14.5),
        ]
        early = build_diurnal(
            station_id="KSFO",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_min",
            unit="C",
            series=early_series,
            yesterday_series=yesterday,
            now=datetime(2026, 9, 7, 5, 30, tzinfo=zone),
            running_value=14,
            observation_status="intraday",
        )
        self.assertTrue(early["learned_min_valid"])
        self.assertEqual(early["learned_min_buy_local"], "2026-09-07 06:00")
        self.assertFalse(early["trigger"])
        self.assertEqual(early["block_reason"], "before_learned_buy")
        falling = early_series + [_point("2026-09-07 10:30", 13)]
        still = build_diurnal(
            station_id="KSFO",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_min",
            unit="C",
            series=falling,
            yesterday_series=yesterday,
            now=datetime(2026, 9, 7, 11, 0, tzinfo=zone),
            running_value=13,
            observation_status="intraday",
        )
        self.assertFalse(still["trigger"])
        self.assertEqual(still["block_reason"], "still_falling")
        self.assertEqual(still["phase"], "pending")
        ready = build_diurnal(
            station_id="KATL",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_min",
            unit="C",
            series=early_series + [_point("2026-09-07 11:00", 18)],
            yesterday_series=yesterday,
            now=datetime(2026, 9, 7, 12, 0, tzinfo=zone),
            running_value=14,
            running_bucket={"lower": 13.8, "upper": 15},
            observation_status="intraday",
        )
        self.assertTrue(ready["trigger"])
        self.assertEqual(ready["city_class"], "A")
        self.assertTrue(ready["near_edge"])
        self.assertEqual(ready["learned_min_start_local"], "2026-09-07 04:00")
        self.assertEqual(ready["learned_min_end_local"], "2026-09-07 05:00")

    def test_short_series_does_not_trigger(self) -> None:
        zone = load_timezone("America/Los_Angeles")
        solar = solar_day("KSFO", date(2026, 9, 7), zone)
        payload = build_diurnal(
            station_id="KSFO",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_max",
            unit="C",
            series=[_point("2026-09-07 12:00", 20), _point("2026-09-07 15:00", 24)],
            yesterday_series=[],
            now=solar["solar_noon"] + timedelta(hours=4),
            running_value=24,
            observation_status="intraday",
        )
        self.assertFalse(payload["trigger"])
        self.assertEqual(payload["block_reason"], "sample_count")
        learned_but_short = build_diurnal(
            station_id="KSFO",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_max",
            unit="C",
            series=[_point("2026-09-07 12:00", 20), _point("2026-09-07 15:00", 24)],
            yesterday_series=[
                _point("2026-09-06 08:00", 16),
                _point("2026-09-06 12:00", 22),
            ],
            now=datetime(2026, 9, 7, 16, 0, tzinfo=zone),
            running_value=24,
            observation_status="intraday",
        )
        self.assertEqual(learned_but_short["block_reason"], "sample_count")
        self.assertEqual(learned_but_short["phase"], "pending")
        self.assertTrue(learned_but_short["passed"])

    def test_one_fall_buys_two_hours_after_its_low(self) -> None:
        series = [
            _point("2026-09-07 04:00", 15),
            _point("2026-09-07 05:00", 14),
            _point("2026-09-07 06:00", 14.5),
            _point("2026-09-07 11:00", 18),
        ]
        payload = build_diurnal(
            station_id="KATL",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_min",
            unit="C",
            series=series,
            yesterday_series=[
                _point("2026-09-06 01:00", 18),
                _point("2026-09-06 03:00", 9),
                _point("2026-09-06 03:40", 9),
            ],
            now=datetime(2026, 9, 7, 12, 0, tzinfo=load_timezone("America/Los_Angeles")),
            running_value=14,
            observation_status="intraday",
        )
        self.assertTrue(payload["learned_min_valid"])
        self.assertEqual(payload["yesterday_min_at_local"], "2026-09-06 03:40")
        self.assertEqual(payload["learned_min_buy_local"], "2026-09-07 05:40")
        self.assertTrue(payload["min_window_start_local"])
        self.assertTrue(payload["trigger"])

    def test_two_falls_block_only_the_min_side(self) -> None:
        zone = load_timezone("America/Los_Angeles")
        yesterday = [
            _point("2026-09-06 00:10", 16),
            _point("2026-09-06 03:00", 10),
            _point("2026-09-06 04:00", 15),
            _point("2026-09-06 08:00", 18),
            _point("2026-09-06 12:00", 24),
            _point("2026-09-06 15:00", 28),
            _point("2026-09-06 18:00", 22),
            _point("2026-09-06 22:00", 16),
        ]
        today = [
            _point("2026-09-07 04:00", 15),
            _point("2026-09-07 08:00", 18),
            _point("2026-09-07 12:00", 24),
            _point("2026-09-07 15:00", 27),
        ]
        minimum = build_diurnal(
            station_id="KATL",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_min",
            unit="C",
            series=today,
            yesterday_series=yesterday,
            now=datetime(2026, 9, 7, 12, 0, tzinfo=zone),
            running_value=15,
            observation_status="intraday",
        )
        maximum = build_diurnal(
            station_id="KATL",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_max",
            unit="C",
            series=today,
            yesterday_series=yesterday,
            now=datetime(2026, 9, 7, 18, 0, tzinfo=zone),
            running_value=27,
            observation_status="intraday",
        )
        self.assertTrue(minimum["learned_min_valid"])
        self.assertTrue(minimum["learned_max_valid"])
        self.assertEqual(minimum["yesterday_min_at_local"], "2026-09-06 03:00")
        self.assertEqual(minimum["learned_min_buy_local"], "2026-09-07 05:00")
        self.assertTrue(minimum["trigger"])
        self.assertEqual(minimum["block_reason"], "")
        self.assertEqual(minimum["phase"], "ready")
        self.assertTrue(maximum["learned_max_valid"])
        self.assertTrue(maximum["learned_min_valid"])
        self.assertEqual(maximum["yesterday_max_at_local"], "2026-09-06 15:00")
        self.assertEqual(maximum["learned_max_buy_local"], "2026-09-07 17:00")
        self.assertTrue(maximum["trigger"])

    def test_unfinished_fall_still_learns_the_min(self) -> None:
        zone = load_timezone("America/Los_Angeles")
        yesterday = [
            _point("2026-09-06 01:00", 24),
            _point("2026-09-06 03:00", 21),
            _point("2026-09-06 05:00", 19),
        ]
        today = [
            _point("2026-09-07 04:00", 15),
            _point("2026-09-07 08:00", 18),
            _point("2026-09-07 12:00", 24),
        ]
        payload = build_diurnal(
            station_id="KATL",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_min",
            unit="C",
            series=today,
            yesterday_series=yesterday,
            now=datetime(2026, 9, 7, 12, 0, tzinfo=zone),
            running_value=15,
            observation_status="intraday",
        )
        self.assertTrue(payload["learned_min_valid"])
        self.assertTrue(payload["learned_max_valid"])
        self.assertEqual(payload["yesterday_max_at_local"], "2026-09-06 01:00")
        self.assertEqual(payload["learned_max_buy_local"], "2026-09-07 03:00")
        self.assertEqual(payload["learned_min_buy_local"], "2026-09-07 07:00")
        self.assertTrue(payload["trigger"])
        late = build_diurnal(
            station_id="KATL",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_min",
            unit="C",
            series=today,
            yesterday_series=[
                _point("2026-09-06 18:00", 24),
                _point("2026-09-06 21:00", 21),
                _point("2026-09-06 23:00", 19),
            ],
            now=datetime(2026, 9, 7, 12, 0, tzinfo=zone),
            running_value=15,
            observation_status="intraday",
        )
        self.assertTrue(late["learned_min_valid"])
        self.assertEqual(late["learned_min_buy_local"], "2026-09-08 01:00")
        self.assertFalse(late["trigger"])
        self.assertEqual(late["block_reason"], "before_learned_buy")

    def test_two_rises_block_only_the_max_side(self) -> None:
        zone = load_timezone("America/Los_Angeles")
        yesterday = [
            _point("2026-09-06 02:00", 10),
            _point("2026-09-06 08:00", 18),
            _point("2026-09-06 12:00", 14),
            _point("2026-09-06 16:00", 22),
        ]
        today = [
            _point("2026-09-07 08:00", 18),
            _point("2026-09-07 10:00", 21),
            _point("2026-09-07 12:00", 24),
            _point("2026-09-07 15:00", 27),
        ]
        payload = build_diurnal(
            station_id="KATL",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_max",
            unit="C",
            series=today,
            yesterday_series=yesterday,
            now=datetime(2026, 9, 7, 20, 0, tzinfo=zone),
            running_value=27,
            observation_status="intraday",
        )
        self.assertTrue(payload["learned_max_valid"])
        self.assertTrue(payload["learned_min_valid"])
        self.assertEqual(payload["yesterday_max_at_local"], "2026-09-06 16:00")
        self.assertEqual(payload["learned_max_buy_local"], "2026-09-07 18:00")
        self.assertTrue(payload["trigger"])
        self.assertEqual(payload["block_reason"], "")
        self.assertEqual(payload["phase"], "ready")

    def test_weather_mismatch_blocks_a_learned_window(self) -> None:
        zone = load_timezone("America/Los_Angeles")
        yesterday = [
            _point("2026-09-06 08:00", 16, "OVC", "precip"),
            _point("2026-09-06 12:00", 20, "OVC", "precip"),
            _point("2026-09-06 16:00", 18, "OVC", "precip"),
        ]
        today = [
            _point("2026-09-07 08:00", 16),
            _point("2026-09-07 12:00", 20),
            _point("2026-09-07 16:00", 18),
        ]
        payload = build_diurnal(
            station_id="KATL",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_max",
            unit="C",
            series=today,
            yesterday_series=yesterday,
            now=datetime(2026, 9, 7, 20, 0, tzinfo=zone),
            running_value=20,
            observation_status="intraday",
        )
        self.assertTrue(payload["learned_max_valid"])
        self.assertEqual(payload["weather_class"], "clear")
        self.assertEqual(payload["yesterday_weather_class"], "precip")
        self.assertFalse(payload["trigger"])
        self.assertEqual(payload["block_reason"], "weather_mismatch")
        self.assertEqual(payload["phase"], "weather_mismatch")
        unknown_today = []
        for local, temp in (("08:00", 16), ("12:00", 20), ("16:00", 18)):
            point = _point("2026-09-07 " + local, temp)
            point["sky"] = ""
            unknown_today.append(point)
        unknown = build_diurnal(
            station_id="KATL",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_max",
            unit="C",
            series=unknown_today,
            yesterday_series=[
                _point("2026-09-06 08:00", 16),
                _point("2026-09-06 12:00", 20),
                _point("2026-09-06 16:00", 18),
            ],
            now=datetime(2026, 9, 7, 20, 0, tzinfo=zone),
            running_value=20,
            observation_status="intraday",
        )
        self.assertEqual(unknown["block_reason"], "weather_class_unknown")
        self.assertEqual(unknown["phase"], "weather_mismatch")
        self.assertFalse(unknown["trigger"])

    def test_flat_day_learns_the_last_reading(self) -> None:
        yesterday = [_point("2026-09-06 {:02d}:00".format(hour), 20) for hour in range(0, 23)]
        today = [
            _point("2026-09-07 08:00", 18),
            _point("2026-09-07 10:00", 21),
            _point("2026-09-07 12:00", 24),
            _point("2026-09-07 15:00", 27),
        ]
        zone = load_timezone("America/Los_Angeles")
        payload = build_diurnal(
            station_id="KATL",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_max",
            unit="C",
            series=today,
            yesterday_series=yesterday,
            now=datetime(2026, 9, 7, 18, 0, tzinfo=zone),
            running_value=27,
            observation_status="intraday",
        )
        self.assertTrue(payload["learned_max_valid"])
        self.assertTrue(payload["learned_min_valid"])
        self.assertEqual(payload["yesterday_max_at_local"], "2026-09-06 22:00")
        self.assertEqual(payload["yesterday_min_at_local"], "2026-09-06 22:00")
        self.assertEqual(payload["learned_max_buy_local"], "2026-09-08 00:00")
        self.assertTrue(payload["max_window_start_local"])
        self.assertFalse(payload["trigger"])
        self.assertEqual(payload["block_reason"], "before_learned_buy")
        self.assertEqual(payload["phase"], "pending")

    def test_city_class_is_metadata_and_every_station_can_trigger(self) -> None:
        from weather_runtime.station_tz import DIURNAL_CLASS_A, STATION_COORDS

        self.assertEqual(len(DIURNAL_CLASS_A), 36)
        self.assertTrue(set(DIURNAL_CLASS_A) <= set(STATION_COORDS))
        zone = load_timezone("America/Los_Angeles")
        series = [
            _point("2026-09-07 08:00", 18),
            _point("2026-09-07 10:00", 21),
            _point("2026-09-07 12:00", 24),
            _point("2026-09-07 15:00", 27),
        ]
        now = datetime(2026, 9, 7, 22, 0, tzinfo=zone)
        chicago = build_diurnal(
            station_id="KORD",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_max",
            unit="C",
            series=series,
            yesterday_series=_yesterday_extreme("KORD", date(2026, 9, 7), kind="max"),
            now=now,
            running_value=27,
            observation_status="intraday",
        )
        self.assertEqual(chicago["city_class"], "A")
        self.assertEqual(chicago["influences"], ["lake_breeze", "heat_island"])
        self.assertTrue(chicago["trigger"])
        san_francisco = build_diurnal(
            station_id="KSFO",
            timezone_name="America/Los_Angeles",
            local_date="2026-09-07",
            metric="daily_max",
            unit="C",
            series=series,
            yesterday_series=_yesterday_extreme("KSFO", date(2026, 9, 7), kind="max"),
            now=now,
            running_value=27,
            observation_status="intraday",
        )
        self.assertEqual(san_francisco["city_class"], "")
        self.assertEqual(san_francisco["influences"], [])
        self.assertTrue(san_francisco["trigger"])
        self.assertEqual(san_francisco["block_reason"], "")
        self.assertEqual(san_francisco["phase"], "ready")

    def test_previous_day_bounds_use_station_local_clock(self) -> None:
        from weather_runtime.models import ObservationEvidence, WeatherRule
        from weather_runtime.sources import WeatherSourceAdapter

        rule = WeatherRule(
            market_id="m",
            event_group_id="e",
            adapter="wrh",
            source={"station_id": "KORD", "provider": "NOAA"},
            metric="daily_max",
            observation_start="2026-09-07T00:00:00",
            observation_end="2026-09-08T00:00:00",
            unit="F",
            buckets=[],
            timezone="America/Chicago",
        )
        adapter = WeatherSourceAdapter()
        seen = {}

        def _payload(shifted, now=None):
            seen["start"] = shifted.observation_start
            seen["end"] = shifted.observation_end
            return {"observations": []}, "https://example.test"

        def _evaluate(shifted, payload, url, current, evidence_hash, window_open=False):
            return ObservationEvidence(status="ok", series=[{"temp": 70}])

        adapter._payload = _payload
        adapter._evaluate = _evaluate
        series = adapter.previous_day_series(rule, now=datetime(2026, 9, 7, 18, tzinfo=timezone.utc))
        self.assertEqual(series, [{"temp": 70}])
        self.assertEqual(seen["start"], "2026-09-06T00:00:00")
        self.assertEqual(seen["end"], "2026-09-07T00:00:00")


class DiurnalScanTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_diurnal_state()
        self._helper = _scanner_helper()

    def _ksfo_books(self, markets, now, cheap_no):
        books = self._helper._priced_books(markets, now, cheap_no)
        for market in markets:
            if market.outcome != "26":
                continue
            yes = market.yes_token_id or market.token_ids[0]
            books[yes]["asks"] = [{"price": 0.90, "size": 20}]
        return books

    def _scan_ksfo_max(self, yesterday_offset: float):
        buckets = [
            {"outcome": "22", "upper": 22, "upper_inclusive": True},
            {
                "outcome": "26",
                "lower": 22,
                "lower_inclusive": False,
                "upper": 26,
                "upper_inclusive": True,
            },
            {"outcome": "30", "lower": 26, "lower_inclusive": False},
        ]
        features = []
        for hour, temp in ((8, 18), (10, 21), (12, 24), (15, 26)):
            features.append(
                {
                    "value": temp,
                    "timestamp": "2026-09-07T{:02d}:00:00-07:00".format(hour),
                    "metar": "KSFO 071500Z 10SM CLR 26/12",
                }
            )
            features.append(
                {
                    "value": temp + yesterday_offset,
                    "timestamp": "2026-09-06T{:02d}:00:00-07:00".format(hour),
                    "metar": "KSFO 061500Z 10SM CLR 24/12",
                }
            )
        zone = load_timezone("America/Los_Angeles")
        solar = solar_day("KATL", date(2026, 9, 7), zone)
        start, end, _band = max_window(
            solar["solar_noon"],
            "clear",
            [],
            zone,
            datetime(2026, 9, 7, 23, tzinfo=zone),
        )
        prev_mid = (start + (end - start) / 2) - timedelta(days=1)
        features.append(
            {
                "value": 40,
                "timestamp": prev_mid.isoformat(),
                "metar": "KSFO 061500Z 10SM CLR 24/12",
            }
        )
        features.append(
            {
                "value": 30,
                "timestamp": (prev_mid + timedelta(hours=2)).isoformat(),
                "metar": "KSFO 061700Z 10SM CLR 24/12",
            }
        )
        markets, rules = self._helper._event_with_static(
            event_id="highest-temperature-in-san-francisco-on-september-7-2026",
            metric="daily_max",
            outcomes=[item["outcome"] for item in buckets],
            buckets=buckets,
            features=features,
            extra_source={"station_id": "KATL", "provider": "NOAA"},
            start="2026-09-07T00:00:00",
            end="2026-09-07T23:59:59",
            timezone_name="America/Los_Angeles",
            unit="C",
        )
        now = datetime(2026, 9, 8, 2, tzinfo=timezone.utc)
        result = WeatherScanner(config=WeatherScannerConfig()).scan(
            markets,
            rules,
            books=self._ksfo_books(markets, now, {"22"}),
            fetch_books=False,
            now=now,
        )
        return result

    def test_scan_emits_diurnal_yes_and_loser_nos(self) -> None:
        result = self._scan_ksfo_max(0.4)
        by_outcome = self._helper._buy_rows_by_outcome(result["rows"])
        self.assertEqual(by_outcome["22"]["reason"], DIURNAL_MAX_LOSER_NO)
        self.assertEqual(by_outcome["22"]["trade_side"], "NO")
        self.assertEqual(by_outcome["22"]["order_side"], "BUY")
        self.assertEqual(by_outcome["22"]["status"], "opportunity")
        self.assertEqual(by_outcome["30"]["match_reason"], DIURNAL_MAX_LOSER_NO)
        self.assertEqual(by_outcome["30"]["trade_side"], "NO")
        self.assertNotEqual(by_outcome["30"]["reason"], "intraday_impossible_no")
        self.assertEqual(by_outcome["26"]["reason"], DIURNAL_MAX_REASON)
        self.assertEqual(by_outcome["26"]["trade_side"], "YES")
        self.assertEqual(by_outcome["26"]["status"], "opportunity")
        sells = [row for row in result["rows"] if str(row.get("order_side") or "").upper() == "SELL"]
        self.assertEqual(sells, [])
        self.assertIn(DIURNAL_MAX_REASON, _OPPORTUNITY_REASONS)
        self.assertIn(DIURNAL_MAX_LOSER_NO, DIURNAL_REASONS)
        self.assertIn(DIURNAL_MIN_LOSER_NO, DIURNAL_REASONS)
        group = result["event_groups"][0]
        self.assertIn("sunrise_local", group["diurnal"])
        self.assertEqual(group["diurnal"]["continuity"]["status"], "similar")
        self.assertEqual(group["diurnal"]["city_class"], "A")
        self.assertEqual(group["diurnal"]["influences"], ["heat_island"])
        shown = _list_rows_for_board(group)[0]
        self.assertNotEqual(shown["reason"], "intraday_impossible_no")
        self.assertEqual(result["summary"]["opportunities"], 0)
        self.assertEqual(result["summary"]["diurnal_opportunities"], 2)
        lock = _lock_snapshot(by_outcome["26"])
        self.assertFalse(lock["locked"])
        self.assertNotIn(DIURNAL_MAX_REASON, _LOCKED_NO_REASONS)
        self.assertNotIn(DIURNAL_MAX_LOSER_NO, _LOCKED_NO_REASONS)
        self.assertNotIn(DIURNAL_MAX_REASON, _LIMIT_ORDER_REASONS)
        self.assertNotIn(DIURNAL_MAX_LOSER_NO, _LIMIT_ORDER_REASONS)
        self.assertEqual(_limit_lock_token(by_outcome["26"]), "")
        self.assertEqual(_limit_lock_token(by_outcome["22"]), "")

    def test_diurnal_depth_uses_five_usdc_not_max_order(self) -> None:
        from weather_runtime.books import normalize_book
        from weather_runtime.models import WeatherMarket

        book = normalize_book(
            "yes",
            {
                "asks": [{"price": "0.99", "size": "10"}],
                "tick_size": "0.01",
                "min_order_size": "5",
            },
        )
        market = WeatherMarket(
            market_id="m",
            active=True,
            closed=False,
            accepting_orders=True,
            enable_order_book=True,
            fee_rate=0.0,
            tick_size=0.01,
            min_order_size=5,
        )
        scanner = WeatherScanner(
            config=WeatherScannerConfig(
                max_usdc=200,
                target_shares=200_000,
                diurnal_order_usdc=5,
            )
        )
        diurnal = {"reason": DIURNAL_MIN_REASON, "order_side": "BUY", "trade_side": "YES"}
        locked = {"reason": "intraday_impossible_no", "order_side": "BUY", "trade_side": "NO"}
        scanner._evaluate_buy_row(diurnal, book, market, 0.0)
        scanner._evaluate_buy_row(locked, book, market, 0.0)
        self.assertEqual(diurnal["status"], "opportunity")
        self.assertLess(diurnal["economics"]["execution_shares"], 11)
        self.assertEqual(locked["status"], "no_trade")
        self.assertEqual(locked["reason"], "insufficient_depth_within_slippage")

    def test_scan_regime_change_keeps_locked_no(self) -> None:
        result = self._scan_ksfo_max(-4.0)
        by_outcome = self._helper._buy_rows_by_outcome(result["rows"])
        self.assertEqual(by_outcome["22"]["reason"], "intraday_impossible_no")
        self.assertNotEqual(by_outcome["26"].get("reason"), DIURNAL_MAX_REASON)
        self.assertEqual(by_outcome["26"]["status"], "waiting")
        self.assertEqual(result["event_groups"][0]["diurnal"]["continuity"]["status"], "regime_change")
        self.assertTrue(result["event_groups"][0]["diurnal"]["sunrise_local"])
        self.assertEqual(result["summary"]["diurnal_opportunities"], 0)

    def test_auto_take_skips_diurnal_without_touching_finality_tokens(self) -> None:
        service = RuntimeService(
            root=Path("."),
            data_dir=Path("/tmp/pm-weather-diurnal-test"),
            sync=False,
        )
        service.fixture = None
        row = {
            "status": "opportunity",
            "reason": DIURNAL_MIN_REASON,
            "order_side": "BUY",
            "trade_side": "YES",
            "book_token_id": "yes-token",
            "event_group_id": "event-a",
            "target_outcome": "26",
        }
        with patch("weather_runtime.env.trading_config", return_value={"live_orders": True, "diurnal_orders": False}):
            taken = service._auto_take_opportunities({"rows": [row]})
        self.assertEqual(taken, [])
        self.assertEqual(service._taken_tokens, set())
        self.assertEqual(service._diurnal_taken_tokens, set())
        self.assertIn(DIURNAL_MIN_REASON, DIURNAL_REASONS)

    def test_diurnal_orders_take_yes_without_enabling_locked_no(self) -> None:
        from weather_runtime.env import public_trading_status, trading_config

        env = {
            "DIURNAL_ORDERS": "true",
            "DIURNAL_YES_ORDERS": "",
            "DIURNAL_NO_ORDERS": "false",
            "DIURNAL_ORDER_USDC": "12.5",
            "DIURNAL_MIN_PRICE": "0.99",
            "LIVE_ORDERS": "false",
        }
        with patch.dict("os.environ", env, clear=False):
            cfg = trading_config()
            pub = public_trading_status()
        self.assertTrue(cfg["diurnal_orders"])
        self.assertTrue(cfg["diurnal_yes_orders"])
        self.assertFalse(cfg["diurnal_no_orders"])
        self.assertTrue(pub["diurnal_yes_orders"])
        self.assertFalse(pub["diurnal_no_orders"])
        self.assertFalse(cfg["live_orders"])
        self.assertAlmostEqual(cfg["diurnal_order_usdc"], 12.5)
        self.assertAlmostEqual(cfg["diurnal_min_price"], 0.99)
        self.assertAlmostEqual(pub["diurnal_order_usdc"], 12.5)
        self.assertAlmostEqual(pub["diurnal_min_price"], 0.99)

        service = RuntimeService(
            root=Path("."),
            data_dir=Path("/tmp/pm-weather-diurnal-test"),
            sync=False,
        )
        service.fixture = None
        service._diurnal_seen_tokens.clear()
        diurnal = {
            "status": "opportunity",
            "reason": DIURNAL_MAX_REASON,
            "order_side": "BUY",
            "trade_side": "YES",
            "book_token_id": "yes-token",
            "event_group_id": "event-a",
            "target_outcome": "26",
        }
        locked = {
            "status": "opportunity",
            "reason": "intraday_impossible_no",
            "order_side": "BUY",
            "trade_side": "NO",
            "book_token_id": "no-token",
            "event_group_id": "event-a",
            "target_outcome": "22",
        }
        with patch("weather_runtime.env.trading_config", return_value=cfg):
            with patch.object(service, "take_opportunity", return_value={"ok": True}) as take:
                taken = service._auto_take_opportunities({"rows": [diurnal, locked]})
        self.assertEqual(len(taken), 1)
        self.assertEqual(take.call_count, 1)
        kwargs = take.call_args.kwargs
        self.assertEqual(kwargs["target_outcome"], "26")
        self.assertEqual(kwargs["token_ledger"], "diurnal")
        self.assertFalse(kwargs["require_locked_no"])
        self.assertTrue(kwargs["live"])

    def test_diurnal_yes_and_no_switches_stay_independent(self) -> None:
        service = RuntimeService(
            root=Path("."),
            data_dir=Path("/tmp/pm-weather-diurnal-test"),
            sync=False,
        )
        service.fixture = None
        service._diurnal_seen_tokens.clear()
        service._taken_tokens.clear()
        service._limit_taken_tokens.clear()
        yes = {
            "status": "opportunity",
            "reason": DIURNAL_MAX_REASON,
            "order_side": "BUY",
            "trade_side": "YES",
            "book_token_id": "yes-token",
            "event_group_id": "event-a",
            "target_outcome": "26",
            "economics": {"best_ask": 0.99},
        }
        no = {
            "status": "opportunity",
            "reason": DIURNAL_MAX_LOSER_NO,
            "order_side": "BUY",
            "trade_side": "NO",
            "book_token_id": "no-token",
            "event_group_id": "event-a",
            "target_outcome": "22",
            "economics": {"best_ask": 0.99},
        }
        yes_only = {
            "live_orders": False,
            "diurnal_orders": True,
            "diurnal_min_price": 0.90,
        }
        with patch("weather_runtime.env.trading_config", return_value=yes_only):
            with patch.object(service, "take_opportunity", return_value={"ok": True}) as take:
                taken = service._auto_take_opportunities({"rows": [yes, no]})
        self.assertEqual(len(taken), 1)
        self.assertEqual(take.call_args.kwargs["target_outcome"], "26")
        self.assertEqual(take.call_args.kwargs["token_ledger"], "diurnal")
        self.assertEqual(service._taken_tokens, set())
        self.assertEqual(service._limit_taken_tokens, set())
        no_only = {
            "live_orders": False,
            "diurnal_orders": False,
            "diurnal_yes_orders": False,
            "diurnal_no_orders": True,
            "diurnal_min_price": 0.90,
        }
        with patch("weather_runtime.env.trading_config", return_value=no_only):
            with patch.object(service, "take_opportunity", return_value={"ok": True}) as take:
                taken = service._auto_take_opportunities({"rows": [yes, no]})
        self.assertEqual(len(taken), 1)
        self.assertEqual(take.call_args.kwargs["target_outcome"], "22")
        self.assertEqual(take.call_args.kwargs["token_ledger"], "diurnal")
        self.assertFalse(take.call_args.kwargs["require_locked_no"])
        self.assertEqual(service._taken_tokens, set())
        self.assertNotIn(DIURNAL_MIN_LOSER_NO, _LIMIT_ORDER_REASONS)

    def test_diurnal_min_price_skips_asks_under_0_99(self) -> None:
        from weather_runtime.service import _price_below

        self.assertFalse(_price_below(0.99, 0.99))
        self.assertTrue(_price_below(0.989, 0.99))
        service = RuntimeService(
            root=Path("."),
            data_dir=Path("/tmp/pm-weather-diurnal-test"),
            sync=False,
        )
        service.fixture = None
        service._diurnal_seen_tokens.clear()

        def _row(ask: float) -> dict:
            return {
                "status": "opportunity",
                "reason": DIURNAL_MAX_REASON,
                "order_side": "BUY",
                "trade_side": "YES",
                "book_token_id": "yes-" + str(ask),
                "event_group_id": "event-" + str(ask),
                "target_outcome": str(ask),
                "economics": {"best_ask": ask},
            }

        cfg = {"live_orders": False, "diurnal_orders": True, "diurnal_min_price": 0.99}
        with patch("weather_runtime.env.trading_config", return_value=cfg):
            with patch.object(service, "take_opportunity", return_value={"ok": True}) as take:
                cheap = service._auto_take_opportunities({"rows": [_row(0.989)]})
                priced = service._auto_take_opportunities({"rows": [_row(0.99)]})
        self.assertEqual(cheap, [])
        self.assertEqual(take.call_count, 1)
        self.assertEqual(len(priced), 1)

    def test_diurnal_book_buys_only_on_first_sighting(self) -> None:
        service = RuntimeService(
            root=Path("."),
            data_dir=Path("/tmp/pm-weather-diurnal-seen"),
            sync=False,
        )
        service.fixture = None
        seen_path = service._diurnal_seen_path()
        if seen_path.exists():
            seen_path.unlink()
        service._diurnal_seen_tokens.clear()
        row = {
            "status": "opportunity",
            "reason": DIURNAL_MAX_REASON,
            "order_side": "BUY",
            "trade_side": "YES",
            "book_token_id": "yes-once",
            "event_group_id": "event-a",
            "target_outcome": "26",
            "economics": {"best_ask": 0.99},
        }
        cheap = dict(row)
        cheap["economics"] = {"best_ask": 0.5}
        cfg = {"live_orders": False, "diurnal_orders": True, "diurnal_min_price": 0.99}
        with patch("weather_runtime.env.trading_config", return_value=cfg):
            with patch.object(service, "take_opportunity", return_value={"ok": True}) as take:
                first = service._auto_take_opportunities({"rows": [row]})
                second = service._auto_take_opportunities({"rows": [row]})
                self.assertEqual(take.call_count, 1)
                take.reset_mock()
                service._diurnal_seen_tokens.clear()
                missed = service._auto_take_opportunities({"rows": [cheap]})
                later = service._auto_take_opportunities({"rows": [row]})
        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])
        self.assertEqual(missed, [])
        self.assertEqual(later, [])
        self.assertEqual(take.call_count, 0)

        seen_path = service._diurnal_seen_path()
        if seen_path.exists():
            seen_path.unlink()
        service.last_result = {"rows": [row]}
        service._diurnal_seen_tokens.clear()
        service._load_diurnal_seen()
        with patch("weather_runtime.env.trading_config", return_value=cfg):
            with patch.object(service, "take_opportunity", return_value={"ok": False, "error": "balance_fetch_failed"}) as take:
                retried = service._auto_take_opportunities({"rows": [row]})
                again = service._auto_take_opportunities({"rows": [row]})
        self.assertEqual(take.call_count, 2)
        self.assertEqual(len(retried), 1)
        self.assertEqual(len(again), 1)

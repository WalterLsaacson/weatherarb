"""Local solar time and a dry-run gate for when today's extremum has likely occurred.

Solar geometry is cached per station and local civil date. Weather class and the
two-day temperature check are recomputed from the latest observations.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from .rules import load_timezone
from .station_tz import coordinates_for_station, diurnal_city_class, diurnal_influences

DIURNAL_MAX_REASON = "diurnal_max_winner_yes"
DIURNAL_MIN_REASON = "diurnal_min_winner_yes"
DIURNAL_MAX_LOSER_NO = "diurnal_max_loser_no"
DIURNAL_MIN_LOSER_NO = "diurnal_min_loser_no"
DIURNAL_YES_REASONS = frozenset({DIURNAL_MAX_REASON, DIURNAL_MIN_REASON})
DIURNAL_NO_REASONS = frozenset({DIURNAL_MAX_LOSER_NO, DIURNAL_MIN_LOSER_NO})
DIURNAL_REASONS = DIURNAL_YES_REASONS | DIURNAL_NO_REASONS

_SOLAR_CACHE: dict[tuple[str, str], dict[str, Any]] = {}
_REGIME_DAYS: dict[tuple[str, str], dict[str, Any]] = {}

_SKY_RE = re.compile(r"\b(CLR|SKC|NCD|CAVOK|FEW|SCT|BKN|OVC|VV)\d{0,3}\b")
_PRECIP_MARKERS = ("RA", "SN", "DZ", "SG", "PL", "GR", "GS", "UP", "SH")
_CLEAR_SKY = {"CLR", "SKC", "NCD", "CAVOK", "FEW"}
_CLOUDY_SKY = {"SCT", "BKN"}
_PRECIP_SKY = {"OVC", "VV"}
_SKY_RANK = {"CLR": 0, "SKC": 0, "NCD": 0, "CAVOK": 0, "FEW": 1, "SCT": 2, "BKN": 3, "OVC": 4, "VV": 4}


def clear_diurnal_state() -> None:
    """Drop solar and regime memory. Tests use this between cases."""

    _SOLAR_CACHE.clear()
    _REGIME_DAYS.clear()


def _deg(radians: float) -> float:
    return radians * 180.0 / math.pi


def _rad(degrees: float) -> float:
    return degrees * math.pi / 180.0


def _julian_day(year: int, month: int, day: int) -> float:
    """Julian day at 0h UTC."""

    a = (14 - month) // 12
    year_2 = year + 4800 - a
    month_2 = month + 12 * a - 3
    jdn = day + (153 * month_2 + 2) // 5 + 365 * year_2 + year_2 // 4 - year_2 // 100 + year_2 // 400 - 32045
    return float(jdn) - 0.5


def _julian_century(jd: float) -> float:
    return (jd - 2451545.0) / 36525.0


def _wrap_360(degrees: float) -> float:
    return degrees % 360.0


def _geom_mean_long_sun(century: float) -> float:
    return _wrap_360(280.46646 + century * (36000.76983 + century * 0.0003032))


def _geom_mean_anomaly_sun(century: float) -> float:
    return 357.52911 + century * (35999.05029 - 0.0001537 * century)


def _eccent_earth_orbit(century: float) -> float:
    return 0.016708634 - century * (0.000042037 + 0.0000001267 * century)


def _sun_eq_of_center(century: float) -> float:
    anomaly = _geom_mean_anomaly_sun(century)
    mrad = _rad(anomaly)
    return (
        math.sin(mrad) * (1.914602 - century * (0.004817 + 0.000014 * century))
        + math.sin(2.0 * mrad) * (0.019993 - 0.000101 * century)
        + math.sin(3.0 * mrad) * 0.000289
    )


def _sun_true_long(century: float) -> float:
    return _geom_mean_long_sun(century) + _sun_eq_of_center(century)


def _sun_apparent_long(century: float) -> float:
    true_long = _sun_true_long(century)
    omega = 125.04 - 1934.136 * century
    return true_long - 0.00569 - 0.00478 * math.sin(_rad(omega))


def _mean_obliquity(century: float) -> float:
    seconds = 21.448 - century * (46.8150 + century * (0.00059 - century * 0.001813))
    return 23.0 + (26.0 + seconds / 60.0) / 60.0


def _obliquity_correction(century: float) -> float:
    omega = 125.04 - 1934.136 * century
    return _mean_obliquity(century) + 0.00256 * math.cos(_rad(omega))


def equation_of_time_minutes(century: float) -> float:
    """NOAA equation of time, minutes."""

    obliquity = _obliquity_correction(century)
    mean_long = _geom_mean_long_sun(century)
    eccentricity = _eccent_earth_orbit(century)
    anomaly = _geom_mean_anomaly_sun(century)
    y = math.tan(_rad(obliquity) / 2.0)
    y *= y
    sin_2l0 = math.sin(2.0 * _rad(mean_long))
    sin_m = math.sin(_rad(anomaly))
    cos_2l0 = math.cos(2.0 * _rad(mean_long))
    sin_4l0 = math.sin(4.0 * _rad(mean_long))
    sin_2m = math.sin(2.0 * _rad(anomaly))
    etime = (
        y * sin_2l0
        - 2.0 * eccentricity * sin_m
        + 4.0 * eccentricity * y * sin_m * cos_2l0
        - 0.5 * y * y * sin_4l0
        - 1.25 * eccentricity * eccentricity * sin_2m
    )
    return _deg(etime) * 4.0


def _sun_declination(century: float) -> float:
    obliquity = _obliquity_correction(century)
    apparent = _sun_apparent_long(century)
    return _deg(math.asin(math.sin(_rad(obliquity)) * math.sin(_rad(apparent))))


def _hour_angle_sunrise(latitude: float, declination: float) -> Optional[float]:
    lat_rad = _rad(latitude)
    dec_rad = _rad(declination)
    cos_lat = math.cos(lat_rad)
    cos_dec = math.cos(dec_rad)
    if abs(cos_lat * cos_dec) < 1e-9:
        return None
    argument = math.cos(_rad(90.833)) / (cos_lat * cos_dec) - math.tan(lat_rad) * math.tan(dec_rad)
    if argument < -1.0 or argument > 1.0:
        return None
    return math.acos(argument)


def _utc_minutes(century: float, latitude: float, longitude: float, *, rise: Optional[bool]) -> Optional[float]:
    """Minutes from 0h UTC. ``rise`` None is solar noon. Longitude is degrees east."""

    eqtime = equation_of_time_minutes(century)
    if rise is None:
        return 720.0 - (4.0 * longitude) - eqtime
    declination = _sun_declination(century)
    hour_angle = _hour_angle_sunrise(latitude, declination)
    if hour_angle is None:
        return None
    signed = _deg(hour_angle)
    if not rise:
        signed = -signed
    delta = longitude + signed
    return 720.0 - (4.0 * delta) - eqtime


def _event_minutes(
    latitude: float,
    longitude: float,
    day: date,
    *,
    rise: Optional[bool],
) -> Optional[float]:
    jd = _julian_day(day.year, day.month, day.day)
    first = _utc_minutes(_julian_century(jd), latitude, longitude, rise=rise)
    if first is None:
        return None
    refined = _utc_minutes(_julian_century(jd + first / 1440.0), latitude, longitude, rise=rise)
    return first if refined is None else refined


def _minutes_to_local(day: date, minutes_utc: float, zone: ZoneInfo) -> datetime:
    stamp = datetime(day.year, day.month, day.day, tzinfo=timezone.utc) + timedelta(minutes=float(minutes_utc))
    return stamp.astimezone(zone)


def solar_day(station_id: str, local_day: date, zone: ZoneInfo) -> Optional[dict[str, Any]]:
    """Sunrise, solar noon, and sunset for one station-date. Cached."""

    station = str(station_id or "").strip().upper()
    key = (station, local_day.isoformat())
    cached = _SOLAR_CACHE.get(key)
    if cached is not None:
        return cached
    coords = coordinates_for_station(station)
    if coords is None:
        return None
    latitude, longitude = coords
    noon = _event_minutes(latitude, longitude, local_day, rise=None)
    sunrise = _event_minutes(latitude, longitude, local_day, rise=True)
    sunset = _event_minutes(latitude, longitude, local_day, rise=False)
    if noon is None or sunrise is None or sunset is None:
        return None
    payload = {
        "station_id": station,
        "local_date": local_day.isoformat(),
        "latitude": latitude,
        "longitude": longitude,
        "sunrise": _minutes_to_local(local_day, sunrise, zone),
        "solar_noon": _minutes_to_local(local_day, noon, zone),
        "sunset": _minutes_to_local(local_day, sunset, zone),
    }
    _SOLAR_CACHE[key] = payload
    return payload


def _metar_body(text: str) -> str:
    upper = " ".join(str(text or "").upper().split())
    remark = upper.find(" RMK")
    if remark >= 0:
        upper = upper[:remark]
    return upper


def parse_metar(text: str) -> tuple[str, str]:
    """Return ``(sky, wx)``. ``wx`` is ``thunder``, ``precip``, or empty."""

    body = _metar_body(text)
    if not body:
        return "", ""
    sky = ""
    rank = -1
    for match in _SKY_RE.finditer(body):
        token = match.group(1)
        token_rank = _SKY_RANK.get(token, -1)
        if token_rank >= rank:
            sky = token
            rank = token_rank
    wx = ""
    for raw in body.split():
        token = raw.strip("+").strip("-")
        if token.startswith("VC"):
            token = token[2:]
        if "TS" in token:
            wx = "thunder"
            break
        if any(marker in token for marker in _PRECIP_MARKERS):
            wx = "precip"
    return sky, wx


def _phrase_sky_wx(sky_text: str, phrase: str) -> tuple[str, str]:
    sky_token = str(sky_text or "").strip().upper()
    if sky_token not in _SKY_RANK:
        sky_token = ""
    text = str(phrase or "").lower()
    wx = ""
    if any(word in text for word in ("thunder", "tstm", "tsra")):
        wx = "thunder"
    elif any(word in text for word in ("rain", "drizzle", "snow", "shower", "sleet", "hail")):
        wx = "precip"
    if not sky_token:
        if any(word in text for word in ("overcast", "mostly cloudy", "cloudy")):
            sky_token = "OVC"
        elif any(word in text for word in ("partly", "scattered")):
            sky_token = "SCT"
        elif any(word in text for word in ("fair", "clear", "sunny")):
            sky_token = "CLR"
    return sky_token, wx


def sky_wx_from_row(row: dict[str, Any]) -> tuple[str, str]:
    if not isinstance(row, dict):
        return "", ""
    metar = str(row.get("metar") or row.get("rawOb") or "").strip()
    if metar:
        return parse_metar(metar)
    return _phrase_sky_wx(str(row.get("clds") or row.get("sky") or ""), str(row.get("wx_phrase") or row.get("wx") or ""))


def _point_local(point: dict[str, Any], zone: ZoneInfo) -> Optional[datetime]:
    text = str(point.get("local_time") or "").strip()
    if len(text) >= 16:
        try:
            parsed = datetime.strptime(text[:16], "%Y-%m-%d %H:%M")
        except ValueError:
            parsed = None
        if parsed is not None:
            return parsed.replace(tzinfo=zone)
    stamp = point.get("timestamp")
    if not stamp:
        return None
    try:
        parsed_utc = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed_utc.tzinfo is None:
        parsed_utc = parsed_utc.replace(tzinfo=timezone.utc)
    return parsed_utc.astimezone(zone)


def _counted_points(series: list[dict[str, Any]], zone: ZoneInfo, now: datetime) -> list[tuple[datetime, dict[str, Any]]]:
    rows: list[tuple[datetime, dict[str, Any]]] = []
    current = now.astimezone(zone)
    for point in series:
        if not isinstance(point, dict):
            continue
        if point.get("counts_for_resolution") is False:
            continue
        local = _point_local(point, zone)
        if local is None or local > current:
            continue
        rows.append((local, point))
    rows.sort(key=lambda item: item[0])
    return rows


def classify_weather(series: list[dict[str, Any]], zone: ZoneInfo, now: datetime) -> str:
    """clear, cloudy, precip, convective, or unknown."""

    points = _counted_points(series, zone, now)
    if any(str(point.get("wx") or "") == "thunder" for _local, point in points):
        return "convective"
    if any(str(point.get("wx") or "") == "precip" for _local, point in points):
        return "precip"
    if any(str(point.get("sky") or "") in _PRECIP_SKY for _local, point in points):
        return "precip"
    cloudy = sum(1 for _local, point in points if str(point.get("sky") or "") in _CLOUDY_SKY)
    clear = sum(1 for _local, point in points if str(point.get("sky") or "") in _CLEAR_SKY)
    if cloudy and cloudy >= clear:
        return "cloudy"
    if clear:
        return "clear"
    return "unknown"


def _fmt_local(value: datetime) -> str:
    return value.astimezone(value.tzinfo).strftime("%Y-%m-%d %H:%M")


def _shift_window(start: datetime, end: datetime, minutes: int) -> tuple[datetime, datetime]:
    delta = timedelta(minutes=minutes)
    return start + delta, end + delta


def max_window(
    solar_noon: datetime,
    weather_class: str,
    series: list[dict[str, Any]],
    zone: ZoneInfo,
    now: datetime,
) -> tuple[datetime, datetime, int]:
    start = solar_noon + timedelta(hours=2)
    end = solar_noon + timedelta(minutes=150)
    uncertainty = 1
    if weather_class == "cloudy":
        start, end = _shift_window(start, end, -30)
    elif weather_class == "precip":
        start, end = _shift_window(start, end, -60)
        uncertainty = 2
    elif weather_class == "convective":
        uncertainty = 2
        first_thunder: Optional[datetime] = None
        for local, point in _counted_points(series, zone, now):
            if str(point.get("wx") or "") == "thunder":
                first_thunder = local
                break
        if first_thunder is not None and first_thunder < start:
            start, end = _shift_window(start, end, -90)
        elif first_thunder is not None:
            end = min(end, first_thunder)
            if end <= start:
                start = end - timedelta(minutes=30)
    return start, end, uncertainty


def threshold_for_unit(unit: str) -> float:
    text = str(unit or "C").upper().replace("°", "").strip()
    if text.startswith("F"):
        return 3.6
    return 2.0


def _hourly_means(points: list[tuple[datetime, dict[str, Any]]], before: datetime) -> dict[int, float]:
    grouped: dict[int, list[float]] = {}
    for local, point in points:
        if local >= before:
            continue
        try:
            temp = float(point.get("temp"))
        except (TypeError, ValueError):
            continue
        grouped.setdefault(local.hour, []).append(temp)
    return {hour: sum(values) / len(values) for hour, values in grouped.items() if values}


def _mean_delta(today: dict[int, float], yesterday: dict[int, float]) -> tuple[Optional[float], int]:
    paired = sorted(set(today) & set(yesterday))
    if len(paired) < 2:
        return None, len(paired)
    today_mean = sum(today[hour] for hour in paired) / len(paired)
    yesterday_mean = sum(yesterday[hour] for hour in paired) / len(paired)
    return abs(today_mean - yesterday_mean), len(paired)


def compare_previous_day(
    *,
    station_id: str,
    local_day: date,
    unit: str,
    today_series: list[dict[str, Any]],
    yesterday_series: list[dict[str, Any]],
    zone: ZoneInfo,
    now: datetime,
    sunrise: datetime,
    solar_noon: datetime,
) -> dict[str, Any]:
    """Two-day continuity. A regime change stays latched for that station-date."""

    threshold = threshold_for_unit(unit)
    today_points = _counted_points(today_series, zone, now)
    yesterday_points = _counted_points(yesterday_series, zone, now + timedelta(days=2))
    pre_sunrise_delta, pre_sunrise_hours = _mean_delta(
        _hourly_means(today_points, sunrise),
        _hourly_means(yesterday_points, sunrise - timedelta(days=1)),
    )
    pre_noon_delta, pre_noon_hours = _mean_delta(
        _hourly_means(today_points, solar_noon),
        _hourly_means(yesterday_points, solar_noon - timedelta(days=1)),
    )
    completed = [delta for delta in (pre_sunrise_delta, pre_noon_delta) if delta is not None]
    tripped = any(delta > threshold + 1e-9 for delta in completed)
    if tripped:
        status = "regime_change"
    elif completed:
        status = "similar"
    else:
        status = "no_baseline"
    representative = None
    paired = 0
    if pre_noon_delta is not None:
        representative = pre_noon_delta
        paired = pre_noon_hours
    elif pre_sunrise_delta is not None:
        representative = pre_sunrise_delta
        paired = pre_sunrise_hours
    payload = {
        "status": status,
        "delta": None if representative is None else round(float(representative), 2),
        "threshold": threshold,
        "unit": "F" if str(unit or "").upper().replace("°", "").startswith("F") else "C",
        "paired_hours": paired,
        "pre_sunrise_delta": None if pre_sunrise_delta is None else round(float(pre_sunrise_delta), 2),
        "pre_sunrise_hours": pre_sunrise_hours,
        "pre_noon_delta": None if pre_noon_delta is None else round(float(pre_noon_delta), 2),
        "pre_noon_hours": pre_noon_hours,
    }
    key = (str(station_id or "").strip().upper(), local_day.isoformat())
    if status == "regime_change":
        _REGIME_DAYS[key] = payload
        return payload
    latched = _REGIME_DAYS.get(key)
    if latched is not None:
        return latched
    return payload


def _on_date(moment: datetime, day: date) -> datetime:
    """Keep the clock time and move it onto another civil date in the same zone."""

    return moment.replace(year=day.year, month=day.month, day=day.day)


def _window_extreme(
    points: list[tuple[datetime, dict[str, Any]]],
    start: datetime,
    end: datetime,
    *,
    pick_min: bool,
) -> tuple[bool, str]:
    """Whether yesterday's extremum turned inside the copied window.

    The last time the daily max or min was seen must fall inside ``start``..``end``,
    and a later counted point that day must have left it. A flat day, or a
    plateau that ends outside the window, does not validate that side.

    The window is 30 minutes. No counted observation inside it fails this side,
    which hourly stations hit often. Follow-up: if the window is empty, consider
    widening the range. Leave the 30-minute rule in place until that is designed.
    """

    day = start.date()
    temps: list[tuple[datetime, float]] = []
    for local, point in points:
        if local.date() != day:
            continue
        try:
            temps.append((local, float(point.get("temp"))))
        except (TypeError, ValueError):
            continue
    if not temps:
        return False, ""
    extreme = min(temp for _local, temp in temps) if pick_min else max(temp for _local, temp in temps)
    hits = [local for local, temp in temps if abs(temp - extreme) <= 1e-6]
    last = max(hits)
    later = [temp for local, temp in temps if local > last]
    if pick_min:
        turned = any(temp > extreme + 1e-6 for temp in later)
    else:
        turned = any(temp < extreme - 1e-6 for temp in later)
    return bool(turned and start <= last <= end), _fmt_local(last)


def _bucket_edge(metric: str, value: Optional[float], bucket: Optional[dict[str, Any]]) -> bool:
    if value is None or not isinstance(bucket, dict):
        return False
    if metric == "daily_max" and bucket.get("upper") is not None:
        return float(bucket["upper"]) - float(value) <= 0.3 + 1e-9
    if metric == "daily_min" and bucket.get("lower") is not None:
        return float(value) - float(bucket["lower"]) <= 0.3 + 1e-9
    return False


def _min_still_falling(points: list[tuple[datetime, dict[str, Any]]], now: datetime) -> bool:
    temps: list[tuple[datetime, float]] = []
    for local, point in points:
        try:
            temps.append((local, float(point.get("temp"))))
        except (TypeError, ValueError):
            continue
    if not temps:
        return True
    minimum = min(temp for _local, temp in temps)
    latest_min = max(local for local, temp in temps if temp <= minimum + 1e-9)
    if now.astimezone(latest_min.tzinfo) - latest_min < timedelta(minutes=90):
        return True
    return False


def _convective_ongoing(points: list[tuple[datetime, dict[str, Any]]]) -> bool:
    if not points:
        return False
    return str(points[-1][1].get("wx") or "") == "thunder"


def build_diurnal(
    *,
    station_id: str,
    timezone_name: str,
    local_date: str,
    metric: str,
    unit: str,
    series: list[dict[str, Any]],
    yesterday_series: Optional[list[dict[str, Any]]],
    now: datetime,
    running_value: Optional[float] = None,
    running_bucket: Optional[dict[str, Any]] = None,
    observation_status: str = "",
) -> Optional[dict[str, Any]]:
    """Display payload plus whether this metric may emit a dry-run Yes."""

    try:
        day = date.fromisoformat(str(local_date)[:10])
    except ValueError:
        return None
    zone = load_timezone(timezone_name)
    solar = solar_day(station_id, day, zone)
    if solar is None:
        return None
    sunrise = solar["sunrise"]
    solar_noon = solar["solar_noon"]
    sunset = solar["sunset"]
    current = now.astimezone(zone)
    weather_class = classify_weather(series or [], zone, current)
    min_start = sunrise - timedelta(hours=1)
    min_end = sunrise - timedelta(minutes=30)
    max_start, max_end, uncertainty = max_window(solar_noon, weather_class, series or [], zone, current)
    if weather_class in {"clear", "cloudy", "unknown"} and metric == "daily_min":
        uncertainty = 1
    continuity = compare_previous_day(
        station_id=station_id,
        local_day=day,
        unit=unit,
        today_series=series or [],
        yesterday_series=yesterday_series or [],
        zone=zone,
        now=current,
        sunrise=sunrise,
        solar_noon=solar_noon,
    )
    points = _counted_points(series or [], zone, current)
    yesterday_points = _counted_points(yesterday_series or [], zone, current + timedelta(days=2))
    previous_day = day - timedelta(days=1)
    min_window_valid, yesterday_min_at = _window_extreme(
        yesterday_points,
        _on_date(min_start, previous_day),
        _on_date(min_end, previous_day),
        pick_min=True,
    )
    max_window_valid, yesterday_max_at = _window_extreme(
        yesterday_points,
        _on_date(max_start, previous_day),
        _on_date(max_end, previous_day),
        pick_min=False,
    )
    metric_window_valid = min_window_valid if metric == "daily_min" else max_window_valid
    min_cutoff = datetime(day.year, day.month, day.day, 10, 0, tzinfo=zone)
    if min_end > min_cutoff:
        min_cutoff = min_end
    if metric == "daily_min":
        trigger_at = min_cutoff
    else:
        trigger_at = max_end
    passed = current >= trigger_at
    near_edge = _bucket_edge(metric, running_value, running_bucket)
    block_reason = ""
    trigger = False
    if observation_status == "final":
        block_reason = "source_final"
    elif continuity.get("status") == "regime_change":
        block_reason = "regime_change"
    elif weather_class == "unknown":
        block_reason = "weather_class_unknown"
    elif len(points) < 3:
        block_reason = "sample_count"
    elif metric == "daily_min":
        if current < trigger_at:
            block_reason = "before_min_cutoff"
        elif _convective_ongoing(points):
            block_reason = "convective_ongoing"
        elif _min_still_falling(points, current):
            block_reason = "still_falling"
        else:
            trigger = True
    elif metric == "daily_max":
        if current < trigger_at:
            block_reason = "before_max_window"
        else:
            trigger = True
    else:
        block_reason = "metric_unsupported"
    if metric in {"daily_min", "daily_max"} and not metric_window_valid:
        if trigger:
            block_reason = "window_unverified"
        trigger = False
    phase = "pending"
    if continuity.get("status") == "regime_change":
        phase = "regime_change"
    elif metric in {"daily_min", "daily_max"} and not metric_window_valid:
        phase = "window_unverified"
    elif trigger:
        phase = "ready"
    elif passed:
        phase = "passed"
    city_class = diurnal_city_class(station_id)
    if city_class != "A":
        if trigger:
            block_reason = "city_class"
        trigger = False
        if phase == "ready":
            phase = "excluded"
    return {
        "station_id": str(station_id or "").strip().upper(),
        "local_date": day.isoformat(),
        "timezone": str(getattr(zone, "key", None) or timezone_name),
        "metric": metric,
        "sunrise_local": _fmt_local(sunrise),
        "solar_noon_local": _fmt_local(solar_noon),
        "sunset_local": _fmt_local(sunset),
        "weather_class": weather_class,
        "uncertainty_hours": uncertainty if metric == "daily_max" else 1,
        "min_window_start_local": _fmt_local(min_start),
        "min_window_end_local": _fmt_local(min_end),
        "max_window_start_local": _fmt_local(max_start),
        "max_window_end_local": _fmt_local(max_end),
        "min_trigger_local": _fmt_local(min_cutoff),
        "max_trigger_local": _fmt_local(max_end),
        "min_window_valid": min_window_valid,
        "max_window_valid": max_window_valid,
        "yesterday_min_at_local": yesterday_min_at,
        "yesterday_max_at_local": yesterday_max_at,
        "passed": passed,
        "trigger": trigger,
        "near_edge": near_edge,
        "block_reason": "" if trigger else block_reason,
        "phase": phase,
        "sample_count": len(points),
        "continuity": continuity,
        "city_class": city_class,
        "influences": list(diurnal_influences(station_id)),
    }

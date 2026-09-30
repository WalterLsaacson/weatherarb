"""IANA timezones for Polymarket NOAA timeseries and Wunderground history stations.

Station IDs come from weather.gov ``?site=`` or Wunderground
``history/daily/{cc}/{city}/{ICAO}`` ICAO codes. Unknown stations are left
unmapped so discovery cannot invent a civil day.
"""

from __future__ import annotations

from typing import Optional

STATION_TIMEZONES = {
    "CYYZ": "America/Toronto",
    "EDDM": "Europe/Berlin",
    "EFHK": "Europe/Helsinki",
    "EGLC": "Europe/London",
    "EHAM": "Europe/Amsterdam",
    "EPWA": "Europe/Warsaw",
    "FACT": "Africa/Johannesburg",
    "HKO": "Asia/Hong_Kong",
    "KAUS": "America/Chicago",
    "KATL": "America/New_York",
    "KBKF": "America/Denver",
    "KDAL": "America/Chicago",
    "KHOU": "America/Chicago",
    "KLAX": "America/Los_Angeles",
    "KLGA": "America/New_York",
    "KMIA": "America/New_York",
    "KORD": "America/Chicago",
    "KSEA": "America/Los_Angeles",
    "KSFO": "America/Los_Angeles",
    "LEMD": "Europe/Madrid",
    "LFPB": "Europe/Paris",
    "LIMC": "Europe/Rome",
    "LLBG": "Asia/Jerusalem",
    "LTAC": "Europe/Istanbul",
    "LTFM": "Europe/Istanbul",
    "MMMX": "America/Mexico_City",
    "MPMG": "America/Panama",
    "NZWN": "Pacific/Auckland",
    "OEJN": "Asia/Riyadh",
    "OPKC": "Asia/Karachi",
    "RJTT": "Asia/Tokyo",
    "RKPK": "Asia/Seoul",
    "RKSI": "Asia/Seoul",
    "RPLL": "Asia/Manila",
    "RCSS": "Asia/Taipei",
    "SAEZ": "America/Argentina/Buenos_Aires",
    "SBGR": "America/Sao_Paulo",
    "UUWW": "Europe/Moscow",
    "VILK": "Asia/Kolkata",
    "WMKK": "Asia/Kuala_Lumpur",
    "WSSS": "Asia/Singapore",
    "ZBAA": "Asia/Shanghai",
    "ZGGG": "Asia/Shanghai",
    "ZGSZ": "Asia/Shanghai",
    "ZHCC": "Asia/Shanghai",
    "ZHHH": "Asia/Shanghai",
    "ZSJN": "Asia/Shanghai",
    "ZSPD": "Asia/Shanghai",
    "ZSQD": "Asia/Shanghai",
    "ZUCK": "Asia/Shanghai",
    "ZUUU": "Asia/Shanghai",
}


def timezone_for_station(station_id: str) -> str:
    return STATION_TIMEZONES.get(str(station_id or "").strip().upper(), "")


# Airport reference points, except HKO which is the Hong Kong Observatory.
# Longitude is degrees east. Used for local solar time, not for settlement.
STATION_COORDS = {
    "CYYZ": (43.6777, -79.6248),
    "EDDM": (48.3538, 11.7861),
    "EFHK": (60.3172, 24.9633),
    "EGLC": (51.5053, 0.0553),
    "EHAM": (52.3105, 4.7683),
    "EPWA": (52.1657, 20.9671),
    "FACT": (-33.9648, 18.6017),
    "HKO": (22.3019, 114.1742),
    "KAUS": (30.1945, -97.6699),
    "KATL": (33.6407, -84.4277),
    "KBKF": (39.7017, -104.7518),
    "KDAL": (32.8471, -96.8518),
    "KHOU": (29.6454, -95.2789),
    "KLAX": (33.9425, -118.4081),
    "KLGA": (40.7769, -73.8740),
    "KMIA": (25.7959, -80.2870),
    "KORD": (41.9742, -87.9073),
    "KSEA": (47.4502, -122.3088),
    "KSFO": (37.6213, -122.3790),
    "LEMD": (40.4983, -3.5676),
    "LFPB": (48.9694, 2.4414),
    "LIMC": (45.6306, 8.7281),
    "LLBG": (32.0114, 34.8867),
    "LTAC": (40.1281, 32.9951),
    "LTFM": (41.2753, 28.7519),
    "MMMX": (19.4363, -99.0721),
    "MPMG": (8.9733, -79.5556),
    "NZWN": (-41.3272, 174.8053),
    "OEJN": (21.6796, 39.1565),
    "OPKC": (24.9065, 67.1608),
    "RJTT": (35.5494, 139.7798),
    "RKPK": (35.1795, 128.9382),
    "RKSI": (37.4602, 126.4407),
    "RPLL": (14.5086, 121.0198),
    "RCSS": (25.0697, 121.5525),
    "SAEZ": (-34.8222, -58.5358),
    "SBGR": (-23.4356, -46.4731),
    "UUWW": (55.5915, 37.2615),
    "VILK": (26.7606, 80.8893),
    "WMKK": (2.7456, 101.7099),
    "WSSS": (1.3644, 103.9915),
    "ZBAA": (40.0799, 116.6031),
    "ZGGG": (23.3924, 113.2988),
    "ZGSZ": (22.6393, 113.8107),
    "ZHCC": (34.5197, 113.8408),
    "ZHHH": (30.7838, 114.2081),
    "ZSJN": (36.8572, 117.2160),
    "ZSPD": (31.1443, 121.8083),
    "ZSQD": (36.2661, 120.3744),
    "ZUCK": (29.7192, 106.6417),
    "ZUUU": (30.5785, 103.9471),
}


def coordinates_for_station(station_id: str) -> Optional[tuple[float, float]]:
    return STATION_COORDS.get(str(station_id or "").strip().upper())


# Class A: radiation-dominated cities. Diurnal Yes is allowed here only.
# Sea breeze, lake breeze, and heat island are noted and are not extra gates.
DIURNAL_CLASS_A = frozenset(
    {
        "CYYZ",  # Toronto
        "EHAM",  # Amsterdam
        "EGLC",  # London
        "EDDM",  # Munich
        "EPWA",  # Warsaw
        "FACT",  # Cape Town
        "HKO",  # Hong Kong
        "KAUS",  # Austin
        "KATL",  # Atlanta
        "KDAL",  # Dallas
        "KLGA",  # NYC
        "KORD",  # Chicago
        "LEMD",  # Madrid
        "LFPB",  # Paris
        "LIMC",  # Milan
        "LLBG",  # Tel Aviv
        "LTAC",  # Ankara
        "LTFM",  # Istanbul
        "MMMX",  # Mexico City
        "OEJN",  # Jeddah
        "OPKC",  # Karachi
        "RCSS",  # Taipei
        "RJTT",  # Tokyo
        "RKPK",  # Busan
        "RKSI",  # Seoul
        "SAEZ",  # Buenos Aires
        "SBGR",  # Sao Paulo
        "VILK",  # Lucknow
        "ZBAA",  # Beijing
        "ZGGG",  # Guangzhou
        "ZGSZ",  # Shenzhen
        "ZHCC",  # Zhengzhou
        "ZHHH",  # Wuhan
        "ZSJN",  # Jinan
        "ZSPD",  # Shanghai
        "ZSQD",  # Qingdao
    }
)

# Lake or sea breeze can shift the peak, but the urban surface still keeps
# the afternoon maximum. These notes never block a Class A buy.
_DIURNAL_BREEZE = {
    "CYYZ": "lake_breeze",
    "HKO": "sea_breeze",
    "KLGA": "sea_breeze",
    "KORD": "lake_breeze",
    "RJTT": "sea_breeze",
    "ZGSZ": "sea_breeze",
    "ZSPD": "sea_breeze",
}


def diurnal_city_class(station_id: str) -> str:
    station = str(station_id or "").strip().upper()
    if station in DIURNAL_CLASS_A:
        return "A"
    return ""


def diurnal_influences(station_id: str) -> tuple[str, ...]:
    """Potential local effects. Empty unless the station is Class A."""

    station = str(station_id or "").strip().upper()
    if station not in DIURNAL_CLASS_A:
        return ()
    breeze = _DIURNAL_BREEZE.get(station)
    if breeze:
        return (breeze, "heat_island")
    return ("heat_island",)

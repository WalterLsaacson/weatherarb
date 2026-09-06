"""IANA timezones for Polymarket NOAA timeseries and Wunderground history stations.

Station IDs come from weather.gov ``?site=`` or Wunderground
``history/daily/{cc}/{city}/{ICAO}`` ICAO codes. Unknown stations are left
unmapped so discovery cannot invent a civil day.
"""

from __future__ import annotations

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

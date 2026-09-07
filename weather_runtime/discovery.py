"""Conservative Weather rule discovery.

Gamma weather events do not ship a machine-readable sourceSpec. Daily high/low
temperature events that use the NOAA timeseries template, Wunderground
``history/daily/{cc}/{city}/{ICAO}`` Daily Observations table, or Hong Kong
Observatory Daily Extract can still be compiled into dry-run rules. Hurricane,
drought, PWS dashboards and other families stay in review.
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable

from .markets import (
    extract_resolution_url,
    group_weather_markets,
    hko_from_url,
    infer_weather_metric,
    local_date_from_event,
    nws_station_from_url,
    wu_history_from_url,
)
from .models import WeatherMarket
from .rules import (
    RuleError,
    buckets_from_outcomes,
    parse_bucket,
    validate_buckets,
    validate_event_group_siblings,
)
from .station_tz import timezone_for_station
from .storage import write_json_atomic


def _source_contract(market: WeatherMarket) -> dict[str, Any]:
    raw = market.raw
    for key in ("sourceSpec", "source_spec", "resolutionRule", "resolution_rule", "dataSource", "data_source"):
        value = raw.get(key)
        if isinstance(value, dict):
            contract = dict(value)
            aliases = {
                "valuePath": "value_path",
                "timestampPath": "timestamp_path",
                "finalPath": "final_path",
                "valueUnit": "value_unit",
                "stationId": "station_id",
                "observationStart": "observation_start",
                "observationEnd": "observation_end",
                "finalityMode": "finality_mode",
                "requiresFollowingDatePoint": "requires_following_date_point",
                "localTimezone": "timezone",
                "local_timezone": "timezone",
            }
            for source_key, canonical_key in aliases.items():
                if canonical_key not in contract and source_key in contract:
                    contract[canonical_key] = contract[source_key]
            return contract
    return {}


_HOURLY_DATA_RE = re.compile(
    r"show\s+hourly\s+data|hourly\s+data\s+provided",
    re.I,
)
_NWS_FAA_TIMEZONES = {"Pacific/Honolulu", "America/Anchorage"}


def sample_set_from_description(*texts: str) -> str:
    blob = " ".join(str(item or "") for item in texts)
    if _HOURLY_DATA_RE.search(blob):
        return "hourly"
    return "all"


def hourly_window_for_timezone(timezone_name: str) -> str:
    name = str(timezone_name or "").strip()
    if name.startswith("America/") or name in _NWS_FAA_TIMEZONES:
        return "nws_faa"
    return "other"


def wrh_hourly_page_url(url: str) -> str:
    """Same WRH timeseries page with Show Hourly Data turned on (hourly=true)."""
    page = str(url or "").strip()
    if not page:
        return page
    if re.search(r"(?:^|[?&])hourly=", page, re.I):
        return page
    return page + ("&" if "?" in page else "?") + "hourly=true"


def _unit_from_outcomes(outcomes: Iterable[str], description: str = "") -> str:
    labels = " ".join(str(item or "") for item in outcomes)
    if re.search(r"°\s*F", labels, re.I):
        return "F"
    if re.search(r"°\s*C", labels, re.I):
        return "C"
    blob = " ".join((labels, description))
    if re.search(r"\bFahrenheit\b", blob, re.I) and not re.search(r"\bCelsius\b", blob, re.I):
        return "F"
    if re.search(r"\bCelsius\b", blob, re.I):
        return "C"
    return ""


def _noaa_template_reasons(first: WeatherMarket, group: list[WeatherMarket]) -> tuple[dict[str, Any], list[str]]:
    reasons: list[str] = []
    title = " ".join((first.question, first.slug, str(first.raw.get("question") or "")))
    metric = infer_weather_metric(title, first.slug)
    if metric not in {"daily_max", "daily_min"}:
        reasons.append("weather_metric_missing_or_unsupported")
    station = nws_station_from_url(first.resolution_source)
    if not station:
        reasons.append("nws_timeseries_station_missing")
    timezone_name = timezone_for_station(station)
    if station and not timezone_name:
        reasons.append("timezone_missing")
    local_date = local_date_from_event(first.slug, first.question, first.end_date)
    if not local_date:
        reasons.append("observation_date_missing")
    outcomes = [market.outcome for market in group]
    unit = _unit_from_outcomes(outcomes, str(first.raw.get("description") or ""))
    if unit not in {"C", "F"}:
        reasons.append("temperature_unit_missing")
    buckets: list[dict[str, Any]] = []
    bucket_error = ""
    try:
        parsed = buckets_from_outcomes(outcomes)
        sibling_reason = validate_event_group_siblings(group, parsed)
        if sibling_reason:
            reasons.append(sibling_reason)
        buckets = [bucket.to_dict() for bucket in parsed]
    except RuleError as exc:
        bucket_error = str(exc)
        reasons.append("bucket_contract_invalid")
    start = ""
    end = ""
    source: dict[str, Any] = {}
    if local_date and timezone_name and station:
        start = "{}T00:00:00".format(local_date)
        end = "{}T23:59:59".format(local_date)
        description = " ".join(
            str(market.raw.get("description") or "") for market in ([first] + list(group))
        )
        sample_set = sample_set_from_description(description)
        source = {
            "provider": "NOAA",
            "station_id": station,
            "url": "https://api.synopticdata.com/v2/stations/timeseries",
            "params": {
                "STID": station,
                "showemptystations": 1,
                "units": "temp|F,speed|mph,english",
                "recent": 4320,
                "complete": 1,
                "obtimezone": "local",
            },
            "resolution_source": first.resolution_source,
            "value_path": "temp",
            "timestamp_path": "timestamp",
            "value_unit": "C",
            "finality_mode": "first_following_date_point",
            "sample_set": sample_set,
        }
        if sample_set == "hourly":
            source["hourly_window"] = hourly_window_for_timezone(timezone_name)
    contract = {
        "metric": metric,
        "unit": unit,
        "timezone": timezone_name,
        "station_id": station,
        "observation_start": start,
        "observation_end": end,
        "rounding": "whole_degree_as_published",
        "source": source,
        "buckets": buckets,
        "bucket_error": bucket_error,
        "notes": "Auto-generated from Gamma NOAA timeseries template; dry-run only.",
    }
    return contract, reasons


def _wu_template_reasons(first: WeatherMarket, group: list[WeatherMarket]) -> tuple[dict[str, Any], list[str]]:
    reasons: list[str] = []
    title = " ".join((first.question, first.slug, str(first.raw.get("question") or "")))
    metric = infer_weather_metric(title, first.slug)
    if metric not in {"daily_max", "daily_min"}:
        reasons.append("weather_metric_missing_or_unsupported")
    history = wu_history_from_url(first.resolution_source)
    station = str(history.get("station_id") or "")
    country = str(history.get("country") or "")
    if not station or not country:
        reasons.append("wu_history_station_missing")
    timezone_name = timezone_for_station(station)
    if station and not timezone_name:
        reasons.append("timezone_missing")
    local_date = local_date_from_event(first.slug, first.question, first.end_date)
    if not local_date:
        reasons.append("observation_date_missing")
    outcomes = [market.outcome for market in group]
    unit = _unit_from_outcomes(outcomes, str(first.raw.get("description") or ""))
    if unit not in {"C", "F"}:
        reasons.append("temperature_unit_missing")
    buckets: list[dict[str, Any]] = []
    bucket_error = ""
    try:
        parsed = buckets_from_outcomes(outcomes)
        sibling_reason = validate_event_group_siblings(group, parsed)
        if sibling_reason:
            reasons.append(sibling_reason)
        buckets = [bucket.to_dict() for bucket in parsed]
    except RuleError as exc:
        bucket_error = str(exc)
        reasons.append("bucket_contract_invalid")
    start = ""
    end = ""
    source: dict[str, Any] = {}
    if local_date and timezone_name and station and country:
        start = "{}T00:00:00".format(local_date)
        end = "{}T23:59:59".format(local_date)
        year, month, day = (int(part) for part in local_date.split("-"))
        following = date(year, month, day) + timedelta(days=1)
        source = {
            "provider": "Wunderground",
            "station_id": station,
            "url": "https://api.weather.com/v1/location/{}:9:{}/observations/historical.json".format(
                station, country
            ),
            "params": {
                "units": "m",
                "startDate": "{:04d}{:02d}{:02d}".format(year, month, day),
                "endDate": following.strftime("%Y%m%d"),
            },
            "resolution_source": first.resolution_source,
            "value_path": "temp",
            "timestamp_path": "valid_time_gmt",
            "value_unit": "C",
            "finality_mode": "first_following_date_point",
        }
    contract = {
        "metric": metric,
        "unit": unit,
        "timezone": timezone_name,
        "station_id": station,
        "observation_start": start,
        "observation_end": end,
        "rounding": "whole_degree_as_published",
        "source": source,
        "buckets": buckets,
        "bucket_error": bucket_error,
        "notes": "Auto-generated from Gamma Wunderground Daily Observations template; dry-run only.",
    }
    return contract, reasons


def _hko_template_reasons(first: WeatherMarket, group: list[WeatherMarket]) -> tuple[dict[str, Any], list[str]]:
    reasons: list[str] = []
    title = " ".join((first.question, first.slug, str(first.raw.get("question") or "")))
    metric = infer_weather_metric(title, first.slug)
    if metric not in {"daily_max", "daily_min"}:
        reasons.append("weather_metric_missing_or_unsupported")
    history = hko_from_url(first.resolution_source) or hko_from_url(
        extract_resolution_url(str(first.raw.get("description") or ""))
    )
    station = str(history.get("station_id") or "")
    if not station:
        reasons.append("hko_daily_extract_missing")
    timezone_name = timezone_for_station(station)
    if station and not timezone_name:
        reasons.append("timezone_missing")
    local_date = local_date_from_event(first.slug, first.question, first.end_date)
    if not local_date:
        reasons.append("observation_date_missing")
    outcomes = [market.outcome for market in group]
    unit = _unit_from_outcomes(outcomes, str(first.raw.get("description") or ""))
    if unit not in {"C", "F"}:
        reasons.append("temperature_unit_missing")
    buckets: list[dict[str, Any]] = []
    bucket_error = ""
    try:
        parsed = buckets_from_outcomes(outcomes)
        sibling_reason = validate_event_group_siblings(group, parsed)
        if sibling_reason:
            reasons.append(sibling_reason)
        buckets = [bucket.to_dict() for bucket in parsed]
    except RuleError as exc:
        bucket_error = str(exc)
        reasons.append("bucket_contract_invalid")
    start = ""
    end = ""
    source: dict[str, Any] = {}
    if local_date and timezone_name and station:
        start = "{}T00:00:00".format(local_date)
        end = "{}T23:59:59".format(local_date)
        year = int(local_date.split("-")[0])
        data_type = "CLMMAXT" if metric == "daily_max" else "CLMMINT"
        source = {
            "provider": "HKO",
            "station_id": station,
            "url": "https://data.weather.gov.hk/weatherAPI/opendata/opendata.php",
            "params": {
                "dataType": data_type,
                "station": station,
                "year": year,
                "rformat": "json",
            },
            "resolution_source": first.resolution_source
            or "https://www.weather.gov.hk/en/cis/climat.htm",
            "value_path": "temp",
            "timestamp_path": "timestamp",
            "value_unit": "C",
            "finality_mode": "daily_row_published",
            "poll_after_end_hours": 180,
        }
    contract = {
        "metric": metric,
        "unit": unit,
        "timezone": timezone_name,
        "station_id": station,
        "observation_start": start,
        "observation_end": end,
        "rounding": "whole_degree_as_published",
        "source": source,
        "buckets": buckets,
        "bucket_error": bucket_error,
        "notes": "Auto-generated from Gamma HKO Daily Extract template; dry-run only.",
    }
    return contract, reasons


def _machine_contract_reasons(first: WeatherMarket, group: list[WeatherMarket]) -> tuple[dict[str, Any], list[str]]:
    contract = _source_contract(first)
    reasons: list[str] = []
    source = dict(contract)
    source.setdefault("url", first.resolution_source)
    source.setdefault("resolution_source", first.resolution_source)
    for key in ("url", "value_path", "timestamp_path"):
        if not source.get(key):
            reasons.append("source_{}_missing".format(key))
    metric = str(
        contract.get("aggregation")
        or contract.get("metric")
        or raw_value(first.raw, "metric", "aggregation")
        or ""
    ).strip().lower()
    if metric not in {"daily_max", "daily_min", "daily_sum", "latest"}:
        reasons.append("weather_metric_missing_or_unsupported")
    timezone_name = str(
        contract.get("timezone")
        or contract.get("local_timezone")
        or raw_value(first.raw, "timezone", "local_timezone")
        or ""
    ).strip()
    if not timezone_name:
        reasons.append("timezone_missing")
    rounding = str(contract.get("rounding") or first.raw.get("rounding") or "whole_degree_as_published")
    start = str(contract.get("observation_start") or first.raw.get("observation_start") or "").strip()
    end = str(contract.get("observation_end") or first.raw.get("observation_end") or "").strip()
    if metric in {"daily_max", "daily_min", "daily_sum"} and (not start or not end):
        reasons.append("observation_bounds_missing")
    buckets = contract.get("buckets") or first.raw.get("buckets")
    bucket_error = ""
    parsed_buckets = []
    if not isinstance(buckets, list) or not buckets:
        reasons.append("bucket_contract_missing")
    else:
        try:
            parsed_buckets = list(validate_buckets(parse_bucket(item) for item in buckets))
            buckets = [bucket.to_dict() for bucket in parsed_buckets]
        except RuleError as exc:
            bucket_error = str(exc)
            reasons.append("bucket_contract_invalid")
    if parsed_buckets:
        sibling_reason = validate_event_group_siblings(group, parsed_buckets)
        if sibling_reason:
            reasons.append(sibling_reason)
    finality = bool(
        contract.get("final_path")
        or contract.get("finality_mode")
        or contract.get("requires_following_date_point")
    )
    if not finality:
        reasons.append("finality_contract_missing")
    compiled = {
        "metric": metric,
        "unit": str(contract.get("unit") or first.raw.get("unit") or "C"),
        "timezone": timezone_name,
        "observation_start": start,
        "observation_end": end,
        "rounding": rounding,
        "source": source,
        "buckets": buckets if isinstance(buckets, list) else [],
        "bucket_error": bucket_error,
        "manual_approval": False,
        "enabled": False,
        "notes": "Auto-discovered machine contract; manual approval required.",
    }
    return compiled, reasons


def discover_rules(markets: Iterable[WeatherMarket]) -> dict[str, Any]:
    groups = group_weather_markets(markets)
    generated: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []
    for event_group_id, group in sorted(groups.items()):
        first = group[0]
        source_url = first.resolution_source or extract_resolution_url(
            str(first.raw.get("description") or "")
        )
        if nws_station_from_url(source_url):
            if not first.resolution_source:
                first.resolution_source = source_url
            contract, reasons = _noaa_template_reasons(first, group)
        elif wu_history_from_url(source_url):
            if not first.resolution_source:
                first.resolution_source = source_url
            contract, reasons = _wu_template_reasons(first, group)
        elif hko_from_url(source_url):
            if not first.resolution_source:
                first.resolution_source = source_url
            contract, reasons = _hko_template_reasons(first, group)
        else:
            contract, reasons = {}, ["resolution_source_unsupported"]
        auto_enable = not reasons
        if reasons:
            machine, machine_reasons = _machine_contract_reasons(first, group)
            if not machine_reasons:
                contract = machine
                reasons = []
                auto_enable = False
        if reasons:
            review.append(
                {
                    "event_group_id": event_group_id,
                    "market_ids": [market.market_id for market in group],
                    "question": first.question,
                    "resolution_source": first.resolution_source,
                    "status": "review",
                    "reasons": reasons,
                    **({"bucket_error": contract.get("bucket_error")} if contract.get("bucket_error") else {}),
                }
            )
            continue
        for market in group:
            generated.append(
                {
                    "market_id": market.market_id,
                    "event_group_id": event_group_id,
                    "adapter": "weather_observation",
                    "metric": contract["metric"],
                    "unit": contract["unit"],
                    "observation_start": contract["observation_start"],
                    "observation_end": contract["observation_end"],
                    "timezone": contract["timezone"],
                    "rounding": contract.get("rounding") or "whole_degree_as_published",
                    "source": contract["source"],
                    "buckets": contract["buckets"],
                    "target_outcome": market.outcome,
                    "manual_approval": True if auto_enable else bool(contract.get("manual_approval")),
                    "enabled": True if auto_enable else bool(contract.get("enabled")),
                    "notes": str(
                        contract.get("notes")
                        or (
                            "Auto-generated from Gamma NOAA timeseries template; dry-run only."
                            if auto_enable
                            else "Auto-discovered; manual approval required."
                        )
                    ),
                }
            )
    return {
        "generated_rules": generated,
        "review": review,
        "summary": {
            "event_groups": len(groups),
            "generated_rules": len(generated),
            "generated_event_groups": len({row["event_group_id"] for row in generated}),
            "review_event_groups": len(review),
            "auto_approved": len({row["event_group_id"] for row in generated if row.get("enabled")}),
        },
    }


def raw_value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if row.get(key) is not None:
            return row[key]
    return None


def write_discovery(result: dict[str, Any], *, rules_out: Path, review_out: Path) -> None:
    write_json_atomic(rules_out, {"rules": result.get("generated_rules") or []})
    write_json_atomic(review_out, {"review": result.get("review") or [], "summary": result.get("summary") or {}})

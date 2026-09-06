"""Standalone Gamma market client and weather event grouping."""

from __future__ import annotations

import json
import os
import re
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo

from .models import WeatherMarket
from .rules import normalize_market
from .sources import JsonHttp, SourceError
from .station_tz import timezone_for_station


_MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


class GammaError(RuntimeError):
    pass


class GammaClient:
    def __init__(self, *, http: Optional[JsonHttp] = None, base_url: str = "https://gamma-api.polymarket.com"):
        self.http = http or JsonHttp()
        self.base_url = base_url.rstrip("/")

    def list_markets(
        self,
        *,
        limit: int = 500,
        max_pages: int = 20,
        active: bool = True,
        closed: bool = False,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        for _ in range(max(1, int(max_pages))):
            try:
                payload = self.http.get_json(
                    self.base_url + "/markets",
                    params={
                        "limit": max(1, min(500, int(limit))),
                        "offset": offset,
                        "active": str(bool(active)).lower(),
                        "closed": str(bool(closed)).lower(),
                        "order": "endDate",
                        "ascending": "true",
                    },
                )
            except SourceError as exc:
                raise GammaError(str(exc)) from exc
            batch = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(batch, list):
                raise GammaError("Gamma /markets returned a non-list payload")
            rows.extend(item for item in batch if isinstance(item, dict))
            if len(batch) < int(limit):
                break
            offset += len(batch)
        return rows

    def list_events(
        self,
        *,
        tag_slug: str = "weather",
        limit: int = 100,
        max_pages: int = 15,
        active: bool = True,
        closed: bool = False,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        for _ in range(max(1, int(max_pages))):
            try:
                payload = self.http.get_json(
                    self.base_url + "/events",
                    params={
                        "limit": max(1, min(100, int(limit))),
                        "offset": offset,
                        "active": str(bool(active)).lower(),
                        "closed": str(bool(closed)).lower(),
                        "tag_slug": tag_slug,
                        "order": "endDate",
                        "ascending": "true",
                    },
                )
            except SourceError as exc:
                raise GammaError(str(exc)) from exc
            batch = payload.get("data") if isinstance(payload, dict) else payload
            if isinstance(payload, dict) and not isinstance(batch, list):
                batch = payload.get("events")
            if not isinstance(batch, list):
                raise GammaError("Gamma /events returned a non-list payload")
            rows.extend(item for item in batch if isinstance(item, dict))
            if len(batch) < int(limit):
                break
            offset += len(batch)
        return rows


def flatten_event_markets(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        slug = str(event.get("slug") or "").strip()
        event_id = event.get("id")
        title = event.get("title") or ""
        markets = event.get("markets") or []
        if isinstance(markets, str):
            try:
                markets = json.loads(markets)
            except json.JSONDecodeError:
                markets = []
        if not isinstance(markets, list):
            continue
        for market in markets:
            if not isinstance(market, dict):
                continue
            row = dict(market)
            row["event_group_id"] = slug or str(event_id or row.get("id") or "")
            row.setdefault(
                "events",
                [{"slug": slug, "id": event_id, "title": title}],
            )
            row.setdefault("category", "weather")
            if row.get("fee") and not row.get("feeSchedule"):
                row["feeSchedule"] = row["fee"]
            rows.append(row)
    return rows


def infer_weather_metric(title: str, slug: str = "") -> str:
    blob = " ".join((title, slug)).lower()
    if "highest temperature" in blob:
        return "daily_max"
    if "lowest temperature" in blob:
        return "daily_min"
    if re.search(r"\b(rain|rainfall|precipitation)\b", blob):
        return "precip"
    if "hurricane" in blob or "tropical" in blob:
        return "hurricane"
    if "snow" in blob:
        return "snow"
    return "other"


def local_date_from_event(slug: str, title: str = "", end_date: str = "") -> str:
    match = re.search(r"on-([a-z]+)-(\d{1,2})-(\d{4})", str(slug or "").lower())
    if match and match.group(1) in _MONTHS:
        return "{:04d}-{:02d}-{:02d}".format(
            int(match.group(3)),
            _MONTHS[match.group(1)],
            int(match.group(2)),
        )
    if end_date:
        return str(end_date)[:10]
    return ""


def nws_station_from_url(url: str) -> str:
    parsed = urllib.parse.urlparse(str(url or "").strip())
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    if host != "weather.gov" and not host.endswith(".weather.gov"):
        return ""
    if "timeseries" not in parsed.path.lower():
        return ""
    site = urllib.parse.parse_qs(parsed.query).get("site") or []
    station = str(site[0] if site else "").strip().upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9]{3,4}", station):
        return ""
    return station


def wu_history_from_url(url: str) -> dict[str, str]:
    match = re.search(
        r"wunderground\.com/history/daily/([a-z]{2})/([^/?#]+)/([A-Za-z0-9]{3,5})",
        str(url or ""),
        re.I,
    )
    if not match:
        return {}
    return {
        "country": match.group(1).upper(),
        "city": match.group(2).strip().lower(),
        "station_id": match.group(3).upper(),
    }


def hko_from_url(url: str) -> dict[str, str]:
    text = str(url or "").lower()
    if "weather.gov.hk" not in text and "hko.gov.hk" not in text:
        return {}
    if "climat" in text or "/cis/" in text or "opendata" in text:
        return {"station_id": "HKO"}
    return {}


def extract_resolution_url(*texts: str) -> str:
    blob = " ".join(str(item or "") for item in texts)
    for match in re.finditer(r"https?://[^\s)\]>\"']+", blob, re.I):
        url = match.group(0).rstrip(".,;\"'")
        if nws_station_from_url(url) or wu_history_from_url(url) or hko_from_url(url):
            return url
    return ""


def station_from_resolution_url(url: str) -> str:
    return (
        nws_station_from_url(url)
        or wu_history_from_url(url).get("station_id")
        or hko_from_url(url).get("station_id")
        or ""
    )


def observation_in_horizon(
    local_date: str,
    timezone_name: str = "",
    *,
    now: Optional[datetime] = None,
    past_hours: float = 24.0,
    future_hours: float = 24.0,
) -> bool:
    """True if the local civil observation day overlaps [now-past, now+future]."""

    text = str(local_date or "").strip()
    if len(text) < 10:
        return False
    try:
        year, month, day = int(text[0:4]), int(text[5:7]), int(text[8:10])
        zone = ZoneInfo(timezone_name) if str(timezone_name or "").strip() else timezone.utc
        start = datetime(year, month, day, 0, 0, 0, tzinfo=zone)
        end = datetime(year, month, day, 23, 59, 59, tzinfo=zone)
    except (ValueError, TypeError):
        return False
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)
    lo = current - timedelta(hours=max(0.0, float(past_hours)))
    hi = current + timedelta(hours=max(0.0, float(future_hours)))
    return start.astimezone(timezone.utc) <= hi and end.astimezone(timezone.utc) >= lo


def _resolution_url(payload: dict[str, Any]) -> str:
    if payload.get("resolution_source"):
        return str(payload.get("resolution_source") or "")
    if payload.get("resolutionSource"):
        return str(payload.get("resolutionSource") or "")
    source = payload.get("source") if isinstance(payload.get("source"), dict) else {}
    if source.get("resolution_source"):
        return str(source.get("resolution_source") or "")
    markets = payload.get("markets") if isinstance(payload.get("markets"), list) else []
    first = markets[0] if markets and isinstance(markets[0], dict) else {}
    explicit = str(first.get("resolutionSource") or first.get("resolution_source") or "")
    if explicit:
        return explicit
    return extract_resolution_url(
        str(payload.get("description") or ""),
        str(first.get("description") or ""),
        str(payload.get("title") or ""),
    )


def _payload_timezone(payload: dict[str, Any], local_date: str = "") -> str:
    explicit = str(payload.get("timezone") or "").strip()
    if explicit:
        return explicit
    station = station_from_resolution_url(_resolution_url(payload))
    mapped = timezone_for_station(station)
    if mapped:
        return mapped
    return "UTC"


def filter_events_in_horizon(
    events: Iterable[dict[str, Any]],
    *,
    now: Optional[datetime] = None,
    past_hours: float = 24.0,
    future_hours: float = 24.0,
) -> list[dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        slug = str(event.get("slug") or "").strip()
        title = str(event.get("title") or "")
        local_date = local_date_from_event(slug, title, str(event.get("endDate") or ""))
        if observation_in_horizon(
            local_date,
            _payload_timezone(event, local_date),
            now=now,
            past_hours=past_hours,
            future_hours=future_hours,
        ):
            kept.append(event)
    return kept


def filter_market_rows_in_horizon(
    rows: Iterable[dict[str, Any]],
    *,
    now: Optional[datetime] = None,
    past_hours: float = 24.0,
    future_hours: float = 24.0,
) -> list[dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        slug = str(row.get("event_group_id") or row.get("slug") or "").strip()
        local_date = local_date_from_event(slug, str(row.get("question") or ""), str(row.get("endDate") or ""))
        if observation_in_horizon(
            local_date,
            _payload_timezone(row, local_date),
            now=now,
            past_hours=past_hours,
            future_hours=future_hours,
        ):
            kept.append(row)
    return kept


def filter_catalog_groups_in_horizon(
    groups: Iterable[dict[str, Any]],
    *,
    now: Optional[datetime] = None,
    past_hours: float = 24.0,
    future_hours: float = 24.0,
) -> list[dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    for group in groups:
        if not isinstance(group, dict):
            continue
        local_date = str(group.get("local_date") or "")
        if not local_date:
            local_date = local_date_from_event(
                str(group.get("slug") or group.get("event_group_id") or ""),
                str(group.get("question") or ""),
                str(group.get("end_date") or ""),
            )
        if observation_in_horizon(
            local_date,
            _payload_timezone(group, local_date),
            now=now,
            past_hours=past_hours,
            future_hours=future_hours,
        ):
            kept.append(group)
    return kept


def catalog_event_groups(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        slug = str(event.get("slug") or "").strip()
        title = str(event.get("title") or slug)
        markets = event.get("markets") or []
        if isinstance(markets, str):
            try:
                markets = json.loads(markets)
            except json.JSONDecodeError:
                markets = []
        markets = [item for item in markets if isinstance(item, dict)] if isinstance(markets, list) else []
        metric = infer_weather_metric(title, slug)
        first = markets[0] if markets else {}
        resolution = str(
            event.get("resolutionSource")
            or first.get("resolutionSource")
            or first.get("resolution_source")
            or ""
        )
        groups.append(
            {
                "event_group_id": slug or str(event.get("id") or title),
                "question": title,
                "slug": slug,
                "station_id": "",
                "local_date": local_date_from_event(slug, title, str(event.get("endDate") or "")),
                "metric": metric,
                "unit": "",
                "source_status": "not_checked",
                "status": "review",
                "reason": "rule_not_approved",
                "catalog_only": True,
                "bucket_match_consistent": True,
                "matched_bucket": None,
                "market_count": len(markets),
                "matched_market_count": 0,
                "candidate_count": 0,
                "status_counts": {"review": 1},
                "resolution_source": resolution,
                "end_date": str(event.get("endDate") or ""),
                "neg_risk": bool(event.get("negRisk") or first.get("negRisk")),
                "markets": [
                    {
                        "market_id": str(item.get("id") or ""),
                        "outcome": str(item.get("groupItemTitle") or item.get("outcome") or ""),
                        "status": "review",
                        "reason": "rule_not_approved",
                        "economics": {},
                        "book": {},
                    }
                    for item in markets
                ],
                "rows": [
                    {
                        "market_id": str(first.get("id") or ""),
                        "event_group_id": slug,
                        "status": "review",
                        "reason": "rule_not_approved",
                        "target_outcome": str(first.get("groupItemTitle") or ""),
                        "lifecycle": {"state": "RULE_REVIEW", "history": ["DISCOVERED", "RULE_REVIEW"]},
                    }
                ],
            }
        )
    return groups


def merge_event_groups(
    catalog: Iterable[dict[str, Any]],
    scanned: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for group in scanned:
        key = _clean_event_key(str(group.get("event_group_id") or ""))
        if key in seen:
            continue
        seen.add(key)
        merged.append(group)
    for group in catalog:
        key = _clean_event_key(str(group.get("event_group_id") or ""))
        if key in seen:
            continue
        seen.add(key)
        merged.append(group)
    return merged


def load_market_rows(path: str) -> list[dict[str, Any]]:
    from pathlib import Path

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = payload.get("markets") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        raise ValueError("market snapshot must be a list or {markets: [...]}")
    return [row for row in rows if isinstance(row, dict)]


def snapshot_payload(rows: Iterable[dict[str, Any]], *, fetched_at: Optional[str] = None) -> dict[str, Any]:
    return {
        "fetched_at": fetched_at or datetime.now(timezone.utc).isoformat(),
        "source": "polymarket-gamma",
        "markets": list(rows),
    }


def weather_markets(rows: Iterable[dict[str, Any]]) -> list[WeatherMarket]:
    result = []
    for row in rows:
        market = normalize_market(row)
        if market.category in {"weather", "climate", "climate-science"}:
            result.append(market)
            continue
        # Gamma snapshots from older endpoints may omit category. Only infer
        # weather for an explicit weather/temperature question or slug;
        # never treat every uncategorized market as a weather market.
        text = " ".join((market.question, market.slug)).lower()
        if re.search(r"\b(weather|temperature|rainfall|precipitation|snowfall|snow)\b", text):
            result.append(market)
    return result


def _clean_event_key(value: str) -> str:
    text = value.strip().lower()
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"[^a-z0-9_-]+", "-", text)
    return text.strip("-") or "unknown-weather-event"


def group_weather_markets(markets: Iterable[WeatherMarket]) -> dict[str, list[WeatherMarket]]:
    groups: dict[str, list[WeatherMarket]] = {}
    for market in markets:
        key = (market.event_group_id or "").strip()
        if not key:
            # Unique per market so ungrouped Gamma rows cannot silently merge.
            key = "ungrouped-{}".format(market.market_id or market.slug or "unknown")
        key = _clean_event_key(key)
        groups.setdefault(key, []).append(market)
    return groups


def event_group_snapshot(
    groups: dict[str, list[WeatherMarket]],
    *,
    rows_by_market: Optional[dict[str, dict[str, Any]]] = None,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for event_group_id, markets in sorted(groups.items()):
        first = markets[0] if markets else None
        result.append(
            {
                "event_group_id": event_group_id,
                "question": first.question if first else "",
                "slug": first.slug if first else "",
                "market_count": len(markets),
                "station_id": "",
                "local_date": "",
                "metric": "",
                "unit": "",
                "markets": [market.to_dict() for market in markets],
                "market_raw": {
                    market.market_id: (rows_by_market or {}).get(market.market_id, market.raw)
                    for market in markets
                },
            }
        )
    return result

"""JSON-friendly models and normalizers for the weather POC."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone, tzinfo
from typing import Any, Dict, List, Optional


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def now_iso(value: Optional[datetime] = None) -> str:
    return (value or utc_now()).astimezone(timezone.utc).isoformat()


def as_bool(value: Any, default: Optional[bool] = None) -> Optional[bool]:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def as_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_time(value: Any, *, default_tz: Optional[tzinfo] = None) -> Optional[datetime]:
    if value is None or str(value).strip() == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        if number > 1e12:
            number /= 1000.0
        if 1e9 <= number < 2e10:
            return datetime.fromtimestamp(number, tz=timezone.utc)
        return None
    text = str(value).strip().replace("Z", "+00:00")
    if text.isdigit() and len(text) >= 10:
        return parse_time(int(text), default_tz=default_tz)
    try:
        result = datetime.fromisoformat(text)
    except ValueError:
        return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=default_tz or timezone.utc)
    return result.astimezone(timezone.utc)


@dataclass
class WeatherMarket:
    market_id: str
    event_group_id: str = ""
    condition_id: str = ""
    question: str = ""
    slug: str = ""
    category: str = "weather"
    outcome: str = ""
    outcomes: List[str] = field(default_factory=list)
    token_ids: List[str] = field(default_factory=list)
    yes_token_id: str = ""
    no_token_id: str = ""
    resolution_source: str = ""
    end_date: str = ""
    active: bool = False
    closed: bool = True
    accepting_orders: Optional[bool] = None
    enable_order_book: Optional[bool] = None
    neg_risk: bool = False
    tick_size: Optional[float] = None
    min_order_size: Optional[float] = None
    fee_rate: Optional[float] = None
    fee_source: str = "unknown"
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def tradable(self) -> bool:
        # Exclusive temperature buckets are almost always Polymarket negRisk
        # events. That is collateral wrapping, not a reason to skip a
        # source-final winner Yes on the CLOB.
        return (
            self.active
            and not self.closed
            and self.accepting_orders is True
            and self.enable_order_book is True
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Bucket:
    outcome: str
    lower: Optional[float] = None
    upper: Optional[float] = None
    lower_inclusive: bool = True
    upper_inclusive: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class WeatherRule:
    market_id: str
    event_group_id: str
    adapter: str
    source: Dict[str, Any]
    metric: str
    observation_start: str
    observation_end: str
    unit: str
    buckets: List[Bucket]
    timezone: str = "UTC"
    rounding: str = "whole_degree_as_published"
    target_outcome: str = ""
    rule_version: str = ""
    manual_approval: bool = False
    enabled: bool = True
    notes: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["buckets"] = [bucket.to_dict() for bucket in self.buckets]
        return payload


@dataclass
class ObservationEvidence:
    status: str
    value: Optional[float] = None
    raw_aggregate: Optional[float] = None
    source_timestamp: str = ""
    confirmation_timestamp: str = ""
    observed_at: str = ""
    source_url: str = ""
    provider: str = ""
    station_id: str = ""
    unit: str = ""
    aggregation: str = ""
    reason: str = ""
    raw: Any = None
    evidence_hash: str = ""
    series: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Book:
    token_id: str
    best_ask: Optional[float] = None
    best_bid: Optional[float] = None
    asks: List[Dict[str, float]] = field(default_factory=list)
    bids: List[Dict[str, float]] = field(default_factory=list)
    tick_size: Optional[float] = None
    min_order_size: Optional[float] = None
    fetched_at: str = ""
    book_missing: bool = False
    error: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ScanResult:
    rows: List[Dict[str, Any]]
    event_groups: List[Dict[str, Any]]
    summary: Dict[str, Any]
    books: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


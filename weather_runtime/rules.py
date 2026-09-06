"""Weather rule parsing, bucket validation and deterministic matching."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import replace
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .models import Bucket, WeatherMarket, WeatherRule, as_bool, as_float, parse_time


class RuleError(ValueError):
    """Raised when a rule cannot be safely interpreted."""


ROUNDING_WHOLE = "whole_degree_as_published"
ROUNDING_NONE = "none"
_ROUNDING_ALIASES = {
    "whole_degree_as_published": ROUNDING_WHOLE,
    "whole_degree": ROUNDING_WHOLE,
    "whole": ROUNDING_WHOLE,
    "integer": ROUNDING_WHOLE,
    "none": ROUNDING_NONE,
    "raw": ROUNDING_NONE,
    "identity": ROUNDING_NONE,
}


def _list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except ValueError:
            return [value]
        return parsed if isinstance(parsed, list) else [parsed]
    return []


def _first(row: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return default


def _canonical_version(raw: dict[str, Any]) -> str:
    explicit = str(raw.get("rule_version") or "").strip()
    if explicit:
        return explicit
    encoded = json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def normalize_rounding(value: Any) -> str:
    text = str(value or ROUNDING_WHOLE).strip().lower()
    if not text:
        return ROUNDING_WHOLE
    rounding = _ROUNDING_ALIASES.get(text)
    if rounding is None:
        raise RuleError("unsupported rounding: {}".format(value))
    return rounding


def load_timezone(name: Any) -> ZoneInfo:
    text = str(name or "").strip()
    if not text:
        raise RuleError("weather rule requires IANA timezone")
    try:
        return ZoneInfo(text)
    except (ZoneInfoNotFoundError, KeyError, ValueError) as exc:
        raise RuleError("invalid IANA timezone: {}".format(text)) from exc


def apply_rounding(value: Any, rounding: str = ROUNDING_WHOLE) -> Optional[float]:
    number = _parse_number(value)
    if number is None:
        return None
    mode = normalize_rounding(rounding)
    if mode == ROUNDING_NONE:
        return float(number)
    quantized = Decimal(str(number)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return float(int(quantized))


def norm_outcome(value: Any) -> str:
    text = str(value or "").strip().lower().replace("°", "")
    text = re.sub(r"(?<=\d)\s*[cf](?=\b)", "", text)
    text = text.replace("or more", "or higher").replace("or less", "or below")
    return " ".join(text.split())


def extract_event_group_id(row: dict[str, Any]) -> str:
    explicit = _first(row, "event_group_id", "eventGroupId", "eventSlug", "event_slug")
    if explicit and str(explicit).strip():
        return str(explicit).strip()
    events = row.get("events")
    if isinstance(events, str):
        events = _list(events)
    elif isinstance(events, dict):
        events = [events]
    elif not isinstance(events, list):
        events = []
    nested = row.get("event")
    if isinstance(nested, dict):
        events = [nested, *list(events)]
    for event in events:
        if not isinstance(event, dict):
            continue
        for key in ("slug", "ticker", "eventSlug", "event_slug"):
            value = str(event.get(key) or "").strip()
            if value:
                return value
        event_id = str(event.get("id") or "").strip()
        if event_id:
            return "event-{}".format(event_id)
    return ""


def validate_event_group_siblings(
    markets: Iterable[WeatherMarket],
    buckets: Iterable[Bucket],
) -> Optional[str]:
    market_list = [item for item in markets if getattr(item, "market_id", "")]
    bucket_list = list(buckets)
    if not bucket_list:
        return "bucket_set_empty"
    expected = [norm_outcome(bucket.outcome) for bucket in bucket_list]
    if any(not item for item in expected):
        return "bucket_outcome_missing"
    if len(set(expected)) != len(expected):
        return "duplicate_bucket_outcome"
    actual = [norm_outcome(market.outcome) for market in market_list]
    if any(not item for item in actual):
        return "sibling_outcome_missing"
    if len(actual) != len(expected):
        return "sibling_count_mismatch"
    if len(set(actual)) != len(actual):
        return "duplicate_sibling_outcome"
    if sorted(actual) != sorted(expected):
        return "sibling_outcomes_mismatch"
    return None


def _parse_number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("°", "").replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    return float(match.group(0)) if match else None


def parse_bucket(raw: Any) -> Bucket:
    if isinstance(raw, Bucket):
        return raw
    if not isinstance(raw, dict):
        raise RuleError("bucket must be an object")
    outcome = str(raw.get("outcome") or raw.get("label") or "").strip()
    if not outcome:
        raise RuleError("bucket outcome missing")
    lower = _parse_number(raw.get("lower"))
    upper = _parse_number(raw.get("upper"))
    lowered = raw.get("lower_inclusive")
    uppered = raw.get("upper_inclusive")
    lower_inclusive = True if lowered is None else bool(as_bool(lowered, True))
    upper_inclusive = True if uppered is None else bool(as_bool(uppered, True))
    return Bucket(
        outcome=outcome,
        lower=lower,
        upper=upper,
        lower_inclusive=lower_inclusive,
        upper_inclusive=upper_inclusive,
    )


def buckets_from_outcomes(outcomes: Iterable[str]) -> list[Bucket]:
    """Build a contiguous exclusive-lower / inclusive-upper set from Gamma titles."""

    parsed: list[tuple[str, str, float, float]] = []
    for raw in outcomes:
        outcome = str(raw or "").strip()
        if not outcome:
            raise RuleError("bucket outcome missing")
        text = outcome.lower().replace("°", " ")
        text = re.sub(r"\b[cf]\b", " ", text)
        numbers = [float(item) for item in re.findall(r"(?<![\d])-?\d+(?:\.\d+)?", text)]
        if not numbers:
            raise RuleError("bucket outcome has no numeric bound: {}".format(outcome))
        if re.search(r"\bor\s+(below|less|lower)\b", text):
            parsed.append((outcome, "floor", numbers[0], numbers[0]))
        elif re.search(r"\bor\s+(higher|more|above)\b", text):
            parsed.append((outcome, "ceil", numbers[0], numbers[0]))
        elif len(numbers) >= 2:
            parsed.append((outcome, "span", min(numbers[0], numbers[1]), max(numbers[0], numbers[1])))
        else:
            parsed.append((outcome, "point", numbers[0], numbers[0]))
    if len(parsed) < 2:
        raise RuleError("temperature bucket set is too small")
    ordered = sorted(parsed, key=lambda item: item[2])
    if ordered[0][1] != "floor" or ordered[-1][1] != "ceil":
        raise RuleError("bucket set requires a below floor and a higher ceiling")
    buckets: list[Bucket] = []
    previous_high: Optional[float] = None
    last_index = len(ordered) - 1
    for index, (outcome, kind, low, high) in enumerate(ordered):
        if index == 0:
            buckets.append(Bucket(outcome=outcome, upper=high, upper_inclusive=True))
            previous_high = high
            continue
        if previous_high is None:
            raise RuleError("bucket set is not contiguous")
        if index == last_index:
            if abs(low - (previous_high + 1.0)) > 1e-9 and abs(low - previous_high) > 1e-9:
                raise RuleError("ceiling bucket does not adjoin previous bucket")
            buckets.append(
                Bucket(outcome=outcome, lower=previous_high, lower_inclusive=False)
            )
            continue
        if kind in {"span", "point"} and abs(low - (previous_high + 1.0)) > 1e-9:
            raise RuleError("bucket does not adjoin previous bucket")
        buckets.append(
            Bucket(
                outcome=outcome,
                lower=previous_high,
                lower_inclusive=False,
                upper=high,
                upper_inclusive=True,
            )
        )
        previous_high = high
    return validate_buckets(buckets)


def validate_buckets(buckets: Iterable[Bucket]) -> list[Bucket]:
    values = list(buckets)
    if not values:
        raise RuleError("weather rule requires buckets")
    if any(bucket.lower is None and bucket.upper is None for bucket in values):
        raise RuleError("bucket cannot have both bounds missing")
    ordered = sorted(values, key=lambda b: float("-inf") if b.lower is None else b.lower)
    for index, bucket in enumerate(ordered):
        if (
            bucket.lower is not None
            and bucket.upper is not None
            and bucket.lower > bucket.upper
        ):
            raise RuleError("bucket lower is greater than upper: {}".format(bucket.outcome))
        if index == 0:
            continue
        previous = ordered[index - 1]
        if previous.upper is None or bucket.lower is None:
            raise RuleError("only the first bucket may have no lower bound")
        if previous.upper > bucket.lower:
            raise RuleError("bucket overlap between {} and {}".format(previous.outcome, bucket.outcome))
        if previous.upper < bucket.lower:
            raise RuleError("bucket gap between {} and {}".format(previous.outcome, bucket.outcome))
        if previous.upper == bucket.lower:
            if previous.upper_inclusive and bucket.lower_inclusive:
                raise RuleError("bucket overlap at {}".format(previous.upper))
            if not (previous.upper_inclusive or bucket.lower_inclusive):
                raise RuleError("exclusive boundary gap at {}".format(previous.upper))
    if ordered[-1].upper is not None:
        raise RuleError("last bucket must have no upper bound")
    if ordered[0].lower is not None:
        raise RuleError("first bucket must have no lower bound")
    return ordered


def bucket_lower_ok(number: float, bucket: Bucket) -> bool:
    return (
        bucket.lower is None
        or number > bucket.lower
        or (bucket.lower_inclusive and number == bucket.lower)
    )


def bucket_upper_ok(number: float, bucket: Bucket) -> bool:
    return (
        bucket.upper is None
        or number < bucket.upper
        or (bucket.upper_inclusive and number == bucket.upper)
    )


def bucket_for_value(
    value: Any,
    buckets: Iterable[Bucket],
    *,
    rounding: str = ROUNDING_WHOLE,
) -> Optional[Bucket]:
    number = apply_rounding(value, rounding)
    if number is None:
        return None
    for bucket in buckets:
        if bucket_lower_ok(number, bucket) and bucket_upper_ok(number, bucket):
            return bucket
    return None


def bucket_for_outcome(outcome: Any, buckets: Iterable[Bucket]) -> Optional[Bucket]:
    key = norm_outcome(outcome)
    for bucket in buckets:
        if norm_outcome(bucket.outcome) == key:
            return bucket
    return None


def bucket_impossible_while_open(
    metric: str,
    running: Any,
    bucket: Bucket,
    *,
    rounding: str = ROUNDING_WHOLE,
) -> bool:
    """True when a running min/max can no longer land in ``bucket``."""
    number = apply_rounding(running, rounding)
    if number is None:
        return False
    if metric == "daily_min":
        # Once the rounded running min is R, any bucket entirely above R
        # cannot be the daily minimum (lowest can only stay or fall).
        return not bucket_lower_ok(number, bucket)
    if metric == "daily_max":
        return not bucket_upper_ok(number, bucket)
    return False


def normalize_market(row: dict[str, Any]) -> WeatherMarket:
    outcomes = [str(value) for value in _list(_first(row, "outcomes", default=[]))]
    token_ids = [
        str(value)
        for value in _list(_first(row, "clobTokenIds", "clob_token_ids", "token_ids", default=[]))
        if value
    ]
    category = str(_first(row, "category", "marketCategory", default="") or "").strip().lower()
    event_group_id = extract_event_group_id(row)
    outcome = str(_first(row, "outcome", "groupItemTitle", "group_item_title", default="") or "").strip()
    if not outcome and outcomes:
        outcome = outcomes[0]
    fee_rate = None
    fee_source = "unknown"
    schedule = _first(row, "feeSchedule", "fee_schedule", default={})
    if isinstance(schedule, str):
        try:
            schedule = json.loads(schedule)
        except ValueError:
            schedule = {}
    if isinstance(schedule, dict):
        fee_rate = as_float(_first(schedule, "rate", "feeRate", "fee_rate"))
        if fee_rate is not None:
            fee_source = "market_fee_schedule"
    if fee_rate is None:
        extra = _first(row, "fee", default={})
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except ValueError:
                extra = {}
        if isinstance(extra, dict):
            fee_rate = as_float(_first(extra, "rate", "feeRate", "fee_rate"))
            if fee_rate is not None:
                fee_source = "market_fee"
    if fee_rate is None:
        fee_rate = as_float(_first(row, "feeRate", "fee_rate"))
        if fee_rate is not None:
            fee_source = "market_fee_rate"
    if fee_rate is None and category == "weather":
        # Indicative only; scanner rejects this source by default.
        fee_rate = 0.05
        fee_source = "category_default"
    resolution_source = str(_first(row, "resolutionSource", "resolution_source", default="") or "")
    if not resolution_source:
        from .markets import extract_resolution_url

        resolution_source = extract_resolution_url(
            str(row.get("description") or ""),
            str(row.get("question") or ""),
        )
    return WeatherMarket(
        market_id=str(_first(row, "id", "market_id", default="") or "").strip(),
        event_group_id=event_group_id,
        condition_id=str(_first(row, "conditionId", "condition_id", default="") or ""),
        question=str(_first(row, "question", default="") or ""),
        slug=str(_first(row, "slug", default="") or ""),
        category=category,
        outcome=outcome,
        outcomes=outcomes,
        token_ids=token_ids,
        yes_token_id=token_ids[0] if token_ids else "",
        no_token_id=token_ids[1] if len(token_ids) > 1 else "",
        resolution_source=resolution_source,
        end_date=str(_first(row, "endDate", "end_date", default="") or ""),
        active=bool(as_bool(_first(row, "active", default=False), False)),
        closed=bool(as_bool(_first(row, "closed", default=True), True)),
        accepting_orders=as_bool(_first(row, "acceptingOrders", "accepting_orders")),
        enable_order_book=as_bool(_first(row, "enableOrderBook", "enable_order_book")),
        neg_risk=bool(as_bool(_first(row, "negRisk", "neg_risk", default=False), False)),
        tick_size=as_float(_first(row, "orderPriceMinTickSize", "tick_size", "tickSize")),
        min_order_size=as_float(_first(row, "orderMinSize", "min_order_size", "minOrderSize")),
        fee_rate=fee_rate,
        fee_source=fee_source,
        raw=dict(row),
    )


def parse_rule(raw: dict[str, Any]) -> WeatherRule:
    if not isinstance(raw, dict):
        raise RuleError("rule must be an object")
    market_id = str(raw.get("market_id") or raw.get("marketId") or "").strip()
    event_group_id = str(raw.get("event_group_id") or raw.get("eventGroupId") or "").strip()
    if not market_id or not event_group_id:
        raise RuleError("rule requires market_id and event_group_id")
    adapter = str(raw.get("adapter") or "weather_observation").strip().lower()
    if adapter != "weather_observation":
        raise RuleError("unsupported weather adapter: {}".format(adapter))
    source = raw.get("source") or {}
    if not isinstance(source, dict) or not (source.get("url") or source.get("static")):
        raise RuleError("weather rule source requires url or static")
    resolution_binding = str(
        source.get("resolution_source")
        or source.get("resolution_source_contains")
        or raw.get("resolution_source")
        or ""
    ).strip()
    if not resolution_binding:
        raise RuleError("weather rule requires resolution source binding")
    metric = str(raw.get("metric") or source.get("aggregation") or "").strip().lower()
    if metric not in {"daily_max", "daily_min", "daily_sum", "latest"}:
        raise RuleError("unsupported weather metric: {}".format(metric))
    timezone_name = str(
        raw.get("timezone") or raw.get("local_timezone") or source.get("timezone") or ""
    ).strip()
    zone = load_timezone(timezone_name)
    rounding = normalize_rounding(raw.get("rounding") or source.get("rounding") or ROUNDING_WHOLE)
    start = str(raw.get("observation_start") or source.get("observation_start") or "").strip()
    end = str(raw.get("observation_end") or source.get("observation_end") or "").strip()
    start_at = parse_time(start, default_tz=zone)
    end_at = parse_time(end, default_tz=zone)
    if not start or not end or start_at is None or end_at is None:
        raise RuleError("weather rule requires valid observation_start/observation_end")
    if start_at > end_at:
        raise RuleError("observation_start is after observation_end")
    buckets = validate_buckets(parse_bucket(value) for value in _list(raw.get("buckets")))
    predicate = raw.get("predicate")
    predicate_target = predicate.get("target_outcome") if isinstance(predicate, dict) else ""
    target_outcome = str(
        raw.get("target_outcome") or raw.get("outcome") or predicate_target or ""
    ).strip()
    return WeatherRule(
        market_id=market_id,
        event_group_id=event_group_id,
        adapter=adapter,
        source=dict(source),
        metric=metric,
        observation_start=start,
        observation_end=end,
        unit=str(raw.get("unit") or source.get("unit") or "C").strip().upper(),
        buckets=buckets,
        timezone=zone.key,
        rounding=rounding,
        target_outcome=target_outcome,
        rule_version=_canonical_version(raw),
        manual_approval=bool(as_bool(raw.get("manual_approval"), False)),
        enabled=bool(as_bool(raw.get("enabled"), True)),
        notes=str(raw.get("notes") or ""),
        raw=dict(raw),
    )


def load_rules(path: Any) -> list[WeatherRule]:
    from pathlib import Path

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    defaults = payload.get("defaults") if isinstance(payload, dict) else {}
    defaults = defaults if isinstance(defaults, dict) else {}
    rows = payload.get("rules") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        raise RuleError("rules file must be a list or {rules: [...]}")
    merged_rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        merged = {**defaults, **row}
        if isinstance(defaults.get("source"), dict) and isinstance(row.get("source"), dict):
            merged["source"] = {**defaults["source"], **row["source"]}
        merged_rows.append(merged)
    return [parse_rule(row) for row in merged_rows]


def source_matches(market: WeatherMarket, rule: WeatherRule) -> bool:
    actual = market.resolution_source.strip().rstrip("/").lower()
    expected = str(
        rule.source.get("resolution_source")
        or rule.source.get("resolution_source_contains")
        or rule.raw.get("resolution_source")
        or ""
    ).strip().rstrip("/").lower()
    if not actual or not expected:
        return False
    if rule.source.get("resolution_source_contains"):
        return str(rule.source["resolution_source_contains"]).lower() in actual
    return actual == expected


def with_source_contract(rule: WeatherRule, *, source: dict[str, Any]) -> WeatherRule:
    return replace(rule, source={**rule.source, **source})

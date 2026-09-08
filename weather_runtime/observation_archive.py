"""Timestamped WRH/HTTP observation point archive for day-later review."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .storage import append_jsonl_dedup


def _point_key(row: dict[str, Any]) -> str:
    return "|".join(
        [
            str(row.get("station_id") or ""),
            str(row.get("obs_timestamp") or ""),
            str(row.get("temp")),
            str(row.get("channel") or "wrh_http"),
            str(row.get("event_group_id") or ""),
        ]
    )


def wrh_point_rows_from_evidence(
    evidence: Iterable[dict[str, Any]],
    *,
    polled_at: str,
) -> list[dict[str, Any]]:
    """Flatten polled WRH/Synoptic series into first-seen candidate rows."""

    stamp = polled_at or datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    for item in evidence:
        if not isinstance(item, dict):
            continue
        provider = str(item.get("provider") or "").lower()
        if provider in {"wunderground", "hko"}:
            continue
        station_id = str(item.get("station_id") or "").strip().upper()
        series = item.get("series") if isinstance(item.get("series"), list) else []
        if not series:
            continue
        for point in series:
            if not isinstance(point, dict):
                continue
            obs_ts = point.get("timestamp")
            if obs_ts in {None, ""}:
                continue
            rows.append(
                {
                    "channel": "wrh_http",
                    "station_id": station_id,
                    "event_group_id": item.get("event_group_id"),
                    "provider": provider or "noaa",
                    "sample_set": item.get("sample_set") or "all",
                    "obs_timestamp": obs_ts,
                    "local_time": point.get("local_time"),
                    "timezone": point.get("timezone"),
                    "temp": point.get("temp"),
                    "counts_for_resolution": point.get("counts_for_resolution"),
                    "evidence_hash": item.get("evidence_hash"),
                    "source_timestamp": item.get("source_timestamp"),
                    "first_seen_at": stamp,
                    "polled_at": stamp,
                }
            )
    return rows


def persist_wrh_observation_points(
    path: Path,
    evidence: Iterable[dict[str, Any]],
    *,
    polled_at: str,
) -> int:
    """Append new WRH/HTTP points only; preserves first_seen_at via dedupe key."""

    rows = wrh_point_rows_from_evidence(evidence, polled_at=polled_at)
    if not rows:
        return 0
    return append_jsonl_dedup(path, rows, key_fn=_point_key)

"""Read-only Polymarket CLOB book client and deterministic VWAP walking."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from .models import Book, as_float
from .sources import JsonHttp, SourceError


def _levels(value: Any, *, reverse: bool = False) -> list[dict[str, float]]:
    result: list[dict[str, float]] = []
    if not isinstance(value, list):
        return result
    for row in value:
        if not isinstance(row, dict):
            continue
        price = as_float(row.get("price"))
        size = as_float(row.get("size"))
        if price is None or size is None or price < 0 or size <= 0:
            continue
        result.append({"price": price, "size": size})
    return sorted(result, key=lambda item: item["price"], reverse=reverse)


def normalize_book(token_id: str, payload: Any) -> Book:
    if not isinstance(payload, dict):
        return Book(token_id=token_id, book_missing=True, error="invalid_book_payload")
    asks = _levels(payload.get("asks"))
    bids = _levels(payload.get("bids"), reverse=True)
    # Keep an absent capture time absent. The scanner must fail closed on an
    # un-timestamped live book; fixture loaders may explicitly stamp replay
    # books when they are read.
    fetched_at = str(payload.get("fetched_at") or "")
    return Book(
        token_id=token_id,
        best_ask=asks[0]["price"] if asks else None,
        best_bid=bids[0]["price"] if bids else None,
        asks=asks,
        bids=bids,
        tick_size=as_float(payload.get("tick_size") or payload.get("tickSize")),
        min_order_size=as_float(payload.get("min_order_size") or payload.get("minOrderSize")),
        fetched_at=fetched_at,
        raw=dict(payload),
    )


def walk_asks(book: Book, *, max_price: float, target_shares: float) -> dict[str, Any]:
    target = max(0.0, float(target_shares))
    remaining = target
    filled = 0.0
    cost = 0.0
    worst: Optional[float] = None
    for level in book.asks:
        price = float(level["price"])
        if price > float(max_price) + 1e-12:
            break
        take = min(remaining, float(level["size"]))
        if take <= 0:
            continue
        filled += take
        cost += take * price
        worst = price
        remaining -= take
        if remaining <= 1e-12:
            break
    return {
        "target_shares": target,
        "filled_shares": filled,
        "cost": cost,
        "vwap": cost / filled if filled > 0 else None,
        "worst_price": worst,
        "complete": remaining <= 1e-12,
    }


def ask_depth(book: Book, *, max_price: float) -> float:
    return sum(
        float(level["size"])
        for level in book.asks
        if float(level["price"]) <= float(max_price) + 1e-12
    )


def walk_bids(book: Book, *, min_price: float, target_shares: float) -> dict[str, Any]:
    """Sell into bids at or above min_price (highest bid first)."""

    target = max(0.0, float(target_shares))
    remaining = target
    filled = 0.0
    proceeds = 0.0
    worst: Optional[float] = None
    for level in book.bids:
        price = float(level["price"])
        if price + 1e-12 < float(min_price):
            break
        take = min(remaining, float(level["size"]))
        if take <= 0:
            continue
        filled += take
        proceeds += take * price
        worst = price
        remaining -= take
        if remaining <= 1e-12:
            break
    return {
        "target_shares": target,
        "filled_shares": filled,
        "proceeds": proceeds,
        "vwap": proceeds / filled if filled > 0 else None,
        "worst_price": worst,
        "complete": remaining <= 1e-12,
    }


def bid_depth(book: Book, *, min_price: float) -> float:
    return sum(
        float(level["size"])
        for level in book.bids
        if float(level["price"]) + 1e-12 >= float(min_price)
    )


class ClobClient:
    def __init__(
        self,
        *,
        http: Optional[JsonHttp] = None,
        base_url: str = "https://clob.polymarket.com",
    ):
        self.http = http or JsonHttp()
        self.base_url = base_url.rstrip("/")

    def fetch_books(self, token_ids: Iterable[str]) -> dict[str, Book]:
        ids = [str(token_id) for token_id in token_ids if str(token_id)]
        result: dict[str, Book] = {}
        stopped: Optional[SourceError] = None
        for start in range(0, len(ids), 50):
            chunk = ids[start : start + 50]
            if stopped is not None:
                for token_id in chunk:
                    result[token_id] = Book(token_id=token_id, book_missing=True, error=str(stopped))
                continue
            try:
                payload = self.http.post_json(
                    self.base_url + "/books",
                    [{"token_id": token_id} for token_id in chunk],
                )
                if not isinstance(payload, list):
                    raise SourceError("CLOB /books returned a non-list payload")
                captured_at = datetime.now(timezone.utc).isoformat()
                for row in payload:
                    if not isinstance(row, dict):
                        continue
                    token_id = str(row.get("asset_id") or row.get("token_id") or "")
                    if not token_id:
                        continue
                    normalized = dict(row)
                    normalized.setdefault("fetched_at", captured_at)
                    result[token_id] = normalize_book(token_id, normalized)
            except SourceError as exc:
                stopped = exc
                for token_id in chunk:
                    result[token_id] = Book(token_id=token_id, book_missing=True, error=str(exc))
            for token_id in chunk:
                result.setdefault(
                    token_id,
                    Book(token_id=token_id, book_missing=True, error="token_missing_from_response"),
                )
        return result

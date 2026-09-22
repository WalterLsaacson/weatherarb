"""Optional live FAK and GTC buys. Dry-run never imports the signer."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal, ROUND_DOWN
from typing import Any, Optional


class LiveOrderError(RuntimeError):
    """Raised when live credentials or the CLOB client cannot submit."""


def jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "model_dump"):
        return jsonable(value.model_dump())
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    return str(value)


def _tick_quantum(tick_size: Optional[float]) -> Decimal:
    tick = Decimal(str(tick_size if tick_size is not None else 0.01))
    if tick <= 0:
        raise LiveOrderError("invalid_tick_size")
    return tick


def quantize_buy(
    price: float,
    size: float,
    max_usdc: float,
    *,
    tick_size: Optional[float] = None,
) -> tuple[float, float]:
    """FAK buys: snap price to market tick and cap size by USDC budget."""

    cent = Decimal("0.01")
    tick = _tick_quantum(tick_size)
    px = Decimal(str(price)).quantize(tick, rounding=ROUND_DOWN)
    if px <= 0:
        raise LiveOrderError("invalid_price")
    budget = Decimal(str(max_usdc)).quantize(cent, rounding=ROUND_DOWN)
    shares = Decimal(str(size)).quantize(cent, rounding=ROUND_DOWN)
    if shares <= 0:
        raise LiveOrderError("invalid_size")
    if px * shares > budget:
        shares = (budget / px).quantize(cent, rounding=ROUND_DOWN)
    if shares <= 0:
        raise LiveOrderError("cannot_quantize_buy_amount")
    return float(px), float(shares)


def quantize_sell(
    price: float,
    size: float,
    max_usdc: float,
    *,
    tick_size: Optional[float] = None,
) -> tuple[float, float]:
    """FAK sells: snap price to market tick and cap proceeds by max_usdc."""

    return quantize_buy(price, size, max_usdc, tick_size=tick_size)


def quantize_limit_buy(
    price: float,
    size: float,
    max_usdc: float,
    *,
    tick_size: Optional[float] = None,
    min_order: Optional[float] = None,
) -> tuple[float, float]:
    """GTC buys: fixed min shares, refuse if notional exceeds max_usdc."""

    cent = Decimal("0.01")
    tick = _tick_quantum(tick_size)
    px = Decimal(str(price)).quantize(tick, rounding=ROUND_DOWN)
    shares = Decimal(str(size)).quantize(cent, rounding=ROUND_DOWN)
    min_shares = Decimal(str(min_order)) if min_order is not None else shares
    if px <= 0:
        raise LiveOrderError("invalid_price")
    if shares <= 0:
        raise LiveOrderError("invalid_size")
    if min_shares <= 0:
        raise LiveOrderError("invalid_min_order_size")
    if shares < min_shares:
        # The exchange minimum is authoritative; do not round below it even
        # when the configured minimum has finer precision than 0.01.
        shares = min_shares
    budget = Decimal(str(max_usdc)).quantize(cent, rounding=ROUND_DOWN)
    if px * shares > budget:
        raise LiveOrderError("limit_order_exceeds_usdc")
    return float(px), float(shares)


def submit_fak_buy(
    *,
    token_id: str,
    price: float,
    size: float,
    config: Optional[dict[str, Any]] = None,
) -> Any:
    return _submit_order(
        token_id=token_id,
        price=price,
        size=size,
        side="BUY",
        order_type="FAK",
        config=config,
    )


def submit_fak_sell(
    *,
    token_id: str,
    price: float,
    size: float,
    config: Optional[dict[str, Any]] = None,
) -> Any:
    return _submit_order(
        token_id=token_id,
        price=price,
        size=size,
        side="SELL",
        order_type="FAK",
        config=config,
    )


def submit_gtc_buy(
    *,
    token_id: str,
    price: float,
    size: float,
    config: Optional[dict[str, Any]] = None,
) -> Any:
    return _submit_order(
        token_id=token_id,
        price=price,
        size=size,
        side="BUY",
        order_type="GTC",
        config=config,
    )


def _submit_fak(
    *,
    token_id: str,
    price: float,
    size: float,
    side: str,
    config: Optional[dict[str, Any]] = None,
) -> Any:
    return _submit_order(
        token_id=token_id,
        price=price,
        size=size,
        side=side,
        order_type="FAK",
        config=config,
    )


def cancel_order(
    *,
    order_id: str,
    config: Optional[dict[str, Any]] = None,
) -> Any:
    from .env import trading_config

    cfg = dict(config or trading_config())
    if not cfg.get("live_orders"):
        raise LiveOrderError("LIVE_ORDERS is not true")
    client = _secure_client(cfg)
    try:
        response = client.cancel_order(order_id=str(order_id))
    except Exception as exc:  # noqa: BLE001
        raise LiveOrderError(str(exc)) from exc
    finally:
        client.close()
    payload = jsonable(response)
    if isinstance(payload, dict) and payload.get("ok") is False:
        raise LiveOrderError(str(payload.get("error") or payload.get("error_msg") or "cancel_rejected"))
    return payload


def _secure_client(cfg: dict[str, Any]) -> Any:
    key = str(cfg.get("private_key") or "")
    if not key:
        raise LiveOrderError("PRIVATE_KEY is empty")
    try:
        from polymarket.clients.secure import SecureClient
    except ImportError as exc:
        raise LiveOrderError("polymarket SDK is not installed") from exc

    credentials = None
    if cfg.get("api_key") and cfg.get("api_secret") and cfg.get("api_passphrase"):
        from polymarket.models.clob import ApiKeyCreds

        credentials = ApiKeyCreds(
            key=str(cfg["api_key"]),
            secret=str(cfg["api_secret"]),
            passphrase=str(cfg["api_passphrase"]),
        )
    return SecureClient.create(
        private_key=key,
        wallet=str(cfg.get("funder") or "") or None,
        credentials=credentials,
    )


def _submit_order(
    *,
    token_id: str,
    price: float,
    size: float,
    side: str,
    order_type: str,
    config: Optional[dict[str, Any]] = None,
) -> Any:
    from .env import trading_config

    cfg = dict(config or trading_config())
    if not cfg.get("live_orders"):
        raise LiveOrderError("LIVE_ORDERS is not true")
    client = _secure_client(cfg)
    try:
        signed = client.create_limit_order(
            token_id=str(token_id),
            price=float(price),
            size=float(size),
            side=str(side or "BUY").upper(),
        )
        signed = replace(signed, order_type=str(order_type or "FAK").upper())
        response = client.post_order(signed)
    except Exception as exc:  # noqa: BLE001
        raise LiveOrderError(str(exc)) from exc
    finally:
        client.close()
    payload = jsonable(response)
    if isinstance(payload, dict) and payload.get("ok") is False:
        raise LiveOrderError(str(payload.get("error") or payload.get("error_msg") or "order_rejected"))
    return payload

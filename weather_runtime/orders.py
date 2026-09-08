"""Optional live FAK buys. Dry-run never imports the signer."""

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


def quantize_buy(price: float, size: float, max_usdc: float) -> tuple[float, float]:
    """FAK buys need maker USDC to 2 decimals and size to 2 decimals."""

    cent = Decimal("0.01")
    px = Decimal(str(price)).quantize(cent, rounding=ROUND_DOWN)
    if px <= 0:
        raise LiveOrderError("invalid_price")
    budget = Decimal(str(max_usdc)).quantize(cent, rounding=ROUND_DOWN)
    shares = Decimal(str(size)).quantize(cent, rounding=ROUND_DOWN)
    if px * shares > budget:
        shares = (budget / px).quantize(cent, rounding=ROUND_DOWN)
    while shares > 0:
        maker = px * shares
        if maker == maker.quantize(cent, rounding=ROUND_DOWN):
            return float(px), float(shares)
        shares -= cent
    raise LiveOrderError("cannot_quantize_buy_amount")


def quantize_sell(price: float, size: float, max_usdc: float) -> tuple[float, float]:
    """FAK sells: size to 2 decimals; cap notional proceeds by max_usdc."""

    return quantize_buy(price, size, max_usdc)


def submit_fak_buy(
    *,
    token_id: str,
    price: float,
    size: float,
    config: Optional[dict[str, Any]] = None,
) -> Any:
    return _submit_fak(
        token_id=token_id,
        price=price,
        size=size,
        side="BUY",
        config=config,
    )


def submit_fak_sell(
    *,
    token_id: str,
    price: float,
    size: float,
    config: Optional[dict[str, Any]] = None,
) -> Any:
    return _submit_fak(
        token_id=token_id,
        price=price,
        size=size,
        side="SELL",
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
    from .env import trading_config

    cfg = dict(config or trading_config())
    if not cfg.get("live_orders"):
        raise LiveOrderError("LIVE_ORDERS is not true")
    key = str(cfg.get("private_key") or "")
    if not key:
        raise LiveOrderError("PRIVATE_KEY is empty")
    try:
        from polymarket.clients.secure import SecureClient
    except ImportError as exc:
        raise LiveOrderError("polymarket SDK is not installed") from exc

    client = SecureClient.create(
        private_key=key,
        wallet=str(cfg.get("funder") or "") or None,
    )
    try:
        signed = client.create_limit_order(
            token_id=str(token_id),
            price=float(price),
            size=float(size),
            side=str(side or "BUY").upper(),
        )
        signed = replace(signed, order_type="FAK")
        response = client.post_order(signed)
    except Exception as exc:  # noqa: BLE001
        raise LiveOrderError(str(exc)) from exc
    finally:
        client.close()
    payload = jsonable(response)
    if isinstance(payload, dict) and payload.get("ok") is False:
        raise LiveOrderError(str(payload.get("error") or payload.get("error_msg") or "order_rejected"))
    return payload

"""Load gitignored .env into os.environ without a third-party dotenv package."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

from .models import as_bool, as_float


ROOT = Path(__file__).resolve().parents[1]


def load_dotenv(path: Optional[Path] = None) -> Path:
    env_path = Path(path) if path else ROOT / ".env"
    if not env_path.is_file():
        return env_path
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)
    return env_path


def _first_env(*names: str) -> str:
    for name in names:
        text = str(os.environ.get(name) or "").strip()
        if text:
            return text
    return ""


def trading_config() -> dict[str, Any]:
    live = as_bool(os.environ.get("LIVE_ORDERS"), False)
    limit_orders = as_bool(os.environ.get("LIMIT_ORDERS"), False)
    return {
        "private_key": _first_env("PRIVATE_KEY", "POLYMARKET_PRIVATE_KEY"),
        "address": _first_env("POLY_ADDRESS", "POLYMARKET_WALLET_ADDRESS"),
        "funder": _first_env("FUNDER", "POLYMARKET_FUNDER", "POLYMARKET_WALLET_ADDRESS"),
        "chain_id": int(as_float(os.environ.get("CHAIN_ID"), 137) or 137),
        "signature_type": int(as_float(os.environ.get("SIGNATURE_TYPE"), 0) or 0),
        "clob_host": _first_env("CLOB_HOST") or "https://clob.polymarket.com",
        "api_key": _first_env("POLY_API_KEY", "POLYMARKET_API_KEY"),
        "api_secret": _first_env("POLY_API_SECRET", "POLYMARKET_API_SECRET"),
        "api_passphrase": _first_env("POLY_API_PASSPHRASE", "POLYMARKET_API_PASSPHRASE"),
        "live_orders": bool(live),
        "max_order_usdc": float(as_float(os.environ.get("MAX_ORDER_USDC"), 5.0) or 5.0),
        "min_net_edge": float(as_float(os.environ.get("MIN_NET_EDGE"), 0.0075) or 0.0075),
        "max_ask": float(as_float(os.environ.get("MAX_ASK"), 0.995) or 0.995),
        "max_slippage": float(as_float(os.environ.get("MAX_SLIPPAGE"), 0.003) or 0.003),
        "limit_orders": bool(limit_orders),
        "limit_order_usdc": float(as_float(os.environ.get("LIMIT_ORDER_USDC"), 5.0) or 5.0),
        "limit_order_price": float(as_float(os.environ.get("LIMIT_ORDER_PRICE"), 0.99) or 0.99),
        "limit_order_min_price": float(as_float(os.environ.get("LIMIT_ORDER_MIN_PRICE"), 0.01) or 0.01),
        "limit_order_max_price": float(as_float(os.environ.get("LIMIT_ORDER_MAX_PRICE"), 0.99) or 0.99),
        "has_private_key": bool(_first_env("PRIVATE_KEY", "POLYMARKET_PRIVATE_KEY")),
        "has_api_creds": bool(
            _first_env("POLY_API_KEY", "POLYMARKET_API_KEY")
            and _first_env("POLY_API_SECRET", "POLYMARKET_API_SECRET")
            and _first_env("POLY_API_PASSPHRASE", "POLYMARKET_API_PASSPHRASE")
        ),
    }


def public_trading_status() -> dict[str, Any]:
    cfg = trading_config()
    return {
        "live_orders": cfg["live_orders"],
        "has_private_key": cfg["has_private_key"],
        "has_api_creds": cfg["has_api_creds"],
        "has_funder": bool(cfg["funder"]),
        "signature_type": cfg["signature_type"],
        "chain_id": cfg["chain_id"],
        "clob_host": cfg["clob_host"],
        "max_order_usdc": cfg["max_order_usdc"],
        "min_net_edge": cfg["min_net_edge"],
        "max_ask": cfg["max_ask"],
        "max_slippage": cfg["max_slippage"],
        "limit_orders": cfg["limit_orders"],
        "limit_order_usdc": cfg["limit_order_usdc"],
        "limit_order_price": cfg["limit_order_price"],
        "limit_order_min_price": cfg["limit_order_min_price"],
        "limit_order_max_price": cfg["limit_order_max_price"],
    }

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
    dynamic_exit = as_bool(os.environ.get("DYNAMIC_EXIT"), False)
    legacy_diurnal = as_bool(os.environ.get("DIURNAL_ORDERS"), False)
    yes_text = os.environ.get("DIURNAL_YES_ORDERS")
    if yes_text is None or not str(yes_text).strip():
        diurnal_yes_orders = bool(legacy_diurnal)
    else:
        diurnal_yes_orders = bool(as_bool(yes_text, False))
    diurnal_no_orders = bool(as_bool(os.environ.get("DIURNAL_NO_ORDERS"), False))
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
        "diurnal_orders": bool(diurnal_yes_orders),
        "diurnal_yes_orders": bool(diurnal_yes_orders),
        "diurnal_no_orders": bool(diurnal_no_orders),
        "diurnal_order_usdc": float(as_float(os.environ.get("DIURNAL_ORDER_USDC"), 5.0) or 5.0),
        "diurnal_min_price": float(as_float(os.environ.get("DIURNAL_MIN_PRICE"), 0.90) or 0.90),
        "diurnal_forecast_gate": bool(as_bool(os.environ.get("DIURNAL_FORECAST_GATE"), False)),
        "forecast_cache_ttl_s": float(
            as_float(os.environ.get("FORECAST_CACHE_TTL_S"), 3600.0) or 3600.0
        ),
        "sweep_tail_enabled": bool(as_bool(os.environ.get("SWEEP_TAIL_ENABLED"), False)),
        "sweep_tail_orders": bool(as_bool(os.environ.get("SWEEP_TAIL_ORDERS"), False)),
        "sweep_tail_order_usdc": float(
            as_float(os.environ.get("SWEEP_TAIL_ORDER_USDC"), 5.0) or 5.0
        ),
        "sweep_tail_leader_min": float(
            as_float(os.environ.get("SWEEP_TAIL_LEADER_MIN"), 0.99) or 0.99
        ),
        "sweep_tail_loser_max": float(
            as_float(os.environ.get("SWEEP_TAIL_LOSER_MAX"), 0.01) or 0.01
        ),
        "sweep_tail_min_legs": int(
            as_float(os.environ.get("SWEEP_TAIL_MIN_LEGS"), 3.0) or 3.0
        ),
        "max_order_usdc": float(as_float(os.environ.get("MAX_ORDER_USDC"), 5.0) or 5.0),
        "min_net_edge": float(as_float(os.environ.get("MIN_NET_EDGE"), 0.0075) or 0.0075),
        "max_ask": float(as_float(os.environ.get("MAX_ASK"), 0.995) or 0.995),
        "max_slippage": float(as_float(os.environ.get("MAX_SLIPPAGE"), 0.003) or 0.003),
        "limit_orders": bool(limit_orders),
        "limit_order_usdc": float(as_float(os.environ.get("LIMIT_ORDER_USDC"), 5.0) or 5.0),
        "limit_order_price": float(as_float(os.environ.get("LIMIT_ORDER_PRICE"), 0.99) or 0.99),
        "limit_order_price_001": float(as_float(os.environ.get("LIMIT_ORDER_PRICE_001"), 0.995) or 0.995),
        "limit_order_min_price": float(as_float(os.environ.get("LIMIT_ORDER_MIN_PRICE"), 0.01) or 0.01),
        "limit_order_max_price": float(as_float(os.environ.get("LIMIT_ORDER_MAX_PRICE"), 0.99) or 0.99),
        "dynamic_exit": bool(dynamic_exit and live),
        "exit_price_001": float(as_float(os.environ.get("EXIT_PRICE_001"), 0.999) or 0.999),
        "exit_min_size": float(as_float(os.environ.get("EXIT_MIN_SIZE"), 5.0) or 5.0),
        "exit_interval_s": float(as_float(os.environ.get("EXIT_INTERVAL_S"), 1800.0) or 1800.0),
        "exit_max_orders_per_cycle": int(
            as_float(os.environ.get("EXIT_MAX_ORDERS_PER_CYCLE"), 10.0) or 10.0
        ),
        "exit_unavailable_retry_s": float(
            as_float(os.environ.get("EXIT_UNAVAILABLE_RETRY_S"), 3600.0) or 3600.0
        ),
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
        "diurnal_orders": cfg["diurnal_yes_orders"],
        "diurnal_yes_orders": cfg["diurnal_yes_orders"],
        "diurnal_no_orders": cfg["diurnal_no_orders"],
        "diurnal_order_usdc": cfg["diurnal_order_usdc"],
        "diurnal_min_price": cfg["diurnal_min_price"],
        "diurnal_forecast_gate": cfg["diurnal_forecast_gate"],
        "forecast_cache_ttl_s": cfg["forecast_cache_ttl_s"],
        "sweep_tail_enabled": cfg["sweep_tail_enabled"],
        "sweep_tail_orders": cfg["sweep_tail_orders"],
        "sweep_tail_order_usdc": cfg["sweep_tail_order_usdc"],
        "sweep_tail_leader_min": cfg["sweep_tail_leader_min"],
        "sweep_tail_loser_max": cfg["sweep_tail_loser_max"],
        "sweep_tail_min_legs": cfg["sweep_tail_min_legs"],
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
        "limit_order_price_001": cfg["limit_order_price_001"],
        "limit_order_min_price": cfg["limit_order_min_price"],
        "limit_order_max_price": cfg["limit_order_max_price"],
        "dynamic_exit": cfg["dynamic_exit"],
        "exit_price_001": cfg["exit_price_001"],
        "exit_min_size": cfg["exit_min_size"],
        "exit_interval_s": cfg["exit_interval_s"],
        "exit_max_orders_per_cycle": cfg["exit_max_orders_per_cycle"],
        "exit_unavailable_retry_s": cfg["exit_unavailable_retry_s"],
    }

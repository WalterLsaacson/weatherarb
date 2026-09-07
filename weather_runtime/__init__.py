"""Standalone Polymarket weather finality POC.

Default is read-only. When ``LIVE_ORDERS=true``, each scan auto-submits a
FAK buy for newly locked No opportunities, capped by ``MAX_ORDER_USDC``
and de-duplicated by token id.
"""

__version__ = "0.1.0"


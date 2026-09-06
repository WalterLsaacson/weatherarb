"""Standalone Polymarket weather finality POC.

The package is intentionally read-only: it discovers markets, reads source
observations and CLOB books, and emits dry-run candidates.  It has no private
key loading or order submission path.
"""

__version__ = "0.1.0"


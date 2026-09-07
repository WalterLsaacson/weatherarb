#!/usr/bin/env python3
"""Start the weather board UI and background scanner in one process."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def main() -> int:
    sys.path.insert(0, str(ROOT))
    try:
        runpy.run_module("weather_runtime.server", run_name="__main__")
    except SystemExit as exc:
        return int(exc.code or 0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

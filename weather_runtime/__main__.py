"""Allow ``python -m weather_runtime`` to start the standalone board."""

from .server import main


if __name__ == "__main__":
    raise SystemExit(main())

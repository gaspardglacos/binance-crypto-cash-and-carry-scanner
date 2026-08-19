"""Backward-compatibility shim.

The implementation moved to the :mod:`binance_carry` package. Running this file
still works exactly like before, or use ``python -m binance_carry`` directly.
"""

from __future__ import annotations

import sys

from binance_carry.__main__ import main


if __name__ == "__main__":
    sys.exit(main())

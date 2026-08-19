"""CLI entry point for the Binance cash-and-carry scanner.

Usage:
    python -m binance_carry [options]
    python multicac.py [options]   # legacy shim, equivalent

The default alert webhook is opt-in via the CARRY_ALERT_WEBHOOK_URL environment
variable. With no env var set, alerts are silent.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone

from .dashboard import dashboard_loop
from .scanner import (
    account_risk_loop,
    benchmark_loop,
    scanner_supervisor_loop,
    send_alert,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="binance_carry",
        description="Real-time Binance USDT cash-and-carry arbitrage scanner.",
    )

    parser.add_argument(
        "--asset",
        action="append",
        help=(
            "Manual asset as NAME,SPOT_SYMBOL,FUTURES_SYMBOL,MIN_QTY,MAX_QTY,QTY_STEP,MIN_PROFIT_USDT. "
            "Only used when --no-auto-roll is set."
        ),
    )

    parser.add_argument(
        "--base-pair",
        action="append",
        help="Base pair for auto-roll discovery, e.g. BTCUSDT or ETHUSDT. Can be repeated.",
    )

    parser.add_argument("--no-auto-roll", action="store_true", help="Disable dynamic Binance contract discovery")
    parser.add_argument("--contract-refresh-seconds", type=float, default=300)

    parser.add_argument("--levels", type=int, default=20, choices=[5, 10, 20])
    parser.add_argument("--speed-ms", type=int, default=100, choices=[100, 500, 1000])

    # Binance base-tier taker-fee assumptions.
    # Spot taker: 0.10% = 0.001
    # USDT-M futures taker: 0.05% = 0.0005
    parser.add_argument("--spot-fee-rate", type=float, default=0.001)
    parser.add_argument("--futures-fee-rate", type=float, default=0.0005)

    parser.add_argument("--delivery-fee-rate", type=float, default=0.0)
    parser.add_argument("--annual-capital-cost-rate", type=float, default=0.0)

    parser.add_argument("--fallback-benchmark-yield", type=float, default=0.045)
    parser.add_argument("--benchmark-poll-seconds", type=float, default=300)
    parser.add_argument("--max-benchmark-age-seconds", type=float, default=3600)

    parser.add_argument("--max-book-age-ms", type=float, default=500)
    parser.add_argument("--dashboard-refresh", type=float, default=0.5)

    parser.add_argument("--entry-cutoff-minutes-before-expiry", type=float, default=10)

    parser.add_argument("--no-log", action="store_true", help="Disable JSONL dashboard logging")
    parser.add_argument("--json-log-file", default="carry_dashboard_log.jsonl")
    parser.add_argument("--max-log-mb", type=float, default=50.0)

    parser.add_argument("--state-file", default="carry_state.json")
    parser.add_argument("--health-file", default="carry_health.json")

    parser.add_argument("--alert-webhook-url", default=os.getenv("CARRY_ALERT_WEBHOOK_URL"))
    parser.add_argument("--alert-cooldown-seconds", type=float, default=300)
    parser.add_argument("--no-startup-alert", action="store_true", help="Disable startup online alert")

    parser.add_argument("--enable-account-risk", action="store_true")
    parser.add_argument("--binance-api-key", default=os.getenv("BINANCE_API_KEY"))
    parser.add_argument("--binance-api-secret", default=os.getenv("BINANCE_API_SECRET"))
    parser.add_argument("--risk-margin-buffer-multiplier", type=float, default=3.0)
    parser.add_argument("--assumed-leverage", type=float, default=1.0)
    parser.add_argument("--max-account-age-seconds", type=float, default=120)

    return parser


async def main_async(args: argparse.Namespace) -> None:
    if not args.no_startup_alert:
        base_pairs = args.base_pair or ["BTCUSDT", "ETHUSDT"]

        startup_message = (
            f"CAC bot online | "
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} | "
            f"Auto-roll {'off' if args.no_auto_roll else 'on'} | "
            f"Watching {', '.join(base_pairs)} | "
            f"Spot taker {args.spot_fee_rate * 100:.4f}% | "
            f"Futures taker {args.futures_fee_rate * 100:.4f}%"
        )

        await send_alert(
            args,
            startup_message,
            title="CAC bot online",
            priority="default",
        )

    benchmark_state = {
        "yield": args.fallback_benchmark_yield,
        "source": "fallback",
        "name": "fallback",
        "tenor_days": 91,
        "last_update": None,
        "is_live": False,
        "error": None,
    }

    account_state = {
        "enabled": args.enable_account_risk,
        "futures_available_balance_usdt": None,
        "futures_wallet_balance_usdt": None,
        "spot_usdt_free": None,
        "last_update": None,
        "error": None,
    }

    shared_state = {
        "assets": [],
        "asset_states": {},
        "shared_spot_states": {},
        "contract_key": None,
        "last_contract_update": None,
        "roll_error": None,
    }

    await asyncio.gather(
        benchmark_loop(args, benchmark_state),
        account_risk_loop(args, account_state),
        scanner_supervisor_loop(args, shared_state, benchmark_state, account_state),
        dashboard_loop(args, shared_state, benchmark_state, account_state),
    )


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())

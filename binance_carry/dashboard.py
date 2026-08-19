"""Terminal dashboard loop for the cash-and-carry scanner.

Renders a multi-column ANSI-cleared terminal view, persists health + state to
disk on every refresh, and writes a rolling JSONL audit log. Imports the pure
formatting helpers from :mod:`binance_carry.core`.
"""

from __future__ import annotations

import asyncio
import shutil
from datetime import datetime, timezone
from itertools import zip_longest
from typing import Any

from .core import (
    append_dashboard_log,
    compute_health,
    fit,
    format_panel,
    save_health,
    save_state,
)


async def dashboard_loop(
    args: Any,
    shared_state: dict[str, Any],
    benchmark_state: dict[str, Any],
    account_state: dict[str, Any],
) -> None:
    while True:
        assets = shared_state.get("assets", [])
        asset_states = shared_state.get("asset_states", {})

        terminal_width = shutil.get_terminal_size((160, 50)).columns
        gap = "  │  "
        columns = 2
        panel_width = max(50, (terminal_width - len(gap)) // columns)

        now_dt = datetime.now(timezone.utc)
        now = now_dt.strftime("%Y-%m-%d %H:%M:%S UTC")

        health = compute_health(args, shared_state, benchmark_state, account_state)
        save_health(args, health)
        save_state(args, shared_state, benchmark_state, account_state)

        health_status = (
            "OK"
            if health["benchmark_ok"] and health["roll_ok"] and health["account_ok"]
            else "WARN"
        )

        tbill = benchmark_state["yield"] * 100
        tbill_source = benchmark_state.get("source", "fallback")
        tbill_status = "live" if benchmark_state.get("is_live") else "fallback"

        output_lines: list[str] = []

        output_lines.append("=" * min(terminal_width, 160))
        output_lines.append(
            f"Auto-roll USDT cash-and-carry scanner | {now} | "
            f"T-bill {tbill:.3f}% {tbill_source} {tbill_status}"
        )
        output_lines.append(
            f"Spot taker {args.spot_fee_rate * 100:.4f}% | "
            f"Futures taker {args.futures_fee_rate * 100:.4f}% | "
            f"Spot exit fee included via live spot bid VWAP"
        )
        output_lines.append(
            f"Auto-roll: {'on' if not args.no_auto_roll else 'off'} | "
            f"Contract refresh {args.contract_refresh_seconds:.0f}s | "
            f"Last contract update: {shared_state.get('last_contract_update', 'n/a')}"
        )
        output_lines.append(
            f"Health: {health_status} | "
            f"State: {args.state_file} | "
            f"Health file: {args.health_file} | "
            f"Log: {'off' if args.no_log else args.json_log_file}"
        )

        if shared_state.get("roll_error"):
            output_lines.append(f"Roll discovery error: {shared_state['roll_error']}")

        if benchmark_state.get("error"):
            output_lines.append(f"Benchmark error: {benchmark_state['error']}")

        if account_state.get("error") and args.enable_account_risk:
            output_lines.append(f"Account risk error: {account_state['error']}")

        output_lines.append("=" * min(terminal_width, 160))

        if not assets:
            output_lines.append("Waiting for contract discovery...")
        else:
            panels = [
                format_panel(asset, asset_states[asset["name"]])
                for asset in assets
                if asset["name"] in asset_states
            ]

            for row_start in range(0, len(panels), 2):
                left = panels[row_start]
                right = panels[row_start + 1] if row_start + 1 < len(panels) else []

                for left_line, right_line in zip_longest(left, right, fillvalue=""):
                    output_lines.append(
                        fit(left_line, panel_width)
                        + gap
                        + fit(right_line, panel_width)
                    )

                if row_start + 2 < len(panels):
                    output_lines.append("")
                    output_lines.append("-" * min(terminal_width, 160))
                    output_lines.append("")

        rendered_text = "\n".join(output_lines)

        print("\033[2J\033[H", end="")
        print(rendered_text)

        if not args.no_log:
            log_record = {
                "timestamp": now_dt.isoformat(),
                "rendered_text": rendered_text,
            }

            append_dashboard_log(
                args.json_log_file,
                log_record,
                args.max_log_mb,
            )

        await asyncio.sleep(args.dashboard_refresh)

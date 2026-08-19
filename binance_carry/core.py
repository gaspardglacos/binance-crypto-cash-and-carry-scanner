"""Pure functions for the cash-and-carry scanner.

No I/O happens in this module: no asyncio, no network, no filesystem writes
beyond ``atomic_write_json`` and the JSONL log appender (which are file-local
helpers used by the monitor). Everything here is deterministic given its inputs
and is safe to unit-test in isolation.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from itertools import zip_longest
from pathlib import Path
from typing import Any


# ---- Constants ---------------------------------------------------------------

YFINANCE_BENCHMARKS: list[dict[str, Any]] = [
    {
        "symbol": "^IRX",
        "tenor_days": 91,
        "name": "13-week Treasury Bill",
    },
]


BASE_PAIR_CONFIGS: dict[str, dict[str, Any]] = {
    "BTCUSDT": {
        "base_asset": "BTC",
        "min_qty": 0.001,
        "max_qty": 0.10,
        "qty_step": 0.001,
        "min_profit_usdt": 0.50,
    },
    "ETHUSDT": {
        "base_asset": "ETH",
        "min_qty": 0.001,
        "max_qty": 1.00,
        "qty_step": 0.001,
        "min_profit_usdt": 0.50,
    },
}


FALLBACK_STATIC_ASSETS: list[dict[str, Any]] = [
    {
        "name": "BTC CUR",
        "spot_symbol": "BTCUSDT",
        "futures_symbol": "BTCUSDT_260925",
        "min_qty": 0.001,
        "max_qty": 0.10,
        "qty_step": 0.001,
        "min_profit_usdt": 0.50,
    },
    {
        "name": "BTC NEXT",
        "spot_symbol": "BTCUSDT",
        "futures_symbol": "BTCUSDT_261225",
        "min_qty": 0.001,
        "max_qty": 0.10,
        "qty_step": 0.001,
        "min_profit_usdt": 0.50,
    },
    {
        "name": "ETH CUR",
        "spot_symbol": "ETHUSDT",
        "futures_symbol": "ETHUSDT_260925",
        "min_qty": 0.001,
        "max_qty": 1.00,
        "qty_step": 0.001,
        "min_profit_usdt": 0.50,
    },
    {
        "name": "ETH NEXT",
        "spot_symbol": "ETHUSDT",
        "futures_symbol": "ETHUSDT_261225",
        "min_qty": 0.001,
        "max_qty": 1.00,
        "qty_step": 0.001,
        "min_profit_usdt": 0.50,
    },
]


# ---- Time / symbol helpers ---------------------------------------------------

def parse_expiry_from_symbol(symbol: str) -> datetime:
    date_part = symbol.split("_")[-1]

    if len(date_part) != 6 or not date_part.isdigit():
        raise ValueError(f"Cannot parse expiry from symbol: {symbol}")

    year = 2000 + int(date_part[0:2])
    month = int(date_part[2:4])
    day = int(date_part[4:6])

    return datetime(year, month, day, 8, 0, 0, tzinfo=timezone.utc)


def expiry_from_delivery_ms(delivery_ms: int) -> datetime:
    return datetime.fromtimestamp(delivery_ms / 1000, tz=timezone.utc)


def days_to_expiry(expiry: datetime) -> float:
    now = datetime.now(timezone.utc)
    seconds = (expiry - now).total_seconds()
    return max(seconds / 86400, 1e-9)


def infer_base_asset(spot_symbol: str) -> str:
    if spot_symbol.endswith("USDT"):
        return spot_symbol[:-4]
    return spot_symbol


def contract_label(index: int, futures_symbol: str) -> str:
    suffix = futures_symbol.split("_")[-1] if "_" in futures_symbol else futures_symbol

    if index == 0:
        return f"CUR {suffix}"

    if index == 1:
        return f"NEXT {suffix}"

    return suffix


# ---- JSON / file helpers -----------------------------------------------------

def make_json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, dict):
        return {k: make_json_safe(v) for k, v in value.items()}

    if isinstance(value, list):
        return [make_json_safe(v) for v in value]

    return value


def atomic_write_json(path: str | os.PathLike, data: Any) -> None:
    path = Path(path)
    tmp_path = Path(f"{path}.{os.getpid()}.tmp")

    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(make_json_safe(data), f, ensure_ascii=False, indent=2)

        for attempt in range(5):
            try:
                os.replace(tmp_path, path)
                return
            except PermissionError:
                time.sleep(0.1 * (attempt + 1))

        with open(path, "w", encoding="utf-8") as f:
            json.dump(make_json_safe(data), f, ensure_ascii=False, indent=2)

    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass


def reset_log_if_needed(path: str | os.PathLike, max_mb: float) -> None:
    path = Path(path)

    if not path.exists():
        return

    max_bytes = int(max_mb * 1024 * 1024)

    if path.stat().st_size >= max_bytes:
        path.unlink(missing_ok=True)


def append_dashboard_log(path: str | os.PathLike, record: Any, max_mb: float) -> None:
    reset_log_if_needed(path, max_mb)

    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def benchmark_age_seconds(benchmark_state: dict[str, Any]) -> float | None:
    last_update = benchmark_state.get("last_update")

    if not last_update:
        return None

    try:
        dt = datetime.fromisoformat(last_update)
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        return None


def iso_age_seconds(iso_timestamp: str | None) -> float | None:
    if not iso_timestamp:
        return None

    try:
        dt = datetime.fromisoformat(iso_timestamp)
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        return None


# ---- Order book walking ------------------------------------------------------

def walk_asks(asks: list, target_qty: float) -> dict[str, float] | None:
    remaining = target_qty
    total_cost_usdt = 0.0
    filled_qty = 0.0
    levels_used = 0

    for price_str, qty_str in asks:
        price_usdt = float(price_str)
        qty = float(qty_str)

        take_qty = min(remaining, qty)

        total_cost_usdt += take_qty * price_usdt
        filled_qty += take_qty
        remaining -= take_qty
        levels_used += 1

        if remaining <= 1e-12:
            break

    if filled_qty + 1e-12 < target_qty:
        return None

    return {
        "vwap_usdt": total_cost_usdt / filled_qty,
        "notional_usdt": total_cost_usdt,
        "filled_qty": filled_qty,
        "levels_used": levels_used,
    }


def walk_bids(bids: list, target_qty: float) -> dict[str, float] | None:
    remaining = target_qty
    total_proceeds_usdt = 0.0
    filled_qty = 0.0
    levels_used = 0

    for price_str, qty_str in bids:
        price_usdt = float(price_str)
        qty = float(qty_str)

        take_qty = min(remaining, qty)

        total_proceeds_usdt += take_qty * price_usdt
        filled_qty += take_qty
        remaining -= take_qty
        levels_used += 1

        if remaining <= 1e-12:
            break

    if filled_qty + 1e-12 < target_qty:
        return None

    return {
        "vwap_usdt": total_proceeds_usdt / filled_qty,
        "notional_usdt": total_proceeds_usdt,
        "filled_qty": filled_qty,
        "levels_used": levels_used,
    }


# ---- Signal math -------------------------------------------------------------

def compute_result(
    asset: dict[str, Any],
    spot_book: dict[str, Any],
    futures_book: dict[str, Any],
    target_qty: float,
    spot_fee_rate: float,
    futures_fee_rate: float,
    delivery_fee_rate: float,
    annual_capital_cost_rate: float,
    benchmark_state: dict[str, Any],
    account_state: dict[str, Any],
    args: Any,
) -> dict[str, Any] | None:
    if spot_book["asks"] is None or spot_book["bids"] is None or futures_book["bids"] is None:
        return None

    expiry = asset["expiry"]
    now = datetime.now(timezone.utc)
    dte = days_to_expiry(expiry)

    spot_buy = walk_asks(spot_book["asks"], target_qty)
    futures_short = walk_bids(futures_book["bids"], target_qty)
    spot_exit_sell = walk_bids(spot_book["bids"], target_qty)

    if spot_buy is None or futures_short is None or spot_exit_sell is None:
        return None

    gross_basis_usdt = futures_short["notional_usdt"] - spot_buy["notional_usdt"]

    spot_entry_fee_usdt = spot_buy["notional_usdt"] * spot_fee_rate
    futures_entry_fee_usdt = futures_short["notional_usdt"] * futures_fee_rate
    spot_exit_fee_usdt = spot_exit_sell["notional_usdt"] * spot_fee_rate
    delivery_fee_usdt = futures_short["notional_usdt"] * delivery_fee_rate

    capital_cost_usdt = (
        spot_buy["notional_usdt"]
        * annual_capital_cost_rate
        * dte
        / 365.0
    )

    coc_usdt = (
        spot_entry_fee_usdt
        + futures_entry_fee_usdt
        + spot_exit_fee_usdt
        + delivery_fee_usdt
        + capital_cost_usdt
    )

    profit_usdt = gross_basis_usdt - coc_usdt

    net_return = profit_usdt / spot_buy["notional_usdt"]
    annualized_net_return = net_return * 365.0 / dte

    tbill_yield = benchmark_state["yield"]

    bench_age = benchmark_age_seconds(benchmark_state)

    benchmark_fresh = (
        benchmark_state.get("is_live") is True
        and bench_age is not None
        and bench_age <= args.max_benchmark_age_seconds
    )

    close_start = expiry - timedelta(minutes=5)
    close_end = expiry - timedelta(minutes=1)

    entry_cutoff = expiry - timedelta(minutes=args.entry_cutoff_minutes_before_expiry)
    entry_allowed = now < entry_cutoff
    expired = now >= expiry

    if expired:
        expiry_phase = "expired"
    elif not entry_allowed:
        expiry_phase = "close-window"
    else:
        expiry_phase = "normal"

    estimated_initial_margin_usdt = futures_short["notional_usdt"] / max(args.assumed_leverage, 1e-9)
    required_margin_buffer_usdt = estimated_initial_margin_usdt * args.risk_margin_buffer_multiplier
    spot_required_usdt = spot_buy["notional_usdt"] + spot_entry_fee_usdt

    account_age = iso_age_seconds(account_state.get("last_update"))

    if not args.enable_account_risk:
        account_risk_ok = True
        account_risk_reason = "disabled"
    elif account_state.get("error"):
        account_risk_ok = False
        account_risk_reason = account_state.get("error")
    elif account_age is None or account_age > args.max_account_age_seconds:
        account_risk_ok = False
        account_risk_reason = "account data stale"
    else:
        futures_available = float(account_state.get("futures_available_balance_usdt", 0.0))
        spot_usdt_free = float(account_state.get("spot_usdt_free", 0.0))

        account_risk_ok = (
            futures_available >= required_margin_buffer_usdt
            and spot_usdt_free >= spot_required_usdt
        )

        account_risk_reason = (
            "ok"
            if account_risk_ok
            else "insufficient spot USDT or futures margin buffer"
        )

    signal = (
        annualized_net_return > tbill_yield
        and profit_usdt >= asset["min_profit_usdt"]
        and profit_usdt > 0
        and benchmark_fresh
        and entry_allowed
        and not expired
        and account_risk_ok
    )

    return {
        "time": now,
        "asset_name": asset["name"],
        "base_asset": asset["base_asset"],
        "spot_symbol": asset["spot_symbol"],
        "futures_symbol": asset["futures_symbol"],
        "expiry": expiry,
        "manual_close_start": close_start,
        "manual_close_end": close_end,
        "days_to_expiry": dte,
        "target_qty": target_qty,

        "spot_vwap_ask_usdt": spot_buy["vwap_usdt"],
        "futures_vwap_bid_usdt": futures_short["vwap_usdt"],
        "spot_exit_vwap_bid_usdt": spot_exit_sell["vwap_usdt"],

        "spot_levels_used": spot_buy["levels_used"],
        "futures_levels_used": futures_short["levels_used"],
        "spot_exit_levels_used": spot_exit_sell["levels_used"],

        "gross_basis_usdt": gross_basis_usdt,
        "coc_usdt": coc_usdt,
        "profit_usdt": profit_usdt,
        "annualized_net_return": annualized_net_return,

        "tbill_yield": tbill_yield,
        "benchmark_source": benchmark_state.get("source"),
        "benchmark_name": benchmark_state.get("name"),
        "benchmark_is_live": benchmark_state.get("is_live"),
        "benchmark_fresh": benchmark_fresh,
        "benchmark_age_seconds": bench_age,

        "entry_allowed": entry_allowed,
        "expiry_phase": expiry_phase,
        "expired": expired,

        "estimated_initial_margin_usdt": estimated_initial_margin_usdt,
        "required_margin_buffer_usdt": required_margin_buffer_usdt,
        "spot_required_usdt": spot_required_usdt,
        "account_risk_ok": account_risk_ok,
        "account_risk_reason": account_risk_reason,

        "signal": signal,
    }


def generate_candidate_sizes(min_qty: float, max_qty: float, qty_step: float) -> list[float]:
    sizes: list[float] = []
    n = 0

    while True:
        qty = min_qty + n * qty_step
        if qty > max_qty + 1e-12:
            break

        sizes.append(round(qty, 12))
        n += 1

    return sizes


def find_best_size(
    asset: dict[str, Any],
    spot_book: dict[str, Any],
    futures_book: dict[str, Any],
    spot_fee_rate: float,
    futures_fee_rate: float,
    delivery_fee_rate: float,
    annual_capital_cost_rate: float,
    benchmark_state: dict[str, Any],
    account_state: dict[str, Any],
    args: Any,
) -> dict[str, Any] | None:
    best_overall: dict[str, Any] | None = None
    best_signal: dict[str, Any] | None = None

    candidates_checked = 0
    candidates_with_depth = 0
    signal_candidates = 0

    for candidate_qty in asset["candidate_sizes"]:
        candidates_checked += 1

        result = compute_result(
            asset=asset,
            spot_book=spot_book,
            futures_book=futures_book,
            target_qty=candidate_qty,
            spot_fee_rate=spot_fee_rate,
            futures_fee_rate=futures_fee_rate,
            delivery_fee_rate=delivery_fee_rate,
            annual_capital_cost_rate=annual_capital_cost_rate,
            benchmark_state=benchmark_state,
            account_state=account_state,
            args=args,
        )

        if result is None:
            continue

        candidates_with_depth += 1

        if (
            best_overall is None
            or result["annualized_net_return"] > best_overall["annualized_net_return"]
        ):
            best_overall = result

        if result["signal"]:
            signal_candidates += 1

            if (
                best_signal is None
                or result["annualized_net_return"] > best_signal["annualized_net_return"]
            ):
                best_signal = result

    chosen = best_signal if best_signal is not None else best_overall

    if chosen is not None:
        chosen["auto_size"] = True
        chosen["candidates_checked"] = candidates_checked
        chosen["candidates_with_depth"] = candidates_with_depth
        chosen["signal_candidates"] = signal_candidates
        chosen["selected_reason"] = (
            "best signal by annualized return"
            if best_signal is not None
            else "best available by annualized return, no valid signal"
        )

    return chosen


# ---- Asset preparation -------------------------------------------------------

def parse_asset_spec(spec: str) -> dict[str, Any]:
    parts = [p.strip() for p in spec.split(",")]

    if len(parts) != 7:
        raise ValueError(
            "Asset spec must be: NAME,SPOT_SYMBOL,FUTURES_SYMBOL,MIN_QTY,MAX_QTY,QTY_STEP,MIN_PROFIT_USDT"
        )

    name, spot_symbol, futures_symbol, min_qty, max_qty, qty_step, min_profit = parts

    return {
        "name": name,
        "spot_symbol": spot_symbol,
        "futures_symbol": futures_symbol,
        "min_qty": float(min_qty),
        "max_qty": float(max_qty),
        "qty_step": float(qty_step),
        "min_profit_usdt": float(min_profit),
    }


def prepare_manual_assets(args: Any) -> list[dict[str, Any]]:
    if args.asset:
        raw_assets = [parse_asset_spec(spec) for spec in args.asset]
    else:
        raw_assets = FALLBACK_STATIC_ASSETS

    prepared: list[dict[str, Any]] = []

    for asset in raw_assets:
        asset = dict(asset)
        asset["base_asset"] = infer_base_asset(asset["spot_symbol"])

        if "expiry" not in asset:
            asset["expiry"] = parse_expiry_from_symbol(asset["futures_symbol"])

        asset["candidate_sizes"] = generate_candidate_sizes(
            asset["min_qty"],
            asset["max_qty"],
            asset["qty_step"],
        )

        prepared.append(asset)

    return prepared


def prepare_discovered_assets(raw_assets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []

    for asset in raw_assets:
        asset = dict(asset)

        if "base_asset" not in asset:
            asset["base_asset"] = infer_base_asset(asset["spot_symbol"])

        if "expiry" not in asset:
            asset["expiry"] = parse_expiry_from_symbol(asset["futures_symbol"])

        asset["candidate_sizes"] = generate_candidate_sizes(
            asset["min_qty"],
            asset["max_qty"],
            asset["qty_step"],
        )

        prepared.append(asset)

    return prepared


def asset_contract_key(assets: list[dict[str, Any]]) -> tuple:
    return tuple(
        (asset["name"], asset["spot_symbol"], asset["futures_symbol"], asset["expiry"].isoformat())
        for asset in assets
    )


# ---- Health / state persistence (monitor-side, but pure wrt the network) -----

def compute_health(
    args: Any,
    shared_state: dict[str, Any],
    benchmark_state: dict[str, Any],
    account_state: dict[str, Any],
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)

    bench_age = benchmark_age_seconds(benchmark_state)

    benchmark_ok = (
        benchmark_state.get("is_live") is True
        and bench_age is not None
        and bench_age <= args.max_benchmark_age_seconds
    )

    account_age = iso_age_seconds(account_state.get("last_update"))

    if not args.enable_account_risk:
        account_ok = True
    else:
        account_ok = (
            account_state.get("error") is None
            and account_age is not None
            and account_age <= args.max_account_age_seconds
        )

    assets_health: dict[str, Any] = {}

    for asset in shared_state.get("assets", []):
        state = shared_state.get("asset_states", {}).get(asset["name"], {})

        spot_age = state.get("spot_age_ms")
        futures_age = state.get("futures_age_ms")

        spot_ok = spot_age is not None and spot_age <= args.max_book_age_ms
        futures_ok = futures_age is not None and futures_age <= args.max_book_age_ms

        assets_health[asset["name"]] = {
            "spot_ok": spot_ok,
            "futures_ok": futures_ok,
            "spot_age_ms": spot_age,
            "futures_age_ms": futures_age,
            "spot_error": state.get("spot_error"),
            "futures_error": state.get("futures_error"),
            "latest_result_time": state.get("latest_result_time"),
        }

    return {
        "timestamp": now.isoformat(),
        "benchmark_ok": benchmark_ok,
        "benchmark_age_seconds": bench_age,
        "roll_ok": shared_state.get("roll_error") is None,
        "roll_error": shared_state.get("roll_error"),
        "account_ok": account_ok,
        "account_error": account_state.get("error"),
        "account_age_seconds": account_age,
        "assets": assets_health,
    }


def save_health(args: Any, health: dict[str, Any]) -> None:
    atomic_write_json(args.health_file, health)


def save_state(
    args: Any,
    shared_state: dict[str, Any],
    benchmark_state: dict[str, Any],
    account_state: dict[str, Any],
) -> None:
    state = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "benchmark": {
            "yield": benchmark_state.get("yield"),
            "source": benchmark_state.get("source"),
            "is_live": benchmark_state.get("is_live"),
            "last_update": benchmark_state.get("last_update"),
            "error": benchmark_state.get("error"),
        },
        "account": {
            "enabled": account_state.get("enabled"),
            "futures_available_balance_usdt": account_state.get("futures_available_balance_usdt"),
            "futures_wallet_balance_usdt": account_state.get("futures_wallet_balance_usdt"),
            "spot_usdt_free": account_state.get("spot_usdt_free"),
            "last_update": account_state.get("last_update"),
            "error": account_state.get("error"),
        },
        "contracts": [
            {
                "name": asset["name"],
                "spot_symbol": asset["spot_symbol"],
                "futures_symbol": asset["futures_symbol"],
                "expiry": asset["expiry"],
                "min_qty": asset["min_qty"],
                "max_qty": asset["max_qty"],
                "qty_step": asset["qty_step"],
                "min_profit_usdt": asset["min_profit_usdt"],
            }
            for asset in shared_state.get("assets", [])
        ],
        "last_contract_update": shared_state.get("last_contract_update"),
        "roll_error": shared_state.get("roll_error"),
    }

    atomic_write_json(args.state_file, state)


# ---- Dashboard formatting ----------------------------------------------------

def fit(text: str, width: int) -> str:
    text = str(text)
    if len(text) <= width:
        return text.ljust(width)
    if width <= 1:
        return text[:width]
    return text[: width - 1] + "…"


def fmt_age(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.0f}"


def format_panel(asset: dict[str, Any], asset_state: dict[str, Any]) -> list[str]:
    result = asset_state.get("latest_result")
    spot_error = asset_state.get("spot_error")
    futures_error = asset_state.get("futures_error")

    lines: list[str] = []
    lines.append(f"{asset['name']}  {asset['spot_symbol']} / {asset['futures_symbol']}")
    lines.append("-" * 54)

    if spot_error:
        lines.append(f"Spot WS error: {spot_error[:38]}")
    if futures_error:
        lines.append(f"Futures WS error: {futures_error[:35]}")

    if result is None:
        lines.append("Status: waiting for books / depth")
        lines.append(f"Expiry: {asset['expiry'].strftime('%Y-%m-%d %H:%M:%S UTC')}")
        lines.append(f"DTE: {days_to_expiry(asset['expiry']):.2f}d")
        lines.append(f"Qty range: {asset['min_qty']} → {asset['max_qty']} {asset['base_asset']}")
        lines.append(f"Step: {asset['qty_step']} {asset['base_asset']}")
        lines.append(f"Min profit: {asset['min_profit_usdt']:.4f} USDT")
        return lines

    signal_text = "TRADE OPPORTUNITY" if result["signal"] else "No trade"
    benchmark_status = "live" if result["benchmark_is_live"] else "fallback"

    lines.append(f"Signal: {signal_text}")
    lines.append(f"Phase: {result['expiry_phase']}")
    lines.append(f"Entry allowed: {result['entry_allowed']}")
    lines.append(f"Time: {result['time'].strftime('%H:%M:%S UTC')}")
    lines.append(f"Expiry: {result['expiry'].strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append(
        f"Close: {result['manual_close_start'].strftime('%H:%M')}→"
        f"{result['manual_close_end'].strftime('%H:%M')} UTC"
    )
    lines.append(f"DTE: {result['days_to_expiry']:.2f}d")
    lines.append("")
    lines.append(f"Best size: {result['target_qty']:.6f} {result['base_asset']}")
    lines.append(f"Candidates: {result['candidates_with_depth']}/{result['candidates_checked']}")
    lines.append(f"Signal candidates: {result['signal_candidates']}")
    lines.append("")
    lines.append(f"Spot entry ask VWAP:   {result['spot_vwap_ask_usdt']:,.2f} USDT")
    lines.append(f"Future short bid VWAP: {result['futures_vwap_bid_usdt']:,.2f} USDT")
    lines.append(f"Spot exit bid VWAP:    {result['spot_exit_vwap_bid_usdt']:,.2f} USDT")
    lines.append(
        f"Levels entry/fut/exit: {result['spot_levels_used']}/"
        f"{result['futures_levels_used']}/{result['spot_exit_levels_used']}"
    )
    lines.append("")
    lines.append(f"Gross basis: {result['gross_basis_usdt']:,.4f} USDT")
    lines.append(f"COC:         {result['coc_usdt']:,.4f} USDT")
    lines.append(f"Profit:      {result['profit_usdt']:,.4f} USDT")
    lines.append(f"Ann net:     {result['annualized_net_return'] * 100:.3f}%")
    lines.append(f"T-bill:      {result['tbill_yield'] * 100:.3f}% [{benchmark_status}]")
    lines.append("")
    lines.append(f"Benchmark fresh: {result['benchmark_fresh']}")
    lines.append(f"Risk check:      {result['account_risk_ok']} [{result['account_risk_reason']}]")
    lines.append(f"Req margin buf:  {result['required_margin_buffer_usdt']:,.2f} USDT")
    lines.append(
        f"Book age spot/fut: {fmt_age(asset_state.get('spot_age_ms'))}/"
        f"{fmt_age(asset_state.get('futures_age_ms'))} ms"
    )
    lines.append(f"Reason: {result['selected_reason']}")

    if result["expiry_phase"] == "close-window":
        lines.append("Action: close/settle workflow, no new entries")
    elif result["expiry_phase"] == "expired":
        lines.append("Action: expired, waiting for auto-roll")

    return lines

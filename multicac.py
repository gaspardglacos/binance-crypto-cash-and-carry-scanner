import argparse
import asyncio
import hashlib
import hmac
import json
import os
import shutil
import time
import urllib.parse
from datetime import datetime, timezone, timedelta
from itertools import zip_longest
from pathlib import Path

import aiohttp
import websockets
import yfinance as yf


SPOT_WS_BASE = "wss://stream.binance.com:9443/ws"
FUTURES_WS_BASE = "wss://fstream.binance.com/ws"

SPOT_REST_BASE = "https://api.binance.com"
FUTURES_REST_BASE = "https://fapi.binance.com"

# Replace this with your ntfy topic if you want alerts enabled by default.
# Example: "https://ntfy.sh/gaspard-cac-alerts-9f3k2"
DEFAULT_ALERT_WEBHOOK_URL = os.getenv(
    "CARRY_ALERT_WEBHOOK_URL",
    "https://ntfy.sh/gaspard-cac-alerts-keolis-588369"
)

YFINANCE_BENCHMARKS = [
    {
        "symbol": "^IRX",
        "tenor_days": 91,
        "name": "13-week Treasury Bill",
    },
]


BASE_PAIR_CONFIGS = {
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


FALLBACK_STATIC_ASSETS = [
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


def make_json_safe(value):
    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, dict):
        return {k: make_json_safe(v) for k, v in value.items()}

    if isinstance(value, list):
        return [make_json_safe(v) for v in value]

    return value


def atomic_write_json(path, data):
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


def reset_log_if_needed(path, max_mb):
    path = Path(path)

    if not path.exists():
        return

    max_bytes = int(max_mb * 1024 * 1024)

    if path.stat().st_size >= max_bytes:
        path.unlink(missing_ok=True)


def append_dashboard_log(path, record, max_mb):
    reset_log_if_needed(path, max_mb)

    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def benchmark_age_seconds(benchmark_state):
    last_update = benchmark_state.get("last_update")

    if not last_update:
        return None

    try:
        dt = datetime.fromisoformat(last_update)
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        return None


def iso_age_seconds(iso_timestamp):
    if not iso_timestamp:
        return None

    try:
        dt = datetime.fromisoformat(iso_timestamp)
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        return None


async def fetch_usdtm_delivery_contracts(base_pairs):
    url = f"{FUTURES_REST_BASE}/fapi/v1/exchangeInfo"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=10) as response:
            response.raise_for_status()
            data = await response.json()

    now_ms = int(time.time() * 1000)
    contracts_by_pair = {pair: [] for pair in base_pairs}

    for symbol_info in data.get("symbols", []):
        pair = symbol_info.get("pair")
        symbol = symbol_info.get("symbol")
        contract_type = symbol_info.get("contractType")
        status = symbol_info.get("status")
        delivery_date = symbol_info.get("deliveryDate")
        quote_asset = symbol_info.get("quoteAsset")
        margin_asset = symbol_info.get("marginAsset")

        if pair not in contracts_by_pair:
            continue

        if status != "TRADING":
            continue

        if contract_type == "PERPETUAL":
            continue

        if "_" not in symbol:
            continue

        if not symbol.startswith(pair + "_"):
            continue

        if quote_asset != "USDT" or margin_asset != "USDT":
            continue

        if delivery_date is None or int(delivery_date) <= now_ms:
            continue

        contracts_by_pair[pair].append(
            {
                "symbol": symbol,
                "pair": pair,
                "contract_type": contract_type,
                "delivery_date": int(delivery_date),
            }
        )

    for pair in contracts_by_pair:
        contracts_by_pair[pair].sort(key=lambda item: item["delivery_date"])

    return contracts_by_pair


def build_assets_from_contracts(contracts_by_pair):
    assets = []

    for pair, contracts in contracts_by_pair.items():
        config = BASE_PAIR_CONFIGS.get(
            pair,
            {
                "base_asset": infer_base_asset(pair),
                "min_qty": 0.001,
                "max_qty": 1.00,
                "qty_step": 0.001,
                "min_profit_usdt": 0.50,
            },
        )

        base_asset = config["base_asset"]

        for i, contract in enumerate(contracts[:2]):
            futures_symbol = contract["symbol"]
            expiry = expiry_from_delivery_ms(contract["delivery_date"])
            label = contract_label(i, futures_symbol)

            assets.append(
                {
                    "name": f"{base_asset} {label}",
                    "spot_symbol": pair,
                    "futures_symbol": futures_symbol,
                    "base_asset": base_asset,
                    "expiry": expiry,
                    "min_qty": config["min_qty"],
                    "max_qty": config["max_qty"],
                    "qty_step": config["qty_step"],
                    "min_profit_usdt": config["min_profit_usdt"],
                }
            )

    return assets


def fetch_yfinance_yield_sync(symbol: str) -> float:
    ticker = yf.Ticker(symbol)

    try:
        fast_info = ticker.fast_info
        last_price = fast_info.get("last_price")

        if last_price is not None:
            return float(last_price) / 100.0

    except Exception:
        pass

    history = ticker.history(period="5d", interval="1d")

    if history.empty:
        raise RuntimeError(f"No yfinance data returned for {symbol}")

    close_series = history["Close"].dropna()

    if close_series.empty:
        raise RuntimeError(f"No valid close data returned for {symbol}")

    last_close = float(close_series.iloc[-1])

    return last_close / 100.0


async def fetch_yfinance_yield(symbol: str) -> float:
    return await asyncio.to_thread(fetch_yfinance_yield_sync, symbol)


async def benchmark_loop(args, benchmark_state):
    while True:
        try:
            selected = YFINANCE_BENCHMARKS[0]
            benchmark_yield = await fetch_yfinance_yield(selected["symbol"])

            benchmark_state["yield"] = benchmark_yield
            benchmark_state["source"] = selected["symbol"]
            benchmark_state["name"] = selected["name"]
            benchmark_state["tenor_days"] = selected["tenor_days"]
            benchmark_state["last_update"] = datetime.now(timezone.utc).isoformat()
            benchmark_state["is_live"] = True
            benchmark_state["error"] = None

        except Exception as exc:
            benchmark_state["is_live"] = False
            benchmark_state["error"] = str(exc)

        await asyncio.sleep(args.benchmark_poll_seconds)


def sign_params(secret, params):
    query = urllib.parse.urlencode(params)
    signature = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    params["signature"] = signature
    return params


async def signed_get(args, base_url, path, params=None):
    if not args.binance_api_key or not args.binance_api_secret:
        return None

    params = params or {}
    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 5000
    signed = sign_params(args.binance_api_secret, params)

    headers = {
        "X-MBX-APIKEY": args.binance_api_key,
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(
            base_url + path,
            params=signed,
            headers=headers,
            timeout=10,
        ) as response:
            response.raise_for_status()
            return await response.json()


async def account_risk_loop(args, account_state):
    while True:
        try:
            if not args.enable_account_risk:
                account_state["enabled"] = False
                account_state["error"] = None
                await asyncio.sleep(10)
                continue

            account_state["enabled"] = True

            if not args.binance_api_key or not args.binance_api_secret:
                account_state["error"] = "API keys missing"
                await asyncio.sleep(30)
                continue

            futures_account = await signed_get(
                args,
                FUTURES_REST_BASE,
                "/fapi/v2/account",
            )

            spot_account = await signed_get(
                args,
                SPOT_REST_BASE,
                "/api/v3/account",
            )

            futures_available_balance = float(futures_account.get("availableBalance", 0.0))
            futures_wallet_balance = float(futures_account.get("totalWalletBalance", 0.0))

            spot_usdt_free = 0.0

            for balance in spot_account.get("balances", []):
                if balance.get("asset") == "USDT":
                    spot_usdt_free = float(balance.get("free", 0.0))
                    break

            account_state["futures_available_balance_usdt"] = futures_available_balance
            account_state["futures_wallet_balance_usdt"] = futures_wallet_balance
            account_state["spot_usdt_free"] = spot_usdt_free
            account_state["last_update"] = datetime.now(timezone.utc).isoformat()
            account_state["error"] = None

        except Exception as exc:
            account_state["error"] = str(exc)

        await asyncio.sleep(30)


def walk_asks(asks, target_qty: float):
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


def walk_bids(bids, target_qty: float):
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


def compute_result(
    asset,
    spot_book,
    futures_book,
    target_qty,
    spot_fee_rate,
    futures_fee_rate,
    delivery_fee_rate,
    annual_capital_cost_rate,
    benchmark_state,
    account_state,
    args,
):
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


def generate_candidate_sizes(min_qty, max_qty, qty_step):
    sizes = []
    n = 0

    while True:
        qty = min_qty + n * qty_step
        if qty > max_qty + 1e-12:
            break

        sizes.append(round(qty, 12))
        n += 1

    return sizes


def find_best_size(
    asset,
    spot_book,
    futures_book,
    spot_fee_rate,
    futures_fee_rate,
    delivery_fee_rate,
    annual_capital_cost_rate,
    benchmark_state,
    account_state,
    args,
):
    best_overall = None
    best_signal = None

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


async def shared_spot_depth_listener(spot_symbol, args, shared_spot_state):
    stream = f"{spot_symbol.lower()}@depth{args.levels}@{args.speed_ms}ms"
    url = f"{SPOT_WS_BASE}/{stream}"

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                max_queue=1000,
            ) as ws:
                shared_spot_state["error"] = None

                async for message in ws:
                    data = json.loads(message)

                    shared_spot_state["book"]["bids"] = data.get("bids", [])
                    shared_spot_state["book"]["asks"] = data.get("asks", [])
                    shared_spot_state["book"]["last_update_time"] = time.time()

                    for event in shared_spot_state["subscribers"]:
                        event.set()

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            shared_spot_state["error"] = str(exc)
            await asyncio.sleep(1)


async def futures_depth_listener(asset, args, asset_state):
    stream = f"{asset['futures_symbol'].lower()}@depth{args.levels}@{args.speed_ms}ms"
    url = f"{FUTURES_WS_BASE}/{stream}"

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                max_queue=1000,
            ) as ws:
                asset_state["futures_error"] = None

                async for message in ws:
                    data = json.loads(message)

                    asset_state["futures_book"]["bids"] = data.get("b", [])
                    asset_state["futures_book"]["asks"] = data.get("a", [])
                    asset_state["futures_book"]["last_update_time"] = time.time()

                    asset_state["event"].set()

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            asset_state["futures_error"] = str(exc)
            await asyncio.sleep(1)


async def send_alert(args, message, title="Cash-and-carry bot", priority="default"):
    print("\a", end="")

    if not args.alert_webhook_url:
        return

    try:
        headers = {
            "Title": title,
            "Priority": priority,
            "Tags": "robot,chart_with_upwards_trend,moneybag",
        }

        async with aiohttp.ClientSession() as session:
            await session.post(
                args.alert_webhook_url,
                data=message.encode("utf-8"),
                headers=headers,
                timeout=10,
            )

    except Exception as exc:
        print(f"Alert error: {exc}")


async def asset_calculator_loop(
    asset,
    args,
    asset_state,
    shared_spot_states,
    benchmark_state,
    account_state,
):
    last_alert_time = 0.0

    while True:
        await asset_state["event"].wait()
        asset_state["event"].clear()

        now_ts = time.time()

        shared_spot_state = shared_spot_states[asset["spot_symbol"]]
        spot_book = shared_spot_state["book"]
        futures_book = asset_state["futures_book"]

        asset_state["spot_error"] = shared_spot_state.get("error")

        if spot_book["last_update_time"] is None or futures_book["last_update_time"] is None:
            continue

        spot_age_ms = (now_ts - spot_book["last_update_time"]) * 1000.0
        futures_age_ms = (now_ts - futures_book["last_update_time"]) * 1000.0

        asset_state["spot_age_ms"] = spot_age_ms
        asset_state["futures_age_ms"] = futures_age_ms

        if spot_age_ms > args.max_book_age_ms or futures_age_ms > args.max_book_age_ms:
            continue

        result = find_best_size(
            asset=asset,
            spot_book=spot_book,
            futures_book=futures_book,
            spot_fee_rate=args.spot_fee_rate,
            futures_fee_rate=args.futures_fee_rate,
            delivery_fee_rate=args.delivery_fee_rate,
            annual_capital_cost_rate=args.annual_capital_cost_rate,
            benchmark_state=benchmark_state,
            account_state=account_state,
            args=args,
        )

        asset_state["latest_result"] = result
        asset_state["latest_result_time"] = time.time()

        if result and result["signal"]:
            if now_ts - last_alert_time >= args.alert_cooldown_seconds:
                alert_msg = (
                    f"TRADE OPPORTUNITY | {result['asset_name']} | "
                    f"{result['futures_symbol']} | "
                    f"Size {result['target_qty']:.6f} {result['base_asset']} | "
                    f"Profit {result['profit_usdt']:.4f} USDT | "
                    f"Ann {result['annualized_net_return'] * 100:.3f}% | "
                    f"T-bill {result['tbill_yield'] * 100:.3f}%"
                )

                await send_alert(
                    args,
                    alert_msg,
                    title="CAC trade opportunity",
                    priority="high",
                )

                last_alert_time = now_ts


def parse_asset_spec(spec: str):
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


def prepare_manual_assets(args):
    if args.asset:
        raw_assets = [parse_asset_spec(spec) for spec in args.asset]
    else:
        raw_assets = FALLBACK_STATIC_ASSETS

    prepared = []

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


def prepare_discovered_assets(raw_assets):
    prepared = []

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


def asset_contract_key(assets):
    return tuple(
        (asset["name"], asset["spot_symbol"], asset["futures_symbol"], asset["expiry"].isoformat())
        for asset in assets
    )


def compute_health(args, shared_state, benchmark_state, account_state):
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

    assets_health = {}

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


def save_health(args, health):
    atomic_write_json(args.health_file, health)


def save_state(args, shared_state, benchmark_state, account_state):
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


def fit(text, width):
    text = str(text)
    if len(text) <= width:
        return text.ljust(width)
    if width <= 1:
        return text[:width]
    return text[: width - 1] + "…"


def fmt_age(value):
    if value is None:
        return "n/a"
    return f"{value:.0f}"


def format_panel(asset, asset_state):
    result = asset_state.get("latest_result")
    spot_error = asset_state.get("spot_error")
    futures_error = asset_state.get("futures_error")

    lines = []
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


async def dashboard_loop(args, shared_state, benchmark_state, account_state):
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

        output_lines = []

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


async def start_scanners_for_assets(args, assets, shared_state, benchmark_state, account_state):
    asset_states = {}
    shared_spot_states = {}
    tasks = []

    for asset in assets:
        spot_symbol = asset["spot_symbol"]

        if spot_symbol not in shared_spot_states:
            shared_spot_states[spot_symbol] = {
                "book": {
                    "bids": None,
                    "asks": None,
                    "last_update_time": None,
                },
                "subscribers": [],
                "error": None,
            }

    for asset in assets:
        asset_state = {
            "futures_book": {
                "bids": None,
                "asks": None,
                "last_update_time": None,
            },
            "event": asyncio.Event(),
            "latest_result": None,
            "latest_result_time": None,
            "spot_age_ms": None,
            "futures_age_ms": None,
            "spot_error": None,
            "futures_error": None,
        }

        asset_states[asset["name"]] = asset_state
        shared_spot_states[asset["spot_symbol"]]["subscribers"].append(asset_state["event"])

    for spot_symbol, shared_spot_state in shared_spot_states.items():
        tasks.append(
            asyncio.create_task(
                shared_spot_depth_listener(
                    spot_symbol,
                    args,
                    shared_spot_state,
                )
            )
        )

    for asset in assets:
        asset_state = asset_states[asset["name"]]

        tasks.extend(
            [
                asyncio.create_task(futures_depth_listener(asset, args, asset_state)),
                asyncio.create_task(
                    asset_calculator_loop(
                        asset,
                        args,
                        asset_state,
                        shared_spot_states,
                        benchmark_state,
                        account_state,
                    )
                ),
            ]
        )

    shared_state["assets"] = assets
    shared_state["asset_states"] = asset_states
    shared_state["shared_spot_states"] = shared_spot_states
    shared_state["last_contract_update"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    return tasks


async def scanner_supervisor_loop(args, shared_state, benchmark_state, account_state):
    scanner_tasks = []
    current_key = None

    while True:
        try:
            if args.no_auto_roll:
                assets = prepare_manual_assets(args)
            else:
                base_pairs = args.base_pair or ["BTCUSDT", "ETHUSDT"]
                contracts_by_pair = await fetch_usdtm_delivery_contracts(base_pairs)
                raw_assets = build_assets_from_contracts(contracts_by_pair)
                assets = prepare_discovered_assets(raw_assets)

            new_key = asset_contract_key(assets)

            if new_key != current_key:
                for task in scanner_tasks:
                    task.cancel()

                if scanner_tasks:
                    await asyncio.gather(*scanner_tasks, return_exceptions=True)

                scanner_tasks = await start_scanners_for_assets(
                    args=args,
                    assets=assets,
                    shared_state=shared_state,
                    benchmark_state=benchmark_state,
                    account_state=account_state,
                )

                current_key = new_key
                shared_state["contract_key"] = new_key
                shared_state["roll_error"] = None

            else:
                shared_state["roll_error"] = None

        except Exception as exc:
            shared_state["roll_error"] = str(exc)

            if not scanner_tasks and args.no_auto_roll:
                assets = prepare_manual_assets(args)

                scanner_tasks = await start_scanners_for_assets(
                    args=args,
                    assets=assets,
                    shared_state=shared_state,
                    benchmark_state=benchmark_state,
                    account_state=account_state,
                )

                current_key = asset_contract_key(assets)

        await asyncio.sleep(args.contract_refresh_seconds)


def asset_contract_key(assets):
    return tuple(
        (asset["name"], asset["spot_symbol"], asset["futures_symbol"], asset["expiry"].isoformat())
        for asset in assets
    )


def prepare_manual_assets(args):
    if args.asset:
        raw_assets = [parse_asset_spec(spec) for spec in args.asset]
    else:
        raw_assets = FALLBACK_STATIC_ASSETS

    prepared = []

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


def prepare_discovered_assets(raw_assets):
    prepared = []

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


def parse_asset_spec(spec: str):
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


async def main():
    parser = argparse.ArgumentParser()

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

    parser.add_argument("--alert-webhook-url", default=DEFAULT_ALERT_WEBHOOK_URL)
    parser.add_argument("--alert-cooldown-seconds", type=float, default=300)
    parser.add_argument("--no-startup-alert", action="store_true", help="Disable startup online alert")

    parser.add_argument("--enable-account-risk", action="store_true")
    parser.add_argument("--binance-api-key", default=os.getenv("BINANCE_API_KEY"))
    parser.add_argument("--binance-api-secret", default=os.getenv("BINANCE_API_SECRET"))
    parser.add_argument("--risk-margin-buffer-multiplier", type=float, default=3.0)
    parser.add_argument("--assumed-leverage", type=float, default=1.0)
    parser.add_argument("--max-account-age-seconds", type=float, default=120)

    args = parser.parse_args()

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


if __name__ == "__main__":
    asyncio.run(main())

"""Async IO and orchestration for the cash-and-carry scanner.

This module owns every coroutine that touches the network:
- Binance Spot / Futures WebSocket depth streams
- Binance signed REST (account, exchangeInfo)
- yfinance benchmark fetch (run in a worker thread)
- ntfy.sh alert delivery

The pure-math + parsing + state helpers live in :mod:`binance_carry.core`.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Any

import aiohttp
import websockets
import yfinance as yf

from .core import (
    BASE_PAIR_CONFIGS,
    asset_contract_key,
    contract_label,
    expiry_from_delivery_ms,
    find_best_size,
    infer_base_asset,
    prepare_discovered_assets,
    prepare_manual_assets,
)


# ---- URLs --------------------------------------------------------------------

SPOT_WS_BASE = "wss://stream.binance.com:9443/ws"
FUTURES_WS_BASE = "wss://fstream.binance.com/ws"

SPOT_REST_BASE = "https://api.binance.com"
FUTURES_REST_BASE = "https://fapi.binance.com"


# ---- Benchmark (yfinance) ---------------------------------------------------

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


async def benchmark_loop(args: Any, benchmark_state: dict[str, Any]) -> None:
    # YFINANCE_BENCHMARKS is the only benchmark currently supported.
    # The function selects the closest-tenor benchmark in the original code,
    # but with a single benchmark this collapses to the first entry.
    from .core import YFINANCE_BENCHMARKS

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


# ---- Signed REST (account) --------------------------------------------------

def sign_params(secret: str, params: dict[str, Any]) -> dict[str, Any]:
    query = urllib.parse.urlencode(params)
    signature = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    params["signature"] = signature
    return params


async def signed_get(args: Any, base_url: str, path: str, params: dict[str, Any] | None = None) -> Any:
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


async def account_risk_loop(args: Any, account_state: dict[str, Any]) -> None:
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


# ---- Contract discovery -----------------------------------------------------

async def fetch_usdtm_delivery_contracts(base_pairs: list[str]) -> dict[str, list[dict[str, Any]]]:
    url = f"{FUTURES_REST_BASE}/fapi/v1/exchangeInfo"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=10) as response:
            response.raise_for_status()
            data = await response.json()

    now_ms = int(time.time() * 1000)
    contracts_by_pair: dict[str, list[dict[str, Any]]] = {pair: [] for pair in base_pairs}

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


def build_assets_from_contracts(contracts_by_pair: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    assets: list[dict[str, Any]] = []

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


# ---- WebSocket depth listeners ----------------------------------------------

async def shared_spot_depth_listener(
    spot_symbol: str,
    args: Any,
    shared_spot_state: dict[str, Any],
) -> None:
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


async def futures_depth_listener(asset: dict[str, Any], args: Any, asset_state: dict[str, Any]) -> None:
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


# ---- Alerts ------------------------------------------------------------------

async def send_alert(
    args: Any,
    message: str,
    title: str = "Cash-and-carry bot",
    priority: str = "default",
) -> None:
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


# ---- Asset calc loop --------------------------------------------------------

async def asset_calculator_loop(
    asset: dict[str, Any],
    args: Any,
    asset_state: dict[str, Any],
    shared_spot_states: dict[str, dict[str, Any]],
    benchmark_state: dict[str, Any],
    account_state: dict[str, Any],
) -> None:
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


# ---- Supervisor (auto-roll + scanner lifecycle) -----------------------------

async def start_scanners_for_assets(
    args: Any,
    assets: list[dict[str, Any]],
    shared_state: dict[str, Any],
    benchmark_state: dict[str, Any],
    account_state: dict[str, Any],
) -> list[asyncio.Task]:
    asset_states: dict[str, dict[str, Any]] = {}
    shared_spot_states: dict[str, dict[str, Any]] = {}
    tasks: list[asyncio.Task] = []

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


async def scanner_supervisor_loop(
    args: Any,
    shared_state: dict[str, Any],
    benchmark_state: dict[str, Any],
    account_state: dict[str, Any],
) -> None:
    scanner_tasks: list[asyncio.Task] = []
    current_key: tuple | None = None

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

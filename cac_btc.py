import argparse
import asyncio
import json
import time
from datetime import datetime, timezone, timedelta

import websockets
import yfinance as yf


SPOT_WS_BASE = "wss://stream.binance.com:9443/ws"
FUTURES_WS_BASE = "wss://fstream.binance.com/ws"


YFINANCE_BENCHMARKS = [
    {
        "symbol": "^IRX",
        "tenor_days": 91,
        "name": "13-week Treasury Bill",
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


def days_to_expiry(expiry: datetime) -> float:
    now = datetime.now(timezone.utc)
    seconds = (expiry - now).total_seconds()
    return max(seconds / 86400, 1e-9)


def select_yfinance_benchmark(days_to_expiry_value: float):
    selected = min(
        YFINANCE_BENCHMARKS,
        key=lambda item: abs(item["tenor_days"] - days_to_expiry_value),
    )

    tenor_gap = days_to_expiry_value - selected["tenor_days"]

    return {
        "symbol": selected["symbol"],
        "name": selected["name"],
        "tenor_days": selected["tenor_days"],
        "tenor_gap_days": tenor_gap,
    }


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


async def benchmark_loop(args, benchmark_state, expiry: datetime):
    while True:
        try:
            dte = days_to_expiry(expiry)
            selected = select_yfinance_benchmark(dte)

            benchmark_yield = await fetch_yfinance_yield(selected["symbol"])

            benchmark_state["yield"] = benchmark_yield
            benchmark_state["source"] = selected["symbol"]
            benchmark_state["name"] = selected["name"]
            benchmark_state["tenor_days"] = selected["tenor_days"]
            benchmark_state["tenor_gap_days"] = selected["tenor_gap_days"]
            benchmark_state["last_update"] = datetime.now(timezone.utc).isoformat()
            benchmark_state["is_live"] = True

        except Exception as exc:
            benchmark_state["is_live"] = False
            print("=" * 80)
            print(f"Benchmark error: {exc}")
            print(f"Using previous/fallback T-bill: {benchmark_state['yield'] * 100:.3f}%")

        await asyncio.sleep(args.benchmark_poll_seconds)


def walk_asks(asks, target_btc: float):
    remaining = target_btc
    total_cost_usdt = 0.0
    filled_btc = 0.0
    levels_used = 0

    for price_str, qty_str in asks:
        price_usdt = float(price_str)
        qty_btc = float(qty_str)

        take_btc = min(remaining, qty_btc)

        total_cost_usdt += take_btc * price_usdt
        filled_btc += take_btc
        remaining -= take_btc
        levels_used += 1

        if remaining <= 1e-12:
            break

    if filled_btc + 1e-12 < target_btc:
        return None

    return {
        "vwap_usdt": total_cost_usdt / filled_btc,
        "notional_usdt": total_cost_usdt,
        "filled_btc": filled_btc,
        "levels_used": levels_used,
    }


def walk_bids(bids, target_btc: float):
    remaining = target_btc
    total_proceeds_usdt = 0.0
    filled_btc = 0.0
    levels_used = 0

    for price_str, qty_str in bids:
        price_usdt = float(price_str)
        qty_btc = float(qty_str)

        take_btc = min(remaining, qty_btc)

        total_proceeds_usdt += take_btc * price_usdt
        filled_btc += take_btc
        remaining -= take_btc
        levels_used += 1

        if remaining <= 1e-12:
            break

    if filled_btc + 1e-12 < target_btc:
        return None

    return {
        "vwap_usdt": total_proceeds_usdt / filled_btc,
        "notional_usdt": total_proceeds_usdt,
        "filled_btc": filled_btc,
        "levels_used": levels_used,
    }


def compute_result(
    spot_book,
    futures_book,
    target_btc,
    expiry,
    spot_fee_rate,
    futures_fee_rate,
    delivery_fee_rate,
    annual_capital_cost_rate,
    benchmark_state,
):
    if spot_book["asks"] is None or spot_book["bids"] is None or futures_book["bids"] is None:
        return None

    spot_buy = walk_asks(spot_book["asks"], target_btc)
    futures_short = walk_bids(futures_book["bids"], target_btc)

    # Live estimate for selling BTC back to USDT at exit.
    # This uses the current spot bid VWAP, not the entry ask VWAP.
    spot_exit_sell = walk_bids(spot_book["bids"], target_btc)

    if spot_buy is None or futures_short is None or spot_exit_sell is None:
        return None

    dte = days_to_expiry(expiry)

    gross_basis_usdt = futures_short["notional_usdt"] - spot_buy["notional_usdt"]

    spot_entry_fee_usdt = spot_buy["notional_usdt"] * spot_fee_rate
    futures_entry_fee_usdt = futures_short["notional_usdt"] * futures_fee_rate

    # Updated live using current spot bid VWAP because the BTC exit leg is a sell.
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

    signal = (
        annualized_net_return > tbill_yield
        and profit_usdt > 0
    )

    close_start = expiry - timedelta(minutes=5)
    close_end = expiry - timedelta(minutes=1)

    return {
        "time": datetime.now(timezone.utc),
        "expiry": expiry,
        "manual_close_start": close_start,
        "manual_close_end": close_end,
        "days_to_expiry": dte,
        "target_btc": target_btc,

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

        "signal": signal,
    }


async def spot_depth_listener(symbol, levels, speed_ms, spot_book, book_event):
    stream = f"{symbol.lower()}@depth{levels}@{speed_ms}ms"
    url = f"{SPOT_WS_BASE}/{stream}"

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                max_queue=1000,
            ) as ws:
                async for message in ws:
                    data = json.loads(message)

                    spot_book["bids"] = data.get("bids", [])
                    spot_book["asks"] = data.get("asks", [])
                    spot_book["last_update_time"] = time.time()

                    book_event.set()

        except Exception as exc:
            print("=" * 80)
            print(f"Spot WebSocket error: {exc}")
            await asyncio.sleep(1)


async def futures_depth_listener(symbol, levels, speed_ms, futures_book, book_event):
    stream = f"{symbol.lower()}@depth{levels}@{speed_ms}ms"
    url = f"{FUTURES_WS_BASE}/{stream}"

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                max_queue=1000,
            ) as ws:
                async for message in ws:
                    data = json.loads(message)

                    futures_book["bids"] = data.get("b", [])
                    futures_book["asks"] = data.get("a", [])
                    futures_book["last_update_time"] = time.time()

                    book_event.set()

        except Exception as exc:
            print("=" * 80)
            print(f"Futures WebSocket error: {exc}")
            await asyncio.sleep(1)


def print_result(args, result, spot_age_ms, futures_age_ms):
    signal_text = "TRADE OPPORTUNITY" if result["signal"] else "No trade"
    benchmark_status = "live" if result["benchmark_is_live"] else "fallback"

    print("=" * 80)
    print(f"Time:                  {result['time'].strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Pair:                  {args.spot_symbol} spot / {args.futures_symbol} future")
    print(f"Expiry:                {result['expiry'].strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Manual close window:   {result['manual_close_start'].strftime('%Y-%m-%d %H:%M:%S UTC')} to {result['manual_close_end'].strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Days to expiry:        {result['days_to_expiry']:.2f}")
    print()
    print(f"Size:                  {result['target_btc']:.6f} BTC")
    print(f"Spot entry ask VWAP:   {result['spot_vwap_ask_usdt']:,.2f} USDT")
    print(f"Future short bid VWAP: {result['futures_vwap_bid_usdt']:,.2f} USDT")
    print(f"Spot exit bid VWAP:    {result['spot_exit_vwap_bid_usdt']:,.2f} USDT")
    print(f"Spot entry levels:     {result['spot_levels_used']}")
    print(f"Future levels used:    {result['futures_levels_used']}")
    print(f"Spot exit levels:      {result['spot_exit_levels_used']}")
    print()
    print(f"Gross basis USDT:      {result['gross_basis_usdt']:,.4f}")
    print(f"COC USDT:              {result['coc_usdt']:,.4f} incl. live spot exit fee estimate")
    print(f"Profit USDT:           {result['profit_usdt']:,.4f} for {result['target_btc']:.6f} BTC")
    print(f"Annualized net return: {result['annualized_net_return'] * 100:.3f}%")
    print(f"T-bill benchmark:      {result['tbill_yield'] * 100:.3f}%")
    print()
    print(f"Benchmark source:      {result['benchmark_source']} ({benchmark_status})")
    print(f"Book age:              spot {spot_age_ms:.0f}ms / future {futures_age_ms:.0f}ms")
    print(f"Signal:                {signal_text}")


async def calculator_loop(args, spot_book, futures_book, book_event, benchmark_state, expiry):
    last_print = 0.0
    last_signal_state = False

    while True:
        await book_event.wait()
        book_event.clear()

        now = time.time()

        if spot_book["last_update_time"] is None or futures_book["last_update_time"] is None:
            continue

        spot_age_ms = (now - spot_book["last_update_time"]) * 1000.0
        futures_age_ms = (now - futures_book["last_update_time"]) * 1000.0

        if spot_age_ms > args.max_book_age_ms or futures_age_ms > args.max_book_age_ms:
            continue

        result = compute_result(
            spot_book=spot_book,
            futures_book=futures_book,
            target_btc=args.target_btc,
            expiry=expiry,
            spot_fee_rate=args.spot_fee_rate,
            futures_fee_rate=args.futures_fee_rate,
            delivery_fee_rate=args.delivery_fee_rate,
            annual_capital_cost_rate=args.annual_capital_cost_rate,
            benchmark_state=benchmark_state,
        )

        if result is None:
            continue

        should_print_interval = now - last_print >= args.print_every
        signal_just_triggered = result["signal"] and not last_signal_state

        if should_print_interval or signal_just_triggered:
            print_result(args, result, spot_age_ms, futures_age_ms)
            last_print = now

        last_signal_state = result["signal"]


async def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--spot-symbol", default="BTCUSDT")
    parser.add_argument("--futures-symbol", default="BTCUSDT_260925")
    parser.add_argument("--target-btc", type=float, default=0.1)

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

    parser.add_argument("--max-book-age-ms", type=float, default=500)
    parser.add_argument("--print-every", type=float, default=0.5)

    args = parser.parse_args()

    expiry = parse_expiry_from_symbol(args.futures_symbol)
    dte = days_to_expiry(expiry)
    selected_benchmark = select_yfinance_benchmark(dte)

    close_start = expiry - timedelta(minutes=5)
    close_end = expiry - timedelta(minutes=1)

    spot_book = {
        "bids": None,
        "asks": None,
        "last_update_time": None,
    }

    futures_book = {
        "bids": None,
        "asks": None,
        "last_update_time": None,
    }

    benchmark_state = {
        "yield": args.fallback_benchmark_yield,
        "source": "fallback",
        "name": selected_benchmark["name"],
        "tenor_days": selected_benchmark["tenor_days"],
        "tenor_gap_days": selected_benchmark["tenor_gap_days"],
        "last_update": None,
        "is_live": False,
    }

    book_event = asyncio.Event()

    print("=" * 80)
    print("Starting BTC/USDT cash-and-carry scanner")
    print(f"Spot:                  {args.spot_symbol}")
    print(f"Future:                {args.futures_symbol}")
    print(f"Expiry:                {expiry.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Manual close window:   {close_start.strftime('%Y-%m-%d %H:%M:%S UTC')} to {close_end.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Days to expiry:        {dte:.2f}")
    print(f"Size:                  {args.target_btc:.6f} BTC")
    print(f"Benchmark:             {selected_benchmark['symbol']} - {selected_benchmark['name']}")
    print(f"Fallback T-bill:       {args.fallback_benchmark_yield * 100:.3f}%")
    print(f"Spot taker fee:        {args.spot_fee_rate * 100:.4f}%")
    print(f"Futures taker fee:     {args.futures_fee_rate * 100:.4f}%")
    print("Spot exit fee:         included in COC USDT using live spot bid VWAP estimate")

    await asyncio.gather(
        spot_depth_listener(
            symbol=args.spot_symbol,
            levels=args.levels,
            speed_ms=args.speed_ms,
            spot_book=spot_book,
            book_event=book_event,
        ),
        futures_depth_listener(
            symbol=args.futures_symbol,
            levels=args.levels,
            speed_ms=args.speed_ms,
            futures_book=futures_book,
            book_event=book_event,
        ),
        benchmark_loop(
            args=args,
            benchmark_state=benchmark_state,
            expiry=expiry,
        ),
        calculator_loop(
            args=args,
            spot_book=spot_book,
            futures_book=futures_book,
            book_event=book_event,
            benchmark_state=benchmark_state,
            expiry=expiry,
        ),
    )


if __name__ == "__main__":
    asyncio.run(main())

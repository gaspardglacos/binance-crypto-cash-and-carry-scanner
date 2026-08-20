# Binance Cash-and-Carry Arbitrage Scanner

A real-time scanner that monitors **spot vs. USDT-M futures** basis on Binance, computes
the cost-of-carry of a cash-and-carry trade after fees and capital cost, and emits a
trade signal when the **annualized net return exceeds the risk-free benchmark** (13-week
US T-bill yield).

Two implementations live in this repo:

| File | Role | Status |
|---|---|---|
| `cac_btc.py` | Single-asset scanner (BTC only) | Origin / v1 |
| `multicac.py` | Multi-asset, auto-roll, dashboard, alerts | Current / v2 |

> **Disclaimer.** This is research/educational code. It does **not** place orders. The
> signal is informational; manual execution only. Crypto markets carry significant risk.
> No warranty — use at your own risk.

---

## What is cash-and-carry arbitrage?

A cash-and-carry (CAC) trade profits from the basis between spot and a dated future:

```
Gross basis  =  Futures price  −  Spot price
Cost of carry =  spot taker fee + futures taker fee + exit fee + delivery fee + capital cost
Net basis    =  Gross basis − Cost of carry
```

If the annualized net basis exceeds the risk-free rate, you can:

1. **Buy** spot at the ask (walk the book for VWAP).
2. **Short** the corresponding delivery future at the bid.
3. Hold to expiry, where convergence settles the position at the spot price.
4. Pocket the basis minus all transaction costs.

The scanner watches this in real time and flags windows where the trade is worth more
than parking the capital in T-bills.

---

## Features

- **Live order-book depth** from Binance Spot (`wss://stream.binance.com:9443`) and
  USDT-M Futures (`wss://fstream.binance.com`) WebSockets.
- **True VWAP through depth levels** — not a top-of-book approximation.
- **Multi-asset, multi-contract** — auto-discovers the next two quarterly expiries per
  base pair (BTC, ETH) via `/fapi/v1/exchangeInfo`.
- **Auto-roll** — when the current quarter expires, the scanner drops it and picks up
  the next one without restart.
- **Risk-free benchmark** — live 13-week T-bill yield from `^IRX` via `yfinance`, with
  a configurable fallback.
- **Account-level risk gates** *(optional, signed REST)* — verifies that spot USDT
  free balance and futures available margin cover the trade before signaling.
- **Live exit-fee estimate** — the cost-of-carry uses the *current* spot bid VWAP
  rather than the entry ask, so the projected profit reflects realistic unwind cost.
- **Terminal dashboard** with per-asset panels, ANSI clear-screen refresh, book-age
  display, and health summary.
- **JSONL audit log** (rolling, size-capped) of every dashboard render.
- **Atomic JSON state + health files** — survives restarts.
- **Push alerts** via [ntfy.sh](https://ntfy.sh) (or any compatible webhook) with
  cooldown to avoid alert storms.
- **Manual-close window** — no new entries inside the last few minutes before expiry.

---

## Architecture

```
                  ┌─────────────────────────────────────────┐
                  │           scanner_supervisor_loop       │
                  │  (auto-roll: re-discovers contracts     │
                  │   from /fapi/v1/exchangeInfo)           │
                  └───────────────┬─────────────────────────┘
                                  │ assets
                                  ▼
   ┌─────────────────────┐    ┌──────────────────────────┐
   │  Spot depth WS      │�───┤  per-asset futures WS    │
   │  (shared per pair)  │    │  + asset_calculator_loop │
   └─────────┬───────────┘    └──────────────┬───────────┘
             │ book event                   │ result
             ▼                              ▼
   �─────────────────────────────────────────────────────┐
   │  benchmark_loop (yfinance ^IRX, fallback 4.5%)      │
   │  account_risk_loop (signed REST, optional)          │
   │  dashboard_loop (terminal UI, JSONL log, health)    │
   └─────────────────────────────────────────────────────┘
```

The whole thing runs on `asyncio` — one event loop, fan-out via `asyncio.Event`,
reconnect-on-error with backoff.

---

## Installation

Requires **Python 3.10+**.

```bash
git clone https://github.com/gaspardglacos/binance-crypto-cash-and-carry-scanner.git
cd binance-crypto-cash-and-carry-scanner
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt            # runtime deps
pip install -r requirements-dev.txt        # adds pytest, ruff, mypy
```

### Dependencies

**Runtime** (`requirements.txt`):

| Package | Why |
|---|---|
| `websockets` | Binance Spot + Futures depth streams |
| `aiohttp` | Signed REST (account, exchangeInfo) + ntfy alerts |
| `yfinance` | Live 13-week T-bill yield (`^IRX`) |

**Dev** (`requirements-dev.txt`, layered on top of runtime):

| Package | Why |
|---|---|
| `pytest` + `pytest-asyncio` | Run the unit test suite |
| `ruff`, `mypy` | Linting and type-checking (optional) |

### Docker

A `Dockerfile` is included. Build and run:

```bash
docker build -t binance-cash-and-carry-scanner .

# Default: prints --help and exits (no network calls)
docker run --rm binance-cash-and-carry-scanner

# Run the actual scanner
docker run --rm -it binance-cash-and-carry-scanner \
    --no-startup-alert --base-pair BTCUSDT
```

The image is based on `python:3.12-slim`, runs as a non-root `scanner` user,
and only contains the runtime dependencies — `tests/`, dev tooling, and the
working-tree noise (`.git/`, `.worktrees/`, caches) are excluded by
`.dockerignore`.

---

## Usage

### Single-asset (v1)

```bash
python cac_btc.py \
  --spot-symbol BTCUSDT \
  --futures-symbol BTCUSDT_260925 \
  --target-btc 0.1 \
  --spot-fee-rate 0.001 \
  --futures-fee-rate 0.0005
```

### Multi-asset (v2)

```bash
python multicac.py \
  --base-pair BTCUSDT --base-pair ETHUSDT \
  --spot-fee-rate 0.001 \
  --futures-fee-rate 0.0005
```

With account risk gating (signed REST):

```bash
export BINANCE_API_KEY=...
export BINANCE_API_SECRET=...
python multicac.py --enable-account-risk
```

With push alerts (create a free ntfy.sh topic and put it here):

```bash
export CARRY_ALERT_WEBHOOK_URL=https://ntfy.sh/your-topic-name
python multicac.py
```

---

## Example output (multi-asset dashboard)

```
===============================================================================
Auto-roll USDT cash-and-carry scanner | 2026-08-03 22:02:13 UTC | T-bill 3.700% ^IRX live
Spot taker 0.1000% | Futures taker 0.0500% | Spot exit fee included via live spot bid VWAP
Auto-roll: on | Contract refresh 300s | Last contract update: 2026-08-03 22:01:17 UTC
Health: OK  | State: carry_state.json | Health file: carry_health.json | Log: carry_dashboard_log.jsonl
===============================================================================

BTC CUR 260925  BTCUSDT / BTCUSDT_260925       │  ETH CUR 260925  ETHUSDT / ETHUSDT_260925
------------------------------------------------------│------------------------------------------------------
Signal: No trade                                    │  Signal: TRADE OPPORTUNITY
Phase: normal                                       │  Phase: normal
DTE: 53.04d                                         │  DTE: 53.04d
                                                     │
Spot entry ask VWAP:   118,432.15 USDT             │  Spot entry ask VWAP:   4,521.18 USDT
Future short bid VWAP: 118,901.40 USDT             │  Future short bid VWAP: 4,547.92 USDT
Spot exit bid VWAP:    118,420.50 USDT             │  Spot exit bid VWAP:    4,519.40 USDT
Levels entry/fut/exit: 2/1/2                       │  Levels entry/fut/exit: 3/2/3
                                                     │
Gross basis: 46.9300 USDT                          │  Gross basis: 26.7400 USDT
COC:         0.6124 USDT                           │  COC:         0.0841 USDT
Profit:      46.3176 USDT                          │  Profit:      26.6559 USDT
Ann net:     2.713%                                │  Ann net:     4.025%
T-bill:      3.700% [live]                         │  T-bill:      3.700% [live]
                                                     │
Risk check:      True [ok]                         │  Risk check:      True [ok]
Reason: best available by annualized return        │  Reason: best signal by annualized return
```

---

## Project evolution

The repo tells a story in two layers:

1. **`cac_btc.py`** — the original single-asset scanner. Easier to read, shows the
   core math (basis, COC, signal) in isolation. Kept deliberately as v1.
2. **`multicac.py`** — the production-grade version that grew out of v1: multi-asset,
   auto-roll, dashboard, account risk, alerts, state persistence.

Reading `cac_btc.py` first is the recommended path for understanding what the scanner
actually does; `multicac.py` shows what productionizing it takes.

---

## Roadmap ideas

- [ ] Split `multicac.py` into `binance/`, `scanner/`, `monitor/`, `alerts/` modules
- [ ] Persist detected trade opportunities to a CSV/Parquet for backtesting
- [ ] Add a basic backtest harness that replays historical order-book snapshots
- [ ] Web dashboard (FastAPI + minimal HTML) alongside the terminal one
- [ ] Add spread/impact models beyond the simple level-walking

---

## License

MIT — see `LICENSE`.

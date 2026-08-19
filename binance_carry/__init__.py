"""binance_carry — Binance cash-and-carry arbitrage scanner.

Public API: the pure-math + helper functions live in :mod:`binance_carry.core`
and are re-exported here for convenience. The async IO and orchestration live in
:mod:`binance_carry.scanner`, and the terminal UI lives in :mod:`binance_carry.dashboard`.
"""

from .core import (
    BASE_PAIR_CONFIGS,
    FALLBACK_STATIC_ASSETS,
    YFINANCE_BENCHMARKS,
    asset_contract_key,
    compute_health,
    compute_result,
    contract_label,
    days_to_expiry,
    expiry_from_delivery_ms,
    find_best_size,
    generate_candidate_sizes,
    infer_base_asset,
    parse_asset_spec,
    parse_expiry_from_symbol,
    prepare_discovered_assets,
    prepare_manual_assets,
    walk_asks,
    walk_bids,
)

__all__ = [
    "BASE_PAIR_CONFIGS",
    "FALLBACK_STATIC_ASSETS",
    "YFINANCE_BENCHMARKS",
    "asset_contract_key",
    "compute_health",
    "compute_result",
    "contract_label",
    "days_to_expiry",
    "expiry_from_delivery_ms",
    "find_best_size",
    "generate_candidate_sizes",
    "infer_base_asset",
    "parse_asset_spec",
    "parse_expiry_from_symbol",
    "prepare_discovered_assets",
    "prepare_manual_assets",
    "walk_asks",
    "walk_bids",
]

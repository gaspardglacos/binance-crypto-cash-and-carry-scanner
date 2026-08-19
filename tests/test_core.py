"""Unit tests for binance_carry.core — the pure-math + helpers layer.

Run with:
    pytest tests/

These tests deliberately avoid any asyncio, network, or filesystem state —
they pin down the math so that future refactors of the scanner / dashboard
modules cannot silently change the signal logic.
"""

from __future__ import annotations

from argparse import Namespace
from datetime import datetime, timedelta, timezone

import pytest

from binance_carry.core import (
    BASE_PAIR_CONFIGS,
    asset_contract_key,
    compute_result,
    contract_label,
    days_to_expiry,
    expiry_from_delivery_ms,
    find_best_size,
    generate_candidate_sizes,
    infer_base_asset,
    parse_asset_spec,
    parse_expiry_from_symbol,
    prepare_manual_assets,
    walk_asks,
    walk_bids,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def make_args(**overrides) -> Namespace:
    """Build a minimal argparse-like namespace with all defaults the math needs."""
    defaults = dict(
        spot_fee_rate=0.001,
        futures_fee_rate=0.0005,
        delivery_fee_rate=0.0,
        annual_capital_cost_rate=0.0,
        max_benchmark_age_seconds=3600,
        entry_cutoff_minutes_before_expiry=10,
        enable_account_risk=False,
        assumed_leverage=1.0,
        risk_margin_buffer_multiplier=3.0,
        max_account_age_seconds=120,
    )
    defaults.update(overrides)
    return Namespace(**defaults)


def spot_book(bids, asks, last_update_time: float = 1000.0) -> dict:
    return {"bids": bids, "asks": asks, "last_update_time": last_update_time}


def futures_book(bids, asks=None, last_update_time: float = 1000.0) -> dict:
    return {"bids": bids, "asks": asks or [], "last_update_time": last_update_time}


def live_benchmark(yield_value: float = 0.045) -> dict:
    return {
        "yield": yield_value,
        "source": "^IRX",
        "name": "13-week Treasury Bill",
        "tenor_days": 91,
        "last_update": datetime.now(timezone.utc).isoformat(),
        "is_live": True,
        "error": None,
    }


def disabled_account() -> dict:
    return {
        "enabled": False,
        "futures_available_balance_usdt": None,
        "futures_wallet_balance_usdt": None,
        "spot_usdt_free": None,
        "last_update": None,
        "error": None,
    }


def make_asset(expiry_offset_days: float = 30.0, **overrides) -> dict:
    """Build an asset dict whose expiry is `now + offset_days`."""
    expiry = datetime.now(timezone.utc) + timedelta(days=expiry_offset_days)
    base = {
        "name": "BTC CUR TEST",
        "spot_symbol": "BTCUSDT",
        "futures_symbol": "BTCUSDT_990000",
        "base_asset": "BTC",
        "expiry": expiry,
        "min_qty": 0.001,
        "max_qty": 0.1,
        "qty_step": 0.001,
        "min_profit_usdt": 0.50,
    }
    base.update(overrides)
    base["candidate_sizes"] = generate_candidate_sizes(
        base["min_qty"], base["max_qty"], base["qty_step"]
    )
    return base


# ---------------------------------------------------------------------------
# walk_asks / walk_bids — order-book VWAP walking
# ---------------------------------------------------------------------------


class TestWalkAsks:
    def test_single_level_full_fill(self):
        asks = [("100.00", "1.0")]
        result = walk_asks(asks, 1.0)
        assert result == {
            "vwap_usdt": 100.0,
            "notional_usdt": 100.0,
            "filled_qty": 1.0,
            "levels_used": 1,
        }

    def test_single_level_partial_fill(self):
        asks = [("100.00", "0.5")]
        result = walk_asks(asks, 0.2)
        assert result["vwap_usdt"] == 100.0
        assert result["notional_usdt"] == 20.0
        assert result["filled_qty"] == 0.2
        assert result["levels_used"] == 1

    def test_multi_level_vwap(self):
        # Take 0.7 from level 1 (price 100, qty 0.5) then 0.2 from level 2 (price 101, qty 0.5).
        asks = [("100", "0.5"), ("101", "0.5")]
        result = walk_asks(asks, 0.7)
        assert result["vwap_usdt"] == pytest.approx((0.5 * 100 + 0.2 * 101) / 0.7)
        assert result["notional_usdt"] == pytest.approx(0.5 * 100 + 0.2 * 101)
        assert result["filled_qty"] == pytest.approx(0.7)
        assert result["levels_used"] == 2

    def test_insufficient_depth_returns_none(self):
        asks = [("100", "0.1"), ("101", "0.1")]
        assert walk_asks(asks, 0.5) is None


class TestWalkBids:
    def test_single_level(self):
        bids = [("100", "1.0")]
        result = walk_bids(bids, 0.5)
        assert result["vwap_usdt"] == 100.0
        assert result["notional_usdt"] == 50.0
        assert result["filled_qty"] == 0.5

    def test_multi_level(self):
        bids = [("100", "0.5"), ("99", "0.5")]
        result = walk_bids(bids, 0.8)
        # 0.5 @ 100 + 0.3 @ 99 → 79.7 notional, 0.8 qty, vwap = 99.625
        assert result["vwap_usdt"] == pytest.approx(99.625)
        assert result["notional_usdt"] == pytest.approx(79.7)
        assert result["filled_qty"] == pytest.approx(0.8)
        assert result["levels_used"] == 2

    def test_insufficient_depth_returns_none(self):
        bids = [("100", "0.1")]
        assert walk_bids(bids, 0.5) is None


# ---------------------------------------------------------------------------
# Symbol / time helpers
# ---------------------------------------------------------------------------


class TestParseExpiryFromSymbol:
    def test_valid_symbol(self):
        # 260925 → 2026-09-25 08:00 UTC
        dt = parse_expiry_from_symbol("BTCUSDT_260925")
        assert dt == datetime(2026, 9, 25, 8, 0, tzinfo=timezone.utc)

    def test_eth_symbol(self):
        dt = parse_expiry_from_symbol("ETHUSDT_261225")
        assert dt.year == 2026
        assert dt.month == 12
        assert dt.day == 25

    def test_invalid_symbol_raises(self):
        with pytest.raises(ValueError):
            parse_expiry_from_symbol("BTCUSDT_INVALID")

    def test_short_suffix_raises(self):
        with pytest.raises(ValueError):
            parse_expiry_from_symbol("BTCUSDT_12345")


class TestExpiryFromDeliveryMs:
    def test_round_trip(self):
        ms = int(datetime(2026, 9, 25, 8, 0, tzinfo=timezone.utc).timestamp() * 1000)
        dt = expiry_from_delivery_ms(ms)
        assert dt == datetime(2026, 9, 25, 8, 0, tzinfo=timezone.utc)


class TestDaysToExpiry:
    def test_future(self):
        future = datetime.now(timezone.utc) + timedelta(days=30)
        dte = days_to_expiry(future)
        assert 29.99 < dte < 30.01

    def test_past_clamped_to_epsilon(self):
        past = datetime.now(timezone.utc) - timedelta(days=5)
        assert days_to_expiry(past) == pytest.approx(1e-9)


class TestInferBaseAsset:
    def test_usdt_pair(self):
        assert infer_base_asset("BTCUSDT") == "BTC"
        assert infer_base_asset("ETHUSDT") == "ETH"

    def test_non_usdt_pair(self):
        assert infer_base_asset("BTCUSD") == "BTCUSD"


class TestContractLabel:
    def test_first_is_cur(self):
        assert contract_label(0, "BTCUSDT_260925") == "CUR 260925"

    def test_second_is_next(self):
        assert contract_label(1, "BTCUSDT_261225") == "NEXT 261225"

    def test_third_is_raw(self):
        assert contract_label(2, "BTCUSDT_270325") == "270325"


# ---------------------------------------------------------------------------
# Asset preparation
# ---------------------------------------------------------------------------


class TestGenerateCandidateSizes:
    def test_basic_grid(self):
        sizes = generate_candidate_sizes(min_qty=0.001, max_qty=0.005, qty_step=0.001)
        assert sizes == pytest.approx([0.001, 0.002, 0.003, 0.004, 0.005])

    def test_single_step(self):
        sizes = generate_candidate_sizes(min_qty=0.01, max_qty=0.01, qty_step=0.001)
        assert sizes == pytest.approx([0.01])

    def test_step_larger_than_range(self):
        sizes = generate_candidate_sizes(min_qty=0.001, max_qty=0.001, qty_step=0.01)
        assert sizes == pytest.approx([0.001])


class TestParseAssetSpec:
    def test_valid_spec(self):
        spec = "BTC CUR,BTCUSDT,BTCUSDT_260925,0.001,0.1,0.001,0.5"
        asset = parse_asset_spec(spec)
        assert asset["name"] == "BTC CUR"
        assert asset["spot_symbol"] == "BTCUSDT"
        assert asset["futures_symbol"] == "BTCUSDT_260925"
        assert asset["min_qty"] == 0.001
        assert asset["max_qty"] == 0.1
        assert asset["qty_step"] == 0.001
        assert asset["min_profit_usdt"] == 0.5

    def test_invalid_spec_raises(self):
        with pytest.raises(ValueError):
            parse_asset_spec("BTC CUR,BTCUSDT,BTCUSDT_260925")  # missing fields


class TestAssetContractKey:
    def test_same_assets_same_key(self):
        a1 = make_asset()
        a2 = make_asset()
        assert asset_contract_key([a1, a2]) == asset_contract_key([a1, a2])

    def test_different_assets_different_keys(self):
        a1 = make_asset(expiry_offset_days=30)
        a2 = make_asset(expiry_offset_days=60)
        assert asset_contract_key([a1]) != asset_contract_key([a2])

    def test_empty_returns_empty_tuple(self):
        assert asset_contract_key([]) == ()


# ---------------------------------------------------------------------------
# compute_result — the signal math
# ---------------------------------------------------------------------------


class TestComputeResult:
    """Pin the signal math down. All inputs are synthetic; expected values
    are computed by hand."""

    def test_huge_basis_emits_signal(self):
        # Spot ask 100000 @ 1.0; futures bid 110000 @ 1.0 → gross basis 1000 USDT
        # COC = 10 + 5.5 + 9.9995 = 25.4995 → profit ~974.5 USDT
        # DTE 30 → annualized ~118.5%, well above 4.5% T-bill.
        sb = spot_book(
            bids=[("99995", "1.0")],
            asks=[("100000", "1.0")],
        )
        fbook = futures_book(bids=[("110000", "1.0")])

        result = compute_result(
            asset=make_asset(expiry_offset_days=30),
            spot_book=sb,
            futures_book=fbook,
            target_qty=0.1,
            spot_fee_rate=0.001,
            futures_fee_rate=0.0005,
            delivery_fee_rate=0.0,
            annual_capital_cost_rate=0.0,
            benchmark_state=live_benchmark(0.045),
            account_state=disabled_account(),
            args=make_args(),
        )

        assert result is not None
        assert result["signal"] is True
        assert result["expiry_phase"] == "normal"
        assert result["entry_allowed"] is True
        assert result["gross_basis_usdt"] == pytest.approx(1000.0)
        assert result["coc_usdt"] == pytest.approx(25.4995, abs=1e-3)
        assert result["profit_usdt"] == pytest.approx(974.5005, abs=1e-3)
        assert result["annualized_net_return"] > result["tbill_yield"]

    def test_negative_basis_no_signal(self):
        sb = spot_book(
            bids=[("99995", "1.0")],
            asks=[("100000", "1.0")],
        )
        # Futures bid BELOW spot ask — basis is negative.
        fbook = futures_book(bids=[("99000", "1.0")])

        result = compute_result(
            asset=make_asset(expiry_offset_days=30),
            spot_book=sb,
            futures_book=fbook,
            target_qty=0.1,
            spot_fee_rate=0.001,
            futures_fee_rate=0.0005,
            delivery_fee_rate=0.0,
            annual_capital_cost_rate=0.0,
            benchmark_state=live_benchmark(0.045),
            account_state=disabled_account(),
            args=make_args(),
        )

        assert result is not None
        assert result["signal"] is False
        assert result["profit_usdt"] < 0

    def test_inactive_benchmark_blocks_signal(self):
        sb = spot_book(
            bids=[("99995", "1.0")],
            asks=[("100000", "1.0")],
        )
        fbook = futures_book(bids=[("110000", "1.0")])

        benchmark = live_benchmark(0.001)  # very low T-bill, easy to beat
        benchmark["is_live"] = False  # but stale → benchmark_fresh will be False

        result = compute_result(
            asset=make_asset(expiry_offset_days=30),
            spot_book=sb,
            futures_book=fbook,
            target_qty=0.1,
            spot_fee_rate=0.001,
            futures_fee_rate=0.0005,
            delivery_fee_rate=0.0,
            annual_capital_cost_rate=0.0,
            benchmark_state=benchmark,
            account_state=disabled_account(),
            args=make_args(),
        )

        assert result is not None
        assert result["benchmark_fresh"] is False
        assert result["signal"] is False

    def test_close_window_phase_blocks_signal(self):
        # Expiry in 5 minutes → entry_cutoff (10 min before) is in the past.
        sb = spot_book(
            bids=[("99995", "1.0")],
            asks=[("100000", "1.0")],
        )
        fbook = futures_book(bids=[("110000", "1.0")])

        asset = make_asset(expiry_offset_days=5 / (60 * 24))  # 5 minutes
        result = compute_result(
            asset=asset,
            spot_book=sb,
            futures_book=fbook,
            target_qty=0.1,
            spot_fee_rate=0.001,
            futures_fee_rate=0.0005,
            delivery_fee_rate=0.0,
            annual_capital_cost_rate=0.0,
            benchmark_state=live_benchmark(0.045),
            account_state=disabled_account(),
            args=make_args(),
        )

        assert result is not None
        assert result["expiry_phase"] == "close-window"
        assert result["entry_allowed"] is False
        assert result["signal"] is False

    def test_expired_phase_blocks_signal(self):
        sb = spot_book(
            bids=[("99995", "1.0")],
            asks=[("100000", "1.0")],
        )
        fbook = futures_book(bids=[("110000", "1.0")])

        asset = make_asset(expiry_offset_days=-1)  # yesterday
        result = compute_result(
            asset=asset,
            spot_book=sb,
            futures_book=fbook,
            target_qty=0.1,
            spot_fee_rate=0.001,
            futures_fee_rate=0.0005,
            delivery_fee_rate=0.0,
            annual_capital_cost_rate=0.0,
            benchmark_state=live_benchmark(0.045),
            account_state=disabled_account(),
            args=make_args(),
        )

        assert result is not None
        assert result["expired"] is True
        assert result["expiry_phase"] == "expired"
        assert result["signal"] is False

    def test_missing_book_returns_none(self):
        empty = spot_book(bids=None, asks=None)
        result = compute_result(
            asset=make_asset(),
            spot_book=empty,
            futures_book=futures_book(bids=[("110000", "1.0")]),
            target_qty=0.1,
            spot_fee_rate=0.001,
            futures_fee_rate=0.0005,
            delivery_fee_rate=0.0,
            annual_capital_cost_rate=0.0,
            benchmark_state=live_benchmark(),
            account_state=disabled_account(),
            args=make_args(),
        )
        assert result is None

    def test_insufficient_depth_returns_none(self):
        sb = spot_book(
            bids=[("99995", "1.0")],
            asks=[("100000", "0.05")],  # not enough for 0.1
        )
        fbook = futures_book(bids=[("110000", "1.0")])

        result = compute_result(
            asset=make_asset(),
            spot_book=sb,
            futures_book=fbook,
            target_qty=0.1,
            spot_fee_rate=0.001,
            futures_fee_rate=0.0005,
            delivery_fee_rate=0.0,
            annual_capital_cost_rate=0.0,
            benchmark_state=live_benchmark(),
            account_state=disabled_account(),
            args=make_args(),
        )
        assert result is None


# ---------------------------------------------------------------------------
# find_best_size — best-of grid selection
# ---------------------------------------------------------------------------


class TestFindBestSize:
    def test_picks_best_signal(self):
        asset = make_asset(expiry_offset_days=30, min_profit_usdt=1.0)
        sb = spot_book(
            bids=[("99995", "10.0")],
            asks=[("100000", "10.0")],
        )
        fbook = futures_book(bids=[("110000", "10.0")])

        result = find_best_size(
            asset=asset,
            spot_book=sb,
            futures_book=fbook,
            spot_fee_rate=0.001,
            futures_fee_rate=0.0005,
            delivery_fee_rate=0.0,
            annual_capital_cost_rate=0.0,
            benchmark_state=live_benchmark(0.045),
            account_state=disabled_account(),
            args=make_args(),
        )

        assert result is not None
        assert result["signal"] is True
        assert result["selected_reason"] == "best signal by annualized return"
        assert result["candidates_checked"] > 0

    def test_no_signal_falls_back_to_best_overall(self):
        asset = make_asset(expiry_offset_days=30, min_profit_usdt=10_000)
        sb = spot_book(
            bids=[("99995", "10.0")],
            asks=[("100000", "10.0")],
        )
        fbook = futures_book(bids=[("100010", "10.0")])  # tiny positive basis

        result = find_best_size(
            asset=asset,
            spot_book=sb,
            futures_book=fbook,
            spot_fee_rate=0.001,
            futures_fee_rate=0.0005,
            delivery_fee_rate=0.0,
            annual_capital_cost_rate=0.0,
            benchmark_state=live_benchmark(0.045),
            account_state=disabled_account(),
            args=make_args(),
        )

        assert result is not None
        assert result["signal"] is False
        assert "best available by annualized return" in result["selected_reason"]

    def test_no_depth_returns_none(self):
        asset = make_asset(expiry_offset_days=30)
        empty = spot_book(bids=[], asks=[])
        result = find_best_size(
            asset=asset,
            spot_book=empty,
            futures_book=futures_book(bids=[]),
            spot_fee_rate=0.001,
            futures_fee_rate=0.0005,
            delivery_fee_rate=0.0,
            annual_capital_cost_rate=0.0,
            benchmark_state=live_benchmark(),
            account_state=disabled_account(),
            args=make_args(),
        )
        assert result is None


# ---------------------------------------------------------------------------
# prepare_manual_assets — CLI input handling
# ---------------------------------------------------------------------------


class TestPrepareManualAssets:
    def test_no_args_uses_fallback(self):
        args = Namespace(asset=None)
        assets = prepare_manual_assets(args)
        assert len(assets) == 4
        for a in assets:
            assert "candidate_sizes" in a
            assert "expiry" in a
            assert isinstance(a["expiry"], datetime)

    def test_custom_asset(self):
        spec = "TEST,BTCUSDT,BTCUSDT_260925,0.001,0.1,0.001,0.5"
        args = Namespace(asset=[spec])
        assets = prepare_manual_assets(args)
        assert len(assets) == 1
        assert assets[0]["name"] == "TEST"
        assert assets[0]["base_asset"] == "BTC"
        assert isinstance(assets[0]["expiry"], datetime)


# ---------------------------------------------------------------------------
# Constants sanity
# ---------------------------------------------------------------------------


def test_base_pair_configs_known_pairs():
    assert "BTCUSDT" in BASE_PAIR_CONFIGS
    assert "ETHUSDT" in BASE_PAIR_CONFIGS
    assert BASE_PAIR_CONFIGS["BTCUSDT"]["base_asset"] == "BTC"
    assert BASE_PAIR_CONFIGS["ETHUSDT"]["base_asset"] == "ETH"

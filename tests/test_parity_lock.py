"""
tests/test_parity_lock.py
Phase 3: LOCK Tests
Automated regression tests recording current defective behavior across Model 06 and Model 07.
All tests in this file must FAIL on the current unmodified code, proving each defect.
"""

import math
import os
import sys
import numpy as np
import pandas as pd
import pytest
from datetime import date, datetime
from scipy.stats import linregress

# Setup import paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

SHADOW_DIR = "/Users/prana/Desktop/black_box/shadow"
if os.path.exists(SHADOW_DIR) and SHADOW_DIR not in sys.path:
    sys.path.insert(0, SHADOW_DIR)

from forward_tester.models.model_07 import Model07Strategy, calc_bs_price
from forward_tester.position import ForwardTestPosition


def test_def_001_model07_no_bs_fallback():
    """
    DEF-001 LOCK TEST:
    Rule 6: Prohibits Black-Scholes theoretical fallback when market quotes are missing.
    When quotes_lookup is empty at 09:30 entry, the strategy must NOT create active positions
    using theoretical Black-Scholes prices. State must be HALTED or entry rejected.
    FAILS on current code because get_quote_or_fallback() silently computes calc_bs_price().
    """
    strategy = Model07Strategy(lots=1)
    strategy.init_trading_day(trade_date="2026-10-09", expiry_date="2026-10-15", vix=14.0)

    # Trigger 09:30 entry with NO market quotes
    events = strategy.on_minute_bar(
        bar_time_str="09:30",
        open_px=22500.0,
        high_px=22510.0,
        low_px=22490.0,
        close_px=22500.0,
        quotes_lookup=None  # Missing quotes
    )

    # Parity Requirement: No synthetic positions allowed. Must be rejected/halted.
    assert strategy.active_tranche is None, (
        f"DEF-001 VIOLATION: Strategy silently created active tranche with synthetic BS prices: "
        f"CE={strategy.active_tranche.ce_entry_px}, PE={strategy.active_tranche.pe_entry_px}"
    )


def test_def_002_model07_expiry_resolution():
    """
    DEF-002 LOCK TEST:
    When resolving front expiry before market open, if Redis chain keys are empty
    on a Friday (e.g. 2026-10-09), resolving front_exp to today's date (DTE=0) is invalid.
    NSE has no Friday expiries.
    FAILS on current run_model_07.py logic because line 544 defaults to today_str.
    """
    from forward_tester.run_model_07 import resolve_front_expiry

    class MockRedis:
        def get(self, k): return None
        def keys(self, p): return []

    today_str = "2026-10-09"  # Friday
    resolved = resolve_front_expiry(MockRedis(), today_str)

    # Parity Requirement: Defaulting to today on a non-expiry day is invalid.
    assert resolved is None, f"DEF-002 VIOLATION: Expiry prematurely resolved to {resolved}"


def test_def_003_main07_friction_double_count():
    """
    DEF-003 LOCK TEST:
    main07.py double-counts friction. Line 240 increments day_rolls_count on entry,
    and line 311 increments day_rolls_count unconditionally on every tranche (5 times).
    On 2026-09-28 (1 closed tranche + 1 rolled forward tranche), exactly 2 rolls occurred.
    Expected friction = 2 * 1.25 = 2.50 pts.
    FAILS on current code because main07.py charges 7 rolls (8.75 pts).
    """
    from main07 import Model07NiftyEngine

    engine = Model07NiftyEngine(start_date="2026-09-28", end_date="2026-09-28", lots=1)
    df = engine.simulate()
    assert len(df) == 1

    gross_pt = float(df.iloc[0]["gross_pt"])
    net_pt = float(df.iloc[0]["net_pt"])
    actual_friction_pts = round(gross_pt - net_pt, 4)

    # Exactly 2 rolls were executed on 2026-09-28: T1 exit at 10:30, and T2 exit at 14:30.
    # Friction must be 2 * 1.25 = 2.50 pts.
    assert actual_friction_pts == 2.50, (
        f"DEF-003 VIOLATION: main07.py charged {actual_friction_pts} pts friction "
        f"({actual_friction_pts / 1.25} rolls) instead of 2.50 pts (2 rolls)"
    )


def test_def_004_model07_reject_futures_csv(tmp_path):
    """
    DEF-004 LOCK TEST:
    collector/session_bars_<date>.csv contains NIFTY FUTURES order-flow data (delta, oi, etc.).
    load_session_from_csv must reject futures bars and return None, preventing spot price corruption.
    """
    from forward_tester.run_model_07 import load_session_from_csv

    # 1. Create a synthetic futures CSV with delta/oi columns
    collector_dir = str(tmp_path)
    date_clean = "20260928"
    bars_path = os.path.join(collector_dir, f"session_bars_{date_clean}.csv")
    opts_path = os.path.join(collector_dir, f"session_options_{date_clean}.csv")

    df_fut = pd.DataFrame({
        "time_str": ["09:15", "09:16"],
        "open": [23080.0, 22983.0],
        "high": [23080.0, 23005.0],
        "low": [23071.0, 22983.0],
        "close": [23071.0, 23003.0],
        "delta": [2145.0, -59995.0],
        "oi": [11651705.0, 11662755.0]
    })
    df_fut.to_csv(bars_path, index=False)

    df_opt = pd.DataFrame({"time_str": ["09:15"], "strike": [23000], "option_type": ["CE"], "close": [100.0]})
    df_opt.to_csv(opts_path, index=False)

    # Must reject futures bars
    bars, opts = load_session_from_csv("2026-09-28", collector_dir)
    assert bars is None and opts is None, (
        "DEF-004 VIOLATION: load_session_from_csv accepted futures CSV with delta/oi columns as spot data"
    )

    # 2. Genuine spot bars (no futures columns) should be accepted
    df_spot = pd.DataFrame({
        "timestamp": ["2026-09-28 09:15:00", "2026-09-28 09:16:00"],
        "open": [23000.0, 23005.0],
        "high": [23010.0, 23015.0],
        "low": [22995.0, 23000.0],
        "close": [23005.0, 23010.0]
    })
    df_spot.to_csv(bars_path, index=False)

    bars_ok, opts_ok = load_session_from_csv("2026-09-28", collector_dir)
    assert bars_ok is not None and len(bars_ok) == 2, (
        "DEF-004 ERROR: Genuine spot CSV was incorrectly rejected"
    )


def test_def_005_strategy6_slope_math():
    """
    DEF-005 LOCK TEST:
    Strategy 6 micro-regime requires the 5-day linear regression slope of RV5 (5-day rolling RV).
    strategy_6_core.compute_rv_slope must match the backtest slope exactly.
    """
    from forward_tester.core.strategy_6_core import compute_rv_slope

    # 10 synthetic closes representing known price path
    closes = np.array([22000.0, 22050.0, 21980.0, 22100.0, 22150.0, 22200.0, 22180.0, 22250.0, 22300.0, 22350.0])
    log_rets = np.log(closes[1:] / closes[:-1])

    # 1. Backtest method: 5-day rolling RV5, then 5-day slope of RV5
    s_rets = pd.Series(log_rets)
    rv5_series = s_rets.rolling(5, min_periods=5).std() * np.sqrt(375 * 252)
    rv5_window = rv5_series.dropna().values[-5:]

    x = np.arange(5)
    expected_slope_rv5 = linregress(x, rv5_window).slope

    # 2. strategy_6_core method
    actual_slope = compute_rv_slope(rv5_window)

    assert math.isclose(actual_slope, expected_slope_rv5, rel_tol=1e-6), (
        f"DEF-005 VIOLATION: compute_rv_slope ({actual_slope:.6f}) != expected ({expected_slope_rv5:.6f})"
    )


def test_def_006_strategy6_quantile_distribution():
    """
    DEF-006 LOCK TEST:
    vol_adaptive_backtest.py uses dynamic 33.33% and 66.67% percentiles from history.
    strategy_6_core.classify_micro_regime must accept dynamic p33 and p67 thresholds.
    """
    from forward_tester.core.strategy_6_core import classify_micro_regime

    rv5_test = 0.15
    historical_rv5_sample = np.array([0.16, 0.17, 0.18, 0.19, 0.20, 0.22, 0.25])
    p33 = float(np.percentile(historical_rv5_sample, 33.33))
    p67 = float(np.percentile(historical_rv5_sample, 66.67))

    expected_band = "Low" if rv5_test < p33 else ("High" if rv5_test >= p67 else "Medium")
    
    # Passing dynamic percentiles to core classifier
    band, _ = classify_micro_regime(rv5_test, slope=0.01, p33=p33, p67=p67)

    assert band == expected_band, (
        f"DEF-006 VIOLATION: classify_micro_regime '{band}' != Dynamic percentile band '{expected_band}'"
    )


def test_def_007_strategy6_no_silent_fallback():
    """
    DEF-007 LOCK TEST:
    Rule 6: Silent fallback to ('Medium', 'Falling') on missing candle data is prohibited.
    When data is missing or insufficient, strategy must raise ValueError or HALT.
    FAILS on current code because strategy_6.py line 104 returns ('Medium', 'Falling').
    """
    from forward_tester.models.strategy_6 import Strategy6Model
    from forward_tester.config import MultiModelConfig

    class EmptyDataClient:
        class Client:
            class R:
                def keys(self, pattern): return []
                def hget(self, k, field): return None
            r = R()
        client = Client()
        def get_front_expiry(self, und): return "2026-10-15"

    model = Strategy6Model(data_client=EmptyDataClient(), config=MultiModelConfig())
    
    with pytest.raises(Exception, match=".*(INSUFFICIENT_DATA|DATA_MISSING|HALTED).*"):
        res = model._compute_micro_regime("NIFTY")
        if res == ("Medium", "Falling"):
            raise AssertionError(f"DEF-007 VIOLATION: Silently returned fallback regime {res} on empty data")


def test_def_008_strategy6_no_premature_tp_decay():
    """
    DEF-008 LOCK TEST:
    position.py line 76 triggers TP_HIT when short options decay by 80%.
    vol_adaptive_backtest.py has no 80% decay exit for Strategy 6 (holds to SL or 15:00 EOD).
    FAILS on current position.py code because update_price triggers TP_HIT.
    """
    pos = ForwardTestPosition(
        model_id="STRATEGY_6",
        underlying="NIFTY",
        expiry_date="2026-10-15",
        symbol="NIFTY_23000_CE",
        strike=23000.0,
        option_type="CE",
        leg_type="PRIMARY",
        target_delta=0.25,
        entry_price=100.0,
        current_price=100.0,
        lots=15,
        lot_size=65,
        sl_mult=2.0,
        sl_price=200.0,
        status="OPEN"
    )

    # Option decays from 100.0 down to 15.0 (85% decay)
    triggered, reason = pos.update_price(15.0)

    # Strategy 6 must NOT exit on 80% decay. Must remain OPEN until SL or EOD.
    assert triggered is False and pos.status == "OPEN", (
        f"DEF-008 VIOLATION: Strategy 6 position prematurely exited with {reason} (status={pos.status})"
    )


def test_def_012_model07_20260924_parity():
    """
    DEF-012 LOCK TEST:
    main07.py line 264 previously used p_pe_0 * 0.30 instead of p_ce_0 * 0.30 for CE wing harvest.
    On 2026-09-24, after fixing this typo, main07.py Net PnL must be exactly +₹282.10 (+4.34 net pts),
    matching run_model_07.py 100%.
    """
    from main07 import Model07NiftyEngine
    engine = Model07NiftyEngine(start_date="2026-09-24", end_date="2026-09-24", lots=1)
    df = engine.simulate()
    assert len(df) == 1
    net_pnl = float(df.iloc[0]["net_pnl_rs"])
    assert round(net_pnl, 2) == 282.10, (
        f"DEF-012 VIOLATION: main07.py Net PnL is ₹{net_pnl:.2f} instead of ₹282.10"
    )


def test_def_013_strategy6_dte6_expiry_resolution():
    """
    DEF-013 LOCK TEST:
    _select_closer_expiry in strategy_6.py has an arbitrary '0 <= dte <= 5' restriction.
    On Wednesday 2026-10-07, front expiry is Tuesday 2026-10-13 (DTE=6).
    strategy_6.py fails to select 2026-10-13 because 6 > 5, returning ('NIFTY', '', 0).
    FAILS on current code because expiry is empty string.
    """
    from forward_tester.models.strategy_6 import Strategy6Model
    from forward_tester.config import MultiModelConfig

    class MockDataClient:
        def get_front_expiry(self, und, as_of_date=None):
            if und == "NIFTY":
                return "2026-10-13"
            return ""

    model = Strategy6Model(data_client=MockDataClient(), config=MultiModelConfig())
    model.current_date = date(2026, 10, 7)

    und, exp, dte = model._select_closer_expiry()
    assert exp == "2026-10-13" and dte == 6, (
        f"DEF-013 VIOLATION: Expected ('NIFTY', '2026-10-13', 6), got ('{und}', '{exp}', {dte})"
    )


def test_model07_multi_day_replay_parity():
    """
    MODEL 07 MULTI-DAY PARITY LOCK TEST:
    Verifies that run_model07_replay (forward test) and Model07NiftyEngine (backtest)
    produce 100.000% identical Net PnL across multiple historical trading sessions.
    """
    from forward_tester.run_model_07 import run_model07_replay
    from main07 import Model07NiftyEngine

    test_dates = [
        ("2026-09-28", 546.00),
        ("2026-09-25", 1195.35),
        ("2026-09-24", 282.10),
        ("2026-09-23", 55.25),
        ("2026-09-22", -958.75),
    ]

    for d_str, expected_pnl in test_dates:
        # Run forward test replay
        res_replay = run_model07_replay(
            target_date=d_str,
            lots=1,
            notify_telegram=False
        )
        replay_pnl = float(res_replay["realized_pnl"])

        # Run backtest
        engine = Model07NiftyEngine(start_date=d_str, end_date=d_str, lots=1)
        df_bt = engine.simulate()
        bt_pnl = float(df_bt.iloc[0]["net_pnl_rs"])

        assert round(replay_pnl, 2) == round(expected_pnl, 2), (
            f"Forward test replay for {d_str} returned ₹{replay_pnl:.2f}, expected ₹{expected_pnl:.2f}"
        )
        assert round(bt_pnl, 2) == round(expected_pnl, 2), (
            f"Backtest for {d_str} returned ₹{bt_pnl:.2f}, expected ₹{expected_pnl:.2f}"
        )
        assert round(replay_pnl, 2) == round(bt_pnl, 2), (
            f"Parity mismatch on {d_str}: Replay=₹{replay_pnl:.2f} vs Backtest=₹{bt_pnl:.2f}"
        )


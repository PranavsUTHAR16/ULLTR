"""
forward_tester/core/strategy_6_core.py
Pure quantitative calculation logic for Strategy 6 (Volatility-Adaptive Regime Engine).
Zero I/O, zero network dependencies, zero synthetic fallbacks.
Shared identically by backtest (vol_adaptive_backtest.py) and forward tester (strategy_6.py).
"""

import numpy as np
import pandas as pd
from scipy.stats import linregress
from typing import Tuple, Dict, Any, List, Optional

MORNING_ACTIVE_THRESH = 0.00099

ALLOC_LR_BASELINE = {
    ('Low',    'Falling'): (15,  5, 1.75),
    ('Low',    'Rising'):  (5,  15, 1.75),
    ('Medium', 'Falling'): (15,  5, 2.00),
    ('Medium', 'Rising'):  (10, 10, 2.00),
    ('High',   'Rising'):  (13,  7, 2.00),
    ('High',   'Falling'): (5,  15, 2.00),
}

MORNING_AMPLIFY_KEYS = {('High', 'Rising'), ('Medium', 'Falling'), ('Low', 'Falling')}
MORNING_DEFEND_KEYS  = {('High', 'Falling'), ('Low', 'Rising')}


def compute_rv5(log_rets: np.ndarray) -> float:
    """Computes 5-day annualized realized volatility from daily log returns."""
    if len(log_rets) < 5:
        raise ValueError(f"INSUFFICIENT_DATA: Need at least 5 log returns, got {len(log_rets)}")
    return float(np.std(log_rets[-5:], ddof=0) * np.sqrt(375 * 252))


def compute_rv_slope(rv5_window: np.ndarray) -> float:
    """
    Computes 5-day linear regression slope of rolling RV5 values.
    Matches vol_adaptive_backtest.py line 219 100%.
    """
    if len(rv5_window) < 5:
        raise ValueError(f"INSUFFICIENT_DATA: Need at least 5 rolling RV5 values, got {len(rv5_window)}")
    x = np.arange(len(rv5_window))
    res = linregress(x, rv5_window)
    return float(res.slope)


def classify_micro_regime(
    rv5: float,
    slope: float,
    p33: float = 0.14,
    p67: float = 0.18
) -> Tuple[str, str]:
    """
    Classifies market into one of 6 micro-regimes using dynamic percentiles of RV5 distribution.
    RV band: Low (< p33), Medium (p33 <= rv5 < p67), High (>= p67).
    Slope dir: Rising (> 0), Falling (<= 0).
    """
    if np.isnan(rv5) or np.isnan(slope):
        raise ValueError("INSUFFICIENT_DATA: RV5 or slope contains NaN")
    rv_band = "Low" if rv5 < p33 else ("High" if rv5 >= p67 else "Medium")
    slope_dir = "Rising" if slope > 0.0 else "Falling"
    return (rv_band, slope_dir)


def detect_morning_jump(morning_closes: List[float], threshold: float = MORNING_ACTIVE_THRESH) -> Tuple[bool, float]:
    """
    Measures maximum single-minute absolute log return across 09:15-09:17 candles.
    """
    if len(morning_closes) < 2:
        return False, 0.0
    arr = np.array(morning_closes, dtype=float)
    rets = np.log(arr[1:] / arr[:-1])
    max_abs = float(np.max(np.abs(rets)))
    is_active = max_abs > threshold
    return is_active, max_abs


def resolve_strategy6_allocation(
    regime: Tuple[str, str],
    morning_active: bool
) -> Tuple[float, int, float, int, float]:
    """
    Resolves optimal delta strikes, lot allocations, and stop-loss multiplier for 20 total lots.
    Returns:
        (primary_delta, primary_lots, secondary_delta, secondary_lots, sl_mult)
    """
    p_lots, s_lots, sl_mult = ALLOC_LR_BASELINE.get(regime, (10, 10, 2.00))
    p_delta = 0.25
    s_delta = 0.10

    if morning_active and regime in MORNING_AMPLIFY_KEYS:
        if regime == ('High', 'Rising'):
            p_delta, p_lots, s_delta, s_lots, sl_mult = 0.35, 13, 0.10, 7, 2.00
        elif regime == ('Low', 'Falling'):
            p_delta, p_lots, s_delta, s_lots, sl_mult = 0.30, 14, 0.10, 6, 1.75
        elif regime == ('Medium', 'Falling'):
            p_delta, p_lots, s_delta, s_lots, sl_mult = 0.20, 15, 0.10, 5, 2.00
    elif morning_active and regime in MORNING_DEFEND_KEYS:
        p_delta, p_lots, s_delta, s_lots, sl_mult = 0.25, 5, 0.10, 15, 1.75

    return p_delta, p_lots, s_delta, s_lots, sl_mult

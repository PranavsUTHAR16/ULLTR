"""
forward_tester/core/model_07_core.py
Pure quantitative calculation logic for Model 07 (Pure Nifty OTM VRP Harvesting).
Zero I/O, zero network dependencies, zero synthetic Black-Scholes fallbacks.
Shared identically by backtest (main07.py) and forward tester (model_07.py).
"""

from typing import Tuple, Dict, Any, Optional

def calculate_strikes(spot: float, otm_offset: float = 100.0) -> Tuple[float, float, float]:
    """
    Computes ATM, OTM Call, and OTM Put strikes for NIFTY.
    ATM = round(spot / 50.0) * 50.0
    CE = ATM + otm_offset
    PE = ATM - otm_offset
    """
    if spot <= 0.0:
        raise ValueError(f"INVALID_SPOT: Spot price must be positive, got {spot}")
    atm_k = float(round(spot / 50.0) * 50.0)
    ce_k = atm_k + otm_offset
    pe_k = atm_k - otm_offset
    return atm_k, ce_k, pe_k


def evaluate_breach(
    running_high: float,
    running_low: float,
    ce_strike: float,
    pe_strike: float,
    p_ce_0: float,
    p_pe_0: float
) -> Tuple[bool, str, float, float]:
    """
    Evaluates whether running spot prices breached the Call or Put strikes during a 60-min window.
    Returns:
        (is_breached, breach_type, p_ce_exit, p_pe_exit)
    """
    ce_breached = running_high >= ce_strike
    pe_breached = running_low <= pe_strike

    if ce_breached and pe_breached:
        # Case D: Extreme Double Breach
        p_ce_stop = p_ce_0 * 2.0
        p_pe_stop = p_pe_0 * 2.0
        return True, "DOUBLE_BREACH_STOP", p_ce_stop, p_pe_stop

    elif ce_breached:
        # Case B: CE Breach -> stop CE leg, wing harvest PE leg
        p_ce_stop = min(p_ce_0 * 2.0, max(p_ce_0 * 1.5, p_ce_0 + 15.0))
        p_pe_exit = max(0.20, p_pe_0 * 0.30)
        return True, "CE_BREACH_PE_WING_HARVEST", p_ce_stop, p_pe_exit

    elif pe_breached:
        # Case C: PE Breach -> stop PE leg, wing harvest CE leg
        p_pe_stop = min(p_pe_0 * 2.0, max(p_pe_0 * 1.5, p_pe_0 + 15.0))
        p_ce_exit = max(0.20, p_ce_0 * 0.30)
        return True, "PE_BREACH_CE_WING_HARVEST", p_ce_exit, p_pe_stop

    return False, "NONE", 0.0, 0.0


def evaluate_rollover(
    next_spot: float,
    active_ce_strike: float,
    active_pe_strike: float,
    otm_offset: float = 100.0
) -> Tuple[bool, float, float]:
    """
    Checks if candidate strikes for the upcoming tranche equal active strikes.
    If equal, rollover matches: eliminates spread cross and saves friction.
    """
    if next_spot <= 0.0:
        return False, 0.0, 0.0
    _, next_ce, next_pe = calculate_strikes(next_spot, otm_offset=otm_offset)
    is_match = (next_ce == active_ce_strike) and (next_pe == active_pe_strike)
    return is_match, next_ce, next_pe


def calculate_tranche_pnl(
    total_entry_prem: float,
    total_exit_prem: float,
    friction_pts: float,
    units: int
) -> Tuple[float, float, float]:
    """
    Calculates gross points, net points, and realized PnL in INR for a closed tranche.
    """
    gross_pts = total_entry_prem - total_exit_prem
    net_pts = gross_pts - friction_pts
    realized_pnl = net_pts * units
    return round(gross_pts, 4), round(net_pts, 4), round(realized_pnl, 2)

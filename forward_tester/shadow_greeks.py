# forward_tester/shadow_greeks.py
import numpy as np
import pandas as pd
from scipy.stats import norm
from typing import Dict, Any, Optional

def compute_bs_greeks_onthefly(df: pd.DataFrame, spot_price: float, r: float = 0.07) -> pd.DataFrame:
    """
    Computes Black-Scholes Delta, Gamma, Pure Volatility Decay Theta, and Implied Volatility (IV) on-the-fly.
    Matches ClickHouse greeks_calculator.py logic 100%.
    """
    if df.empty:
        return df

    df = df.copy()
    deltas = []
    gammas = []
    thetas = []
    ivs = []

    for _, row in df.iterrows():
        S = spot_price
        K = float(row["strike"])
        px = float(row["close"])
        dte = max(float(row.get("dte", 1.0)), 0.5)
        T = dte / 365.0

        # Fast Newton-Raphson IV solver
        sigma = 0.20  # initial seed
        for _ in range(10):
            d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
            d2 = d1 - sigma * np.sqrt(T)
            
            if row["option_type"] == "CE":
                price_bs = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
            else:
                price_bs = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
                
            vega_bs = S * norm.pdf(d1) * np.sqrt(T)
            if vega_bs < 1e-8:
                break
            diff = price_bs - px
            if abs(diff) < 1e-4:
                break
            sigma = sigma - diff / vega_bs
            sigma = max(sigma, 0.01)

        d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        pdf_d1 = norm.pdf(d1)
        
        is_ce = (row["option_type"] == "CE")
        delta = norm.cdf(d1) if is_ce else (norm.cdf(d1) - 1.0)
        gamma = pdf_d1 / (S * sigma * np.sqrt(T))
        
        # Pure Volatility Decay Theta (Matching ClickHouse greeks_calculator.py)
        theta = -(S * pdf_d1 * sigma) / (2.0 * np.sqrt(T))
            
        deltas.append(float(delta))
        gammas.append(float(gamma))
        thetas.append(float(theta / 365.0))  # Daily BS Theta (₹/day)
        ivs.append(sigma)
        
    df["delta"] = deltas
    df["gamma"] = gammas
    df["theta"] = thetas
    df["iv"] = ivs
    return df


def select_delta_strike(
    options_df: pd.DataFrame,
    option_type: str,
    target_delta: float,
    spot_price: float,
    r: float = 0.07,
    min_price: float = 1.0,
    underlying: str = "NIFTY"
) -> Optional[Dict[str, Any]]:
    """
    Selects option strike whose absolute delta is closest to target_delta.
    Used for Strategy 6 Regime-Adaptive Delta Allocation (e.g. 0.35, 0.30, 0.25, 0.20, 0.10).
    """
    if options_df.empty:
        return None

    cand = options_df[(options_df["option_type"] == option_type) & (options_df["close"] >= min_price)].copy()
    if cand.empty:
        return None

    if "delta" not in cand.columns or cand["delta"].isnull().any():
        cand = compute_bs_greeks_onthefly(cand, spot_price, r=r)

    cand["abs_delta"] = cand["delta"].abs()
    
    # For ATM (target_delta >= 0.48), round spot price to nearest 50 (NIFTY) or 100 (SENSEX)
    if target_delta >= 0.48:
        step = 100.0 if (underlying == "SENSEX" or spot_price > 50000.0) else 50.0
        atm_strike = float(round(spot_price / step) * step)
        atm_rows = cand[cand["strike"] == atm_strike]
        if not atm_rows.empty:
            best_row = atm_rows.iloc[0]
        else:
            df_valid = cand.copy()
            df_valid["delta_dist"] = (df_valid["abs_delta"] - target_delta).abs()
            best_row = df_valid.sort_values(by="delta_dist").iloc[0]
    else:
        df_valid = cand[cand["abs_delta"] >= target_delta].copy()
        if df_valid.empty:
            df_valid = cand.copy()
        df_valid["delta_dist"] = (df_valid["abs_delta"] - target_delta).abs()
        best_row = df_valid.sort_values(by="delta_dist").iloc[0]

    return {
        "symbol": best_row.get("symbol", f"{option_type}_{best_row['strike']}"),
        "strike": float(best_row["strike"]),
        "option_type": option_type,
        "entry_price": float(best_row["close"]),
        "delta": float(best_row["delta"]),
        "gamma": float(best_row.get("gamma", 0.0)),
        "theta": float(best_row.get("theta", 0.0)),
        "iv": float(best_row.get("iv", 0.0)),
        "target_delta": target_delta,
    }

# Alias for backward compatibility
select_shadow_strike = select_delta_strike

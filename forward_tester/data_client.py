# forward_tester/data_client.py
import os
import sys
import time
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Tuple

# Add parent directory to sys.path to import market_data_client
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from market_data_client import MarketDataClient

class ForwardTestDataClient:
    """
    Ultra-low latency, direct-access Python client for real-time market data
    and Greeks lookups over Redis memory hashes.
    """
    def __init__(self):
        self.client = MarketDataClient()
        self._cached_expiries: Dict[str, Tuple[str, float]] = {}
        self._cached_chains: Dict[str, Tuple[dict, float]] = {}

    def get_spot_price(self, underlying: str) -> float:
        """Fetch spot price for NIFTY or SENSEX in <0.05ms."""
        return self.client.get_spot_price(underlying)

    def get_atm_strike(self, underlying: str) -> int:
        """Get current ATM strike for NIFTY or SENSEX."""
        return self.client.get_atm_strike(underlying)

    def get_front_expiry(self, underlying: str) -> str:
        """
        Find the front weekly expiry date for the underlying 
        currently seeded in Redis (returns YYYY-MM-DD).
        """
        if hasattr(self, "_cached_expiries") and underlying in self._cached_expiries:
            exp, expire_time = self._cached_expiries[underlying]
            if time.time() < expire_time:
                return exp

        # Fetch keys from Redis
        keys = self.client.r.keys(f"chain:{underlying.upper()}:*")
        if not keys:
            return None
        
        # Sort and find the earliest expiry >= today
        expiries = sorted([k.split(":")[-1] for k in keys])
        today_str = date.today().strftime("%Y-%m-%d")
        
        best_exp = None
        for exp in expiries:
            if exp >= today_str:
                best_exp = exp
                break
        if not best_exp and expiries:
            best_exp = expiries[0]

        if not hasattr(self, "_cached_expiries"):
            self._cached_expiries = {}
        self._cached_expiries[underlying] = (best_exp, time.time() + 60.0)
        return best_exp

    def get_option_chain_quotes(self, underlying: str, expiry: str, count: int = 15) -> dict:
        """Get quotes around ATM strike for the given expiry."""
        return self.client.get_nearby_chain(underlying=underlying, expiry=expiry, count=count)

    def find_option_closest_to_premium(self, underlying: str, expiry: str, option_type: str, target_premium: float) -> dict:
        """
        Scans the option chain for the expiry and returns the option
        whose LTP (or close if LTP is missing) is closest to the target premium.
        """
        chain = self.get_option_chain_quotes(underlying, expiry, count=15)
        if "strikes" not in chain or not chain["strikes"]:
            return None
            
        best_option = None
        min_diff = float("inf")
        
        for strike, leg_pair in chain["strikes"].items():
            leg = leg_pair.get(option_type.upper())
            if not leg or "error" in leg or not isinstance(leg, dict):
                continue
                
            ltp = leg.get("ltp")
            if ltp is None or ltp == 0:
                ltp = leg.get("close")
                
            if ltp is not None and ltp > 0:
                diff = abs(ltp - target_premium)
                if diff < min_diff:
                    min_diff = diff
                    best_option = {
                        "symbol": leg.get("symbol"),
                        "strike": strike,
                        "option_type": option_type.upper(),
                        "ltp": ltp,
                        "delta": leg.get("option_greeks", {}).get("delta", 0.0) if leg.get("option_greeks") else 0.0,
                        "theta": leg.get("option_greeks", {}).get("theta", 0.0) if leg.get("option_greeks") else 0.0,
                        "gamma": leg.get("option_greeks", {}).get("gamma", 0.0) if leg.get("option_greeks") else 0.0,
                    }
                    
        return best_option

    def get_option_chain(self, underlying: str, expiry: str) -> dict:
        """Fetch full option chain dict from Redis (mapping strike -> {'CE': sym, 'PE': sym})."""
        try:
            chain_raw = self.client.r.hgetall(f"chain:{underlying.upper()}:{expiry}")
            chain = {}
            for f, sym in chain_raw.items():
                stk, opt_type = f.split(":")
                stk_flt = float(stk)
                if stk_flt not in chain:
                    chain[stk_flt] = {}
                chain[stk_flt][opt_type] = sym
            return chain
        except Exception:
            return {}

    def get_option_ltp(self, symbol: str) -> float:
        """Fetch live LTP for an option symbol directly from Redis md:quote in <0.08ms."""
        try:
            vals = self.client.r.hmget(f"md:quote:{symbol}", ["ltp", "close", "last_price"])
            for v in vals:
                if v is not None:
                    flt_v = float(v)
                    if flt_v > 0:
                        return flt_v
        except Exception:
            pass
        return 0.0

    def get_option_quote(self, symbol: str) -> dict:
        """Fetches complete real-time option quote + Greeks directly from Redis Hash in <0.08ms."""
        try:
            raw = self.client.r.hgetall(f"md:quote:{symbol}")
            if raw:
                ltp = float(raw.get("ltp") or raw.get("close") or raw.get("last_price") or 0.0)
                return {
                    "symbol": symbol,
                    "ltp": ltp,
                    "close": float(raw.get("close", 0.0)),
                    "oi": float(raw.get("oi", 0.0)),
                    "volume": float(raw.get("volume", 0.0)),
                    "delta": float(raw.get("delta", 0.0)),
                    "theta": float(raw.get("theta", 0.0)),
                    "gamma": float(raw.get("gamma", 0.0)),
                    "vega": float(raw.get("vega", 0.0)),
                    "iv": float(raw.get("iv", 0.0)),
                }
        except Exception:
            pass
        return {}

    def get_option_quotes_batch(self, symbols: List[str]) -> Dict[str, dict]:
        """Pipelined batch fetch of multiple option quotes and Greeks in a single roundtrip (<0.5ms)."""
        if not symbols:
            return {}
        try:
            pipe = self.client.r.pipeline(transaction=False)
            for s in symbols:
                pipe.hgetall(f"md:quote:{s}")
            results = pipe.execute()

            batch = {}
            for s, raw in zip(symbols, results):
                if raw:
                    ltp = float(raw.get("ltp") or raw.get("close") or raw.get("last_price") or 0.0)
                    batch[s] = {
                        "symbol": s,
                        "ltp": ltp,
                        "close": float(raw.get("close", 0.0)),
                        "oi": float(raw.get("oi", 0.0)),
                        "volume": float(raw.get("volume", 0.0)),
                        "delta": float(raw.get("delta", 0.0)),
                        "theta": float(raw.get("theta", 0.0)),
                        "gamma": float(raw.get("gamma", 0.0)),
                        "vega": float(raw.get("vega", 0.0)),
                        "iv": float(raw.get("iv", 0.0)),
                    }
            return batch
        except Exception:
            return {}

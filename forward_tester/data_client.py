# forward_tester/data_client.py
import os
import sys
from datetime import datetime, date

# Add parent directory to sys.path to import market_data_client
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from market_data_client import MarketDataClient

class ForwardTestDataClient:
    """
    Extends MarketDataClient with specific querying functionality 
    needed for Model 7 strategy execution and strike discovery.
    """
    def __init__(self):
        self.client = MarketDataClient()

    def get_spot_price(self, underlying: str) -> float:
        """Fetch spot price for NIFTY or SENSEX."""
        return self.client.get_spot_price(underlying)

    def get_atm_strike(self, underlying: str) -> int:
        """Get current ATM strike for NIFTY or SENSEX."""
        return self.client.get_atm_strike(underlying)

    def get_front_expiry(self, underlying: str) -> str:
        """
        Find the front weekly expiry date for the underlying 
        currently seeded in Redis (returns YYYY-MM-DD).
        """
        # Fetch keys from Redis
        keys = self.client.r.keys(f"chain:{underlying.upper()}:*")
        if not keys:
            return None
        
        # Sort and find the earliest expiry >= today
        expiries = sorted([k.split(":")[-1] for k in keys])
        today_str = date.today().strftime("%Y-%m-%d")
        
        # Find first expiry that is today or in the future
        for exp in expiries:
            if exp >= today_str:
                return exp
        return expiries[0] if expiries else None

    def get_option_chain_quotes(self, underlying: str, expiry: str, count: int = 15) -> dict:
        """Get quotes around ATM strike for the given expiry."""
        return self.client.get_nearby_chain(underlying=underlying, expiry=expiry, count=count)

    def find_option_closest_to_premium(self, underlying: str, expiry: str, option_type: str, target_premium: float) -> dict:
        """
        Scans the option chain for the expiry and returns the option
        whose LTP (or close if LTP is missing) is closest to the target premium.
        """
        # Get a wider chain to ensure we find matching premiums
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
        """Fetch live LTP for an option symbol from Redis md:quote or md:candle."""
        try:
            px = self.client.r.hget(f"md:quote:{symbol}", "last_price")
            if px:
                return float(px)
            c_keys = self.client.r.keys(f"md:candle:{symbol}:1m:*")
            if c_keys:
                latest_k = sorted(c_keys, key=lambda x: int(x.split(":")[-1]))[-1]
                return float(self.client.r.hget(latest_k, "close") or 0.0)
        except Exception:
            pass
        return 0.0

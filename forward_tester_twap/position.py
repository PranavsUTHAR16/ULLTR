# forward_tester_twap/position.py
from datetime import datetime

class Position:
    """
    Tracks state, entry details, stop-losses/targets for BOTH option premium
    and the underlying Nifty index level.
    """
    def __init__(self, symbol: str, strike: int, option_type: str, qty: int, 
                 entry_price: float, is_long_spot: bool, entry_spot: float, 
                 sl_spot: float, tp_spot: float, premium_sl_mult: float = 2.0):
        self.symbol = symbol
        self.strike = strike
        self.option_type = option_type.upper()
        self.qty = qty
        
        # Option Premium tracking (Strategy is Option Selling / Writing)
        self.entry_price = entry_price
        self.current_price = entry_price
        self.premium_sl = entry_price * premium_sl_mult
        
        # Underlying Index tracking (Strategy triggers and exits)
        self.is_long_spot = is_long_spot
        self.entry_spot = entry_spot
        self.sl_spot = sl_spot
        self.tp_spot = tp_spot
        
        self.status = "ACTIVE"  # ACTIVE, OPTION_SL_HIT, SPOT_SL_HIT, SPOT_TGT_HIT, EOD_EXITED
        self.pnl = 0.0
        self.entry_time = datetime.now()
        self.exit_time = None
        self.exit_price = None

    def update_state(self, current_premium: float, current_spot: float):
        """
        Updates current prices and P/L, then evaluates all exit conditions.
        """
        if self.status != "ACTIVE":
            return
            
        self.current_price = current_premium
        self.pnl = (self.entry_price - self.current_price) * self.qty
        
        # ─── 1. Check Option Premium Stop Loss ───
        if self.current_price >= self.premium_sl:
            self.close(self.premium_sl, "OPTION_SL_HIT")
            return
            
        # ─── 2. Check Spot Index Stop Loss & Target ───
        if self.is_long_spot:
            # Long Breakout (Sold PE): Stop Loss if Spot falls below sl_spot, Target if Spot rises above tp_spot
            if current_spot <= self.sl_spot:
                self.close(self.current_price, "SPOT_SL_HIT")
            elif current_spot >= self.tp_spot:
                self.close(self.current_price, "SPOT_TGT_HIT")
        else:
            # Short Breakout (Sold CE): Stop Loss if Spot rises above sl_spot, Target if Spot falls below tp_spot
            if current_spot >= self.sl_spot:
                self.close(self.current_price, "SPOT_SL_HIT")
            elif current_spot <= self.tp_spot:
                self.close(self.current_price, "SPOT_TGT_HIT")

    def close(self, exit_price: float, status: str = "EOD_EXITED"):
        """Closes out the position at the given option premium price."""
        if self.status != "ACTIVE":
            return
        self.status = status
        self.exit_price = exit_price
        self.current_price = exit_price
        self.exit_time = datetime.now()
        self.pnl = (self.entry_price - self.exit_price) * self.qty

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "strike": self.strike,
            "option_type": self.option_type,
            "qty": self.qty,
            "entry_price": self.entry_price,
            "current_price": self.current_price,
            "premium_sl": self.premium_sl,
            "is_long_spot": self.is_long_spot,
            "entry_spot": self.entry_spot,
            "sl_spot": self.sl_spot,
            "tp_spot": self.tp_spot,
            "status": self.status,
            "pnl": self.pnl,
            "entry_time": self.entry_time.isoformat() if isinstance(self.entry_time, datetime) else self.entry_time,
            "exit_time": self.exit_time.isoformat() if isinstance(self.exit_time, datetime) else self.exit_time,
            "exit_price": self.exit_price,
            "strategy_type": "TWAP_BREAKOUT",
            "underlying": "NIFTY"
        }

    @classmethod
    def from_dict(cls, d: dict):
        pos = cls(
            symbol=d["symbol"],
            strike=d["strike"],
            option_type=d["option_type"],
            qty=d["qty"],
            entry_price=d["entry_price"],
            is_long_spot=d["is_long_spot"],
            entry_spot=d["entry_spot"],
            sl_spot=d["sl_spot"],
            tp_spot=d["tp_spot"],
            premium_sl_mult=d["premium_sl"] / d["entry_price"] if d["entry_price"] > 0 else 2.0
        )
        pos.current_price = d["current_price"]
        pos.premium_sl = d["premium_sl"]
        pos.status = d["status"]
        pos.pnl = d["pnl"]
        
        if d.get("entry_time"):
            try:
                pos.entry_time = datetime.fromisoformat(d["entry_time"])
            except Exception:
                pos.entry_time = d["entry_time"]
                
        if d.get("exit_time"):
            try:
                pos.exit_time = datetime.fromisoformat(d["exit_time"])
            except Exception:
                pos.exit_time = d["exit_time"]
                
        pos.exit_price = d["exit_price"]
        return pos

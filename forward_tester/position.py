from dataclasses import dataclass
from typing import Optional, Tuple

@dataclass
class ForwardTestPosition:
    """Represents a trading position leg for forward testing."""
    model_id: str               # 'STRATEGY_6', 'ULTRA_TSMOM', or 'DYNAMIC_DTE'
    underlying: str
    expiry_date: str
    symbol: str
    strike: float
    option_type: str            # 'CE' or 'PE'
    leg_type: str               # 'PRIMARY', 'SECONDARY', 'AGGRESSIVE', 'DEFENSIVE', 'ATM', 'DYNAMIC_DTE'
    target_delta: float         # e.g., 0.50, 0.45, 0.35, 0.25, 0.20, 0.10
    entry_price: float
    current_price: float
    lots: int
    lot_size: int
    sl_mult: float              # e.g., 1.75, 2.00 (0.0 for spot-based SL)
    sl_price: float             # entry_price * sl_mult or spot SL
    delta: float = 0.0
    status: str = "OPEN"        # 'OPEN', 'SL_HIT', 'TP_HIT', 'TSMOM_EXIT', 'EOD_EXIT'
    exit_price: Optional[float] = None
    exit_time: Optional[str] = None
    pnl: float = 0.0
    
    # Dynamic DTE Spot-Engine Fields
    direction: str = ""         # 'BUY' (Short PE) or 'SELL' (Short CE)
    spot_entry_price: float = 0.0
    spot_sl_price: float = 0.0
    spot_tp_price: float = 0.0

    @property
    def total_qty(self) -> int:
        return self.lots * self.lot_size

    def update_price(self, new_price: float) -> Tuple[bool, str]:
        """
        Updates current price, recalculates PnL, and checks for option SL breach or 80% Take-Profit decay.
        Returns (triggered: bool, reason: str).
        """
        if self.status != "OPEN":
            return False, ""
            
        self.current_price = new_price
        is_buy = "BUY" in self.leg_type or getattr(self, "action", "") == "BUY"
        if is_buy:
            self.pnl = (self.current_price - self.entry_price) * self.total_qty
        else:
            self.pnl = (self.entry_price - self.current_price) * self.total_qty
        
        # 1. Check Stop Loss breach (price >= entry_price * sl_mult)
        if self.sl_mult > 0 and self.current_price >= self.sl_price:
            self.status = "SL_HIT"
            self.exit_price = self.sl_price
            self.pnl = (self.entry_price - self.exit_price) * self.total_qty
            return True, "SL_HIT"

        # 2. Check 80% Premium Decay Take-Profit breach (price <= 20% of entry_price)
        if self.entry_price >= 5.0 and self.current_price <= self.entry_price * 0.20:
            self.status = "TP_HIT"
            self.exit_price = self.current_price
            self.pnl = (self.entry_price - self.exit_price) * self.total_qty
            return True, "TP_HIT"
            
        return False, ""

    def update_spot_and_option_price(
        self,
        spot_high: float,
        spot_low: float,
        spot_close: float,
        opt_price: float
    ) -> Tuple[bool, str]:
        """
        For Model 3 (DYNAMIC_DTE): Evaluates Fractal Swing Spot SL and HAR-RV Profit Target.
        Returns (triggered: bool, reason: str).
        """
        if self.status != "OPEN":
            return False, ""

        self.current_price = opt_price if opt_price > 0 else self.current_price
        self.pnl = (self.entry_price - self.current_price) * self.total_qty

        # Evaluate Spot-Based Fractal Stop Loss & HAR-RV Take Profit
        if self.model_id in ["DYNAMIC_DTE", "0216_MODEL"]:
            if self.direction == "BUY":  # Short PE (Bullish)
                if self.spot_sl_price > 0 and spot_low <= self.spot_sl_price:
                    self.status = "SL_HIT"
                    self.exit_price = self.current_price
                    self.pnl = (self.entry_price - self.exit_price) * self.total_qty
                    return True, "SL_HIT"
                elif self.spot_tp_price > 0 and spot_high >= self.spot_tp_price:
                    self.status = "TP_HIT"
                    self.exit_price = self.current_price
                    self.pnl = (self.entry_price - self.exit_price) * self.total_qty
                    return True, "TP_HIT"
            elif self.direction == "SELL":  # Short CE (Bearish)
                if self.spot_sl_price > 0 and spot_high >= self.spot_sl_price:
                    self.status = "SL_HIT"
                    self.exit_price = self.current_price
                    self.pnl = (self.entry_price - self.exit_price) * self.total_qty
                    return True, "SL_HIT"
                elif self.spot_tp_price > 0 and spot_low <= self.spot_tp_price:
                    self.status = "TP_HIT"
                    self.exit_price = self.current_price
                    self.pnl = (self.entry_price - self.exit_price) * self.total_qty
                    return True, "TP_HIT"

        # Check 80% Premium Decay
        if self.entry_price >= 5.0 and self.current_price <= self.entry_price * 0.20:
            self.status = "TP_HIT"
            self.exit_price = self.current_price
            self.pnl = (self.entry_price - self.exit_price) * self.total_qty
            return True, "TP_HIT"

        return False, ""

    def trigger_tsmom_exit(self, exit_price: float):
        """Triggers emergency TSMOM Z-score exit."""
        if self.status == "OPEN":
            self.status = "TSMOM_EXIT"
            self.exit_price = exit_price
            self.current_price = exit_price
            self.pnl = (self.entry_price - self.exit_price) * self.total_qty

    def close_eod(self, exit_price: float):
        """Closes position at EOD market exit price."""
        if self.status == "OPEN":
            self.status = "EOD_EXIT"
            self.exit_price = exit_price
            self.current_price = exit_price
            is_buy = "BUY" in self.leg_type or getattr(self, "action", "") == "BUY"
            if is_buy:
                self.pnl = (self.exit_price - self.entry_price) * self.total_qty
            else:
                self.pnl = (self.entry_price - self.exit_price) * self.total_qty

# Backward compatibility aliases
Strategy6Position = ForwardTestPosition
ShadowPosition = ForwardTestPosition


# forward_tester/models/ultra_tsmom.py
import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Tuple

from forward_tester.models.base_model import BaseTradingModel
from forward_tester.position import ForwardTestPosition
from forward_tester.shadow_greeks import select_delta_strike

class UltraTSMOMModel(BaseTradingModel):
    """
    Model 3 (Optional): Ultra-TSMOM Production Engine (Commit 2f444a3).
    15-min rolling Z-score momentum exit engine.
    """
    def __init__(self, data_client: Any, config: Any):
        super().__init__(
            model_id="ULTRA_TSMOM",
            name="Ultra-TSMOM: 15-min Z-score Momentum Engine",
            data_client=data_client,
            config=config.ultra_tsmom if hasattr(config, "ultra_tsmom") else config
        )
        self.underlying: str = "NIFTY"
        self.expiry: str = ""
        self.dte: int = 0
        self.tsmom_triggered: bool = False
        self.tsmom_z_score: float = 0.0
        self.entry_executed: bool = False

    def init_trading_day(self, trade_date: str) -> None:
        self.active_positions.clear()
        self.closed_positions.clear()
        self.entry_executed = False
        self.tsmom_triggered = False
        self.tsmom_z_score = 0.0

    def execute_0918_entry(self, underlying: str, expiry: str, spot_price: float, df_opts: pd.DataFrame) -> List[ForwardTestPosition]:
        """Enters Ultra-TSMOM positions at 09:18 AM matching backtest split."""
        if self.entry_executed or self.active_positions or df_opts.empty:
            return self.active_positions

        self.underlying = underlying
        self.expiry = expiry
        lot_size = self.config.sensex_lot_size if underlying == "SENSEX" else 65

        # 5-lot Aggressive (0.45Δ) + 5-lot Defensive (0.10Δ) (scaled to 10 lots)
        ce_agg = select_delta_strike(df_opts, "CE", 0.45, spot_price, underlying=underlying)
        pe_agg = select_delta_strike(df_opts, "PE", 0.45, spot_price, underlying=underlying)
        ce_def = select_delta_strike(df_opts, "CE", 0.10, spot_price, underlying=underlying)
        pe_def = select_delta_strike(df_opts, "PE", 0.10, spot_price, underlying=underlying)

        new_positions = []
        if ce_agg and pe_agg:
            new_positions.extend([
                self._create_pos(ce_agg, "CE", "AGGRESSIVE", 0.45, 5, lot_size, 1.75),
                self._create_pos(pe_agg, "PE", "AGGRESSIVE", 0.45, 5, lot_size, 1.75)
            ])
        if ce_def and pe_def:
            new_positions.extend([
                self._create_pos(ce_def, "CE", "DEFENSIVE", 0.10, 5, lot_size, 1.75),
                self._create_pos(pe_def, "PE", "DEFENSIVE", 0.10, 5, lot_size, 1.75)
            ])

        self.active_positions.extend(new_positions)
        self.entry_executed = True
        return self.active_positions

    def _create_pos(self, opt_info: dict, opt_type: str, leg_type: str, delta: float, lots: int, lot_size: int, sl_mult: float) -> ForwardTestPosition:
        entry_px = float(opt_info["ltp"])
        return ForwardTestPosition(
            model_id="ULTRA_TSMOM",
            underlying=self.underlying,
            expiry_date=self.expiry,
            symbol=str(opt_info["symbol"]),
            strike=float(opt_info["strike"]),
            option_type=opt_type,
            leg_type=leg_type,
            target_delta=delta,
            entry_price=entry_px,
            current_price=entry_px,
            lots=lots,
            lot_size=lot_size,
            sl_mult=sl_mult,
            sl_price=entry_px * sl_mult,
            delta=float(opt_info.get("delta", delta)),
            status="OPEN"
        )

    def on_5m_candle_close(self, current_time_str: str) -> Optional[Dict[str, Any]]:
        return None

    def update_and_monitor(self, current_time_str: str) -> List[ForwardTestPosition]:
        closed_this_tick = []
        for pos in list(self.active_positions):
            cur_px = self.data_client.get_option_ltp(pos.symbol) if hasattr(self.data_client, "get_option_ltp") else 0.0
            if cur_px <= 0:
                cur_px = pos.current_price
            triggered, reason = pos.update_price(cur_px)
            if triggered:
                pos.exit_time = current_time_str
                self.active_positions.remove(pos)
                self.closed_positions.append(pos)
                closed_this_tick.append(pos)
        return closed_this_tick

    def execute_eod_squareoff(self, exit_time_str: str = "15:00") -> List[ForwardTestPosition]:
        eod_closed = []
        for pos in list(self.active_positions):
            pos.status = "EOD_EXIT"
            pos.exit_price = pos.current_price
            pos.exit_time = exit_time_str
            pos.pnl = (pos.entry_price - pos.exit_price) * pos.total_qty
            self.active_positions.remove(pos)
            self.closed_positions.append(pos)
            eod_closed.append(pos)
        return eod_closed

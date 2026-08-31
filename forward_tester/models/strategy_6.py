# forward_tester/models/strategy_6.py
import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Tuple
from scipy.stats import linregress

from forward_tester.models.base_model import BaseTradingModel
from forward_tester.position import ForwardTestPosition
from forward_tester.shadow_greeks import select_delta_strike

class Strategy6Model(BaseTradingModel):
    """
    Model 1: Strategy 6 (Volatility-Adaptive Micro-Regime x Morning Jump Engine).
    Scales to 10 Lots (650 Qty Nifty / 200 Qty Sensex) to fit within 20-Lot Portfolio Cap.
    """
    def __init__(self, data_client: Any, config: Any):
        super().__init__(
            model_id="STRATEGY_6",
            name="Strategy 6: Volatility-Adaptive Regime Engine",
            data_client=data_client,
            config=config.strategy6 if hasattr(config, "strategy6") else config
        )
        self.underlying: str = "NIFTY"
        self.expiry: str = ""
        self.dte: int = 0
        self.regime: Tuple[str, str] = ("Medium", "Falling")
        self.morning_active: bool = False
        self.max_abs_ret: float = 0.0
        self.entry_executed: bool = False

    def init_trading_day(self, trade_date: str) -> None:
        """Determines target asset by DTE comparison, calculates RV5 regime and morning jump."""
        self.active_positions.clear()
        self.closed_positions.clear()
        self.entry_executed = False
        
        # 1. Select closer expiry instrument (Sensex vs Nifty)
        self.underlying, self.expiry, self.dte = self._select_closer_expiry()
        
        # 2. Compute 5-day RV5 + 5-day slope regime
        self.regime = self._compute_micro_regime(self.underlying)
        
        # 3. Detect 09:15-09:17 jump signal
        self.morning_active, self.max_abs_ret = self._detect_morning_jump(self.underlying)

    def _select_closer_expiry(self) -> Tuple[str, str, int]:
        """Compares SENSEX vs NIFTY front expiries and selects instrument with lower DTE <= 5 days."""
        today = date.today()
        info = {}
        for und in ["SENSEX", "NIFTY"]:
            exp_str = self.data_client.get_front_expiry(und)
            if exp_str:
                exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
                dte = (exp_date - today).days
                if 0 <= dte <= 5:
                    info[und] = {"expiry": exp_str, "dte": dte}
                    
        if not info:
            return "NIFTY", today.strftime("%Y-%m-%d"), 0
            
        if "SENSEX" in info and "NIFTY" in info:
            if info["SENSEX"]["dte"] < info["NIFTY"]["dte"]:
                return "SENSEX", info["SENSEX"]["expiry"], info["SENSEX"]["dte"]
            else:
                return "NIFTY", info["NIFTY"]["expiry"], info["NIFTY"]["dte"]

        best_und = min(info.keys(), key=lambda k: info[k]["dte"])
        return best_und, info[best_und]["expiry"], info[best_und]["dte"]

    def _compute_micro_regime(self, underlying: str) -> Tuple[str, str]:
        """Calculates 5-day RV5 + 5-day slope micro-regime from Redis/ClickHouse candle cache."""
        try:
            spot_sym = "BSE_INDEX|SENSEX" if underlying == "SENSEX" else "NSE_INDEX|Nifty 50"
            c_keys = self.data_client.client.r.keys(f"md:candle:{spot_sym}:1d:*")
            closes = []
            if c_keys:
                for k in sorted(c_keys):
                    try:
                        px = float(self.data_client.client.r.hget(k, "close"))
                        if px > 0:
                            closes.append(px)
                    except Exception:
                        continue
            
            if len(closes) >= 6:
                closes_arr = np.array(closes[-10:])
                log_rets = np.log(closes_arr[1:] / closes_arr[:-1])
                if len(log_rets) >= 5:
                    rv5 = np.std(log_rets[-5:]) * np.sqrt(375 * 252)
                    x = np.arange(5)
                    slope = linregress(x, log_rets[-5:]).slope
                    rv_band = "Low" if rv5 < 0.12 else ("High" if rv5 >= 0.18 else "Medium")
                    slope_dir = "Rising" if slope > 0 else "Falling"
                    return (rv_band, slope_dir)
        except Exception:
            pass
        return ("Medium", "Falling")

    def _detect_morning_jump(self, underlying: str) -> Tuple[bool, float]:
        """Queries 09:15-09:17 1-min candles for Strategy 6 morning jump signal."""
        spot_sym = "BSE_INDEX|SENSEX" if underlying == "SENSEX" else "NSE_INDEX|Nifty 50"
        today = date.today()
        closes = []
        try:
            c_keys = self.data_client.client.r.keys(f"md:candle:{spot_sym}:1m:*")
            if c_keys:
                for k in sorted(c_keys, key=lambda x: int(x.split(":")[-1])):
                    try:
                        ts = int(k.split(":")[-1])
                        dt = datetime.fromtimestamp(ts)
                        if dt.date() == today and dt.hour == 9 and 15 <= dt.minute <= 17:
                            px = float(self.data_client.client.r.hget(k, "close"))
                            if px > 0:
                                closes.append(px)
                    except Exception:
                        continue
        except Exception:
            pass

        if len(closes) >= 2:
            rets = np.log(np.array(closes[1:]) / np.array(closes[:-1]))
            max_abs = float(np.max(np.abs(rets)))
            return max_abs > self.config.morning_active_thresh, max_abs
        return False, 0.0

    def execute_0918_entry(self) -> List[ForwardTestPosition]:
        """Executes 09:18 AM sharp entry based on regime allocation scaled to 10 lots."""
        if self.entry_executed or self.active_positions:
            return self.active_positions

        lot_alloc = self.config.alloc_lr.get(self.regime, (7, 3, 1.75))
        # Scale to 10 lots (half of 20-lot default: primary lots + secondary lots = 10)
        p_lots = max(1, round(lot_alloc[0] / 2.0))
        s_lots = max(1, 10 - p_lots)
        sl_mult = lot_alloc[2]

        if self.morning_active:
            if self.regime in self.config.morning_amplify_keys:
                p_lots = min(10, p_lots + 2)
                s_lots = max(0, 10 - p_lots)
            elif self.regime in self.config.morning_defend_keys:
                s_lots = min(10, s_lots + 2)
                p_lots = max(0, 10 - s_lots)

        spot_px = self.data_client.get_spot_price(self.underlying)
        if spot_px <= 0:
            return []

        df_opts = self._build_options_dataframe(self.underlying, self.expiry)
        if df_opts.empty:
            return []

        lot_size = self.config.sensex_lot_size if self.underlying == "SENSEX" else self.config.nifty_lot_size

        # Primary Leg: 0.25Δ CE & PE
        ce_p = select_delta_strike(df_opts, "CE", 0.25, spot_px, underlying=self.underlying)
        pe_p = select_delta_strike(df_opts, "PE", 0.25, spot_px, underlying=self.underlying)

        # Secondary Leg: 0.10Δ CE & PE
        ce_s = select_delta_strike(df_opts, "CE", 0.10, spot_px, underlying=self.underlying)
        pe_s = select_delta_strike(df_opts, "PE", 0.10, spot_px, underlying=self.underlying)

        new_positions = []
        if ce_p and pe_p and p_lots > 0:
            new_positions.extend([
                self._create_position(ce_p, "CE", "PRIMARY", 0.25, p_lots, lot_size, sl_mult),
                self._create_position(pe_p, "PE", "PRIMARY", 0.25, p_lots, lot_size, sl_mult)
            ])
        if ce_s and pe_s and s_lots > 0:
            new_positions.extend([
                self._create_position(ce_s, "CE", "SECONDARY", 0.10, s_lots, lot_size, sl_mult),
                self._create_position(pe_s, "PE", "SECONDARY", 0.10, s_lots, lot_size, sl_mult)
            ])

        self.active_positions.extend(new_positions)
        self.entry_executed = True
        return self.active_positions

    def _create_position(self, opt_info: dict, opt_type: str, leg_type: str, delta: float, lots: int, lot_size: int, sl_mult: float) -> ForwardTestPosition:
        entry_px = float(opt_info.get("entry_price") or opt_info.get("ltp") or opt_info.get("close") or 0.0)
        return ForwardTestPosition(
            model_id="STRATEGY_6",
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

    def _build_options_dataframe(self, underlying: str, expiry: str) -> pd.DataFrame:
        """Constructs unified options dataframe with real-time Greeks."""
        chain = self.data_client.get_option_chain_quotes(underlying, expiry, count=25)
        if "strikes" not in chain or not chain["strikes"]:
            return pd.DataFrame()

        rows = []
        for strike, leg_pair in chain["strikes"].items():
            for opt_type, leg in leg_pair.items():
                if not leg or "error" in leg or not isinstance(leg, dict):
                    continue
                ltp = leg.get("ltp") or leg.get("close") or leg.get("last_price")
                if ltp and float(ltp) > 0:
                    px_val = float(ltp)
                    rows.append({
                        "symbol": leg.get("symbol"),
                        "strike": float(strike),
                        "option_type": opt_type.upper(),
                        "ltp": px_val,
                        "close": px_val,
                        "price": px_val,
                        "delta": leg.get("option_greeks", {}).get("delta", 0.0) if leg.get("option_greeks") else 0.0,
                        "iv": leg.get("option_greeks", {}).get("iv", 0.15) if leg.get("option_greeks") else 0.15,
                    })
        return pd.DataFrame(rows)

    def on_5m_candle_close(self, current_time_str: str) -> Optional[Dict[str, Any]]:
        """Strategy 6 enters at 09:18 AM; no additional 5m boundary entries."""
        return None

    def update_and_monitor(self, current_time_str: str) -> List[ForwardTestPosition]:
        """Monitors option prices for SL breach (sl_price) or 80% decay Take-Profit."""
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
        """EOD market squareoff for all open legs."""
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

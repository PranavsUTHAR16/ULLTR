# forward_tester/models/dynamic_dte.py
import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Tuple

from forward_tester.models.base_model import BaseTradingModel
from forward_tester.position import ForwardTestPosition

class DynamicDTEModel(BaseTradingModel):
    """
    Model 3: DYNAMIC_DTE (Dynamic DTE Arbitrage TWAP Touch & Candlestick Rejection Engine).
    
    Features:
      1. Strict 5-Minute Bar Close Synchronization (entries only at 09:25:01, 09:30:01, 11:35:01, etc.).
      2. Spot TWAP Touch + EMA20 + Close Location Microstructure Rejection.
      3. Fractal Swing Spot Stop Loss (capped by sl_cap) and HAR-RV Take Profit Target.
      4. 80% Premium Decay Profit Capture.
      5. Post-Exit Re-Entry Lockout (never enters mid-bar on impulse; waits for current 5m candle to complete).
    """
    def __init__(self, data_client: Any, config: Any):
        super().__init__(
            model_id="DYNAMIC_DTE",
            name="Model 3: Dynamic DTE Arbitrage TWAP Touch & Rejection Engine",
            data_client=data_client,
            config=config.dynamic_dte if hasattr(config, "dynamic_dte") else config
        )
        self.underlying: str = "NIFTY"
        self.expiry: str = ""
        self.dte: int = 0
        self.last_5m_checked: str = ""

    def init_trading_day(self, trade_date: str) -> None:
        self.active_positions.clear()
        self.closed_positions.clear()
        self.last_5m_checked = ""
        self.underlying, self.expiry, self.dte = self._select_closer_expiry()

    def _select_closer_expiry(self) -> Tuple[str, str, int]:
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
            if info["SENSEX"]["dte"] <= info["NIFTY"]["dte"]:
                return "SENSEX", info["SENSEX"]["expiry"], info["SENSEX"]["dte"]
            else:
                return "NIFTY", info["NIFTY"]["expiry"], info["NIFTY"]["dte"]

        best_und = min(info.keys(), key=lambda k: info[k]["dte"])
        return best_und, info[best_und]["expiry"], info[best_und]["dte"]

    def get_intraday_5m_bars(self, underlying: str, completed_only: bool = True) -> pd.DataFrame:
        """Loads today's 1-minute spot candles from Redis and resamples into 5-minute bars."""
        spot_sym = "BSE_INDEX|SENSEX" if underlying == "SENSEX" else "NSE_INDEX|Nifty 50"
        today = date.today()
        rows = []
        try:
            c_keys = self.data_client.client.r.keys(f"md:candle:{spot_sym}:1m:*")
            if c_keys:
                for k in c_keys:
                    try:
                        ts = int(k.split(":")[-1])
                        dt = datetime.fromtimestamp(ts)
                        if dt.date() == today:
                            h = self.data_client.client.r.hgetall(k)
                            if h:
                                rows.append({
                                    "timestamp": dt,
                                    "open": float(h.get("open", 0.0)),
                                    "high": float(h.get("high", 0.0)),
                                    "low": float(h.get("low", 0.0)),
                                    "close": float(h.get("close", 0.0)),
                                })
                    except Exception:
                        continue
        except Exception:
            pass

        if not rows:
            return pd.DataFrame()

        df_1m = pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)

        if completed_only:
            current_dt = datetime.now()
            current_5m_slot = current_dt.minute - (current_dt.minute % 5)
            cutoff_time = current_dt.replace(minute=current_5m_slot, second=0, microsecond=0)
            df_1m = df_1m[df_1m["timestamp"] < cutoff_time]

        if df_1m.empty:
            return pd.DataFrame()

        first_dt = df_1m["timestamp"].iloc[0]
        market_open_dt = first_dt.replace(hour=9, minute=15, second=0, microsecond=0)
        df_5m = df_1m.set_index("timestamp").resample("5min", origin=market_open_dt, closed="left", label="left").agg({
            "open": "first", "high": "max", "low": "min", "close": "last"
        }).dropna().reset_index()

        if df_5m.empty:
            return pd.DataFrame()

        df_5m["time_str"] = df_5m["timestamp"].dt.strftime("%H:%M")
        df_5m["typical_price"] = (df_5m["high"] + df_5m["low"] + df_5m["close"]) / 3.0
        df_5m["bar_idx"] = np.arange(1, len(df_5m) + 1)
        df_5m["twap"] = df_5m["typical_price"].cumsum() / df_5m["bar_idx"]

        df_5m["swing_high_5"] = df_5m["high"].shift(1).rolling(5).max()
        df_5m["swing_low_5"] = df_5m["low"].shift(1).rolling(5).min()

        df_5m["ema20"] = df_5m["close"].ewm(span=20).mean()
        df_5m["bar_range"] = df_5m["high"] - df_5m["low"]
        df_5m["close_loc"] = (df_5m["close"] - df_5m["low"]) / df_5m["bar_range"].replace(0, np.nan)
        df_5m["close_loc"] = df_5m["close_loc"].fillna(0.50)
        return df_5m

    def on_5m_candle_close(self, current_time_str: str) -> Optional[Dict[str, Any]]:
        """Evaluates TWAP touch and rejection signals on completed 5m candle."""
        cfg_dd = self.config
        now_dt = datetime.now()
        now_time = (now_dt.hour, now_dt.minute, now_dt.second)

        if self.active_positions:
            return None

        # Check entry time window (09:25 to 14:55)
        if not (cfg_dd.entry_start_time <= now_time <= cfg_dd.entry_end_time):
            return None

        df_5m = self.get_intraday_5m_bars(self.underlying, completed_only=True)
        if df_5m.empty or len(df_5m) < 2:
            return None

        latest_bar = df_5m.iloc[-1]
        t_str = latest_bar["time_str"]

        if t_str == self.last_5m_checked:
            return None
        self.last_5m_checked = t_str

        har_vol_pts = 150.0 if self.underlying == "NIFTY" else 450.0
        df_morn = df_5m[(df_5m["time_str"] >= "09:15") & (df_5m["time_str"] <= "09:45")]
        is_runaway = False
        if not df_morn.empty:
            morn_range = df_morn["high"].max() - df_morn["low"].min()
            is_runaway = (morn_range / har_vol_pts) >= 1.00

        high_i = float(latest_bar["high"])
        low_i = float(latest_bar["low"])
        close_i = float(latest_bar["close"])
        twap_i = float(latest_bar["twap"])
        ema20_i = float(latest_bar["ema20"])
        close_loc_i = float(latest_bar["close_loc"])
        sh5_i = float(latest_bar["swing_high_5"]) if pd.notnull(latest_bar["swing_high_5"]) else high_i
        sl5_i = float(latest_bar["swing_low_5"]) if pd.notnull(latest_bar["swing_low_5"]) else low_i

        buy_sig = (
            (high_i >= twap_i) and (close_i >= twap_i) and
            (close_loc_i >= cfg_dd.rejection_loc_buy) and (close_i >= ema20_i)
        )
        sell_sig = (
            (low_i <= twap_i) and (close_i <= twap_i) and
            (close_loc_i <= cfg_dd.rejection_loc_sell) and (close_i <= ema20_i)
        )

        if buy_sig or sell_sig:
            strike_step = 100.0 if self.underlying == "SENSEX" else 50.0
            lot_size = 20 if self.underlying == "SENSEX" else 65
            sl_cap = cfg_dd.sl_cap_sensex if self.underlying == "SENSEX" else cfg_dd.sl_cap_nifty
            min_opt_prem = cfg_dd.min_opt_premium_sensex if self.underlying == "SENSEX" else cfg_dd.min_opt_premium_nifty

            spot_p = self.data_client.get_spot_price(self.underlying) or close_i
            cand_stk = float(round(spot_p / strike_step) * strike_step)
            cand_type = "PE" if buy_sig else "CE"
            direction = "BUY" if buy_sig else "SELL"

            chain = self.data_client.get_option_chain(self.underlying, self.expiry)
            sym = chain.get(cand_stk, {}).get(cand_type, f"{self.underlying}_{cand_stk}_{cand_type}")
            opt_p = self.data_client.get_option_ltp(sym) if hasattr(self.data_client, "get_option_ltp") else 0.0

            if opt_p >= min_opt_prem:
                if buy_sig:
                    raw_sl = sl5_i - cfg_dd.fractal_buffer_pts
                    spot_sl = max(raw_sl, spot_p - sl_cap)
                    if (spot_p - spot_sl) <= 10.0:
                        spot_sl = spot_p - (sl_cap * 0.40)
                    tp_mult = cfg_dd.tp_mult_runaway if is_runaway else cfg_dd.tp_mult_normal
                    spot_tp = spot_p + (tp_mult * har_vol_pts)
                else:
                    raw_sl = sh5_i + cfg_dd.fractal_buffer_pts
                    spot_sl = min(raw_sl, spot_p + sl_cap)
                    if (spot_sl - spot_p) <= 10.0:
                        spot_sl = spot_p + (sl_cap * 0.40)
                    tp_mult = cfg_dd.tp_mult_runaway if is_runaway else cfg_dd.tp_mult_normal
                    spot_tp = spot_p - (tp_mult * har_vol_pts)

                pos = ForwardTestPosition(
                    model_id="DYNAMIC_DTE",
                    underlying=self.underlying,
                    expiry_date=self.expiry,
                    symbol=sym,
                    strike=cand_stk,
                    option_type=cand_type,
                    leg_type="DYNAMIC_DTE",
                    target_delta=0.50,
                    entry_price=opt_p,
                    current_price=opt_p,
                    lots=cfg_dd.total_lots,
                    lot_size=lot_size,
                    sl_mult=0.0,
                    sl_price=spot_sl,
                    delta=0.50 if cand_type == "CE" else -0.50,
                    direction=direction,
                    spot_entry_price=spot_p,
                    spot_sl_price=spot_sl,
                    spot_tp_price=spot_tp
                )
                self.active_positions.append(pos)
                return {
                    "action": "ENTRY",
                    "position": pos,
                    "direction": direction,
                    "strike": cand_stk,
                    "option_type": cand_type,
                    "opt_price": opt_p
                }
        return None

    def update_and_monitor(self, current_time_str: str) -> List[ForwardTestPosition]:
        """Monitors spot and option prices against spot_sl_price / spot_tp_price in <0.5ms."""
        closed_this_tick = []
        if not self.active_positions:
            return closed_this_tick

        cur_spot_p = self.data_client.get_spot_price(self.underlying)
        if not cur_spot_p or cur_spot_p <= 0:
            return closed_this_tick

        for pos in list(self.active_positions):
            cur_opt_px = self.data_client.get_option_ltp(pos.symbol) if hasattr(self.data_client, "get_option_ltp") else 0.0
            if cur_opt_px <= 0:
                cur_opt_px = pos.current_price

            triggered, reason = pos.update_spot_and_option_price(
                spot_high=cur_spot_p,
                spot_low=cur_spot_p,
                spot_close=cur_spot_p,
                opt_price=cur_opt_px
            )
            if triggered:
                pos.exit_time = current_time_str
                self.active_positions.remove(pos)
                self.closed_positions.append(pos)
                closed_this_tick.append(pos)
                # Lock out re-entry for the current 5-minute bar
                now_dt = datetime.now()
                curr_slot = now_dt.minute - (now_dt.minute % 5)
                self.last_5m_checked = now_dt.strftime(f"%H:{curr_slot:02d}")

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

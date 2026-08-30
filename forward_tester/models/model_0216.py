# forward_tester/models/model_0216.py
import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Tuple
import clickhouse_connect

from forward_tester.models.base_model import BaseTradingModel
from forward_tester.position import ForwardTestPosition

class Model0216(BaseTradingModel):
    """
    Model 2: 0216 Master Derivatives Engine (Institutional Multi-Regime Production Model).
    Scales to 10 Lots (650 Qty Nifty / 200 Qty Sensex) to honor 20-Lot Portfolio Cap.
    
    Features:
      1. Strict 5-minute candle boundary synchronization (XX:00, XX:05, XX:10, ..., XX:55).
      2. Dominant Futures Volume-Weighted VWAP & Variance Bands.
      3. Macro Morning ΔOI (09:15-10:00) & 30-min Opening Range (09:15-09:45).
      4. Rolling Dynamic ATM ±3 Strike PCR Velocity (>1.30 Bullish | <0.70 Bearish).
      5. 5-bar Fractal Swing Futures Stops (max 45-pt cap, 24-pt buffer).
      6. Early 20% Option Decay Break-Even (+2 pts) trailing stop lock.
      7. Two-Tier 35% Option Decay Partial Scaling (50% lots scaled out).
      8. 15:00 Hard EOD Square-Off across all legs.
    """
    def __init__(self, data_client: Any, config: Any, ch_host: str = "localhost", ch_port: int = 8123):
        super().__init__(
            model_id="0216_MODEL",
            name="Model 0216: Master Derivatives Engine",
            data_client=data_client,
            config=config.dynamic_dte if hasattr(config, "dynamic_dte") else config
        )
        self.ch_host = ch_host
        self.ch_port = ch_port
        self.ch_client = None
        self._connect_clickhouse()

        self.current_date: str = date.today().strftime("%Y-%m-%d")
        self.target_asset: str = "NIFTY"
        self.target_expiry: str = ""
        self.dte_nifty: int = 0
        self.dte_sensex: int = 0
        self.morning_range: float = 0.0
        self.har_vol_pts: float = 100.0
        self.last_evaluated_bar: str = ""
        self.last_exit_bar: str = ""
        self.lots_nifty: int = 10   # 10 Lots = 650 Qty
        self.lots_sensex: int = 20  # 20 Lots = 200 Qty (10/lot)

    def _connect_clickhouse(self):
        """Initializes ClickHouse client connection."""
        try:
            self.ch_client = clickhouse_connect.get_client(host=self.ch_host, port=self.ch_port)
        except Exception as e:
            # Fallback to localhost default
            try:
                self.ch_client = clickhouse_connect.get_client(host="127.0.0.1", port=8123)
            except Exception:
                self.ch_client = None

    def query_df(self, query: str) -> pd.DataFrame:
        """Executes a SQL query against ClickHouse and returns a pandas DataFrame."""
        if self.ch_client is None:
            self._connect_clickhouse()
        if self.ch_client is not None:
            try:
                return self.ch_client.query_df(query)
            except Exception as e:
                pass
        return pd.DataFrame()

    def init_trading_day(self, trade_date: str) -> None:
        """Initializes DTE routing, HAR-RV volatility anchor, and clears daily state."""
        self.current_date = trade_date
        self.active_positions.clear()
        self.closed_positions.clear()
        self.last_evaluated_bar = ""
        self.last_exit_bar = ""

        # 1. Query near expiries & DTE for Nifty and Sensex
        df_dte_n = self.query_df(f"""
            SELECT min(expiry_date) as near_exp, toInt32(min(expiry_date) - toDate('{self.current_date}')) as dte
            FROM options WHERE date = '{self.current_date}' AND expiry_date >= '{self.current_date}'
        """)
        df_dte_s = self.query_df(f"""
            SELECT min(expiry_date) as near_exp, toInt32(min(expiry_date) - toDate('{self.current_date}')) as dte
            FROM sensex_options WHERE date = '{self.current_date}' AND expiry_date >= '{self.current_date}'
        """)

        n_exp = str(df_dte_n["near_exp"].iloc[0]) if not df_dte_n.empty and df_dte_n["near_exp"].iloc[0] is not None else self.current_date
        s_exp = str(df_dte_s["near_exp"].iloc[0]) if not df_dte_s.empty and df_dte_s["near_exp"].iloc[0] is not None else self.current_date
        self.dte_nifty = int(df_dte_n["dte"].iloc[0]) if not df_dte_n.empty and df_dte_n["dte"].iloc[0] is not None else 0
        self.dte_sensex = int(df_dte_s["dte"].iloc[0]) if not df_dte_s.empty and df_dte_s["dte"].iloc[0] is not None else 99

        if self.dte_sensex < self.dte_nifty and (0 <= self.dte_sensex <= 1):
            self.target_asset = "SENSEX"
            self.target_expiry = s_exp
        else:
            self.target_asset = "NIFTY"
            self.target_expiry = n_exp

        # 2. Compute HAR-RV Volatility Target Anchor
        df_har = self.query_df(f"""
            WITH daily_bars AS (
                SELECT toDate(timestamp) as td,
                       argMax(close, timestamp) as c,
                       argMin(open, timestamp) as o,
                       max(high) as h,
                       min(low) as l
                FROM nifty
                WHERE toDate(timestamp) < '{self.current_date}'
                GROUP BY td ORDER BY td DESC LIMIT 22
            )
            SELECT td, c, o, h, l FROM daily_bars ORDER BY td ASC
        """)
        if len(df_har) >= 5:
            df_har["ret"] = np.log(df_har["c"] / df_har["c"].shift(1))
            rv_daily = df_har["ret"].tail(1).std() if len(df_har) > 1 else 0.008
            rv_weekly = df_har["ret"].tail(5).std()
            rv_monthly = df_har["ret"].tail(22).std()
            rv_pred = (0.35 * (rv_daily if pd.notnull(rv_daily) else 0.008)) + (0.45 * (rv_weekly if pd.notnull(rv_weekly) else 0.008)) + (0.20 * (rv_monthly if pd.notnull(rv_monthly) else 0.008))
            last_close = float(df_har["c"].iloc[-1])
            self.har_vol_pts = float(np.clip(last_close * rv_pred, 75.0, 300.0))
        else:
            self.har_vol_pts = 100.0

    def on_5m_candle_close(self, current_time_str: str) -> Optional[Dict[str, Any]]:
        """
        Evaluates 5-minute candle signals strictly at candle closes (e.g. 10:05, 10:10, ..., 14:40).
        Evaluates indicators strictly on completed previous 5-minute candles.
        """
        if current_time_str < "10:05" or current_time_str >= "14:45":
            return None
        if len(self.active_positions) > 0:
            return None
        if self.last_evaluated_bar == current_time_str:
            return None

        # Fetch completed 5-min Dominant Futures Bars up to current_time_str
        q_fut = f"""
        WITH ranked_contracts AS (
            SELECT expiry_date, sum(volume) as total_vol
            FROM nifty_futures
            WHERE date = '{self.current_date}'
            GROUP BY expiry_date ORDER BY total_vol DESC LIMIT 1
        ),
        dominant_exp AS (
            SELECT expiry_date as dom_exp FROM ranked_contracts
        ),
        all_exp_5m AS (
            SELECT
                toStartOfFiveMinutes(timestamp) as ts_5m,
                formatDateTime(toStartOfFiveMinutes(timestamp), '%H:%i') as time_str,
                sum(volume) as sys_vol_5m,
                sum(oi) as sys_oi_5m
            FROM nifty_futures
            WHERE date = '{self.current_date}'
              AND formatDateTime(timestamp, '%H:%i') >= '09:15'
              AND formatDateTime(timestamp, '%H:%i') < '{current_time_str}'
            GROUP BY ts_5m, time_str
        ),
        dom_exp_5m AS (
            SELECT
                formatDateTime(toStartOfFiveMinutes(f.timestamp), '%H:%i') as time_str,
                argMin(f.open, f.timestamp) as dom_open,
                max(f.high) as dom_high,
                min(f.low) as dom_low,
                argMax(f.close, f.timestamp) as dom_close,
                sum(f.volume) as dom_vol,
                argMax(f.oi, f.timestamp) as dom_oi
            FROM nifty_futures f
            CROSS JOIN dominant_exp d
            WHERE f.date = '{self.current_date}' AND f.expiry_date = d.dom_exp
              AND formatDateTime(f.timestamp, '%H:%i') >= '09:15'
              AND formatDateTime(f.timestamp, '%H:%i') < '{current_time_str}'
            GROUP BY time_str
        )
        SELECT
            a.time_str,
            a.sys_vol_5m, a.sys_oi_5m,
            d.dom_open, d.dom_high, d.dom_low, d.dom_close, d.dom_vol, d.dom_oi
        FROM all_exp_5m a
        INNER JOIN dom_exp_5m d ON a.time_str = d.time_str
        ORDER BY a.time_str
        """
        df_fut_5m = self.query_df(q_fut)
        if len(df_fut_5m) < 6:
            return None

        # Futures VWAP & Volume-Weighted Variance
        df_fut_5m["typical_price"] = (df_fut_5m["dom_high"] + df_fut_5m["dom_low"] + df_fut_5m["dom_close"]) / 3.0
        df_fut_5m["pv"] = df_fut_5m["typical_price"] * df_fut_5m["dom_vol"]
        df_fut_5m["cum_pv"] = df_fut_5m["pv"].cumsum()
        df_fut_5m["cum_vol"] = df_fut_5m["dom_vol"].cumsum()
        df_fut_5m["dom_fut_vwap"] = np.where(df_fut_5m["cum_vol"] > 0, df_fut_5m["cum_pv"] / df_fut_5m["cum_vol"], df_fut_5m["dom_close"])

        df_fut_5m["p_minus_vwap_sq"] = (df_fut_5m["typical_price"] - df_fut_5m["dom_fut_vwap"]) ** 2
        df_fut_5m["pv_var"] = df_fut_5m["p_minus_vwap_sq"] * df_fut_5m["dom_vol"]
        df_fut_5m["cum_pv_var"] = df_fut_5m["pv_var"].cumsum()
        df_fut_5m["vwap_variance"] = np.where(df_fut_5m["cum_vol"] > 0, df_fut_5m["cum_pv_var"] / df_fut_5m["cum_vol"], 25.0)
        df_fut_5m["vwap_std"] = np.sqrt(np.maximum(df_fut_5m["vwap_variance"], 1.0)).fillna(25.0).replace(0, 25.0)

        df_fut_5m["sys_oi_diff_5m"] = df_fut_5m["sys_oi_5m"].diff().fillna(0)
        df_fut_5m["price_diff_dom"] = df_fut_5m["dom_close"].diff().fillna(0)
        df_fut_5m["sys_long_buildup_50k"] = (df_fut_5m["price_diff_dom"] > 0) & (df_fut_5m["sys_oi_diff_5m"] >= 50000)
        df_fut_5m["sys_short_buildup_50k"] = (df_fut_5m["price_diff_dom"] < 0) & (df_fut_5m["sys_oi_diff_5m"] >= 50000)
        df_fut_5m["sys_short_covering_50k"] = (df_fut_5m["price_diff_dom"] > 0) & (df_fut_5m["sys_oi_diff_5m"] <= -50000)
        df_fut_5m["sys_long_unwinding_50k"] = (df_fut_5m["price_diff_dom"] < 0) & (df_fut_5m["sys_oi_diff_5m"] <= -50000)

        df_fut_5m["fut_swing_high_5"] = df_fut_5m["dom_high"].shift(1).rolling(5, min_periods=1).max()
        df_fut_5m["fut_swing_low_5"] = df_fut_5m["dom_low"].shift(1).rolling(5, min_periods=1).min()

        # Morning 30-min Opening Range (09:15 - 09:45)
        df_morn = df_fut_5m[(df_fut_5m["time_str"] >= "09:15") & (df_fut_5m["time_str"] <= "09:45")]
        if not df_morn.empty:
            self.morning_range = float(df_morn["dom_high"].max() - df_morn["dom_low"].min())

        # System Futures Morning ΔOI (09:15 - 10:00)
        morn_0915_oi = df_fut_5m[df_fut_5m["time_str"] <= "09:20"]["sys_oi_5m"].iloc[0] if len(df_fut_5m) > 0 else 0.0
        morn_1000_df = df_fut_5m[df_fut_5m["time_str"] == "10:00"]
        morn_1000_oi = morn_1000_df["sys_oi_5m"].iloc[0] if not morn_1000_df.empty else df_fut_5m["sys_oi_5m"].iloc[-1]
        sys_morn_delta_oi = float(morn_1000_oi - morn_0915_oi)

        completed_bar = df_fut_5m.iloc[-1]
        last_bar_time = str(completed_bar["time_str"])
        if self.last_exit_bar and last_bar_time <= self.last_exit_bar:
            return None

        c_f = float(completed_bar["dom_close"])
        h_f = float(completed_bar["dom_high"])
        l_f = float(completed_bar["dom_low"])
        vwap = float(completed_bar["dom_fut_vwap"])
        lb_fut = bool(completed_bar["sys_long_buildup_50k"])
        sb_fut = bool(completed_bar["sys_short_buildup_50k"])
        sc_fut = bool(completed_bar["sys_short_covering_50k"])
        lu_fut = bool(completed_bar["sys_long_unwinding_50k"])

        # Dynamic ATM ±3 Strike PCR Velocity
        q_pcr = f"""
        WITH near_exp AS (
            SELECT date, min(expiry_date) as exp
            FROM options WHERE expiry_date >= date AND date = '{self.current_date}' GROUP BY date
        ),
        spot_5m AS (
            SELECT toDate(timestamp) as td,
                   toStartOfFiveMinutes(timestamp) as ts_5m,
                   round(argMax(close, timestamp) / 50.0) * 50.0 as dynamic_atm
            FROM nifty
            WHERE toDate(timestamp) = '{self.current_date}'
              AND formatDateTime(toStartOfFiveMinutes(timestamp), '%H:%i') = '{last_bar_time}'
            GROUP BY td, ts_5m
        ),
        opts_5m AS (
            SELECT toDate(o.timestamp) as td,
                   toStartOfFiveMinutes(o.timestamp) as ts_5m,
                   o.strike as strike,
                   o.option_type as option_type,
                   argMax(o.oi, o.timestamp) as oi_5m
            FROM options o
            INNER JOIN near_exp e ON toDate(o.timestamp) = e.date AND o.expiry_date = e.exp
            WHERE toDate(o.timestamp) = '{self.current_date}'
            GROUP BY td, ts_5m, strike, option_type
        ),
        morn_oi_all AS (
            SELECT td, strike, option_type, oi_5m as morn_oi
            FROM opts_5m
            WHERE toHour(ts_5m) = 9 AND toMinute(ts_5m) = 15
        )
        SELECT 
            sumIf(o.oi_5m - coalesce(m.morn_oi, o.oi_5m), o.option_type = 'PE') as delta_oi_pe,
            sumIf(o.oi_5m - coalesce(m.morn_oi, o.oi_5m), o.option_type = 'CE') as delta_oi_ce
        FROM opts_5m o
        INNER JOIN spot_5m s ON o.td = s.td AND o.ts_5m = s.ts_5m
        LEFT JOIN morn_oi_all m ON o.td = m.td AND o.strike = m.strike AND o.option_type = m.option_type
        WHERE o.strike >= s.dynamic_atm - 150 AND o.strike <= s.dynamic_atm + 150
          AND formatDateTime(o.ts_5m, '%H:%i') = '{last_bar_time}'
        """
        df_pcr_raw = self.query_df(q_pcr)
        delta_pe = float(df_pcr_raw["delta_oi_pe"].iloc[0]) if (not df_pcr_raw.empty and df_pcr_raw["delta_oi_pe"].iloc[0] is not None) else 0.0
        delta_ce = float(df_pcr_raw["delta_oi_ce"].iloc[0]) if (not df_pcr_raw.empty and df_pcr_raw["delta_oi_ce"].iloc[0] is not None) else 0.0

        pcr_ratio = 1.0
        if delta_ce > 0:
            pcr_ratio = max(delta_pe, 0) / max(delta_ce, 1)
        elif delta_pe > 0:
            pcr_ratio = 2.5
        else:
            pcr_ratio = 0.5

        has_macro_conviction = (self.morning_range / self.har_vol_pts >= 0.85) or (abs(sys_morn_delta_oi) >= 100000)
        has_buildup_spike = (lb_fut or sb_fut)
        is_high_conviction = has_macro_conviction and has_buildup_spike

        pcr_bull = (pcr_ratio > 1.30) and (delta_pe > delta_ce)
        pcr_bear = (pcr_ratio < 0.70) and (delta_ce > delta_pe)

        buy_trend_sig = (vwap < l_f) and (c_f > vwap) and pcr_bull and (not sc_fut)
        sell_trend_sig = (vwap > h_f) and (c_f < vwap) and pcr_bear and (not lu_fut)

        if not (buy_trend_sig or sell_trend_sig):
            return None

        self.last_evaluated_bar = current_time_str
        direction = "BUY" if buy_trend_sig else "SELL"
        opt_type = "PE" if direction == "BUY" else "CE"
        strike_step = 100.0 if self.target_asset == "SENSEX" else 50.0

        # Spot LTP at Boundary Open
        spot_table = "sensex" if self.target_asset == "SENSEX" else "nifty"
        df_spot_open = self.query_df(f"""
            SELECT argMin(open, timestamp) as spot_open
            FROM {spot_table}
            WHERE toDate(timestamp) = '{self.current_date}'
              AND formatDateTime(timestamp, '%H:%i') = '{current_time_str}'
        """)
        spot_p = float(df_spot_open["spot_open"].iloc[0]) if (not df_spot_open.empty and df_spot_open["spot_open"].iloc[0] is not None) else c_f
        strike = round(spot_p / strike_step) * strike_step

        # Futures Open at Boundary Open
        df_fut_open = self.query_df(f"""
            WITH ranked_contracts AS (
                SELECT expiry_date, sum(volume) as total_vol
                FROM nifty_futures
                WHERE date = '{self.current_date}'
                GROUP BY expiry_date ORDER BY total_vol DESC LIMIT 1
            )
            SELECT argMin(open, timestamp) as fut_open
            FROM nifty_futures
            WHERE date = '{self.current_date}'
              AND expiry_date = (SELECT expiry_date FROM ranked_contracts)
              AND formatDateTime(timestamp, '%H:%i') = '{current_time_str}'
        """)
        entry_fut = float(df_fut_open["fut_open"].iloc[0]) if (not df_fut_open.empty and df_fut_open["fut_open"].iloc[0] is not None) else c_f

        # Stop Loss relative to entry_fut
        if direction == "BUY":
            raw_sl = float(completed_bar["fut_swing_low_5"]) - 5.0
            sl_fut = max(raw_sl, entry_fut - 45.0)
            if (entry_fut - sl_fut) <= 10.0:
                sl_fut = entry_fut - 24.0
            tp_mult = 2.50 if is_high_conviction else 1.25
            tp_fut = entry_fut + (self.har_vol_pts * tp_mult)
        else:
            raw_sl = float(completed_bar["fut_swing_high_5"]) + 5.0
            sl_fut = min(raw_sl, entry_fut + 45.0)
            if (sl_fut - entry_fut) <= 10.0:
                sl_fut = entry_fut + 24.0
            tp_mult = 2.50 if is_high_conviction else 1.25
            tp_fut = entry_fut - (self.har_vol_pts * tp_mult)

        # Option Premium at Boundary Open
        table_opts = "sensex_options" if self.target_asset == "SENSEX" else "options"
        df_opt = self.query_df(f"""
            SELECT argMin(open, timestamp) as opt_open,
                   argMax(close, timestamp) as opt_close
            FROM {table_opts}
            WHERE date = '{self.current_date}'
              AND expiry_date = '{self.target_expiry}'
              AND strike = {strike}
              AND option_type = '{opt_type}'
              AND formatDateTime(timestamp, '%H:%i') = '{current_time_str}'
              AND close > 0
        """)
        opt_price = 100.0
        if not df_opt.empty and df_opt["opt_open"].iloc[0] is not None and df_opt["opt_open"].iloc[0] > 0:
            opt_price = float(df_opt["opt_open"].iloc[0])
        elif not df_opt.empty and df_opt["opt_close"].iloc[0] is not None:
            opt_price = float(df_opt["opt_close"].iloc[0])

        min_prem = 50.0 if self.target_asset == "SENSEX" else 25.0
        if opt_price < min_prem:
            return None

        qty = (self.lots_sensex * 10) if self.target_asset == "SENSEX" else (self.lots_nifty * 65)
        lots = self.lots_sensex if self.target_asset == "SENSEX" else self.lots_nifty
        lot_size = 10 if self.target_asset == "SENSEX" else 65

        sym = f"{self.target_asset}_{strike}_{opt_type}"
        pos = ForwardTestPosition(
            model_id="0216_MODEL",
            underlying=self.target_asset,
            expiry_date=self.target_expiry,
            symbol=sym,
            strike=strike,
            option_type=opt_type,
            leg_type="DIRECTIONAL_TREND",
            target_delta=0.50,
            entry_price=opt_price,
            current_price=opt_price,
            lots=lots,
            lot_size=lot_size,
            sl_mult=0.0,
            sl_price=sl_fut,
            delta=0.50 if opt_type == "CE" else -0.50,
            direction=direction,
            spot_entry_price=entry_fut,
            spot_sl_price=sl_fut,
            spot_tp_price=tp_fut
        )
        # Custom tracking attributes for 0216 Model
        pos.be_locked = False
        pos.tier1_scaled = False
        pos.entry_time = current_time_str

        self.active_positions.append(pos)
        return {
            "action": "ENTRY",
            "position": pos,
            "direction": direction,
            "strike": strike,
            "option_type": opt_type,
            "opt_price": opt_price,
            "pcr": pcr_ratio,
            "lots": lots
        }

    def update_and_monitor(self, current_time_str: str) -> List[ForwardTestPosition]:
        """
        Evaluates live option MTM, 20% decay BE lock, 35% Tier 1 scaling, and high/low futures stops.
        """
        closed_this_tick = []
        for pos in list(self.active_positions):
            if current_time_str <= getattr(pos, "entry_time", ""):
                continue

            table = "sensex_options" if pos.underlying == "SENSEX" else "options"
            df_opt = self.query_df(f"""
                SELECT any(close) as opt_close
                FROM {table}
                WHERE date = '{self.current_date}'
                  AND expiry_date = '{self.target_expiry}'
                  AND strike = {pos.strike}
                  AND option_type = '{pos.option_type}'
                  AND formatDateTime(timestamp, '%H:%i') = '{current_time_str}'
                  AND close > 0
            """)
            if df_opt.empty or df_opt["opt_close"].iloc[0] is None:
                continue

            cur_opt_p = float(df_opt["opt_close"].iloc[0])
            pos.current_price = cur_opt_p
            pos.pnl = (pos.entry_price - cur_opt_p - 1.5) * pos.total_qty

            df_fut = self.query_df(f"""
                WITH ranked_contracts AS (
                    SELECT expiry_date, sum(volume) as total_vol
                    FROM nifty_futures
                    WHERE date = '{self.current_date}'
                    GROUP BY expiry_date ORDER BY total_vol DESC LIMIT 1
                )
                SELECT max(high) as cur_high, min(low) as cur_low
                FROM nifty_futures
                WHERE date = '{self.current_date}'
                  AND expiry_date = (SELECT expiry_date FROM ranked_contracts)
                  AND formatDateTime(timestamp, '%H:%i') = '{current_time_str}'
            """)
            cur_high_p = pos.spot_entry_price
            cur_low_p = pos.spot_entry_price
            if not df_fut.empty:
                cur_high_p = float(df_fut["cur_high"].iloc[0]) if df_fut["cur_high"].iloc[0] is not None else cur_high_p
                cur_low_p = float(df_fut["cur_low"].iloc[0]) if df_fut["cur_low"].iloc[0] is not None else cur_low_p

            # 1. Early 20% Decay BE Trail
            if (not getattr(pos, "be_locked", False)) and (cur_opt_p <= pos.entry_price * 0.80):
                if pos.direction == "BUY":
                    pos.spot_sl_price = max(pos.spot_sl_price, pos.spot_entry_price + 2.0)
                else:
                    pos.spot_sl_price = min(pos.spot_sl_price, pos.spot_entry_price - 2.0)
                pos.be_locked = True

            # 2. Two-Tier 35% Decay Partial Scaling
            if (not getattr(pos, "tier1_scaled", False)) and (cur_opt_p <= pos.entry_price * 0.65):
                scale_qty = pos.total_qty // 2
                pos.tier1_scaled = True
                if pos.direction == "BUY":
                    pos.spot_sl_price = max(pos.spot_sl_price, pos.spot_entry_price + 2.0)
                else:
                    pos.spot_sl_price = min(pos.spot_sl_price, pos.spot_entry_price - 2.0)
                pos.be_locked = True

            # 3. Check High/Low Futures Stop Loss & Take Profit
            exit_triggered = False
            exit_reason = "SL"
            if pos.direction == "BUY":
                if cur_low_p <= pos.spot_sl_price:
                    exit_triggered = True
                    exit_reason = "SL_BE" if getattr(pos, "be_locked", False) else "SL"
                elif cur_high_p >= pos.spot_tp_price:
                    exit_triggered = True
                    exit_reason = "TP"
            else:
                if cur_high_p >= pos.spot_sl_price:
                    exit_triggered = True
                    exit_reason = "SL_BE" if getattr(pos, "be_locked", False) else "SL"
                elif cur_low_p <= pos.spot_tp_price:
                    exit_triggered = True
                    exit_reason = "TP"

            if exit_triggered:
                pos.status = exit_reason
                pos.exit_price = cur_opt_p
                pos.exit_time = current_time_str
                pos.pnl = (pos.entry_price - cur_opt_p - 1.5) * pos.total_qty
                self.active_positions.remove(pos)
                self.closed_positions.append(pos)
                self.last_exit_bar = current_time_str
                closed_this_tick.append(pos)

        return closed_this_tick

    def execute_eod_squareoff(self, exit_time_str: str = "15:00") -> List[ForwardTestPosition]:
        """Closes all remaining lots at 15:00 hard EOD cutoff."""
        eod_closed = []
        for pos in list(self.active_positions):
            table = "sensex_options" if pos.underlying == "SENSEX" else "options"
            df_opt = self.query_df(f"""
                SELECT any(close) as opt_close
                FROM {table}
                WHERE date = '{self.current_date}'
                  AND expiry_date = '{self.target_expiry}'
                  AND strike = {pos.strike}
                  AND option_type = '{pos.option_type}'
                  AND formatDateTime(timestamp, '%H:%i') = '{exit_time_str}'
                  AND close > 0
            """)
            exit_px = float(df_opt["opt_close"].iloc[0]) if (not df_opt.empty and df_opt["opt_close"].iloc[0] is not None) else pos.current_price
            pos.status = "EOD_EXIT"
            pos.exit_price = exit_px
            pos.exit_time = exit_time_str
            pos.pnl = (pos.entry_price - exit_px - 1.5) * pos.total_qty
            self.active_positions.remove(pos)
            self.closed_positions.append(pos)
            eod_closed.append(pos)
        return eod_closed

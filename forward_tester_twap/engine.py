# forward_tester_twap/engine.py
import os
import sys
import json
import csv
import time
from datetime import datetime, date, time as dtime
from typing import Any
import pandas as pd
import numpy as np
import requests

# Add parent directory to sys.path to import local config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from forward_tester_twap.twap_config import (
    LIMIT_MARGIN, BASE_LOTS, LOT_SIZE, STD_MULTIPLIER, TIMEFRAME_MINS,
    ENTRY_TIME, LAST_ENTRY_TIME, EXIT_TIME, OPTION_SL_MULT, MODEL_PATH,
    CH_HISTORICAL_DAYS
)
from forward_tester_twap.data_client import ForwardTestDataClient
from forward_tester_twap.position import Position

# Gracefully import lightgbm
try:
    import lightgbm as lgb
except ImportError:
    lgb = None


class TWAPForwardTestEngine:
    """
    Core strategy execution engine for TWAP Band Breakout.
    Calculates intraday TWAP/Bands, performs LightGBM model scoring,
    manages options positions, and logs trade actions.
    """
    def __init__(self, dry_run: bool = False, disable_ml: bool = False):
        self.client = ForwardTestDataClient()
        self.dry_run = dry_run
        self.disable_ml = disable_ml or (lgb is None)
        
        # Position tracking
        self.active_position: Position = None
        self.closed_positions: list = []
        
        # Telemetry & Telegram Configuration
        self.telegram_bot_token = "8234942867:AAFdoNjo72DsEYo9DSicTJm8-t5n_B_G30g"
        self.telegram_chat_id = "-5009029141"
        self.telegram_enabled = True
        self.last_tg_update_time = time.time()
        self.sent_messages = []
        
        # Historical memory state (to calculate rolling indicators)
        self.df_historical_nifty = pd.DataFrame()
        self.df_historical_vix = pd.DataFrame()
        
        # Intraday tracking state
        self.last_processed_bar_time = None
        self.today_candles_1m = pd.DataFrame()
        
        # Log directory
        self.log_dir = "/Users/prana/Desktop/black_box/options/backtest_results"
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file = os.path.join(self.log_dir, "forward_test_twap_trades.csv")
        self.state_file = os.path.join(self.log_dir, "forward_test_twap_state.json")
        
        self._initialize_log_file()
        self._load_state()
        
        # Initialize LightGBM Model
        self.model = None
        if not self.disable_ml:
            self._load_model()

        # Load Morning ClickHouse Cache
        self._load_clickhouse_history()

    def _load_model(self):
        """Loads pre-trained LightGBM model booster."""
        if not os.path.exists(MODEL_PATH):
            print(f"⚠️ Model path not found at {MODEL_PATH}. Disabling ML filter.")
            self.disable_ml = True
            return
        try:
            self.model = lgb.Booster(model_file=MODEL_PATH)
            print(f"✅ Loaded LightGBM model successfully from {MODEL_PATH}")
        except Exception as e:
            print(f"⚠️ Error loading LightGBM model ({e}). Disabling ML filter.")
            self.disable_ml = True

    def _load_clickhouse_history(self):
        """Pre-seeds the engine's memory with Nifty spot & VIX history from ClickHouse."""
        print(f"⏳ Pre-seeding indicators with {CH_HISTORICAL_DAYS} days of ClickHouse history...")
        self.df_historical_nifty = self.client.load_historical_nifty(days=CH_HISTORICAL_DAYS)
        self.df_historical_vix = self.client.load_historical_vix(days=CH_HISTORICAL_DAYS + 30)
        
        if self.df_historical_nifty.empty:
            print("⚠️ Failed to load historical Nifty from ClickHouse. Volatility indicators will initialize on standard defaults.")
        if self.df_historical_vix.empty:
            print("⚠️ Failed to load VIX history. VIX indicators will default.")

    def _initialize_log_file(self):
        """Creates the trade log CSV file with headers if it doesn't exist."""
        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "Timestamp", "Underlying", "Strategy", "Strike", "OptionType", 
                    "Qty", "EntryPrice", "ExitPrice", "PNL", "Status", "Direction",
                    "EntrySpot", "ExitSpot", "SLSpot", "TPSpot", "ML_Confidence"
                ])

    def save_state(self):
        """Saves current strategy state to a JSON file for recovery on restarts."""
        state = {
            "date": str(date.today()),
            "last_tg_update_time": self.last_tg_update_time,
            "sent_messages": self.sent_messages,
            "active_position": self.active_position.to_dict() if self.active_position else None,
            "closed_positions": [p.to_dict() for p in self.closed_positions],
            "last_processed_bar_time": self.last_processed_bar_time.isoformat() if isinstance(self.last_processed_bar_time, datetime) else self.last_processed_bar_time
        }
        try:
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            print(f"  [State Error] Failed to save state: {e}")

    def _load_state(self):
        """Loads strategy state from JSON file if it exists and matches today's date."""
        if not os.path.exists(self.state_file):
            return
            
        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)
                
            if state.get("date") == str(date.today()):
                self.last_tg_update_time = state.get("last_tg_update_time", time.time())
                self.sent_messages = state.get("sent_messages", [])
                
                act_pos = state.get("active_position")
                self.active_position = Position.from_dict(act_pos) if act_pos else None
                self.closed_positions = [Position.from_dict(p) for p in state.get("closed_positions", [])]
                
                lpb = state.get("last_processed_bar_time")
                if lpb:
                    self.last_processed_bar_time = datetime.fromisoformat(lpb)
                print(f"✅ State loaded successfully from {self.state_file} for {state['date']}")
            else:
                print(f"⚠️ Stale state file from {state.get('date')}. Ignoring state.")
        except Exception as e:
            print(f"  [State Error] Failed to load state: {e}")

    def log_trade(self, pos: Position, confidence: float):
        """Appends a closed trade to the CSV log file."""
        with open(self.log_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "NIFTY",
                "TWAP_BREAKOUT",
                pos.strike,
                pos.option_type,
                pos.qty,
                round(pos.entry_price, 2),
                round(pos.exit_price, 2) if pos.exit_price else 0.0,
                round(pos.pnl, 2),
                pos.status,
                "LONG" if pos.is_long_spot else "SHORT",
                round(pos.entry_spot, 2),
                round(pos.current_price, 2) if pos.exit_price is None else round(pos.exit_price, 2), # spot close/exit spot
                round(pos.sl_spot, 2),
                round(pos.tp_spot, 2),
                round(confidence, 4)
            ])

    def send_telegram(self, message: str) -> Any:
        """Sends an HTML-formatted message to Telegram and returns the message_id on success."""
        if not self.telegram_enabled:
            return None
        if self.dry_run:
            print(f"[Telegram MOCK] {message}")
            return 999999
            
        url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
        payload = {
            "chat_id": self.telegram_chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
        try:
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("result", {}).get("message_id")
            else:
                print(f"  [Telegram Error] API returned status {resp.status_code}: {resp.text}")
        except Exception as e:
            print(f"  [Telegram Error] Failed to send message: {e}")
        return None

    def delete_telegram_message(self, message_id: int):
        """Deletes a message from Telegram."""
        if not self.telegram_enabled or self.dry_run or not message_id:
            return
            
        url = f"https://api.telegram.org/bot{self.telegram_bot_token}/deleteMessage"
        payload = {
            "chat_id": self.telegram_chat_id,
            "message_id": message_id
        }
        try:
            requests.post(url, json=payload, timeout=5)
        except Exception as e:
            print(f"  [Telegram Error] Failed to delete message {message_id}: {e}")

    def send_periodic_pnl_update(self):
        """Sends a periodic status update of active position and current daily P/L, and prunes old messages."""
        realized_pnl = sum(p.pnl for p in self.closed_positions)
        unrealized_pnl = self.active_position.pnl if self.active_position else 0.0
        total_pnl = realized_pnl + unrealized_pnl
        
        status_lines = []
        if self.active_position:
            p = self.active_position
            direction = "LONG" if p.is_long_spot else "SHORT"
            status_lines.append(
                f"• <b>{direction} Option Sale</b> ({p.symbol})\n"
                f"  LTP: {p.current_price:.2f} (Entry: {p.entry_price:.2f}) | Trail SL: {p.premium_sl:.2f}\n"
                f"  Spot Nifty: {self.client.get_spot_price('NIFTY'):.2f} (Entry: {p.entry_spot:.2f} | SL: {p.sl_spot:.2f} | TP: {p.tp_spot:.2f})\n"
                f"  PnL: ₹{p.pnl:,.2f}"
            )
            
        tg_msg = (
            f"📊 <b>TWAP Breakout Periodic Performance Update</b>\n"
            f"---------------------------------------\n"
            f"<b>Realized PnL</b>: ₹{realized_pnl:,.2f}\n"
            f"<b>Unrealized PnL</b>: ₹{unrealized_pnl:,.2f}\n"
            f"<b>Total Daily PnL</b>: <b>{f'₹{total_pnl:,.2f}' if total_pnl >= 0 else f'-₹{abs(total_pnl):,.2f}'}</b>\n\n"
        )
        if status_lines:
            tg_msg += "<b>Active Position:</b>\n" + "\n".join(status_lines)
        else:
            tg_msg += "No active positions."
            
        msg_id = self.send_telegram(tg_msg)
        self.last_tg_update_time = time.time()
        
        if msg_id:
            self.sent_messages.append({"msg_id": msg_id, "timestamp": time.time()})
            
        # Prune messages older than 1 minute (60 seconds)
        now = time.time()
        to_keep = []
        for item in self.sent_messages:
            if now - item["timestamp"] > 60:
                self.delete_telegram_message(item["msg_id"])
            else:
                to_keep.append(item)
        self.sent_messages = to_keep
        self.save_state()

    def publish_telemetry(self):
        """
        Publishes the current position state, prices, and P/L metrics to Redis 
        as a JSON message in a sorted set, and prunes messages older than 1 minute (60 seconds).
        """
        now_ts = time.time()
        positions_data = []
        if self.active_position:
            p = self.active_position
            positions_data.append({
                "underlying": "NIFTY",
                "strategy": "TWAP_BREAKOUT",
                "strike": p.strike,
                "type": p.option_type,
                "qty": p.qty,
                "entry": round(p.entry_price, 2),
                "ltp": round(p.current_price, 2),
                "initial_sl": round(p.premium_sl, 2),
                "current_sl": round(p.premium_sl, 2),
                "pnl": round(p.pnl, 2),
                "status": p.status,
                "is_reentry": False
            })
            
        realized_pnl = sum(p.pnl for p in self.closed_positions)
        unrealized_pnl = self.active_position.pnl if self.active_position else 0.0
        
        status_msg = {
            "timestamp": int(now_ts),
            "time_str": datetime.now().strftime("%H:%M:%S"),
            "active_positions": positions_data,
            "realized_pnl": round(realized_pnl, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "total_pnl": round(realized_pnl + unrealized_pnl, 2)
        }
        
        status_str = json.dumps(status_msg)
        
        try:
            self.client.client.r.zadd("strategy:twap:telemetry", {status_str: now_ts})
            self.client.client.r.zremrangebyscore("strategy:twap:telemetry", "-inf", now_ts - 60)
            self.client.client.r.publish("strategy:twap:stream", status_str)
        except Exception as e:
            print(f"  [Telemetry Error] Failed to publish metrics to Redis: {e}")

    # ─── Feature Engineering & ML Scoring ───

    def _rsi(self, series: pd.Series, period: int = 14) -> pd.Series:
        delta = series.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        return (100 - (100 / (1 + rs))).fillna(50.0)

    def _rolling_slope(self, series: pd.Series, window: int = 10) -> pd.Series:
        x = np.arange(window)
        x_mean = x.mean()
        x_dev = x - x_mean
        x_var = (x_dev ** 2).sum()
        def _slope(y):
            if len(y) < window: return np.nan
            return (x_dev * (y - y.mean())).sum() / x_var
        return series.rolling(window).apply(_slope, raw=True)

    def get_live_options_implied_metrics(self, front_expiry: str, monthly_expiry: str, atm_strike: int) -> dict:
        """Queries Redis to calculate live options-implied metrics (skew, term structure, PCR, delta spread)."""
        metrics = {
            'atm_iv': 0.15,
            'iv_skew': 0.01,
            'iv_term_struct': 1.0,
            'pcr_oi': 0.95,
            'atm_delta_spread': 0.02,
            'atm_iv_z60d': 0.0,
            'atm_iv_5d_change': 0.0
        }
        if not front_expiry or not monthly_expiry:
            return metrics
            
        try:
            # 1. Fetch Option Chain Quotes
            chain_front = self.client.get_option_chain_quotes("NIFTY", front_expiry, count=10)
            chain_monthly = self.client.get_option_chain_quotes("NIFTY", monthly_expiry, count=1)
            
            # ATM CE/PE for Front weekly
            front_strikes = chain_front.get("strikes", {})
            atm_pair_front = front_strikes.get(atm_strike) or front_strikes.get(str(atm_strike))
            
            front_atm_iv = None
            if atm_pair_front:
                ce = atm_pair_front.get("CE")
                pe = atm_pair_front.get("PE")
                ce_iv = ce.get("iv") if (ce and isinstance(ce, dict)) else None
                pe_iv = pe.get("iv") if (pe and isinstance(pe, dict)) else None
                
                if ce_iv is not None and pe_iv is not None:
                    ce_iv_val = float(ce_iv)
                    pe_iv_val = float(pe_iv)
                    if ce_iv_val > 0 and pe_iv_val > 0:
                        metrics['atm_iv'] = (ce_iv_val + pe_iv_val) / 2.0
                        metrics['iv_skew'] = pe_iv_val - ce_iv_val
                        front_atm_iv = metrics['atm_iv']
                        
                        ce_delta = ce.get("option_greeks", {}).get("delta") if (ce and ce.get("option_greeks")) else 0.5
                        pe_delta = pe.get("option_greeks", {}).get("delta") if (pe and pe.get("option_greeks")) else -0.5
                        metrics['atm_delta_spread'] = float(ce_delta) - abs(float(pe_delta))
                        
            # 2. PCR OI Calculation Centered around Spot +/- 500 points (using Nifty strike step 50, count=10)
            total_pe_oi = 0
            total_ce_oi = 0
            for strike, leg_pair in front_strikes.items():
                ce = leg_pair.get("CE")
                pe = leg_pair.get("PE")
                if ce and isinstance(ce, dict):
                    total_ce_oi += float(ce.get("oi") or 0)
                if pe and isinstance(pe, dict):
                    total_pe_oi += float(pe.get("oi") or 0)
                    
            if total_ce_oi > 0:
                metrics['pcr_oi'] = total_pe_oi / total_ce_oi
                
            # 3. Monthly Expiry ATM IV (for Term Structure)
            monthly_strikes = chain_monthly.get("strikes", {})
            monthly_atm_strike = self.client.get_atm_strike("NIFTY")
            atm_pair_monthly = monthly_strikes.get(monthly_atm_strike) or monthly_strikes.get(str(monthly_atm_strike))
            
            if not atm_pair_monthly and monthly_strikes:
                first_strike = list(monthly_strikes.keys())[0]
                atm_pair_monthly = monthly_strikes[first_strike]
                
            if atm_pair_monthly and front_atm_iv:
                m_ce = atm_pair_monthly.get("CE")
                m_pe = atm_pair_monthly.get("PE")
                m_ce_iv = m_ce.get("iv") if (m_ce and isinstance(m_ce, dict)) else None
                m_pe_iv = m_pe.get("iv") if (m_pe and isinstance(m_pe, dict)) else None
                
                if m_ce_iv is not None and m_pe_iv is not None:
                    m_ce_iv_val = float(m_ce_iv)
                    m_pe_iv_val = float(m_pe_iv)
                    if m_ce_iv_val > 0 and m_pe_iv_val > 0:
                        monthly_atm_iv = (m_ce_iv_val + m_pe_iv_val) / 2.0
                        metrics['iv_term_struct'] = front_atm_iv / monthly_atm_iv
                        
        except Exception as e:
            print(f"⚠️ Error calculating live option metrics: {e}")
            
        return metrics

    def calculate_features(self, df_3m: pd.DataFrame, direction: str) -> dict:
        """
        Calculates all 48 lookahead-free features for the current closed 3-minute bar.
        Merges historical Nifty and VIX data.
        """
        # Ensure we have sorted Nifty bars
        df = df_3m.copy()
        
        # 1. Volatility features
        hl_term = np.log(df['high'] / df['low']) ** 2
        co_term = np.log(df['close'] / df['open']) ** 2
        df['vol_park_20'] = np.sqrt(hl_term.rolling(20).sum() / (4 * np.log(2) * 20))
        df['vol_park_60'] = np.sqrt(hl_term.rolling(60).sum() / (4 * np.log(2) * 60))
        
        gk_20 = (hl_term.rolling(20).sum() / (2 * 20)) - ((2 * np.log(2) - 1) * co_term.rolling(20).sum() / 20)
        gk_60 = (hl_term.rolling(60).sum() / (2 * 60)) - ((2 * np.log(2) - 1) * co_term.rolling(60).sum() / 60)
        df['vol_gk_20'] = np.sqrt(np.maximum(gk_20, 0.0))
        df['vol_gk_60'] = np.sqrt(np.maximum(gk_60, 0.0))
        
        h_l = df['high'] - df['low']
        h_pc = (df['high'] - df['close'].shift(1)).abs()
        l_pc = (df['low'] - df['close'].shift(1)).abs()
        tr = pd.concat([h_l, h_pc, l_pc], axis=1).max(axis=1)
        df['atr_14'] = tr.rolling(14).mean()
        
        df['atr_tod_mean'] = df.groupby('tt')['atr_14'].transform(lambda x: x.shift(1).rolling(20, min_periods=1).mean())
        df['atr_ratio'] = (df['atr_14'] / df['atr_tod_mean'].replace(0, np.nan)).fillna(1.0)
        df['vol_regime'] = (df['vol_park_20'] / df['vol_park_60'].replace(0, np.nan)).fillna(1.0)
        
        # 2. Geometry features
        df['twap_slope'] = self._rolling_slope(df['twap'], window=10)
        bw = df['upper_band'] - df['lower_band']
        df['band_width'] = bw
        df['band_expansion_rate'] = self._rolling_slope(bw, window=10)
        
        # Band width percentile (trailing 20-day daily max band width)
        daily_max = bw.groupby(df['td']).transform('max')
        daily_max_1 = daily_max.groupby(df['td']).first().shift(1)
        roll_max = daily_max_1.rolling(20, min_periods=1).max()
        df['rolling_max_width_20d'] = df['td'].map(roll_max)
        df['band_width_pctile'] = (bw / df['rolling_max_width_20d'].replace(0, np.nan)).fillna(0.5)
        
        # Touch counts today
        tagged_upper = (df['high'] >= df['upper_band']).astype(int)
        tagged_lower = (df['low'] <= df['lower_band']).astype(int)
        df['touch_count_upper'] = tagged_upper.groupby(df['td']).cumsum() - tagged_upper
        df['touch_count_lower'] = tagged_lower.groupby(df['td']).cumsum() - tagged_lower
        
        # 3. Momentum features
        df['rsi_14'] = self._rsi(df['close'], period=14)
        ema9 = df['close'].ewm(span=9, adjust=False).mean()
        ema20 = df['close'].ewm(span=20, adjust=False).mean()
        df['ema_fast_slope'] = (ema9 - ema9.shift(3)) / ema9.shift(3).replace(0, np.nan)
        df['close_vs_ema20'] = (df['close'] - ema20) / ema20.replace(0, np.nan)
        df['momentum_5'] = df['close'] / df['close'].shift(5).replace(0, np.nan) - 1.0
        df['momentum_15'] = df['close'] / df['close'].shift(15).replace(0, np.nan) - 1.0
        
        bar_range = (df['high'] - df['low']).replace(0, np.nan)
        body = df['close'] - df['open']
        upper_wick = df['high'] - df[['open', 'close']].max(axis=1)
        lower_wick = df[['open', 'close']].min(axis=1) - df['low']
        
        df['bar_body_pct'] = (body / bar_range).clip(-1.0, 1.0).fillna(0.0)
        df['upper_wick_pct'] = (upper_wick / bar_range).clip(0.0, 1.0).fillna(0.0)
        df['lower_wick_pct'] = (lower_wick / bar_range).clip(0.0, 1.0).fillna(0.0)
        
        up = (df['close'] > df['close'].shift(1)).astype(int)
        down = (df['close'] < df['close'].shift(1)).astype(int)
        streak = (up != up.shift(1)).cumsum()
        df['close_streak'] = up.groupby(streak).cumsum() - down.groupby(streak).cumsum()
        
        # 4. Session context
        df['day_open'] = df.groupby('td')['open'].transform('first')
        df['intraday_return'] = (df['close'] - df['day_open']) / df['day_open'].replace(0, np.nan)
        
        df['session_high_so_far'] = df.groupby('td')['high'].transform(lambda x: x.shift(1).expanding().max())
        df['session_low_so_far'] = df.groupby('td')['low'].transform(lambda x: x.shift(1).expanding().min())
        session_span = (df['session_high_so_far'] - df['session_low_so_far']).replace(0, np.nan)
        df['session_high_pct'] = ((df['close'] - df['session_low_so_far']) / session_span).clip(0.0, 1.0).fillna(0.5)
        
        above_twap = (df['close'] > df['twap']).astype(int)
        df['cum_bars_above_twap'] = above_twap.groupby(df['td']).cumsum() - above_twap
        
        bar_return = df['close'].pct_change()
        realized_vol_series = []
        for td, grp in df.groupby('td'):
            realized_vol_series.append(bar_return.loc[grp.index].shift(1).expanding().std())
        df['realized_vol_today'] = pd.concat(realized_vol_series).reindex(df.index).fillna(0.0)
        
        # 5. Gap / prior day
        daily_stats = df.groupby('td').agg(
            day_high=('high', 'max'),
            day_low=('low', 'min'),
            day_close=('close', 'last'),
        )
        prior_stats = daily_stats.shift(1)
        df['prior_high'] = df['td'].map(prior_stats['day_high'])
        df['prior_low'] = df['td'].map(prior_stats['day_low'])
        df['prior_close'] = df['td'].map(prior_stats['day_close'])
        df['prior_range'] = df['prior_high'] - df['prior_low']
        
        df['opening_gap_pct'] = ((df['day_open'] - df['prior_close']) / df['prior_close'].replace(0, np.nan) * 100.0).fillna(0.0)
        df['open_pos_relative'] = ((df['day_open'] - df['prior_low']) / df['prior_range'].replace(0, np.nan)).fillna(0.5)
        df['session_range_pct'] = (session_span / df['prior_range'].replace(0, np.nan)).fillna(0.5)
        
        # 6. Expiry calendar & Options implied & VIX (Dynamic calculations)
        front_expiry = self.client.get_front_expiry("NIFTY")
        monthly_expiry = self.client.get_monthly_expiry("NIFTY")
        atm_strike = self.client.get_atm_strike("NIFTY")
        
        # Calculate expiry days
        days_weekly = 2.0
        days_monthly = 16.0
        try:
            today_dt = date.today()
            if front_expiry:
                front_expiry_dt = datetime.strptime(front_expiry, "%Y-%m-%d").date()
                days_weekly = float((front_expiry_dt - today_dt).days)
            if monthly_expiry:
                monthly_expiry_dt = datetime.strptime(monthly_expiry, "%Y-%m-%d").date()
                days_monthly = float((monthly_expiry_dt - today_dt).days)
        except Exception as e:
            print(f"⚠️ Error parsing expiry date: {e}")
            
        df['days_to_weekly_expiry'] = days_weekly
        df['days_to_monthly_expiry'] = days_monthly
        
        # 7. India VIX level (Live calculation using ClickHouse history + Today's live close)
        vix_close = self.client.get_spot_price("VIX", fallback=15.0)
        vix_5d_change = 0.0
        vix_z60d = 0.0
        
        if not self.df_historical_vix.empty:
            try:
                today_dt = date.today()
                today_vix_row = pd.DataFrame([{'date': today_dt, 'vix_close': vix_close, 'vix_1d_change': 0.0}])
                df_vix_combined = pd.concat([self.df_historical_vix, today_vix_row]).drop_duplicates(subset=['date']).sort_values('date').reset_index(drop=True)
                
                df_vix_combined['vix_5d_change'] = df_vix_combined['vix_close'].diff(5)
                
                roll_mean = df_vix_combined['vix_close'].shift(1).rolling(60, min_periods=15).mean()
                roll_std = df_vix_combined['vix_close'].shift(1).rolling(60, min_periods=15).std()
                df_vix_combined['vix_z60d'] = ((df_vix_combined['vix_close'].shift(1) - roll_mean) / roll_std.replace(0, np.nan)).fillna(0.0)
                
                last_row = df_vix_combined.iloc[-1]
                vix_5d_change = float(last_row['vix_5d_change']) if not pd.isna(last_row['vix_5d_change']) else 0.0
                vix_z60d = float(last_row['vix_z60d'])
            except Exception as e:
                print(f"⚠️ Error computing live VIX features: {e}")
                
        df['vix_close'] = vix_close
        df['vix_1d_change'] = 0.0
        df['vix_5d_change'] = vix_5d_change
        df['vix_z60d'] = vix_z60d
        
        # 8. Options implied details (Get live option chain metrics from Redis)
        opt_metrics = self.get_live_options_implied_metrics(front_expiry, monthly_expiry, atm_strike)
        df['atm_iv'] = opt_metrics['atm_iv']
        df['iv_skew'] = opt_metrics['iv_skew']
        df['iv_term_struct'] = opt_metrics['iv_term_struct']
        df['pcr_oi'] = opt_metrics['pcr_oi']
        df['atm_delta_spread'] = opt_metrics['atm_delta_spread']
        df['atm_iv_z60d'] = opt_metrics['atm_iv_z60d']
        df['atm_iv_5d_change'] = opt_metrics['atm_iv_5d_change']
        
        # 9. Time features
        df['mins_since_920'] = (df['timestamp'].dt.hour * 60 + df['timestamp'].dt.minute) - (9 * 60 + 20)
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        
        # Get last closed row
        row = df.iloc[-2]  # -2 is the closed bar
        
        # Extract features dictionary
        feats = {
            'vol_park_20': float(row['vol_park_20']),
            'vol_park_60': float(row['vol_park_60']),
            'vol_gk_20': float(row['vol_gk_20']),
            'vol_gk_60': float(row['vol_gk_60']),
            'atr_14': float(row['atr_14']),
            'atr_ratio': float(row['atr_ratio']),
            'vol_regime': float(row['vol_regime']),
            'twap_slope': float(row['twap_slope']),
            'rsi_14': float(row['rsi_14']),
            'ema_fast_slope': float(row['ema_fast_slope']),
            'close_vs_ema20': float(row['close_vs_ema20']),
            'momentum_5': float(row['momentum_5']),
            'momentum_15': float(row['momentum_15']),
            'bar_body_pct': float(row['bar_body_pct']),
            'upper_wick_pct': float(row['upper_wick_pct']),
            'lower_wick_pct': float(row['lower_wick_pct']),
            'close_streak': float(row['close_streak']),
            'intraday_return': float(row['intraday_return']),
            'session_high_pct': float(row['session_high_pct']),
            'session_range_pct': float(row['session_range_pct']),
            'cum_bars_above_twap': float(row['cum_bars_above_twap']),
            'realized_vol_today': float(row['realized_vol_today']),
            'prior_range': float(row['prior_range']) if 'prior_range' in row else 200.0,
            'opening_gap_pct': float(row['opening_gap_pct']),
            'open_pos_relative': float(row['open_pos_relative']),
            'days_to_weekly_expiry': float(row['days_to_weekly_expiry']),
            'days_to_monthly_expiry': float(row['days_to_monthly_expiry']),
            'vix_close': float(row['vix_close']),
            'vix_1d_change': float(row['vix_1d_change']),
            'vix_5d_change': float(row['vix_5d_change']),
            'vix_z60d': float(row['vix_z60d']),
            'atm_iv': float(row['atm_iv']),
            'iv_skew': float(row['iv_skew']),
            'iv_term_struct': float(row['iv_term_struct']),
            'pcr_oi': float(row['pcr_oi']),
            'atm_delta_spread': float(row['atm_delta_spread']),
            'atm_iv_z60d': float(row['atm_iv_z60d']),
            'atm_iv_5d_change': float(row['atm_iv_5d_change']),
            'mins_since_920': float(row['mins_since_920']),
            'day_of_week': float(row['day_of_week']),
            'band_width': float(row['band_width']),
            'band_width_pctile': float(row['band_width_pctile']),
            'band_expansion_rate': float(row['band_expansion_rate'])
        }
        
        # Signal direction specific features
        close_val = row['close']
        bw_val = feats['band_width']
        if direction == 'LONG':
            band_level = row['upper_band']
            feats['penetration_depth'] = float((close_val - band_level) / bw_val) if bw_val else 0.0
            feats['touch_count_today'] = float(row['touch_count_upper'])
        else:
            band_level = row['lower_band']
            feats['penetration_depth'] = float((band_level - close_val) / bw_val) if bw_val else 0.0
            feats['touch_count_today'] = float(row['touch_count_lower'])
            
        feats['signal_rank_today'] = 1.0  # Default fallback
        return feats

    def check_ml_confidence(self, features: dict) -> float:
        """Loads features, executes prediction, and returns win probability."""
        if self.disable_ml or not self.model:
            return 1.0
            
        try:
            feature_cols = [
                'penetration_depth',
                'open_pos_relative',
                'momentum_15',
                'intraday_return',
                'iv_term_struct',
                'vix_5d_change',
                'pcr_oi',
                'vol_regime',
                'prior_range',
                'mins_since_920'
            ]
            
            # Construct row
            row_vals = []
            for col in feature_cols:
                # Handle categorical bucket (e.g. session_bucket is string but we can bypass it or set index)
                row_vals.append(features.get(col, 0.0))
                
            X = np.array([row_vals])
            pred = self.model.predict(X)
            prob_win = float(pred[0])
            print(f"🧠 LightGBM Model Prediction (Win Probability): {prob_win:.4f}")
            return prob_win
        except Exception as e:
            print(f"⚠️ Error during LightGBM model prediction: {e}. Defaulting to 1.0 (approved).")
            return 1.0

    # ─── Live Entry & Exit Executions ───

    def execute_option_entry(self, direction: str, spot_price: float, sl_spot: float, tp_spot: float, confidence: float):
        """Discovers the ATM option strike and enters a simulated trade by selling the option contract."""
        underlying = "NIFTY"
        expiry = self.client.get_front_expiry(underlying)
        if not expiry:
            print("❌ Cannot enter: No expiry found in Redis.")
            return

        atm_strike = self.client.get_atm_strike(underlying)
        opt_type = "PE" if direction == "LONG" else "CE"
        
        # Get quotes around ATM
        chain = self.client.get_option_chain_quotes(underlying, expiry, count=1)
        strikes_dict = chain.get("strikes", {})
        leg_pair = strikes_dict.get(atm_strike) or strikes_dict.get(str(atm_strike))
        
        if not leg_pair:
            print(f"❌ Option quotes for ATM {atm_strike} {opt_type} missing from Redis chain.")
            return
            
        leg = leg_pair.get(opt_type)
        if not leg or not isinstance(leg, dict) or "error" in leg:
            print(f"❌ Option quotes for ATM {atm_strike} {opt_type} are missing or invalid in Redis.")
            return
            
        entry_price = leg.get("ltp") or leg.get("close")
        if not entry_price or entry_price <= 0:
            print(f"❌ Stale option entry price ({entry_price}) for {leg.get('symbol', 'Unknown')}.")
            return
            
        qty = BASE_LOTS * LOT_SIZE
        
        self.active_position = Position(
            symbol=leg["symbol"],
            strike=atm_strike,
            option_type=opt_type,
            qty=qty,
            entry_price=entry_price,
            is_long_spot=(direction == "LONG"),
            entry_spot=spot_price,
            sl_spot=sl_spot,
            tp_spot=tp_spot,
            premium_sl_mult=OPTION_SL_MULT
        )
        
        print(f"🚀 [Trade Entered] Sold {underlying} ATM {atm_strike} {opt_type} | Qty: {qty} | Price: {entry_price}")
        print(f"   Index Spot Entry: {spot_price:.2f} | Spot SL: {sl_spot:.2f} | Spot Target: {tp_spot:.2f}")
        
        # Send Telegram notification
        tg_msg = (
            f"🚀 <b>TWAP Breakout Option Sold</b>\n"
            f"---------------------------------------\n"
            f"• <b>NIFTY {direction} breakout</b>\n"
            f"• <b>Contract</b>: {leg['symbol']} (ATM {atm_strike} {opt_type})\n"
            f"• <b>Qty</b>: {qty} | <b>Price</b>: {entry_price:.2f}\n"
            f"• <b>Index Spot</b>: {spot_price:.2f} | <b>Spot SL</b>: {sl_spot:.2f} | <b>Spot Target</b>: {tp_spot:.2f}\n"
            f"• <b>ML Win Probability</b>: {confidence:.4f}"
        )
        self.send_telegram(tg_msg)
        self.publish_telemetry()
        self.save_state()

    def update_and_monitor(self):
        """
        Updates the active position with live quote updates from Redis 
        and evaluates SL and TP criteria.
        """
        if not self.active_position:
            return
            
        p = self.active_position
        spot_price = self.client.get_spot_price("NIFTY")
        
        # Query active symbol quote from Redis
        raw_quote = self.client.client.r.hgetall(f"md:quote:{p.symbol}")
        if not raw_quote:
            return
            
        ltp = raw_quote.get("ltp")
        if ltp is None or ltp == "0.0":
            ltp = raw_quote.get("close")
            
        if ltp:
            current_premium = float(ltp)
            p.update_state(current_premium, spot_price)
            
            # Check if stopped out/target hit
            if p.status != "ACTIVE":
                self.active_position = None
                self.closed_positions.append(p)
                self.log_trade(p, getattr(p, "ml_confidence", 1.0))
                
                print(f"🚨 [Position Closed] {p.symbol} at {p.exit_price:.2f} | Reason: {p.status} | PnL: ₹{p.pnl:,.2f}")
                
                # Send telegram alert
                tg_msg = (
                    f"🚨 <b>TWAP Breakout Trade Closed</b>\n"
                    f"---------------------------------------\n"
                    f"• <b>Contract</b>: {p.symbol} ({p.strike} {p.option_type})\n"
                    f"• <b>Exit Reason</b>: {p.status} | <b>Exit Price</b>: {p.exit_price:.2f}\n"
                    f"• <b>Spot Nifty</b>: {spot_price:.2f} (Entry: {p.entry_spot:.2f} | SL: {p.sl_spot:.2f} | Target: {p.tp_spot:.2f})\n"
                    f"• <b>PnL</b>: <b>{f'₹{p.pnl:,.2f}' if p.pnl >= 0 else f'-₹{abs(p.pnl):,.2f}'}</b>"
                )
                self.send_telegram(tg_msg)
                self.publish_telemetry()
                self.save_state()

    def force_eod_square_off(self):
        """Squares off active position at EOD."""
        if not self.active_position:
            return
            
        p = self.active_position
        raw_quote = self.client.client.r.hgetall(f"md:quote:{p.symbol}")
        ltp = raw_quote.get("ltp") or raw_quote.get("close")
        exit_price = float(ltp) if ltp else p.entry_price
        
        p.close(exit_price, "EOD_EXITED")
        self.active_position = None
        self.closed_positions.append(p)
        self.log_trade(p, getattr(p, "ml_confidence", 1.0))
        
        print(f"🏁 [EOD Exited] Squared off {p.symbol} at {p.exit_price:.2f} | PnL: ₹{p.pnl:,.2f}")
        
        # Telegram alert
        tg_msg = (
            f"🏁 <b>TWAP Breakout EOD Square-off Executed</b>\n"
            f"---------------------------------------\n"
            f"• <b>Contract</b>: {p.symbol} @ {p.exit_price:.2f}\n"
            f"• <b>Total Trade PnL</b>: <b>{f'₹{p.pnl:,.2f}' if p.pnl >= 0 else f'-₹{abs(p.pnl):,.2f}'}</b>"
        )
        self.send_telegram(tg_msg)
        self.publish_telemetry()
        self.save_state()

    # ─── Main Strategy Processing ───

    def process_closed_bar(self, today_df_3m: pd.DataFrame):
        """
        Called when a new 3-minute bar closes. Evaluates breakout signals
        and schedules option entries.
        """
        if len(today_df_3m) < 3:
            return
            
        last_closed = today_df_3m.iloc[-2]  # -2 is the closed bar
        prev_closed = today_df_3m.iloc[-3]  # -3 is the bar before it
        
        bar_time = last_closed['timestamp']
        if self.last_processed_bar_time and bar_time <= self.last_processed_bar_time:
            return
            
        self.last_processed_bar_time = bar_time
        
        # Check Entry Window
        now_time = datetime.now().time()
        start_ent = dtime(*ENTRY_TIME)
        stop_ent = dtime(*LAST_ENTRY_TIME)
        
        if not (start_ent <= now_time <= stop_ent) and not self.dry_run:
            return
            
        # Check that standard deviation is valid (> 0)
        if pd.isna(last_closed['upper_band']) or pd.isna(prev_closed['upper_band']):
            return
            
        close_val = last_closed['close']
        prev_close = prev_closed['close']
        upper = last_closed['upper_band']
        prev_upper = prev_closed['upper_band']
        lower = last_closed['lower_band']
        prev_lower = prev_closed['lower_band']
        
        long_breakout = (close_val > upper) and (prev_close <= prev_upper)
        short_breakout = (close_val < lower) and (prev_close >= prev_lower)
        
        if (long_breakout or short_breakout) and not self.active_position:
            direction = "LONG" if long_breakout else "SHORT"
            print(f"\n⚡ [{datetime.now().strftime('%H:%M:%S')}] {direction} Breakout Signal Triggered at Nifty spot {close_val:.2f}!")
            
            # 1. Feature Engineering
            try:
                # Concatenate ClickHouse history + today's resampled bars
                # to form a continuous Nifty 3-minute bar series for indicator rolling windows
                hist_resampled = []
                if not self.df_historical_nifty.empty:
                    # Resample historical 1m data to 3m
                    agg_dict = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'}
                    hist_3m = self.df_historical_nifty.set_index('timestamp').resample('3min', closed='left', label='left').agg(agg_dict).dropna().reset_index()
                    hist_3m['td'] = hist_3m['timestamp'].dt.date
                    hist_3m['tt'] = hist_3m['timestamp'].dt.time
                    hist_resampled.append(hist_3m)
                
                hist_resampled.append(today_df_3m)
                df_combined = pd.concat(hist_resampled).sort_values('timestamp').drop_duplicates(subset=['timestamp']).reset_index(drop=True)
                
                # Re-calculate TWAP and Bands on combined df to ensure lookback is correct
                df_combined['twap'] = df_combined.groupby('td')['close'].transform(lambda x: x.expanding().mean())
                df_combined['std'] = df_combined.groupby('td')['close'].transform(lambda x: x.expanding().std())
                df_combined['upper_band'] = df_combined['twap'] + STD_MULTIPLIER * df_combined['std']
                df_combined['lower_band'] = df_combined['twap'] - STD_MULTIPLIER * df_combined['std']
                
                # Compute features
                features = self.calculate_features(df_combined, direction)
            except Exception as e:
                print(f"⚠️ Feature calculation failed ({e}). Bypassing ML filter.")
                features = {}
            
            # 2. Score using LightGBM model
            confidence = 1.0
            if not self.disable_ml and features:
                confidence = self.check_ml_confidence(features)
                
            # If approved (Confidence > 0.53)
            if confidence > 0.53:
                # Setup Index Stop Loss and Target
                sl_spot = last_closed['twap']
                if direction == "LONG":
                    risk = close_val - sl_spot
                    tp_spot = close_val + risk
                else:
                    risk = sl_spot - close_val
                    tp_spot = close_val - risk
                    
                self.execute_option_entry(direction, close_val, sl_spot, tp_spot, confidence)
                if self.active_position:
                    self.active_position.ml_confidence = confidence
                    self.save_state()
            else:
                print(f"❌ Signal blocked by ML filter (Confidence {confidence:.4f} <= 0.53).")

    def run_tick(self):
        """Processes a single live tick iteration."""
        # 1. Fetch live Nifty Spot 1m candles from Redis
        live_candles = self.client.get_live_candles("NSE_INDEX|Nifty 50", timeframe="1m", count=120)
        if not live_candles:
            return
            
        df_1m = pd.DataFrame(live_candles)
        df_1m['timestamp'] = pd.to_datetime(df_1m['timestamp'], unit='s')
        df_1m['td'] = df_1m['timestamp'].dt.date
        df_1m['tt'] = df_1m['timestamp'].dt.time
        
        # 2. Resample Nifty Spot candles to 3m
        agg_dict = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'}
        df_3m = df_1m.set_index('timestamp').resample('3min', closed='left', label='left').agg(agg_dict).dropna().reset_index()
        df_3m['td'] = df_3m['timestamp'].dt.date
        df_3m['tt'] = df_3m['timestamp'].dt.time
        
        # 3. Calculate Expanding TWAP and Bands for Today
        df_3m['twap'] = df_3m.groupby('td')['close'].transform(lambda x: x.expanding().mean())
        df_3m['std'] = df_3m.groupby('td')['close'].transform(lambda x: x.expanding().std())
        df_3m['upper_band'] = df_3m['twap'] + STD_MULTIPLIER * df_3m['std']
        df_3m['lower_band'] = df_3m['twap'] - STD_MULTIPLIER * df_3m['std']
        
        # Save today's candles to local memory for EOD ClickHouse inserts
        self.today_candles_1m = df_1m[df_1m['td'] == date.today()].copy()
        
        # 4. Check closed 3-minute bars for breakout triggers
        self.process_closed_bar(df_3m)
        
        # 5. Update active positions
        self.update_and_monitor()

    def execute_eod_pipeline(self):
        """Executes EOD square-offs and writes cached data to ClickHouse."""
        print("\n🏁 Executing TWAP Forward Tester EOD Pipeline...")
        # 1. Square-off any active position
        self.force_eod_square_off()
        
        # 2. Save today's Nifty spot 1-minute bars to ClickHouse
        if not self.today_candles_1m.empty:
            self.client.save_today_nifty(self.today_candles_1m)
            
        # 3. Save today's VIX close to ClickHouse
        # Fetch current spot VIX from Redis
        try:
            vix_quote = self.client.client.get_spot_quote("VIX")
            if vix_quote and vix_quote.get("ltp"):
                self.client.save_today_vix(float(vix_quote["ltp"]))
            else:
                # Standard fallback close value if VIX quote is missing in Redis
                self.client.save_today_vix(15.0)
        except Exception as e:
            print(f"⚠️ Failed to update EOD VIX ({e}).")
            
        # 4. Clear/Reset temporary state file for next morning
        if os.path.exists(self.state_file):
            try:
                os.remove(self.state_file)
                print("🧹 Cleared daily state file successfully.")
            except Exception as e:
                print(f"⚠️ Failed to clear state file ({e}).")

    def render_dashboard(self):
        """Renders terminal dashboard showing live TWAP metrics and positions."""
        if sys.stdout.isatty():
            os.system('cls' if os.name == 'nt' else 'clear')
            
        print("=" * 80)
        print("⚡ TWAP BREAKOUT: DYNAMIC FORWARD TESTER & EXECUTION ENGINE ⚡")
        print("=" * 80)
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Mode: {'DRY-RUN (Simulated)' if self.dry_run else 'LIVE PRODUCTION'}")
        print("-" * 80)
        
        # Spot Index & TWAP Bands
        spot = self.client.get_spot_price("NIFTY")
        atm = self.client.get_atm_strike("NIFTY")
        print(f"NIFTY SPOT: {spot:<8.2f} | ATM Strike: {atm:<6} | ML Filter: {'DISABLED' if self.disable_ml else 'ENABLED'}")
        
        print("-" * 80)
        print("ACTIVE POSITION:")
        print(f"{'Asset':<6} | {'Contract':<15} | {'Strike':<6} | {'Type':<4} | {'Qty':<4} | {'Entry':<6} | {'LTP':<6} | {'Trail SL':<8} | {'PnL (₹)':<10}")
        print("-" * 80)
        
        realized_pnl = sum(p.pnl for p in self.closed_positions)
        unrealized_pnl = self.active_position.pnl if self.active_position else 0.0
        
        if not self.active_position:
            print("  No active position.")
        else:
            p = self.active_position
            print(f"NIFTY  | {p.symbol:<15} | {p.strike:<6} | {p.option_type:<4} | {p.qty:<4} | {p.entry_price:<6.2f} | {p.current_price:<6.2f} | {p.premium_sl:<8.2f} | ₹{p.pnl:>8.2f}")
            print(f"  └─ Spot Entry: {p.entry_spot:.2f} | Spot SL: {p.sl_spot:.2f} | Spot Target: {p.tp_spot:.2f}")
            
        print("-" * 80)
        print(f"Realized PnL:   ₹{realized_pnl:,.2f}")
        print(f"Unrealized PnL: ₹{unrealized_pnl:,.2f}")
        print(f"Total Daily PnL:₹{realized_pnl + unrealized_pnl:,.2f}")
        print("=" * 80)

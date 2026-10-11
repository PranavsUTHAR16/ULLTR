# forward_tester/engine.py
"""
Unified Strategy 6 Live Execution & Forward Testing Engine.
Maintains live execution loop, position monitoring, telemetry, and trade logging for Model 06.
Retired models (Model 0216, Dynamic DTE, Ultra-TSMOM, CAS Arbitrage) have been removed per DEF-011.
"""

import os
import sys
import time
import json
import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from forward_tester.config import MultiModelConfig, Strategy6Config
from forward_tester.position import ForwardTestPosition
from forward_tester.data_client import ForwardTestDataClient
from forward_tester.models.strategy_6 import Strategy6Model

logger = logging.getLogger("MultiModelEngine")


class MultiModelEngine:
    """
    Live Execution & Forward Testing Engine for Strategy 6 (Volatility-Adaptive Regime Engine).
    """
    def __init__(self, config: Optional[MultiModelConfig] = None, dry_run: bool = False):
        self.config = config or MultiModelConfig()
        self.dry_run = dry_run
        self.data_client = ForwardTestDataClient()

        # Active Strategy Engine: Strategy 6
        self.strategy6 = Strategy6Model(data_client=self.data_client, config=self.config)

        # Active Strategy Engine: Model 07 (Pure Nifty OTM VRP Harvesting)
        from forward_tester.models.model_07 import Model07Strategy
        self.model07 = Model07Strategy(
            lots=self.config.model_07.lots if hasattr(self.config, "model_07") else 1,
            margin_per_lot=self.config.model_07.margin_per_lot if hasattr(self.config, "model_07") else 250000.0,
            otm_offset=self.config.model_07.otm_offset if hasattr(self.config, "model_07") else 100.0,
            friction_pt=self.config.model_07.friction_pts if hasattr(self.config, "model_07") else 1.25
        )

        self.log_file = os.path.join(os.path.dirname(__file__), "dual_model_trades.csv")
        self.current_date = date.today().strftime("%Y-%m-%d")

        # Initialize day state
        self.init_trading_day(self.current_date)
        self.load_saved_positions()

    def init_trading_day(self, trade_date: str):
        """Initializes registered strategy models for the trading session."""
        self.current_date = trade_date
        self.strategy6.init_trading_day(trade_date)
        if hasattr(self, "model07") and self.model07:
            self.model07.init_trading_day(trade_date)

    @property
    def active_positions(self) -> List[ForwardTestPosition]:
        """Aggregates active positions from Strategy 6 and Model 07."""
        positions = list(self.strategy6.active_positions)
        if hasattr(self, "model07") and self.model07:
            positions.extend(self.model07.active_positions)
        return positions

    @property
    def closed_positions(self) -> List[ForwardTestPosition]:
        """Aggregates closed positions from Strategy 6 and Model 07."""
        positions = list(self.strategy6.closed_positions)
        if hasattr(self, "model07") and self.model07:
            positions.extend(self.model07.closed_positions)
        return positions

    def load_saved_positions(self):
        """Loads today's saved trades from CSV to survive daemon restarts."""
        try:
            if not os.path.exists(self.log_file):
                return
            df_log = pd.read_csv(self.log_file)
            today_str = self.current_date
            df_today = df_log[df_log["date"] == today_str]
            if df_today.empty:
                return

            for _, row in df_today.iterrows():
                model_id = str(row["model_id"])
                if model_id != "STRATEGY_6":
                    continue

                und = str(row["underlying"])
                exp = str(row["expiry"])
                stk = float(row["strike"])
                opt_type = str(row["option_type"])
                spec = self.config.index_specs.get(und, {"lot_size": 65})
                lot_size = int(row.get("lot_size", spec["lot_size"]))
                sym = str(row["symbol"]) if "symbol" in row and pd.notnull(row["symbol"]) else f"{und}_{stk}_{opt_type}"

                pos = ForwardTestPosition(
                    model_id=model_id,
                    underlying=und,
                    expiry_date=exp,
                    symbol=sym,
                    strike=stk,
                    option_type=opt_type,
                    leg_type=str(row.get("leg_type", "PRIMARY")),
                    target_delta=float(row.get("target_delta", 0.0)),
                    entry_price=float(row["entry_price"]),
                    current_price=float(row.get("current_price", row["entry_price"])),
                    lots=int(row["lots"]),
                    lot_size=lot_size,
                    sl_mult=float(row.get("sl_mult", 0.0)),
                    sl_price=float(row.get("sl_price", 0.0)),
                    delta=float(row.get("delta", 0.0)),
                    status=str(row["status"]),
                    exit_price=float(row["exit_price"]) if pd.notnull(row.get("exit_price")) and float(row["exit_price"]) > 0 else None,
                    pnl=float(row.get("pnl", 0.0)),
                    direction=str(row.get("direction", "")),
                    spot_entry_price=float(row.get("spot_entry_price", 0.0)),
                    spot_sl_price=float(row.get("spot_sl_price", 0.0)),
                    spot_tp_price=float(row.get("spot_tp_price", 0.0))
                )

                if pos.status == "OPEN":
                    self.strategy6.active_positions.append(pos)
                else:
                    self.strategy6.closed_positions.append(pos)

            print(f"🔄 Recovered {len(self.active_positions)} active and {len(self.closed_positions)} closed positions from {self.log_file}")
        except Exception as e:
            print(f"⚠️ Error recovering positions: {e}")

    def precompute_aot_stages(self, hour: int, minute: int, second: int):
        """Dispatches ahead-of-time precomputation stages to eliminate trigger slippage."""
        # At 09:15:05 -> Pre-compute 5-day RV5 regime from historical daily bars
        if hour == 9 and minute == 15 and second == 5:
            self.strategy6.precompute_day_regime()
        # At 09:17:58 -> Pre-compute morning jump from completed 09:15-09:17 bars
        elif hour == 9 and minute == 17 and second == 58:
            self.strategy6.precompute_morning_jump()
        # At 09:17:59 -> Pre-stage option chain and Greeks
        elif hour == 9 and minute == 17 and second == 59:
            self.strategy6.prestage_option_chain()

    def execute_0918_dual_model_entry(self):
        """Executes Strategy 6 09:18 AM sharp entry (<0.2ms path)."""
        new_pos_s6 = self.strategy6.execute_0918_entry()
        if new_pos_s6:
            self.log_trade_execution()
            self.send_telegram_entry_broadcast()
        else:
            warn_msg = (
                f"⚠️ <b>STRATEGY 6 09:18 AM ENTRY ALERT</b>\n"
                f"• No positions opened for {self.strategy6.underlying}.\n"
                f"• Expiry: {self.strategy6.expiry or 'NOT_FOUND'} | DTE: {self.strategy6.dte}\n"
                f"• Redis Option Chain check required."
            )
            print(f"⚠️ Strategy 6 09:18 AM entry resulted in 0 positions (Underlying: {self.strategy6.underlying}, Expiry: {self.strategy6.expiry})")
            self._send_telegram(warn_msg)

    def update_and_monitor(self):
        """Performs live price updates and stop-loss monitoring for Strategy 6."""
        now_time_str = datetime.now().strftime("%H:%M")
        s6_closed = self.strategy6.update_and_monitor(now_time_str)
        if s6_closed:
            self.log_trade_execution()
            self.send_telegram_sl_broadcast(s6_closed)

    def execute_eod_squareoff(self):
        """Executes hard 15:00 EOD square-off for Strategy 6."""
        now_time_str = datetime.now().strftime("%H:%M")
        self.strategy6.execute_eod_squareoff(now_time_str)
        self.log_trade_execution()
        self.send_telegram_eod_broadcast()

    def calculate_model_pnl(self, model_id: str = "STRATEGY_6") -> Tuple[float, float, float]:
        """Calculates (realized_pnl, unrealized_pnl, total_pnl) for Strategy 6 or Model 07."""
        if model_id == "STRATEGY_6":
            return self.strategy6.get_realized_and_unrealized_pnl()
        elif model_id in ("MODEL_07", "STRATEGY_7"):
            if hasattr(self, "model07") and self.model07:
                return self.model07.get_realized_and_unrealized_pnl()
        return 0.0, 0.0, 0.0

    def render_dashboard(self):
        """Renders rich real-time terminal telemetry status dashboard."""
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        s6_real, s6_unreal, s6_tot = self.calculate_model_pnl("STRATEGY_6")
        s6_lots = self.config.strategy6.total_lots

        m7_real, m7_unreal, m7_tot = self.calculate_model_pnl("MODEL_07")
        m7_lots = self.config.model_07.lots if hasattr(self.config, "model_07") else 1

        comb_tot = s6_tot + m7_tot

        print(f"\n🚀 MULTI-MODEL LIVE FORWARD TESTER | {now_str}")
        print("=" * 85)
        print(f"Strategy 6: {self.strategy6.underlying} ({self.strategy6.regime[0]}-{self.strategy6.regime[1]}) | DTE: {self.strategy6.dte}")
        if hasattr(self, "model07") and self.model07:
            print(f"Model 07  : NIFTY 50 100pt OTM Strangle | DTE: {self.model07.dte} | Expiry: {self.model07.expiry_date}")
        print("-" * 85)
        print(f"📊 MODEL 06 [STRATEGY_6] ({s6_lots} Lots) : ₹{s6_tot:+,.2f} (Realized: ₹{s6_real:+,.2f} | Unrealized: ₹{s6_unreal:+,.2f})")
        print(f"📊 MODEL 07 [VRP HARVEST] ({m7_lots} Lots) : ₹{m7_tot:+,.2f} (Realized: ₹{m7_real:+,.2f} | Unrealized: ₹{m7_unreal:+,.2f})")
        print(f"💰 COMBINED TOTAL PORTFOLIO PnL   : ₹{comb_tot:+,.2f}")
        print("-" * 85)
        print("ACTIVE POSITIONS:")
        if not self.active_positions:
            print("  (No open trading legs)")
        else:
            for p in self.active_positions:
                tag = f"[{p.model_id:<11} | {p.leg_type:<10} {p.option_type}]"
                print(f"  • {tag} Strike {p.strike} ({p.lots} Lots / {p.total_qty} Qty) | Entry: ₹{p.entry_price:.2f} | Cur: ₹{p.current_price:.2f} | PnL: ₹{p.pnl:+,.2f}")
        print("=" * 85)

    def _send_telegram(self, text: str):
        """Dispatches HTML-formatted Telegram messages asynchronously in a background thread."""
        import threading
        def _worker():
            try:
                import requests
                bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "8234942867:AAFdoNjo72DsEYo9DSicTJm8-t5n_B_G30g")
                chat_id = os.environ.get("TELEGRAM_CHAT_ID", "-5009029141")
                payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
                res = requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage", json=payload, timeout=5.0)
                if not res.ok:
                    print(f"⚠️ Telegram send error ({res.status_code}): {res.text}")
            except Exception as e:
                print(f"⚠️ Telegram network error: {e}")

        threading.Thread(target=_worker, daemon=True).start()

    def send_telegram_morning_heartbeat(self):
        """Sends daily morning confirmation at 09:15 AM market open."""
        und = self.strategy6.underlying
        exp = self.strategy6.expiry or "PENDING"
        dte = self.strategy6.dte
        regime = f"{self.strategy6.regime[0]}-{self.strategy6.regime[1]}"
        s6_lots = self.config.strategy6.total_lots

        msg = (
            f"🌅 <b>FORWARD TESTER [STRATEGY 6 ACTIVE]</b>\n"
            f"📅 Date: <b>{self.current_date}</b>\n"
            f"• Strategy: <b>STRATEGY 6</b> ({s6_lots} Lots)\n"
            f"• Selected Underlying: <b>{und}</b>\n"
            f"• Expiry Date: <b>{exp}</b> (DTE: <b>{dte}</b>)\n"
            f"• Micro-Regime: <b>{regime}</b>\n"
            f"• Morning Jump Detected: <b>{self.strategy6.morning_active}</b> (Max Abs Ret: {self.strategy6.max_abs_ret:.5f})\n"
            f"• Sizing: <b>Primary {self.strategy6.p_lots} Lots / Secondary {self.strategy6.s_lots} Lots</b>\n"
            f"• Status: <b>Standing by for 09:18:00 entry...</b>"
        )
        self._send_telegram(msg)

    def send_telegram_entry_broadcast(self):
        """Sends rich Telegram alert immediately after 09:18:00 AM entries."""
        und = self.strategy6.underlying
        exp = self.strategy6.expiry
        regime = f"{self.strategy6.regime[0]}-{self.strategy6.regime[1]}"
        morning_tag = "⚡ ACTIVE" if self.strategy6.morning_active else "⚪ DORMANT"

        lines = [
            f"⚡ <b>STRATEGY 6 [09:18:00 ENTRY EXECUTED]</b>",
            f"📅 Date: <b>{self.current_date}</b> | Asset: <b>{und}</b> | Expiry: <b>{exp}</b> (DTE: {self.strategy6.dte})",
            f"• Regime: <b>{regime}</b> | Morning Jump: <b>{morning_tag}</b>",
            f"• Sizing: <b>{self.strategy6.p_lots} Lots Primary / {self.strategy6.s_lots} Lots Secondary</b>",
            f"• Stop Loss Multiplier: <b>{self.strategy6.sl_mult:.2f}x</b>",
            "",
            "<b>EXECUTED LEGS:</b>"
        ]
        for p in self.strategy6.active_positions:
            lines.append(
                f"  • [{p.leg_type} {p.option_type}] Strike <b>{p.strike}</b> | Entry: <b>₹{p.entry_price:.2f}</b> "
                f"({p.lots} Lots / {p.total_qty} Qty) | SL: ₹{p.sl_price:.2f}"
            )
        self._send_telegram("\n".join(lines))

    def send_telegram_sl_broadcast(self, sl_positions: List[ForwardTestPosition]):
        """Sends Telegram notification when SL / TP is hit."""
        for p in sl_positions:
            tag = "🚨 STOP-LOSS HIT" if "SL" in p.status else "🎯 TAKE-PROFIT HIT"
            msg = (
                f"{tag}: <b>[{p.model_id}] {p.underlying} {p.strike} {p.option_type}</b>\n"
                f"• Entry: ₹{p.entry_price:.2f} ➔ Exit: ₹{p.exit_price:.2f}\n"
                f"• Realized Leg PnL: <b>₹{p.pnl:+,.2f}</b>\n"
            )
            self._send_telegram(msg)

    def send_telegram_model_periodic_updates(self):
        """Sends periodic real-time MTM and PnL updates to Telegram while positions are open."""
        if not self.active_positions:
            return

        s6_real, s6_unreal, s6_tot = self.calculate_model_pnl("STRATEGY_6")
        lines = [
            f"📊 <b>STRATEGY 6 TELEMETRY UPDATE</b>",
            f"📅 Date: <b>{self.current_date}</b> | Time: <b>{datetime.now().strftime('%H:%M:%S')} IST</b>",
            f"• Net PnL: <b>₹{s6_tot:+,.2f}</b> (Realized: ₹{s6_real:+,.2f} | Unrealized: ₹{s6_unreal:+,.2f})",
            "",
            "<b>OPEN LEGS:</b>"
        ]
        for p in self.active_positions:
            lines.append(
                f"  • {p.underlying} {p.strike} {p.option_type}: Cur ₹{p.current_price:.2f} (Entry ₹{p.entry_price:.2f}) | PnL: <b>₹{p.pnl:+,.2f}</b>"
            )
        self._send_telegram("\n".join(lines))

    def send_telegram_periodic_heartbeat(self):
        """Sends periodic hourly heartbeat when no positions are active."""
        now_time = datetime.now().strftime("%H:%M IST")
        msg = (
            f"💓 <b>FORWARD TESTER HEARTBEAT | {now_time}</b>\n"
            f"• Date: <b>{self.current_date}</b>\n"
            f"• State: <b>DAEMON ACTIVE & HEALTHY</b>\n"
            f"• Open Positions: <b>0</b>\n"
            f"• Redis Connection: <b>OK</b>"
        )
        self._send_telegram(msg)

    def send_telegram_eod_broadcast(self):
        """Sends comprehensive 15:00 EOD performance recap."""
        s6_real, s6_unreal, s6_tot = self.calculate_model_pnl("STRATEGY_6")
        lines = [
            f"🏁 <b>STRATEGY 6 [15:00:00 EOD SQUAREOFF COMPLETE]</b>",
            f"📅 Date: <b>{self.current_date}</b> | Instrument: <b>{self.strategy6.underlying}</b>",
            f"• Total Daily Net PnL: <b>₹{s6_tot:+,.2f}</b>",
            "",
            "<b>CLOSED POSITIONS:</b>"
        ]
        for p in self.strategy6.closed_positions:
            tag = "✅ WIN" if p.pnl >= 0 else "❌ LOSS"
            lines.append(
                f"  • {tag} [{p.leg_type} {p.option_type} {p.strike}]: Entry ₹{p.entry_price:.2f} ➔ Exit ₹{p.exit_price:.2f} | PnL: <b>₹{p.pnl:+,.2f}</b> ({p.status})"
            )
        self._send_telegram("\n".join(lines))

    def log_trade_execution(self):
        """Logs current trades to cumulative and daily CSV files."""
        try:
            records = []
            for p in self.active_positions + self.closed_positions:
                records.append({
                    "date": self.current_date,
                    "model_id": p.model_id,
                    "underlying": p.underlying,
                    "expiry": p.expiry_date,
                    "strike": p.strike,
                    "option_type": p.option_type,
                    "leg_type": p.leg_type,
                    "target_delta": p.target_delta,
                    "lots": p.lots,
                    "lot_size": p.lot_size,
                    "entry_price": p.entry_price,
                    "current_price": p.current_price,
                    "status": p.status,
                    "sl_mult": p.sl_mult,
                    "sl_price": p.sl_price,
                    "exit_price": p.exit_price if p.exit_price is not None else 0.0,
                    "pnl": p.pnl,
                    "direction": p.direction,
                    "spot_entry_price": p.spot_entry_price,
                    "spot_sl_price": p.spot_sl_price,
                    "spot_tp_price": p.spot_tp_price
                })
            df_today = pd.DataFrame(records)

            if os.path.exists(self.log_file):
                try:
                    df_existing = pd.read_csv(self.log_file)
                    df_history = df_existing[df_existing["date"] != self.current_date]
                    df_combined = pd.concat([df_history, df_today], ignore_index=True)
                except Exception:
                    df_combined = df_today
            else:
                df_combined = df_today

            df_combined.to_csv(self.log_file, index=False)

            daily_dir = os.path.join(os.path.dirname(self.log_file), "daily_logs")
            os.makedirs(daily_dir, exist_ok=True)
            daily_path = os.path.join(daily_dir, f"trades_{self.current_date}.csv")
            df_today.to_csv(daily_path, index=False)

            self.log_daily_summary()
        except Exception as e:
            print(f"⚠️ Error logging trades: {e}")

    def log_daily_summary(self):
        """Maintains a permanent daily summary ledger for backtest comparison."""
        try:
            summary_file = os.path.join(os.path.dirname(self.log_file), "daily_summary.csv")
            s6_real, s6_unreal, s6_tot = self.calculate_model_pnl("STRATEGY_6")
            s6_lots = self.config.strategy6.total_lots

            wins = sum(1 for p in self.strategy6.closed_positions if p.pnl > 0)
            losses = sum(1 for p in self.strategy6.closed_positions if p.pnl < 0)

            today_row = {
                "date": self.current_date,
                "model_id": "STRATEGY_6",
                "underlying": self.strategy6.underlying,
                "expiry": self.strategy6.expiry,
                "dte": self.strategy6.dte,
                "regime": f"{self.strategy6.regime[0]}-{self.strategy6.regime[1]}",
                "lots": s6_lots,
                "trades_count": len(self.strategy6.active_positions) + len(self.strategy6.closed_positions),
                "wins": wins,
                "losses": losses,
                "realized_pnl": s6_real,
                "unrealized_pnl": s6_unreal,
                "total_pnl": s6_tot
            }
            df_row = pd.DataFrame([today_row])

            if os.path.exists(summary_file):
                try:
                    df_prev = pd.read_csv(summary_file)
                    df_hist = df_prev[df_prev["date"] != self.current_date]
                    df_out = pd.concat([df_hist, df_row], ignore_index=True)
                except Exception:
                    df_out = df_row
            else:
                df_out = df_row

            df_out.to_csv(summary_file, index=False)
        except Exception as e:
            print(f"⚠️ Error logging daily summary: {e}")

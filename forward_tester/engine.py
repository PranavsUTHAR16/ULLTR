# forward_tester/engine.py
import os
import sys
import time
import json
import numpy as np
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Tuple

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from forward_tester.config import MultiModelConfig, Strategy6Config, Model0216Config, DynamicDTEConfig, UltraTSMOMConfig
from forward_tester.position import ForwardTestPosition
from forward_tester.data_client import ForwardTestDataClient
from forward_tester.models.strategy_6 import Strategy6Model
from forward_tester.models.model_0216 import Model0216
from forward_tester.models.dynamic_dte import DynamicDTEModel
from forward_tester.models.ultra_tsmom import UltraTSMOMModel

class MultiModelEngine:
    """
    Unified Modular Multi-Model Live Execution & Forward Testing Engine:
      • Model 1: STRATEGY_6 (Volatility-Adaptive Regime Engine @ 10 Lots)
      • Model 2: 0216_MODEL (Master Derivatives Engine @ 10 Lots / 5m Candle Boundary)
      • Model 3: DYNAMIC_DTE (Dynamic DTE Arbitrage TWAP Touch & Rejection Engine)
      • Model 4: ULTRA_TSMOM (15-min Z-score Momentum Engine)
    """
    def __init__(self, config: Optional[MultiModelConfig] = None, dry_run: bool = False):
        self.config = config or MultiModelConfig()
        self.dry_run = dry_run
        self.data_client = ForwardTestDataClient()
        
        # Instantiate Modular Strategy Engines
        self.strategy6 = Strategy6Model(data_client=self.data_client, config=self.config)
        self.model_0216 = Model0216(data_client=self.data_client, config=self.config)
        self.dynamic_dte = DynamicDTEModel(data_client=self.data_client, config=self.config)
        self.ultra_tsmom = UltraTSMOMModel(data_client=self.data_client, config=self.config)
        
        self.log_file = os.path.join(os.path.dirname(__file__), "dual_model_trades.csv")
        self.last_5m_checked: str = ""
        self.current_date = date.today().strftime("%Y-%m-%d")
        
        # Initialize day state across all models
        self.init_trading_day(self.current_date)
        self.load_saved_positions()

    def init_trading_day(self, trade_date: str):
        """Initializes all registered models for the trading session."""
        self.current_date = trade_date
        self.strategy6.init_trading_day(trade_date)
        self.model_0216.init_trading_day(trade_date)
        self.dynamic_dte.init_trading_day(trade_date)
        self.ultra_tsmom.init_trading_day(trade_date)

    @property
    def active_positions(self) -> List[ForwardTestPosition]:
        """Aggregates active positions from all modular sub-models."""
        return (
            self.strategy6.active_positions +
            self.model_0216.active_positions +
            self.dynamic_dte.active_positions +
            self.ultra_tsmom.active_positions
        )

    @property
    def closed_positions(self) -> List[ForwardTestPosition]:
        """Aggregates closed positions from all modular sub-models."""
        return (
            self.strategy6.closed_positions +
            self.model_0216.closed_positions +
            self.dynamic_dte.closed_positions +
            self.ultra_tsmom.closed_positions
        )

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
                
                if model_id == "STRATEGY_6":
                    target_model = self.strategy6
                elif model_id == "0216_MODEL":
                    target_model = self.model_0216
                elif model_id == "DYNAMIC_DTE":
                    target_model = self.dynamic_dte
                else:
                    target_model = self.ultra_tsmom

                if pos.status == "OPEN":
                    target_model.active_positions.append(pos)
                else:
                    target_model.closed_positions.append(pos)

            print(f"🔄 Recovered {len(self.active_positions)} active and {len(self.closed_positions)} closed positions from {self.log_file}")
        except Exception as e:
            print(f"⚠️ Error recovering positions: {e}")

    def execute_0918_dual_model_entry(self):
        """Executes Strategy 6 and Ultra-TSMOM 09:18 AM sharp entries."""
        new_pos_s6 = self.strategy6.execute_0918_entry()
        if new_pos_s6:
            self.log_trade_execution()
            self.send_telegram_entry_broadcast()

    def evaluate_5m_boundary(self):
        """Evaluates 5-minute candle boundary execution for Model 0216 and Model 3 (Dynamic DTE)."""
        now_dt = datetime.now()
        is_5m_boundary = (now_dt.minute % 5 == 0) and (now_dt.second <= 15)
        if not is_5m_boundary:
            return

        time_str = now_dt.strftime("%H:%M")
        if time_str == self.last_5m_checked:
            return
        self.last_5m_checked = time_str

        # Evaluate 0216 Model
        res_0216 = self.model_0216.on_5m_candle_close(time_str)
        if res_0216 and res_0216.get("action") == "ENTRY":
            self.log_trade_execution()
            self.send_telegram_0216_entry(res_0216)

        # Evaluate Dynamic DTE Model
        res_dd = self.dynamic_dte.on_5m_candle_close(time_str)
        if res_dd and res_dd.get("action") == "ENTRY":
            self.log_trade_execution()
            self.send_telegram_dynamic_dte_entry(res_dd["position"])

    def update_and_monitor(self):
        """Performs live price updates, trailing stop evaluations, and 5m candle evaluations."""
        now_time_str = datetime.now().strftime("%H:%M")
        
        # 1. Update Strategy 6
        s6_closed = self.strategy6.update_and_monitor(now_time_str)
        if s6_closed:
            self.log_trade_execution()
            self.send_telegram_sl_broadcast(s6_closed)

        # 2. Evaluate 5-Minute Candle Boundary (0216 Model & Dynamic DTE)
        self.evaluate_5m_boundary()

        # 3. Update Model 0216 Active Positions
        m2_closed = self.model_0216.update_and_monitor(now_time_str)
        if m2_closed:
            self.log_trade_execution()
            self.send_telegram_sl_broadcast(m2_closed)

        # 4. Update Dynamic DTE Active Positions
        dd_closed = self.dynamic_dte.update_and_monitor(now_time_str)
        if dd_closed:
            self.log_trade_execution()
            self.send_telegram_sl_broadcast(dd_closed)

        # 5. Update Ultra-TSMOM Active Positions
        ut_closed = self.ultra_tsmom.update_and_monitor(now_time_str)
        if ut_closed:
            self.log_trade_execution()
            self.send_telegram_sl_broadcast(ut_closed)

    def execute_eod_squareoff(self):
        """Executes hard 15:00 EOD square-off across all models."""
        now_time_str = datetime.now().strftime("%H:%M")
        self.strategy6.execute_eod_squareoff(now_time_str)
        self.model_0216.execute_eod_squareoff(now_time_str)
        self.dynamic_dte.execute_eod_squareoff(now_time_str)
        self.ultra_tsmom.execute_eod_squareoff(now_time_str)
        self.log_trade_execution()
        self.send_telegram_eod_broadcast()

    def calculate_model_pnl(self, model_id: str) -> Tuple[float, float, float]:
        """Calculates (realized_pnl, unrealized_pnl, total_pnl) for a specific model."""
        if model_id == "STRATEGY_6":
            model = self.strategy6
        elif model_id == "0216_MODEL":
            model = self.model_0216
        elif model_id == "DYNAMIC_DTE":
            model = self.dynamic_dte
        else:
            model = self.ultra_tsmom
        return model.get_realized_and_unrealized_pnl()

    def render_dashboard(self):
        """Renders rich real-time terminal telemetry status dashboard across all active models."""
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        s6_real, s6_unreal, s6_tot = self.calculate_model_pnl("STRATEGY_6")
        m2_real, m2_unreal, m2_tot = self.calculate_model_pnl("0216_MODEL")
        dd_real, dd_unreal, dd_tot = self.calculate_model_pnl("DYNAMIC_DTE")
        comb_tot = s6_tot + m2_tot + dd_tot

        print(f"\n🚀 MULTI-MODEL LIVE FORWARD TESTER | {now_str}")
        print("=" * 85)
        print(f"Strategy 6: {self.strategy6.underlying} ({self.strategy6.regime[0]}-{self.strategy6.regime[1]}) | 0216 Asset: {self.model_0216.target_asset} | Dynamic DTE: {self.dynamic_dte.underlying}")
        print("-" * 85)
        print(f"📊 MODEL 1 [STRATEGY_6] (10 Lots) : ₹{s6_tot:+,.2f} (Realized: ₹{s6_real:+,.2f} | Unrealized: ₹{s6_unreal:+,.2f})")
        print(f"📊 MODEL 2 [0216_MODEL] (10 Lots) : ₹{m2_tot:+,.2f} (Realized: ₹{m2_real:+,.2f} | Unrealized: ₹{m2_unreal:+,.2f})")
        print(f"📊 MODEL 3 [DYNAMIC_DTE]          : ₹{dd_tot:+,.2f} (Realized: ₹{dd_real:+,.2f} | Unrealized: ₹{dd_unreal:+,.2f})")
        print(f"💰 COMBINED TOTAL PORTFOLIO PnL   : ₹{comb_tot:+,.2f} (Max Cap: 20 Lots / ₹50L Capital)")
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

    def send_telegram_entry_broadcast(self):
        """Sends Telegram notification on Strategy 6 entry."""
        lines = [
            "⚡ <b>MODEL 1 [STRATEGY 6] 09:18 AM LIVE ENTRY EXECUTED (10 Lots)</b>",
            f"• Underlying: <b>{self.strategy6.underlying}</b> | Expiry: <b>{self.strategy6.expiry}</b>",
            f"• Micro-Regime: <b>{self.strategy6.regime[0]}-{self.strategy6.regime[1]}</b>",
            "\n<b>Active Positions:</b>"
        ]
        for p in self.strategy6.active_positions:
            lines.append(f"  • <b>{p.leg_type} {p.strike} {p.option_type}</b>: {p.lots} Lots @ ₹{p.entry_price:.2f} (SL: ₹{p.sl_price:.2f})")
        self._send_telegram("\n".join(lines))

    def send_telegram_0216_entry(self, res: dict):
        """Sends Telegram notification on 0216 Model entry."""
        pos = res["position"]
        msg = (
            f"⚡ <b>MODEL 2 [0216 MASTER ENGINE] 5M CANDLE ENTRY (10 Lots)</b>\n"
            f"• Asset: <b>{pos.underlying} {pos.strike} {pos.option_type}</b>\n"
            f"• Direction: <b>{res['direction']}</b> | PCR Velocity: <b>{res['pcr']:.2f}</b>\n"
            f"• Entry Premium: <b>₹{res['opt_price']:.2f}</b> ({pos.lots} Lots / {pos.total_qty} Qty)\n"
            f"• Futures Anchor: Entry ₹{pos.spot_entry_price:.2f} | Stop Loss ₹{pos.spot_sl_price:.2f}\n"
        )
        self._send_telegram(msg)

    def send_telegram_dynamic_dte_entry(self, pos: ForwardTestPosition):
        """Sends Telegram entry notification for Model 3 (Dynamic DTE)."""
        msg = (
            f"🎯 <b>MODEL 3 [DYNAMIC_DTE] 5M REJECTION ENTRY</b>\n"
            f"• Asset: <b>{pos.underlying} {pos.strike} {pos.option_type}</b>\n"
            f"• Direction: <b>{pos.direction}</b> | Lots: {pos.lots} ({pos.total_qty} Qty)\n"
            f"• Option Entry: <b>₹{pos.entry_price:.2f}</b>\n"
            f"• Spot Anchor: Entry ₹{pos.spot_entry_price:.2f} | Spot SL ₹{pos.spot_sl_price:.2f} | Spot TP ₹{pos.spot_tp_price:.2f}"
        )
        self._send_telegram(msg)

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
        m2_real, m2_unreal, m2_tot = self.calculate_model_pnl("0216_MODEL")
        dd_real, dd_unreal, dd_tot = self.calculate_model_pnl("DYNAMIC_DTE")
        comb_tot = s6_tot + m2_tot + dd_tot

        now_str = datetime.now().strftime("%H:%M:%S")
        lines = [
            f"📡 <b>LIVE TELEMETRY UPDATE | {now_str}</b>",
            f"• Model 1 [Strategy 6] (10L): <b>₹{s6_tot:+,.2f}</b> (Unrealized: ₹{s6_unreal:+,.2f})",
            f"• Model 2 [0216 Model] (10L): <b>₹{m2_tot:+,.2f}</b> (Unrealized: ₹{m2_unreal:+,.2f})",
            f"• Model 3 [Dynamic DTE]: <b>₹{dd_tot:+,.2f}</b> (Unrealized: ₹{dd_unreal:+,.2f})",
            f"💰 <b>Combined Portfolio PnL: ₹{comb_tot:+,.2f}</b>",
            "\n<b>Active Open Legs:</b>"
        ]
        for p in self.active_positions:
            lines.append(f"  • <b>[{p.model_id}] {p.strike} {p.option_type}</b>: {p.lots}L @ ₹{p.entry_price:.2f} ➔ ₹{p.current_price:.2f} (PnL: <b>₹{p.pnl:+,.2f}</b>)")
        
        self._send_telegram("\n".join(lines))

    def send_telegram_eod_broadcast(self):
        """Sends Telegram daily summary at 15:00 market close."""
        s6_real, _, _ = self.calculate_model_pnl("STRATEGY_6")
        m2_real, _, _ = self.calculate_model_pnl("0216_MODEL")
        dd_real, _, _ = self.calculate_model_pnl("DYNAMIC_DTE")
        comb = s6_real + m2_real + dd_real
        msg = (
            f"🏁 <b>EOD MARKET SQUARE-OFF COMPLETED</b>\n"
            f"• Model 1 [Strategy 6] (10 Lots): <b>₹{s6_real:+,.2f}</b>\n"
            f"• Model 2 [0216 Model] (10 Lots): <b>₹{m2_real:+,.2f}</b>\n"
            f"• Model 3 [Dynamic DTE]: <b>₹{dd_real:+,.2f}</b>\n"
            f"• <b>Total Realized Portfolio PnL: ₹{comb:+,.2f}</b>"
        )
        self._send_telegram(msg)

    def log_trade_execution(self):
        """Saves current trade records to CSV."""
        try:
            records = []
            for p in self.active_positions + self.closed_positions:
                records.append({
                    "date": self.current_date,
                    "model_id": p.model_id,
                    "underlying": p.underlying,
                    "expiry": p.expiry_date,
                    "symbol": p.symbol,
                    "strike": p.strike,
                    "option_type": p.option_type,
                    "leg_type": p.leg_type,
                    "target_delta": p.target_delta,
                    "entry_price": p.entry_price,
                    "current_price": p.current_price,
                    "lots": p.lots,
                    "lot_size": p.lot_size,
                    "sl_mult": p.sl_mult,
                    "sl_price": p.sl_price,
                    "delta": p.delta,
                    "status": p.status,
                    "exit_price": p.exit_price if p.exit_price is not None else 0.0,
                    "pnl": p.pnl,
                    "direction": p.direction,
                    "spot_entry_price": p.spot_entry_price,
                    "spot_sl_price": p.spot_sl_price,
                    "spot_tp_price": p.spot_tp_price
                })
            df = pd.DataFrame(records)
            df.to_csv(self.log_file, index=False)
        except Exception as e:
            print(f"⚠️ Error logging trades: {e}")

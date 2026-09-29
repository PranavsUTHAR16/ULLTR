#!/usr/bin/env python3
"""
ULLTR Unified Portfolio Telegram Sidecar Daemon
===============================================
Author: ULLTR Advanced Systems
Environment: Python 3.10+ (Native Redis Unix Socket / TCP)

Role:
  1. Subscribes in real time to Redis Pub/Sub channel 'ulltr:trades:stream'
     to immediately dispatch instant trade alerts (ENTRY, EXIT, TARGET_ROLLED).
  2. Every 10 seconds, samples 'ulltr:portfolio:state' and broadcasts live
     telemetry updates to Telegram while positions are actively running.
  3. Dispatches scheduled morning session confirmations (09:15 IST) and
     end-of-day (15:20 IST) consolidated performance reports.
"""

import os
import sys
import time
import json
import logging
import urllib.parse
import urllib.request
from datetime import datetime, time as dtime
import threading
from typing import Dict, Any, Optional

import redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("TelegramPortfolioDaemon")

# Configuration Defaults
DEFAULT_REDIS_SOCKET = "/Users/prana/Desktop/open_source/web/redis.sock"
DEFAULT_BOT_TOKEN = "8234942867:AAFdoNjo72DsEYo9DSicTJm8-t5n_B_G30g"
DEFAULT_CHAT_ID = "-5009029141"
TELEMETRY_INTERVAL_SECS = 10.0


class TelegramDispatcher:
    """Handles Telegram HTTP requests with thread pooling and rate protection."""

    def __init__(self, bot_token: str, chat_id: str, thread_id: Optional[int] = None):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.thread_id = thread_id
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

    def send_message(self, text: str) -> bool:
        """Sends an HTML formatted message to Telegram synchronously."""
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML"
        }
        if self.thread_id:
            payload["message_thread_id"] = self.thread_id

        try:
            data = urllib.parse.urlencode(payload).encode("utf-8")
            req = urllib.request.Request(
                self.base_url,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"Telegram dispatch failed: {e}")
            return False

    def send_async(self, text: str) -> None:
        """Dispatches Telegram message in a background worker thread."""
        threading.Thread(target=self.send_message, args=(text,), daemon=True).start()


def get_redis_client() -> redis.Redis:
    """Connects to Redis via Unix socket if present, else TCP localhost."""
    socket_path = os.getenv("REDIS_UNIX_SOCKET", DEFAULT_REDIS_SOCKET)
    if os.path.exists(socket_path):
        try:
            r = redis.Redis(unix_socket_path=socket_path, decode_responses=True)
            r.ping()
            logger.info(f"Connected to Redis via Unix socket: {socket_path}")
            return r
        except Exception as e:
            logger.warning(f"Failed to connect via unix socket ({e}), trying TCP fallback...")

    host = os.getenv("REDIS_HOST", "127.0.0.1")
    port = int(os.getenv("REDIS_PORT", "6379"))
    r = redis.Redis(host=host, port=port, decode_responses=True)
    r.ping()
    logger.info(f"Connected to Redis via TCP: {host}:{port}")
    return r


class PortfolioTelegramDaemon:
    """Coordinates real-time stream subscription and 10-second periodic telemetry."""

    def __init__(self):
        bot_token = os.getenv("UNIFIED_TELEGRAM_BOT_TOKEN", os.getenv("TELEGRAM_BOT_TOKEN", DEFAULT_BOT_TOKEN))
        chat_id = os.getenv("UNIFIED_TELEGRAM_CHAT_ID", os.getenv("TELEGRAM_CHAT_ID", DEFAULT_CHAT_ID))
        thread_id_str = os.getenv("UNIFIED_TELEGRAM_THREAD_ID", os.getenv("TELEGRAM_THREAD_ID", ""))
        thread_id = int(thread_id_str) if thread_id_str.isdigit() else None

        self.tg = TelegramDispatcher(bot_token, chat_id, thread_id)
        self.r = get_redis_client()
        self.running = True

        # State tracking
        self.last_heartbeat_hour = -1
        self.eod_sent_date: Optional[str] = None
        self.morning_sent_date: Optional[str] = None

    def handle_trade_event(self, data: Dict[str, Any]) -> None:
        """Formats and broadcasts real-time trade events to Telegram."""
        event = data.get("event", "")
        model = data.get("model", "")
        symbol = data.get("symbol", "")
        otype = data.get("option_type", "")
        strike = data.get("strike", 0)
        lots = data.get("lots", 0)
        qty = data.get("quantity", 0)
        entry_opt = data.get("entry_opt", 0.0)
        entry_fut = data.get("entry_fut", 0.0)
        exit_opt = data.get("exit_opt", 0.0)
        pnl = data.get("pnl", 0.0)
        reason = data.get("reason", "")
        free_cash = data.get("free_cash", 0.0)
        margin_locked = data.get("margin_locked", 0.0)

        now_str = datetime.now().strftime("%H:%M:%S")

        if event == "POSITION_OPENED":
            msg = (
                f"🚀 <b>[NEW ENTRY] {model} | {otype} Long</b>\n"
                f"• <b>Symbol:</b> {symbol}\n"
                f"• <b>Strike:</b> {strike} {otype} ({lots} Lots / {qty} Qty)\n"
                f"• <b>Entry Premium:</b> ₹{entry_opt:.2f}\n"
                f"• <b>Futures Ref:</b> {entry_fut:.2f}\n"
                f"• <b>Margin Locked:</b> ₹{margin_locked:,.2f}\n"
                f"• <b>Remaining Free Cash:</b> ₹{free_cash:,.2f}\n"
                f"• ⏱️ <b>Time:</b> {now_str} IST"
            )
            logger.info(f"Broadcasting ENTRY: {model} {symbol}")
            self.tg.send_async(msg)

        elif event == "POSITION_CLOSED":
            icon = "🟢" if pnl >= 0 else "🔴"
            pts = (exit_opt - entry_opt) if entry_opt > 0 else 0.0
            msg = (
                f"{icon} <b>[CLOSED] {model} | {symbol}</b>\n"
                f"• <b>Exit Reason:</b> {reason}\n"
                f"• <b>Entry:</b> ₹{entry_opt:.2f} ➔ <b>Exit:</b> ₹{exit_opt:.2f} ({pts:+5.2f} pts)\n"
                f"• <b>Net PnL:</b> ₹{pnl:+,.2f}\n"
                f"• <b>Released Margin:</b> ₹{margin_locked:,.2f}\n"
                f"• <b>Free Cash:</b> ₹{free_cash:,.2f}\n"
                f"• ⏱️ <b>Time:</b> {now_str} IST"
            )
            logger.info(f"Broadcasting EXIT: {model} {symbol} (PnL: ₹{pnl:+,.2f})")
            self.tg.send_async(msg)

        elif event == "TARGET_ROLLED":
            msg = (
                f"🎯 <b>[TARGET ROLLED] {model} | {symbol}</b>\n"
                f"• <b>Status:</b> Target 1 (+60 pts) Hit with Macro CVD confirmation!\n"
                f"• <b>Extended Target:</b> +120 pts / 4R\n"
                f"• <b>Trailed SL:</b> +30 pts guaranteed profit\n"
                f"• ⏱️ <b>Time:</b> {now_str} IST"
            )
            logger.info(f"Broadcasting TARGET_ROLLED: {model} {symbol}")
            self.tg.send_async(msg)

        elif event == "T1_BANKED":
            msg = (
                f"🎯 <b>[T1 BANKED (+30pt FUT)] {model} | {symbol}</b>\n"
                f"• <b>Status:</b> Lot 1 Banked at ₹{exit_opt:.2f} (1.5R Gain)!\n"
                f"• <b>Realized Profit:</b> ₹{pnl:+,.2f}\n"
                f"• <b>Lot 2 Runner:</b> SL locked at Breakeven (+2.0 pt) & Trailing Session VWAP\n"
                f"• ⏱️ <b>Time:</b> {now_str} IST"
            )
            logger.info(f"Broadcasting T1_BANKED: {model} {symbol}")
            self.tg.send_async(msg)

    def listen_trade_events(self) -> None:
        """Background thread worker subscribing to 'ulltr:trades:stream'."""
        pubsub = self.r.pubsub()
        pubsub.subscribe("ulltr:trades:stream")
        logger.info("Subscribed to Redis channel 'ulltr:trades:stream' for instant trade execution alerts.")

        for item in pubsub.listen():
            if not self.running:
                break
            if item["type"] == "message":
                try:
                    payload = json.loads(item["data"])
                    self.handle_trade_event(payload)
                except Exception as e:
                    logger.error(f"Error decoding trade event: {e}")

    def run_telemetry_loop(self) -> None:
        """Main loop: broadcasts live telemetry every 10 seconds while positions exist."""
        logger.info("Starting 10-second telemetry polling loop...")

        while self.running:
            try:
                now_dt = datetime.now()
                now_time = now_dt.time()
                today_str = now_dt.strftime("%Y-%m-%d")

                # 1. Check Morning Session Initialization at 09:15 IST
                if now_time >= dtime(9, 15, 0) and now_time < dtime(15, 30, 0) and self.morning_sent_date != today_str:
                    init_msg = (
                        f"🟢 <b>[C++ FORWARD TESTER ACTIVE] Session Initialized</b>\n"
                        f"📅 <b>Date:</b> {today_str}\n"
                        f"⚡ <b>Engine:</b> Native C++ Sub-Microsecond Execution (In-Memory)\n"
                        f"🎯 <b>Strategies:</b> ModelPOC V2 (Two-Tier Morning VWAP) + ModelSpatialBox (Dual AVWAP)\n"
                        f"🛡️ <b>Risk:</b> ₹20k Pool, 2 Max Lots, ₹10k Margin/Lot, Directional Conflict Filter"
                    )
                    self.tg.send_async(init_msg)
                    self.morning_sent_date = today_str

                # 2. Query latest state from Redis key
                raw_state = self.r.get("ulltr:portfolio:state")
                if raw_state:
                    state = json.loads(raw_state)
                    active_positions = state.get("active_positions", [])
                    tot_val = state.get("total_portfolio_value", 20000.0)
                    start_cap = state.get("starting_capital", 20000.0)
                    realized_pnl = state.get("realized_pnl_today", 0.0)
                    unrealized_pnl = state.get("unrealized_pnl", 0.0)
                    free_cash = state.get("free_cash", 0.0)
                    locked_margin = state.get("locked_margin", 0.0)
                    roi_pct = ((tot_val - start_cap) / start_cap) * 100.0 if start_cap > 0 else 0.0
                    now_str = now_dt.strftime("%H:%M:%S")

                    # Live Telemetry Broadcast: Sent every 10 seconds when positions are active
                    if active_positions:
                        lines = [
                            f"📡 <b>LIVE TELEMETRY UPDATE | {now_str} IST</b>",
                            f"• <b>Portfolio Value:</b> ₹{tot_val:,.2f} ({roi_pct:+5.2f}%)",
                            f"• <b>Realized PnL Today:</b> ₹{realized_pnl:+,.2f}",
                            f"• <b>Unrealized MTM:</b> ₹{unrealized_pnl:+,.2f}",
                            f"• <b>Free Cash:</b> ₹{free_cash:,.2f} | <b>Locked Margin:</b> ₹{locked_margin:,.2f}",
                            f"\n<b>ACTIVE POSITIONS ({len(active_positions)}):</b>"
                        ]
                        for p in active_positions:
                            m_name = p.get("model_name", "")
                            stk = p.get("strike", 0)
                            otype = p.get("option_type", "")
                            lots = p.get("lots", 0)
                            e_opt = p.get("entry_opt", 0.0)
                            c_opt = p.get("current_opt", 0.0)
                            u_pnl = p.get("unrealized_pnl", 0.0)
                            pts = p.get("points", 0.0)
                            lines.append(
                                f"  • [{m_name}] {stk} {otype} ({lots}L) | Entry: ₹{e_opt:.2f} ➔ LTP: ₹{c_opt:.2f} | MTM: ₹{u_pnl:+,.2f} ({pts:+5.2f} pts)"
                            )

                        logger.info(f"Broadcasting 10-second telemetry update ({len(active_positions)} active positions)")
                        self.tg.send_async("\n".join(lines))

                    # Hourly Heartbeat when flat
                    elif now_dt.minute == 0 and now_dt.hour != self.last_heartbeat_hour and (9 <= now_dt.hour <= 15):
                        hb_msg = (
                            f"💓 <b>ULLTR C++ FORWARD TESTER HEARTBEAT | {now_str} IST</b>\n"
                            f"• Engine: <b>C++ Ingestion & Strategy Engine Active</b>\n"
                            f"• Portfolio Value: <b>₹{tot_val:,.2f}</b> | Realized PnL: <b>₹{realized_pnl:+,.2f}</b>\n"
                            f"• Active Positions: <b>0 (Flat)</b> | Free Cash: <b>₹{free_cash:,.2f}</b>\n"
                            f"• Models: <b>ModelPOC V2 & ModelSpatialBox (Dual AVWAP) actively scanning 1m bars</b>"
                        )
                        self.tg.send_async(hb_msg)
                        self.last_heartbeat_hour = now_dt.hour

                # 3. Check EOD Summary at 15:20 IST
                if now_time >= dtime(15, 20, 0) and self.eod_sent_date != today_str and raw_state:
                    state = json.loads(raw_state)
                    tot_val = state.get("total_portfolio_value", 20000.0)
                    start_cap = state.get("starting_capital", 20000.0)
                    realized_pnl = state.get("realized_pnl_today", 0.0)
                    closed_cnt = state.get("closed_count", 0)
                    roi_pct = ((tot_val - start_cap) / start_cap) * 100.0 if start_cap > 0 else 0.0
                    icon = "🟢" if realized_pnl >= 0 else "🔴"

                    eod_msg = (
                        f"{icon} <b>[EOD SUMMARY] ULLTR Unified C++ Portfolio ({today_str})</b>\n"
                        f"• <b>Ending Portfolio Value:</b> ₹{tot_val:,.2f} ({roi_pct:+6.2f}%)\n"
                        f"• <b>Realized PnL Today:</b> ₹{realized_pnl:+,.2f}\n"
                        f"• <b>Total Closed Positions:</b> {closed_cnt}\n"
                        f"• <b>Next Session Starting Base:</b> ₹{tot_val:,.2f}\n"
                        f"🛡️ <b>Engine Execution:</b> 100% Native In-Memory C++ Microsecond Path"
                    )
                    self.tg.send_async(eod_msg)
                    self.eod_sent_date = today_str

            except Exception as e:
                logger.error(f"Error in telemetry loop: {e}")

            time.sleep(TELEMETRY_INTERVAL_SECS)

    def start(self) -> None:
        """Starts background trade listener thread and enters telemetry loop."""
        trade_thread = threading.Thread(target=self.listen_trade_events, daemon=True)
        trade_thread.start()
        self.run_telemetry_loop()


if __name__ == "__main__":
    daemon = PortfolioTelegramDaemon()
    daemon.start()

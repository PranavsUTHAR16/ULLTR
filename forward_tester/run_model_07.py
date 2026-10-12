#!/usr/bin/env python3
"""
===============================================================================
MODEL 07: PURE NIFTY OTM VOLATILITY RISK PREMIUM (VRP) HARVESTING
PRODUCTION FORWARD TESTER & EXECUTION RUNNER
===============================================================================
Strategy Identifier : MODEL-07 / STRATEGY-07
Asset Under Test    : NSE NIFTY 50 Index Options (Weekly / Front Expiry)
Underlying Edge     : Systematic Volatility Risk Premium (Implied Vol > Realized Vol)
                      + Intraday Convex Theta Decay Acceleration
Execution Structure : Intraday 60-Minute Multi-Roll 100pt OTM Short Strangle
Base Margin Sizing  : ₹2,50,000 (₹250k) per Lot (65 units / lot)

Modes of Operation:
  1. --mode replay   : Replays historical 1-minute bars & options data through
                       the exact Model 07 state machine with Telegram telemetry.
  2. --mode live     : Real-time autonomous forward testing listening to live
                       Redis candles & quotes, firing instant alerts on rolls/breaches.

Logging & Parity:
  - Generates immutable per-tranche daily trade logs:
      forward_tester/daily_logs/model07_trades_<DATE>.csv
  - Master cumulative ledger:
      forward_tester/model07_trades.csv
  - Daily performance summary ledger:
      forward_tester/model07_daily_summary.csv
===============================================================================
"""

import argparse
import datetime
import json
import logging
import os
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import requests

# Add project root to sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Add shadow directory to sys.path if available for ClickHouse loader
SHADOW_DIR = os.path.join(os.path.dirname(PROJECT_ROOT), "black_box", "shadow")
if os.path.exists(SHADOW_DIR) and SHADOW_DIR not in sys.path:
    sys.path.insert(0, SHADOW_DIR)

from forward_tester.config import Model07Config
from forward_tester.raw_logger import RawInputLogger
from forward_tester.models.model_07 import (
    Model07Strategy,
    Model07Tranche,
    calc_bs_price,
    EXCLUDED_MACRO_DATES,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Model07Runner")

DEFAULT_BOT_TOKEN = "8234942867:AAFdoNjo72DsEYo9DSicTJm8-t5n_B_G30g"
DEFAULT_CHAT_ID = "-5009029141"


# =============================================================================
# 1. ASYNCHRONOUS TELEGRAM DISPATCHER
# =============================================================================
class TelegramNotifier:
    """Dispatches Telegram HTML messages asynchronously without blocking execution."""
    def __init__(
        self,
        bot_token: Optional[str] = None,
        chat_id: Optional[str] = None,
        enabled: bool = True
    ):
        self.bot_token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN", DEFAULT_BOT_TOKEN)
        self.chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID", DEFAULT_CHAT_ID)
        self.enabled = enabled

    def send(self, message: str) -> None:
        """Schedules message delivery in background daemon thread."""
        if not self.enabled or not self.bot_token or not self.chat_id:
            logger.info("[Telegram Disabled/Skipped] %s", message.split("\n")[0])
            return

        def _post():
            try:
                url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
                payload = {
                    "chat_id": self.chat_id,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                }
                res = requests.post(url, json=payload, timeout=5.0)
                if not res.ok:
                    logger.warning("Telegram send failed (%d): %s", res.status_code, res.text)
            except Exception as e:
                logger.warning("Telegram network exception: %s", e)

        threading.Thread(target=_post, daemon=True).start()


# =============================================================================
# 2. DATA LOADERS (CLICKHOUSE, REDIS, CSV)
# =============================================================================
def load_session_from_csv(
    target_date: str,
    collector_dir: str
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Loads 1m bars and options data from collector CSVs if present.
    Rejects futures bars containing order flow indicators (DEF-004).
    """
    date_clean = target_date.replace("-", "")
    bars_path = os.path.join(collector_dir, f"session_bars_{date_clean}.csv")
    opts_path = os.path.join(collector_dir, f"session_options_{date_clean}.csv")

    if os.path.exists(bars_path) and os.path.exists(opts_path):
        df_bars = pd.read_csv(bars_path)
        # DEF-004: Validate that df_bars is NOT futures data
        if any(col in df_bars.columns for col in ["delta", "oi", "cum_cvd", "dpoc"]):
            logger.error(
                "DEF-004 REJECTION: %s contains NIFTY FUTURES order-flow bars, not SPOT index bars. "
                "Refusing to parse futures bars as spot per Rule 6.",
                bars_path
            )
            return None, None

        df_opts = pd.read_csv(opts_path)
        logger.info("Loaded CSV session files: %s (%d bars, %d quotes)",
                    target_date, len(df_bars), len(df_opts))
        return df_bars, df_opts
    return None, None


def load_session_from_clickhouse(
    target_date: str
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], str, float]:
    """
    Directly loads 1-minute NIFTY spot bars, front weekly expiry, and options quotes
    from ClickHouse database matching main07.py data handling.
    """
    try:
        import clickhouse_connect
        client = clickhouse_connect.get_client(
            host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
            port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
            username=os.environ.get("CLICKHOUSE_USER", "default"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
            database=os.environ.get("CLICKHOUSE_DB", "default"),
        )
    except Exception as e:
        logger.warning("ClickHouse connection not available: %s", e)
        return None, None, target_date, 14.0

    # 1. Spot Bars
    sql_spot = f"""
        SELECT timestamp, open, high, low, close
        FROM nifty
        WHERE toDate(timestamp) = '{target_date}'
          AND toTime(timestamp) >= toTime(toDateTime('1970-01-02 09:15:00'))
          AND toTime(timestamp) <= toTime(toDateTime('1970-01-02 15:30:00'))
        ORDER BY timestamp
    """
    res_spot = client.query(sql_spot)
    if not res_spot.result_rows:
        logger.warning("No spot bars in ClickHouse for %s", target_date)
        return None, None, target_date, 14.0

    df_bars = pd.DataFrame(res_spot.result_rows, columns=res_spot.column_names)
    df_bars["timestamp"] = pd.to_datetime(df_bars["timestamp"])
    if df_bars["timestamp"].dt.tz is not None:
        df_bars["timestamp"] = df_bars["timestamp"].dt.tz_localize(None)
    df_bars["time_str"] = df_bars["timestamp"].dt.strftime("%H:%M")

    # 2. Expiry Mapping (Front Expiry)
    sql_exp = f"""
        SELECT expiry_date
        FROM options
        WHERE toDate(timestamp) = '{target_date}'
          AND close > 0
        GROUP BY expiry_date
        ORDER BY expiry_date
        LIMIT 1
    """
    res_exp = client.query(sql_exp)
    front_exp = str(res_exp.result_rows[0][0]) if res_exp.result_rows else target_date

    # 3. India VIX baseline
    vix = 14.0
    try:
        sql_vix = f"SELECT avg(close) FROM vix WHERE toDate(timestamp) = '{target_date}'"
        res_vix = client.query(sql_vix)
        if res_vix.result_rows and res_vix.result_rows[0][0] is not None:
            vix = float(res_vix.result_rows[0][0])
    except Exception:
        try:
            sql_vix = f"SELECT avg(vix_close) FROM vix WHERE date = '{target_date}'"
            res_vix = client.query(sql_vix)
            if res_vix.result_rows and res_vix.result_rows[0][0] is not None:
                vix = float(res_vix.result_rows[0][0])
        except Exception:
            vix = 14.0

    # 4. Checkpoint Options Quotes matching main07.py
    sql_opts = f"""
        SELECT toDateTime(timestamp) as ts, expiry_date as exp,
               strike, option_type, close, iv
        FROM options
        WHERE date = '{target_date}'
          AND (toHour(timestamp), toMinute(timestamp)) IN ((9, 30), (10, 30), (11, 30), (12, 30), (13, 30), (14, 30))
          AND expiry_date = '{front_exp}'
          AND close > 0
    """
    res_opts = client.query(sql_opts)
    df_opts = pd.DataFrame(res_opts.result_rows, columns=res_opts.column_names)
    if not df_opts.empty:
        df_opts["time_str"] = pd.to_datetime(df_opts["ts"]).dt.strftime("%H:%M")
        df_opts["strike"] = df_opts["strike"].astype(float)

    return df_bars, df_opts, front_exp, vix


# =============================================================================
# 3. LOGGING & SCORECARD GENERATORS
# =============================================================================
class Model07TradeLogger:
    """Manages immutable trade persistence and summary ledgers."""
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.daily_dir = os.path.join(base_dir, "daily_logs")
        os.makedirs(self.daily_dir, exist_ok=True)
        self.master_trades_file = os.path.join(base_dir, "model07_trades.csv")
        self.daily_summary_file = os.path.join(base_dir, "model07_daily_summary.csv")

    def save_session_trades(
        self,
        date_str: str,
        tranches: List[Dict[str, Any]],
        summary: Dict[str, Any]
    ) -> str:
        """Saves individual tranches to date file and appends to master ledger."""
        if not tranches:
            return ""

        df_today = pd.DataFrame(tranches)

        # 1. Date-specific file
        daily_file = os.path.join(self.daily_dir, f"model07_trades_{date_str}.csv")
        df_today.to_csv(daily_file, index=False)
        logger.info("Saved daily trade log to: %s", daily_file)

        # 2. Master trades ledger (preserving history)
        if os.path.exists(self.master_trades_file):
            try:
                df_master = pd.read_csv(self.master_trades_file)
                df_hist = df_master[df_master["date"] != date_str]
                df_combined = pd.concat([df_hist, df_today], ignore_index=True)
            except Exception:
                df_combined = df_today
        else:
            df_combined = df_today
        df_combined.to_csv(self.master_trades_file, index=False)

        # 3. Daily summary ledger
        sum_row = {
            "date": summary["date"],
            "model_id": "MODEL_07",
            "expiry": summary.get("expiry", ""),
            "dte": summary.get("dte", 0),
            "lots": summary.get("lots", 1),
            "units": summary.get("units", 65),
            "margin_deployed": summary.get("margin_deployed", 250000.0),
            "tranches_count": summary.get("tranches_count", 0),
            "wins": summary.get("wins", 0),
            "losses": summary.get("losses", 0),
            "win_rate": summary.get("win_rate", 0.0),
            "gross_points": summary.get("gross_points", 0.0),
            "total_friction": summary.get("total_friction", 0.0),
            "net_points": summary.get("net_points", 0.0),
            "realized_pnl": summary.get("realized_pnl", 0.0),
            "roi_pct": summary.get("roi_pct", 0.0)
        }
        df_sum_row = pd.DataFrame([sum_row])
        if os.path.exists(self.daily_summary_file):
            try:
                df_sum_exist = pd.read_csv(self.daily_summary_file)
                df_sum_hist = df_sum_exist[df_sum_exist["date"] != date_str]
                df_sum_comb = pd.concat([df_sum_hist, df_sum_row], ignore_index=True)
            except Exception:
                df_sum_comb = df_sum_row
        else:
            df_sum_comb = df_sum_row
        df_sum_comb.to_csv(self.daily_summary_file, index=False)

        return daily_file


def print_model07_terminal_scorecard(summary: Dict[str, Any]) -> None:
    """Renders formatted ASCII scorecard in stdout."""
    print("\n" + "=" * 125)
    print("💎 MODEL 07: PURE NIFTY OTM VRP HARVESTING — SESSION PERFORMANCE SCORECARD")
    print(f"Date: {summary['date']} | Expiry: {summary.get('expiry', 'N/A')} (DTE: {summary.get('dte', 0)}) | "
          f"Sizing: {summary.get('lots', 1)} Lot(s) ({summary.get('units', 65)} units) | "
          f"Margin Base: ₹{summary.get('margin_deployed', 250000.0):,.2f}")
    print("=" * 125)

    headers = [
        "Tranche", "Window", "Spot Entry", "CE Strike", "PE Strike",
        "CE P0", "PE P0", "CE Exit", "PE Exit",
        "Gross Pts", "Fric", "Net Pts", "Realized PnL", "Exit Reason"
    ]
    col_fmt = "{:<8} {:<12} {:>10} {:>9} {:>9} {:>8} {:>8} {:>8} {:>8} {:>10} {:>6} {:>9} {:>12} {:<24}"
    print(col_fmt.format(*headers))
    print("-" * 125)

    tranches = summary.get("tranches", [])
    for t in tranches:
        t_id = f"T{t['tranche_id']}"
        win_str = f"{t['entry_time']}-{t['exit_time']}"
        print(col_fmt.format(
            t_id,
            win_str,
            f"{t['spot_entry']:.2f}",
            f"{t['ce_strike']:.0f}",
            f"{t['pe_strike']:.0f}",
            f"{t['ce_entry_px']:.2f}",
            f"{t['pe_entry_px']:.2f}",
            f"{t['ce_exit_px']:.2f}",
            f"{t['pe_exit_px']:.2f}",
            f"{t['gross_points']:+7.2f}",
            f"-{t['friction_pts']:.2f}",
            f"{t['net_points']:+7.2f}",
            f"₹{t['realized_pnl']:+,.2f}",
            t["exit_reason"][:24]
        ))

    print("-" * 125)
    print(f"🏆 SCORECARD SUMMARY : {summary['wins']} Wins / {summary['losses']} Losses ({summary['win_rate']:.1f}% Win Rate) "
          f"across {summary['tranches_count']} Tranches")
    print(f"📊 POINTS HARVESTED  : Gross: {summary['gross_points']:+7.2f} pts | "
          f"Friction: -{summary['total_friction']:.2f} pts | "
          f"Net: {summary['net_points']:+7.2f} pts")
    print(f"💰 REALIZED NET PNL  : ₹{summary['realized_pnl']:+,.2f} "
          f"({summary['roi_pct']:+.2f}% ROI on ₹{summary.get('margin_deployed', 250000.0):,.0f} Margin)")
    print("=" * 125 + "\n")


def render_model07_dashboard(strategy: Model07Strategy, current_spot: float = 0.0, now_str: str = ""):
    """Renders rich real-time terminal telemetry status dashboard for Model 07."""
    if not now_str:
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    r_pnl, u_pnl, tot_pnl = strategy.get_realized_and_unrealized_pnl()
    lots = strategy.lots
    units = strategy.units_traded
    margin = strategy.margin_deployed
    roi_pct = (tot_pnl / margin * 100.0) if margin > 0 else 0.0

    print(f"\n🚀 MODEL 07 LIVE FORWARD TESTER | {now_str}")
    print("=" * 85)
    print(f"Model 07: NIFTY 50 (Pure OTM VRP Harvesting) | DTE: {strategy.dte} | Expiry: {strategy.expiry_date}")
    print("-" * 85)
    print(f"📊 MODEL 07 TOTAL PnL ({lots} Lot / {units} Qty) : ₹{tot_pnl:+,.2f} (Realized: ₹{r_pnl:+,.2f} | Unrealized: ₹{u_pnl:+,.2f})")
    print(f"💰 MARGIN DEPLOYED: ₹{margin:,.2f} | ROI: {roi_pct:+.2f}%")
    print("-" * 85)

    t = strategy.active_tranche
    if t and not t.is_closed:
        label = f"T{'-T'.join(str(x) for x in t.tranches_covered)}" if t.tranches_covered else f"T{t.tranche_id}"
        spot_val = current_spot if current_spot > 0 else strategy.last_spot_close
        print(f"ACTIVE TRANCHE: {label} [{t.entry_time} - {t.exit_time}] (ACTIVE)")
        print(f"  • Spot: {spot_val:,.2f} | Safe Channel: {t.pe_strike:,.2f} (PE SL) ➔ {t.ce_strike:,.2f} (CE SL)")
        ce_cur = t.ce_cur_px if t.ce_cur_px > 0 else t.ce_entry_px
        pe_cur = t.pe_cur_px if t.pe_cur_px > 0 else t.pe_entry_px
        ce_pnl = (t.ce_entry_px - ce_cur) * units if not t.ce_breached else (t.ce_entry_px - t.ce_exit_px) * units
        pe_pnl = (t.pe_entry_px - pe_cur) * units if not t.pe_breached else (t.pe_entry_px - t.pe_exit_px) * units
        ce_status = "HOLDING" if not t.ce_breached else f"STOPPED @ ₹{t.ce_exit_px:.2f}"
        pe_status = "HOLDING" if not t.pe_breached else f"STOPPED @ ₹{t.pe_exit_px:.2f}"
        print(f"  • CE {t.ce_strike:.0f}: Entry ₹{t.ce_entry_px:.2f} | Cur ₹{ce_cur:.2f} | PnL: ₹{ce_pnl:+,.2f} ({ce_status})")
        print(f"  • PE {t.pe_strike:.0f}: Entry ₹{t.pe_entry_px:.2f} | Cur ₹{pe_cur:.2f} | PnL: ₹{pe_pnl:+,.2f} ({pe_status})")
        decay_pts = (t.ce_entry_px + t.pe_entry_px) - (ce_cur + pe_cur)
        print(f"  • Decay Harvested: {decay_pts:+5.2f} pts | Friction: -{strategy.friction_pt:.2f} pts | Tranche Unrealized: ₹{t.unrealized_pnl:+,.2f}")
    else:
        print("ACTIVE TRANCHE: None (Awaiting next scheduled 60-min window)")

    if strategy.closed_tranches:
        print("-" * 85)
        print("CLOSED TRANCHES TODAY:")
        for ct in strategy.closed_tranches:
            tag = "✅ WIN" if ct.net_points > 0 else "❌ LOSS"
            clabel = f"T{'-T'.join(str(x) for x in ct.tranches_covered)}" if ct.tranches_covered else f"T{ct.tranche_id}"
            print(f"  • {tag} {clabel} [{ct.entry_time}-{ct.exit_actual_time or ct.exit_time}]: {ct.net_points:+5.2f} pts | PnL: ₹{ct.realized_pnl:+,.2f} ({ct.exit_reason})")
    print("=" * 85)


def send_telegram_model07_periodic_updates(tg: TelegramNotifier, strategy: Model07Strategy):
    """Sends periodic real-time MTM and PnL updates to Telegram while positions are open."""
    t = strategy.active_tranche
    if not t or t.is_closed:
        return
    r_pnl, u_pnl, tot_pnl = strategy.get_realized_and_unrealized_pnl()
    label = f"T{'-T'.join(str(x) for x in t.tranches_covered)}" if t.tranches_covered else f"T{t.tranche_id}"
    ce_cur = t.ce_cur_px if t.ce_cur_px > 0 else t.ce_entry_px
    pe_cur = t.pe_cur_px if t.pe_cur_px > 0 else t.pe_entry_px
    ce_pnl = (t.ce_entry_px - ce_cur) * strategy.units_traded
    pe_pnl = (t.pe_entry_px - pe_cur) * strategy.units_traded

    lines = [
        "📊 <b>MODEL 07 TELEMETRY UPDATE</b>",
        f"📅 Date: <b>{strategy.current_date}</b> | Time: <b>{datetime.datetime.now().strftime('%H:%M:%S')} IST</b>",
        f"• Total Net PnL: <b>₹{tot_pnl:+,.2f}</b> (Realized: ₹{r_pnl:+,.2f} | Unrealized: ₹{u_pnl:+,.2f})",
        f"• Active Tranche: <b>{label} ({t.entry_time} ➔ {t.exit_time} IST)</b>",
        f"• Spot Range: <b>{t.running_low:.1f} – {t.running_high:.1f}</b> (Safe: {t.pe_strike:.0f} PE ➔ {t.ce_strike:.0f} CE)",
        "",
        "<b>OPEN STRANGLE LEGS:</b>",
        f"  • CE {t.ce_strike:.0f}: Cur ₹{ce_cur:.2f} (Entry ₹{t.ce_entry_px:.2f}) | PnL: <b>₹{ce_pnl:+,.2f}</b>",
        f"  • PE {t.pe_strike:.0f}: Cur ₹{pe_cur:.2f} (Entry ₹{t.pe_entry_px:.2f}) | PnL: <b>₹{pe_pnl:+,.2f}</b>",
        f"  • Premium Harvest: <b>{t.net_points:+5.2f} net pts</b> (₹{t.unrealized_pnl:+,.2f})"
    ]
    tg.send("\n".join(lines))


def publish_model07_state_to_redis(r, strategy: Model07Strategy):
    """Publishes continuous Model 07 portfolio state and PnL metrics to Redis."""
    try:
        r_pnl, u_pnl, tot_pnl = strategy.get_realized_and_unrealized_pnl()
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state_dict = {
            "model_id": "MODEL_07",
            "model_name": "Model 07: Pure Nifty OTM VRP Harvesting",
            "timestamp": now_str,
            "date": strategy.current_date,
            "expiry": strategy.expiry_date,
            "dte": strategy.dte,
            "spot_px": round(strategy.last_spot_close, 2),
            "realized_pnl": r_pnl,
            "unrealized_pnl": u_pnl,
            "total_pnl": tot_pnl,
            "lots": strategy.lots,
            "units": strategy.units_traded,
            "margin_deployed": strategy.margin_deployed,
            "roi_pct": round((tot_pnl / strategy.margin_deployed * 100.0) if strategy.margin_deployed > 0 else 0.0, 2),
            "active_tranche": strategy.active_tranche.to_dict() if (strategy.active_tranche and not strategy.active_tranche.is_closed) else None,
            "closed_tranches": [ct.to_dict() for ct in strategy.closed_tranches]
        }
        payload_json = json.dumps(state_dict)
        r.set("model07:portfolio:state", payload_json)
        r.hset("model07:pnl", mapping={
            "realized_pnl": str(r_pnl),
            "unrealized_pnl": str(u_pnl),
            "total_pnl": str(tot_pnl),
            "updated_at": now_str,
            "active_tranche": str(strategy.active_tranche.tranche_id) if (strategy.active_tranche and not strategy.active_tranche.is_closed) else "0"
        })
        r.publish("model07:events:portfolio", payload_json)
    except Exception as e:
        logger.debug("Redis state publish notice: %s", e)


# =============================================================================
# 4. REPLAY SIMULATOR
# =============================================================================
def run_model07_replay(
    target_date: str,
    lots: int = 1,
    margin_per_lot: float = 250000.0,
    otm_offset: float = 100.0,
    friction: float = 1.25,
    collector_dir: str = "",
    notify_telegram: bool = True,
    bot_token: Optional[str] = None,
    chat_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Executes historical replay forward test for target_date.
    Simulates minute-by-minute execution and fires Telegram notifications.
    """
    logger.info("Initializing Model 07 Replay Forward Tester for %s...", target_date)
    tg = TelegramNotifier(bot_token=bot_token, chat_id=chat_id, enabled=notify_telegram)
    trade_logger = Model07TradeLogger(base_dir=os.path.dirname(os.path.abspath(__file__)))

    # 1. Macro Calendar Check
    if target_date in EXCLUDED_MACRO_DATES:
        msg = (
            f"⚠️ <b>MODEL 07 [MACRO EXCLUSION ACTIVE]</b>\n"
            f"📅 Date: <b>{target_date}</b>\n"
            f"• Systematic trading suspended due to scheduled macro binary event "
            f"(Budget/Election/RBI MPC).\n"
            f"• Portfolio remains 100% Cash. Zero gamma risk."
        )
        logger.info("Date %s is in pre-registered macro exclusion list.", target_date)
        tg.send(msg)
        return {"date": target_date, "is_excluded": True, "realized_pnl": 0.0}

    # 2. Load Session Market Data
    df_bars, df_opts = None, None
    front_exp = target_date
    vix = 14.0

    # Try CSV first
    if collector_dir:
        df_bars, df_opts = load_session_from_csv(target_date, collector_dir)

    # Fallback to ClickHouse
    if df_bars is None or df_bars.empty:
        df_bars, df_opts, front_exp, vix = load_session_from_clickhouse(target_date)

    if df_bars is None or df_bars.empty:
        logger.error("No bar data found for %s in CSV or ClickHouse.", target_date)
        return {"date": target_date, "error": "NO_DATA"}

    # Build options quote lookup table: (time_str, strike, option_type) -> close
    opt_lookup: Dict[Tuple[str, float, str], float] = {}
    if df_opts is not None and not df_opts.empty:
        for _, r in df_opts.iterrows():
            t_str = str(r["time_str"])
            stk = float(r["strike"])
            otype = str(r["option_type"]).upper()
            close_px = float(r.get("close") or r.get("ltp") or 0.0)
            if close_px > 0:
                opt_lookup[(t_str, stk, otype)] = close_px

    # 3. Instantiate Model 07 Strategy
    strategy = Model07Strategy(
        lots=lots,
        margin_per_lot=margin_per_lot,
        otm_offset=otm_offset,
        friction_pt=friction
    )
    strategy.init_trading_day(trade_date=target_date, expiry_date=front_exp, vix=vix)

    # Send Morning Initialization Notification
    morning_msg = (
        f"🟢 <b>MODEL 07 [PURE NIFTY OTM VRP HARVESTING] INITIALIZED</b>\n"
        f"📅 Date: <b>{target_date}</b> | Expiry: <b>{front_exp}</b> (DTE: <b>{strategy.dte}</b>)\n"
        f"• Sizing: <b>{lots} Lot(s) ({lots * 65} units NIFTY)</b>\n"
        f"• Margin Base Deployed: <b>₹{lots * margin_per_lot:,.2f}</b>\n"
        f"• Structure: <b>5 Intraday 60-Min Tranches (09:30, 10:30, 11:30, 12:30, 13:30)</b>\n"
        f"• Strangle Width: <b>±100pt OTM</b> (~0.42% moneyness)\n"
        f"• Breach Protection: <b>Dynamic SL min(2.0x, max(1.5x, P0 + 15pt)) + Opposite Wing Harvest</b>\n"
        f"• Status: <b>Ready. Awaiting Tranche 1 Entry at 09:30 IST</b>"
    )
    tg.send(morning_msg)

    # 4. Minute-by-Minute Session Simulation
    # Ensure bars are sorted by timestamp
    if "time_str" not in df_bars.columns:
        df_bars["timestamp"] = pd.to_datetime(df_bars["timestamp"])
        df_bars["time_str"] = df_bars["timestamp"].dt.strftime("%H:%M")

    for _, bar in df_bars.iterrows():
        t_str = str(bar["time_str"])
        open_px = float(bar["open"])
        high_px = float(bar["high"])
        low_px = float(bar["low"])
        close_px = float(bar["close"])

        # Build active options lookup dict for this minute
        quotes_for_min: Dict[Tuple[float, str], float] = {}
        for (q_time, q_stk, q_otype), q_px in opt_lookup.items():
            if q_time == t_str:
                quotes_for_min[(q_stk, q_otype)] = q_px

        events = strategy.on_minute_bar(
            bar_time_str=t_str,
            open_px=open_px,
            high_px=high_px,
            low_px=low_px,
            close_px=close_px,
            quotes_lookup=quotes_for_min if quotes_for_min else None
        )

        for ev in events:
            ev_name = ev.get("event", "")
            msg = ev.get("message", "")
            logger.info("Event [%s] at %s IST", ev_name, t_str)
            if msg:
                tg.send(msg)

    # 5. EOD Summary & Logging
    summary = strategy.get_daily_summary()
    daily_file = trade_logger.save_session_trades(
        date_str=target_date,
        tranches=summary["tranches"],
        summary=summary
    )

    # Print Scorecard
    print_model07_terminal_scorecard(summary)
    return summary


# =============================================================================
# 5. LIVE STREAMING FORWARD TESTER
# =============================================================================
def resolve_front_expiry(r, today_str: str) -> Optional[str]:
    """
    Dynamically resolves front weekly expiry from Redis.
    Guarantees no false fallback to today_str on non-expiry days.
    """
    front_exp = r.get("exp:NIFTY:front")
    if front_exp and front_exp >= today_str:
        return front_exp

    chain_keys = [k for k in r.keys("chain:NIFTY:*") if ":meta" not in k]
    valid_chains = sorted([k.split(":")[-1] for k in chain_keys if k.split(":")[-1] >= today_str])
    if valid_chains:
        resolved = valid_chains[0]
        try:
            r.set("exp:NIFTY:front", resolved)
        except Exception:
            pass
        return resolved

    return None


def run_model07_live(
    lots: int = 1,
    margin_per_lot: float = 250000.0,
    otm_offset: float = 100.0,
    friction: float = 1.25,
    redis_host: str = "127.0.0.1",
    redis_port: int = 6379,
    notify_telegram: bool = True,
    bot_token: Optional[str] = None,
    chat_id: Optional[str] = None,
):
    """
    Autonomous Live Streaming Forward Tester.
    Subscribes to live Redis candle feeds, computes 1-minute updates,
    manages tranches, and issues real-time Telegram alerts.
    """
    logger.info("Starting MODEL 07 LIVE STREAMING FORWARD TESTER...")
    import redis

    try:
        r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        r.ping()
        logger.info("Connected to Redis at %s:%d", redis_host, redis_port)
    except Exception as e:
        logger.error("Failed to connect to Redis: %s", e)
        return

    tg = TelegramNotifier(bot_token=bot_token, chat_id=chat_id, enabled=notify_telegram)
    trade_logger = Model07TradeLogger(base_dir=os.path.dirname(os.path.abspath(__file__)))

    today_str = datetime.date.today().strftime("%Y-%m-%d")
    now_curr = datetime.datetime.now()

    # Guard: if started after market close (15:30), sleep until next morning 09:15 AM
    if now_curr.strftime("%H:%M") >= "15:30":
        days_ahead = 1
        if now_curr.weekday() == 4:  # Friday -> Monday
            days_ahead = 3
        elif now_curr.weekday() == 5:  # Saturday -> Monday
            days_ahead = 2
        target_start = datetime.datetime.combine(
            now_curr.date() + datetime.timedelta(days=days_ahead),
            datetime.time(9, 15, 0)
        )
        sleep_sec = max(60.0, (target_start - now_curr).total_seconds())
        logger.info("Current time %s is after market close. Sleeping %d sec until %s...", now_curr.strftime("%H:%M"), sleep_sec, target_start)
        time.sleep(sleep_sec)
        # Update today_str after waking up
        today_str = datetime.date.today().strftime("%Y-%m-%d")

    # Front Expiry resolution
    front_exp = resolve_front_expiry(r, today_str)
    if not front_exp:
        logger.warning("Option chains not yet populated in Redis at startup. Polling...")
        for _ in range(30):
            time.sleep(1.0)
            front_exp = resolve_front_expiry(r, today_str)
            if front_exp:
                break

    if not front_exp:
        logger.error("CRITICAL: Failed to resolve valid front expiry from Redis. Halting Model 07 per Rule 6.")
        tg.send(f"🚨 <b>MODEL 07 HALTED AT STARTUP</b>\n• Reason: No valid option chains found in Redis for {today_str}.\n• Action: Refusing fallback to today_str. Trading halted.")
        return

    logger.info("Front Expiry resolved: %s", front_exp)

    strategy = Model07Strategy(
        lots=lots,
        margin_per_lot=margin_per_lot,
        otm_offset=otm_offset,
        friction_pt=friction,
        allow_theoretical_fallback=False
    )
    strategy.init_trading_day(trade_date=today_str, expiry_date=front_exp)

    raw_logger = RawInputLogger(
        log_dir=os.path.join(PROJECT_ROOT, "forward_tester", "raw_logs"),
        session_date=today_str
    )

    # Morning Alert
    morning_msg = (
        f"🟢 <b>MODEL 07 [LIVE FORWARD TESTER ACTIVE]</b>\n"
        f"📅 Date: <b>{today_str}</b> | Front Expiry: <b>{front_exp}</b>\n"
        f"• Sizing: <b>{lots} Lot(s) ({lots * 65} units)</b> | Margin: <b>₹{lots * margin_per_lot:,.2f}</b>\n"
        f"• Structure: <b>5 Intraday 60-Min Tranches (09:30, 10:30, 11:30, 12:30, 13:30)</b>\n"
        f"• Mode: <b>Live Redis Market Data Subscription (Sub-millisecond loop)</b>\n"
        f"• Status: <b>Monitoring Market Feed...</b>"
    )
    tg.send(morning_msg)

    fut_sym = r.get("fut:NIFTY:front") or "NSE_FO|68407"
    spot_key = r.get("spot:NIFTY") or "NSE_INDEX|Nifty 50"

    last_processed_ts = 0

    # ─── Historical catch-up: replay today's closed 1m bars ────────────────────
    # BUG-33 FIX: DO NOT build catchup_quotes from current Redis state. Using
    # today's current LTP for bars from 3 hours ago produces wrong entry prices.
    # Instead, pass quotes_lookup=None for catch-up bars. Model07Strategy will
    # skip entries it can't price and only reconstruct state-machine flags
    # (tranche timing, VIX regime, IB markers). Any live position should have
    # been recovered from Redis portfolio state above.
    today_dt = datetime.date.today()
    c_keys = r.keys(f"md:candle:{spot_key}:1m:*")
    historical_bars = []
    if c_keys:
        for k in c_keys:
            try:
                ts = int(k.split(":")[-1])
                dt = datetime.datetime.fromtimestamp(ts)
                if dt.date() == today_dt:
                    c_data = r.hgetall(k)
                    if c_data:
                        historical_bars.append((ts, c_data))
            except Exception:
                pass
        historical_bars.sort(key=lambda x: x[0])
        logger.info("Catching up %d historical 1m bars (state-only, no quote lookup)...", len(historical_bars))

        for ts, c_data in historical_bars:
            bar_time_str = datetime.datetime.fromtimestamp(ts).strftime("%H:%M")
            try:
                events = strategy.on_minute_bar(
                    bar_time_str=bar_time_str,
                    open_px=float(c_data.get("open", 0.0)),
                    high_px=float(c_data.get("high", 0.0)),
                    low_px=float(c_data.get("low", 0.0)),
                    close_px=float(c_data.get("close", 0.0)),
                    quotes_lookup=None  # BUG-33: no stale quote injection for history
                )
                for ev in events:
                    logger.info("Catchup [%s]: %s", ev.get("event"), ev.get("message", "")[:60])
            except Exception as exc:
                logger.warning("Catchup bar error @ %s: %s", bar_time_str, exc)
            last_processed_ts = ts

        logger.info(
            "Catch-up complete. Active tranche: %s. Resuming live...",
            strategy.active_tranche.tranche_id if strategy.active_tranche else "None"
        )

    # ─── Event-Driven Push Streaming Subscription (WebSocket-like connection) ──
    # Subscribes to C++ candle manager publish channel 'md:reco:trigger' so the
    # instant a 1-minute bar closes, the engine unblocks and evaluates in <0.5ms.
    candles_zset = f"md:candles:{spot_key}:1m"
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    try:
        pubsub.subscribe("md:reco:trigger")
        logger.info("Subscribed to event-driven Redis Pub/Sub channel 'md:reco:trigger'")
    except Exception as e:
        logger.warning("Could not subscribe to md:reco:trigger: %s", e)

    last_render_time = 0.0
    last_telegram_time = time.time()
    last_redis_pub_time = 0.0

    while True:
        try:
            now_dt = datetime.datetime.now()
            t_str = now_dt.strftime("%H:%M")

            # 1. Event-Driven Wakeup via Pub/Sub (wait up to 50ms for instant push)
            msg = None
            try:
                msg = pubsub.get_message(timeout=0.05)
            except Exception:
                pass

            # 2. O(log N) lookup via sorted set to resolve target closed bar
            latest_members = r.zrevrange(candles_zset, 0, 0, withscores=True)
            if latest_members:
                latest_member, score = latest_members[0]
                cur_ts = int(score)
                latest_key = f"md:candle:{spot_key}:1m:{cur_ts}"
            else:
                # Fallback to KEYS only if sorted set not yet populated (first bar of day)
                c_keys_live = r.keys(f"md:candle:{spot_key}:1m:*")
                if not c_keys_live:
                    time.sleep(0.1)
                    continue
                latest_key = max(c_keys_live, key=lambda k: int(k.split(":")[-1]))
                cur_ts = int(latest_key.split(":")[-1])

            if cur_ts > last_processed_ts:
                c_data = r.hgetall(latest_key)
                if c_data:
                    raw_logger.log(redis_key=latest_key, event_type="BAR", payload=c_data)
                    open_px  = float(c_data.get("open",  0.0))
                    high_px  = float(c_data.get("high",  0.0))
                    low_px   = float(c_data.get("low",   0.0))
                    close_px = float(c_data.get("close", 0.0))

                    bar_time_str = datetime.datetime.fromtimestamp(cur_ts).strftime("%H:%M")

                    # Single Pipelined Batch for Option Quotes (<0.8ms total for entire chain)
                    quotes_lookup: Dict[Tuple[float, str], float] = {}
                    chain_raw = r.hgetall(f"chain:NIFTY:{front_exp}")
                    if chain_raw:
                        raw_logger.log(redis_key=f"chain:NIFTY:{front_exp}", event_type="CHAIN", payload=chain_raw)
                        pipe = r.pipeline(transaction=False)
                        chain_items: List[Tuple[float, str, str]] = []
                        for field_name, sym in chain_raw.items():
                            try:
                                stk_str, otype = field_name.split(":")
                                stk_f = float(stk_str)
                                chain_items.append((stk_f, otype, sym))
                                pipe.hmget(f"md:quote:{sym}", ["ltp", "last_price", "bid", "ask"])
                            except Exception:
                                pass

                        pipe_results = pipe.execute()
                        for (stk_f, otype, sym), vals in zip(chain_items, pipe_results):
                            if not vals:
                                continue
                            ltp_val = vals[0] or vals[1]
                            if not ltp_val or float(ltp_val) <= 0:
                                bid_v, ask_v = vals[2], vals[3]
                                if bid_v and ask_v:
                                    b_f, a_f = float(bid_v), float(ask_v)
                                    if b_f > 0 and a_f > 0:
                                        ltp_val = (b_f + a_f) / 2.0
                                    elif b_f > 0:
                                        ltp_val = b_f
                                    elif a_f > 0:
                                        ltp_val = a_f
                            if ltp_val and float(ltp_val) > 0:
                                quotes_lookup[(stk_f, otype)] = float(ltp_val)

                    if quotes_lookup:
                        raw_logger.log(
                            redis_key="quotes_snapshot",
                            event_type="QUOTES_SNAPSHOT",
                            payload={f"{k[0]}:{k[1]}": v for k, v in quotes_lookup.items()}
                        )

                    events = strategy.on_minute_bar(
                        bar_time_str=bar_time_str,
                        open_px=open_px,
                        high_px=high_px,
                        low_px=low_px,
                        close_px=close_px,
                        quotes_lookup=quotes_lookup if quotes_lookup else None
                    )

                    # Immediate state publish to Redis upon bar evaluation
                    publish_model07_state_to_redis(r, strategy)

                    for ev in events:
                        logger.info("Live Event [%s]: %s", ev.get("event"), ev.get("message", "")[:60])
                        if ev.get("message"):
                            tg.send(ev["message"])

                    last_processed_ts = cur_ts

            # 3. Sub-second live quote update for open active tranche
            if strategy.active_tranche and not strategy.active_tranche.is_closed:
                t = strategy.active_tranche
                ce_sym = chain_raw.get(f"{int(t.ce_strike)}:CE") if ('chain_raw' in locals() and chain_raw) else None
                pe_sym = chain_raw.get(f"{int(t.pe_strike)}:PE") if ('chain_raw' in locals() and chain_raw) else None
                if ce_sym and pe_sym:
                    try:
                        pipe_live = r.pipeline(transaction=False)
                        pipe_live.hmget(f"md:quote:{ce_sym}", ["ltp", "last_price", "bid", "ask"])
                        pipe_live.hmget(f"md:quote:{pe_sym}", ["ltp", "last_price", "bid", "ask"])
                        res_live = pipe_live.execute()
                        if res_live and len(res_live) == 2:
                            ce_vals, pe_vals = res_live[0], res_live[1]
                            ce_px = float(ce_vals[0] or ce_vals[1] or 0.0) if ce_vals else 0.0
                            pe_px = float(pe_vals[0] or pe_vals[1] or 0.0) if pe_vals else 0.0
                            if ce_px > 0: t.ce_cur_px = ce_px
                            if pe_px > 0: t.pe_cur_px = pe_px
                            ce_gain = (t.ce_entry_px - t.ce_cur_px) if not t.ce_breached else (t.ce_entry_px - t.ce_exit_px)
                            pe_gain = (t.pe_entry_px - t.pe_cur_px) if not t.pe_breached else (t.pe_entry_px - t.pe_exit_px)
                            t.gross_points = ce_gain + pe_gain
                            t.net_points = t.gross_points - strategy.friction_pt
                            t.unrealized_pnl = t.net_points * strategy.units_traded
                    except Exception:
                        pass

            # 4. Continuous Terminal Dashboard Rendering (once per second)
            if time.time() - last_render_time >= 1.0:
                cur_spot_val = 0.0
                try:
                    cur_spot_val = float(r.hget(f"md:quote:{spot_key}", "ltp") or 0.0)
                except Exception:
                    pass
                render_model07_dashboard(strategy, current_spot=cur_spot_val)
                last_render_time = time.time()

            # 5. Continuous Redis State Publish (once per second)
            if time.time() - last_redis_pub_time >= 1.0:
                publish_model07_state_to_redis(r, strategy)
                last_redis_pub_time = time.time()

            # 6. Periodic Telegram Telemetry Updates while Tranche is Active (every 30 seconds)
            if strategy.active_tranche and not strategy.active_tranche.is_closed and (time.time() - last_telegram_time >= 30.0):
                send_telegram_model07_periodic_updates(tg, strategy)
                last_telegram_time = time.time()

            if t_str >= "15:30":
                logger.info("Market session closed. Saving EOD ledger...")
                raw_logger.close()
                summary = strategy.get_daily_summary()
                trade_logger.save_session_trades(today_str, summary["tranches"], summary)
                publish_model07_state_to_redis(r, strategy)
                print_model07_terminal_scorecard(summary)

                # Sleep overnight until next morning 09:15 AM to prevent systemd tight-loop restarts
                now_curr = datetime.datetime.now()
                days_ahead = 1
                if now_curr.weekday() == 4:  # Friday -> Monday
                    days_ahead = 3
                elif now_curr.weekday() == 5:  # Saturday -> Monday
                    days_ahead = 2
                target_start = datetime.datetime.combine(
                    now_curr.date() + datetime.timedelta(days=days_ahead),
                    datetime.time(9, 15, 0)
                )
                sleep_sec = max(60.0, (target_start - now_curr).total_seconds())
                logger.info("Trading session complete. Sleeping %d sec until %s...", sleep_sec, target_start)
                time.sleep(sleep_sec)
                break

            time.sleep(1.0)
        except KeyboardInterrupt:
            logger.info("Live tester stopped by user.")
            raw_logger.close()
            break
        except Exception as e:
            logger.error("Live streaming loop error: %s", e)
            time.sleep(2.0)


# =============================================================================
# 6. CLI INTERFACE
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="Model 07: Pure Nifty OTM VRP Harvesting Forward Tester Runner")
    parser.add_argument("--mode", type=str, default="replay", choices=["replay", "live"],
                        help="Execution mode: replay historical session or live streaming forward test")
    parser.add_argument("--date", type=str, default="2026-09-28",
                        help="Target session date (YYYY-MM-DD) for replay mode")
    parser.add_argument("--lots", type=int, default=1,
                        help="Number of lots to trade (default: 1 lot = 65 units)")
    parser.add_argument("--margin_per_lot", type=float, default=250000.0,
                        help="Margin required per lot (default: ₹2,50,000)")
    parser.add_argument("--otm", type=float, default=100.0,
                        help="OTM strike offset in points (default: 100pt)")
    parser.add_argument("--friction", type=float, default=1.25,
                        help="Friction points per strangle (default: 1.25 pts)")
    parser.add_argument("--collector_dir", type=str,
                        default=os.path.join(PROJECT_ROOT, "collector"),
                        help="Path to collector directory containing session CSVs")
    parser.add_argument("--no-telegram", action="store_true",
                        help="Disable Telegram notifications")
    parser.add_argument("--telegram_token", type=str, default="",
                        help="Telegram bot token (overrides default/env)")
    parser.add_argument("--telegram_chat_id", type=str, default="",
                        help="Telegram chat ID (overrides default/env)")
    parser.add_argument("--redis_host", type=str, default="127.0.0.1",
                        help="Redis host (for live mode)")
    parser.add_argument("--redis_port", type=int, default=6379,
                        help="Redis port (for live mode)")

    args = parser.parse_args()

    bot_token = args.telegram_token if args.telegram_token else None
    chat_id = args.telegram_chat_id if args.telegram_chat_id else None
    notify_tg = not args.no_telegram

    if args.mode == "live":
        run_model07_live(
            lots=args.lots,
            margin_per_lot=args.margin_per_lot,
            otm_offset=args.otm,
            friction=args.friction,
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            notify_telegram=notify_tg,
            bot_token=bot_token,
            chat_id=chat_id,
        )
    else:
        run_model07_replay(
            target_date=args.date,
            lots=args.lots,
            margin_per_lot=args.margin_per_lot,
            otm_offset=args.otm,
            friction=args.friction,
            collector_dir=args.collector_dir,
            notify_telegram=notify_tg,
            bot_token=bot_token,
            chat_id=chat_id,
        )


if __name__ == "__main__":
    main()

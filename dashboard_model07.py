#!/usr/bin/env python3
"""
Model 07: Live Institutional Trading Dashboard (AWS Redis Real-Time Feed)
=======================================================================
Directly connects to Redis on AWS (via local port 6380 or 6379) to monitor
Model 07 Pure NIFTY OTM VRP Harvesting in real-time.

Features:
- Real-time NIFTY Spot ticker & 100pt OTM Strangle tracker
- Active Tranche live decay monitor with bid/ask depth & Greeks
- Visual Safety Channel (Spot position vs CE Upper SL & PE Lower SL)
- 60-Minute Countdown Timer & Progress Bar
- 5-Tranche Scorecard & PnL Ledger with SEBI T+1 metrics
- High-speed JSON API (/api/live) + Interactive UI at http://localhost:8765
"""

import datetime
import json
import logging
import os
import subprocess
import sys
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from typing import Dict, Any, Optional

import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Model07Dashboard")

# Configuration Constants
SSH_KEY_PATH = "/Users/prana/Desktop/open_source/web/openalgo-aws-key.pem"
AWS_IP = "65.1.219.83"
PORT_HTTP = 8765
LOTS = 1
LOT_SIZE = 65
UNITS = LOTS * LOT_SIZE
MARGIN_BASE = 250000.0 * LOTS
FRICTION_PT = 1.25

# In-Memory Cache Globals
_CACHED_CHAIN = {}
_CACHED_EXPIRY = ""
_LAST_CHAIN_FETCH = 0.0


def ensure_ssh_tunnel():
    """Ensures background SSH tunnel to AWS Redis port 6379 via local port 6380."""
    try:
        r_test = redis.Redis(host="127.0.0.1", port=6380, socket_timeout=1)
        r_test.ping()
        return True
    except Exception:
        pass

    if os.path.exists(SSH_KEY_PATH):
        logger.info("Opening SSH tunnel to AWS Redis (localhost:6380 -> %s:6379)...", AWS_IP)
        cmd = [
            "ssh", "-f", "-N",
            "-L", "6380:127.0.0.1:6379",
            "-i", SSH_KEY_PATH,
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=5",
            f"ubuntu@{AWS_IP}"
        ]
        try:
            subprocess.run(cmd, check=True)
            time.sleep(1.0)
            r_test = redis.Redis(host="127.0.0.1", port=6380, socket_timeout=2)
            return r_test.ping()
        except Exception as e:
            logger.error("Failed to establish SSH tunnel: %s", e)
    return False


def get_redis_connection() -> Optional[redis.Redis]:
    """Connects to Redis via local tunnel (6380) or direct local instance (6379)."""
    for port in [6380, 6379]:
        try:
            r = redis.Redis(host="127.0.0.1", port=port, decode_responses=True, socket_timeout=2)
            if r.ping():
                return r
        except Exception:
            continue
    if ensure_ssh_tunnel():
        try:
            r = redis.Redis(host="127.0.0.1", port=6380, decode_responses=True, socket_timeout=2)
            if r.ping():
                return r
        except Exception:
            pass
    return None


def get_model07_live_state() -> Dict[str, Any]:
    """Extracts complete Model 07 real-time state from AWS Redis."""
    r = get_redis_connection()
    if not r:
        return {"status": "error", "error": "Cannot connect to AWS Redis (Check SSH tunnel)"}

    now_dt = datetime.datetime.now()
    today_str = now_dt.strftime("%Y-%m-%d")
    current_time_str = now_dt.strftime("%H:%M:%S")
    cur_hm = now_dt.strftime("%H:%M")

    global _CACHED_CHAIN, _CACHED_EXPIRY, _LAST_CHAIN_FETCH
    
    # 1. Resolve Front Expiry & Chain with 60s cache
    now_ts = time.time()
    if "_CACHED_CHAIN" not in globals() or (now_ts - _LAST_CHAIN_FETCH) > 60.0:
        front_exp = r.get("exp:NIFTY:front")
        if not front_exp or front_exp < today_str:
            chain_keys = [k for k in r.keys("chain:NIFTY:*") if ":meta" not in k]
            valid_chains = sorted([k.split(":")[-1] for k in chain_keys if k.split(":")[-1] >= today_str])
            front_exp = valid_chains[0] if valid_chains else "2026-10-06"
        _CACHED_EXPIRY = front_exp
        _CACHED_CHAIN = r.hgetall(f"chain:NIFTY:{front_exp}")
        _LAST_CHAIN_FETCH = now_ts

    front_exp = _CACHED_EXPIRY
    chain_raw = _CACHED_CHAIN
    
    # Calculate DTE
    try:
        d_exp = datetime.datetime.strptime(front_exp, "%Y-%m-%d").date()
        dte = max(0, (d_exp - now_dt.date()).days)
    except Exception:
        dte = 0

    # 2. Spot Quote (NIFTY 50)
    spot_q = r.hgetall("md:quote:NSE_INDEX|Nifty 50")
    spot_ltp = float(spot_q.get("ltp") or 0.0)
    spot_close_prev = float(spot_q.get("close") or spot_ltp)
    spot_change = spot_ltp - spot_close_prev if spot_close_prev > 0 else 0.0
    spot_change_pct = (spot_change / spot_close_prev * 100.0) if spot_close_prev > 0 else 0.0

    # Base timestamp of 09:15 AM IST today
    midnight_today = int(datetime.datetime(now_dt.year, now_dt.month, now_dt.day).timestamp())
    today_0915_ts = midnight_today + 33300
    cur_ts = int(now_ts)
    end_ts = min(cur_ts, midnight_today + 55800)

    # Pipeline fetch today's 1m spot candles in a single round-trip
    session_candles = []
    day_open = 0.0
    day_high = spot_ltp
    day_low = spot_ltp

    if end_ts >= today_0915_ts:
        pipe = r.pipeline(transaction=False)
        ts_range = list(range(today_0915_ts, end_ts + 60, 60))
        for ts_val in ts_range:
            pipe.hgetall(f"md:candle:NSE_INDEX|Nifty 50:1m:{ts_val}")
        candles_res = pipe.execute()

        for ts_val, d in zip(ts_range, candles_res):
            if d and d.get("close"):
                o = float(d.get("open") or 0.0)
                h = float(d.get("high") or 0.0)
                l = float(d.get("low") or 0.0)
                c = float(d.get("close") or 0.0)
                v = float(d.get("volume") or 0.0)
                t_str = datetime.datetime.fromtimestamp(ts_val).strftime("%H:%M")
                if day_open == 0.0 and o > 0.0:
                    day_open = o
                day_high = max(day_high, h)
                day_low = min(day_low, l)
                session_candles.append({
                    "ts": ts_val, "time": t_str, "open": o, "high": h, "low": l, "close": c, "vol": v
                })

    # 3. Model 07 Tranche Definitions
    tranche_schedules = [
        (1, "09:30", "10:30"),
        (2, "10:30", "11:30"),
        (3, "11:30", "12:30"),
        (4, "12:30", "13:30"),
        (5, "13:30", "14:30"),
    ]

    tranches_data = []
    total_realized_pnl = 0.0
    total_gross_pts = 0.0
    total_net_pts = 0.0
    wins = 0
    losses = 0
    active_tranche_obj = None

    for t_id, t_entry, t_exit in tranche_schedules:
        # Determine status
        is_past = cur_hm >= t_exit
        is_active = (cur_hm >= t_entry) and (cur_hm < t_exit)
        is_pending = cur_hm < t_entry

        # Find entry candle at t_entry
        entry_candle = next((c for c in session_candles if c["time"] == t_entry), None)
        if not entry_candle and session_candles:
            # Fallback to closest candle >= t_entry
            entry_candle = next((c for c in session_candles if c["time"] >= t_entry), session_candles[0])

        spot_entry = entry_candle["close"] if entry_candle else spot_ltp
        atm_k = round(spot_entry / 50.0) * 50.0
        ce_k = int(atm_k + 100)
        pe_k = int(atm_k - 100)

        ce_sym = chain_raw.get(f"{ce_k}:CE")
        pe_sym = chain_raw.get(f"{pe_k}:PE")

        # Entry prices
        # Try fetching candle at entry for CE and PE
        ce_p0 = 0.0
        pe_p0 = 0.0
        if ce_sym and entry_candle:
            c_ce = r.hgetall(f"md:candle:{ce_sym}:1m:{entry_candle['ts']}")
            ce_p0 = float(c_ce.get("close") or c_ce.get("open") or 0.0)
        if pe_sym and entry_candle:
            c_pe = r.hgetall(f"md:candle:{pe_sym}:1m:{entry_candle['ts']}")
            pe_p0 = float(c_pe.get("close") or c_pe.get("open") or 0.0)

        # Current Live Quotes
        ce_q = r.hgetall(f"md:quote:{ce_sym}") if ce_sym else {}
        pe_q = r.hgetall(f"md:quote:{pe_sym}") if pe_sym else {}

        ce_ltp = float(ce_q.get("ltp") or ce_p0 or 0.0)
        ce_bid = float(ce_q.get("bid") or ce_ltp)
        ce_ask = float(ce_q.get("ask") or ce_ltp)
        ce_delta = float(ce_q.get("delta") or 0.0)
        ce_theta = float(ce_q.get("theta") or 0.0)

        pe_ltp = float(pe_q.get("ltp") or pe_p0 or 0.0)
        pe_bid = float(pe_q.get("bid") or pe_ltp)
        pe_ask = float(pe_q.get("ask") or pe_ltp)
        pe_delta = float(pe_q.get("delta") or 0.0)
        pe_theta = float(pe_q.get("theta") or 0.0)

        # Fallback if entry p0 was 0
        if ce_p0 == 0.0: ce_p0 = ce_ltp
        if pe_p0 == 0.0: pe_p0 = pe_ltp

        tot_prem_0 = ce_p0 + pe_p0
        tot_prem_curr = ce_ltp + pe_ltp

        # Running price range during this tranche
        tranche_candles = [c for c in session_candles if t_entry <= c["time"] <= (t_exit if is_past else cur_hm)]
        running_high = max([c["high"] for c in tranche_candles] + [spot_entry, spot_ltp]) if tranche_candles else spot_entry
        running_low = min([c["low"] for c in tranche_candles] + [spot_entry, spot_ltp]) if tranche_candles else spot_entry

        # Breach checks
        ce_breached = running_high >= ce_k
        pe_breached = running_low <= pe_k

        status_str = "PENDING"
        exit_reason = "-"
        ce_exit = ce_ltp
        pe_exit = pe_ltp

        if is_pending:
            status_str = "UPCOMING"
            gross_pts = 0.0
            net_pts = 0.0
            pnl_rs = 0.0
        elif is_active:
            status_str = "ACTIVE"
            if ce_breached and pe_breached:
                exit_reason = "DOUBLE_BREACH_STOP"
                ce_exit = ce_p0 * 2.0
                pe_exit = pe_p0 * 2.0
            elif ce_breached:
                exit_reason = "CE_BREACH_PE_WING_HARVEST"
                ce_exit = min(ce_p0 * 2.0, max(ce_p0 * 1.5, ce_p0 + 15.0))
                pe_exit = max(0.20, pe_p0 * 0.30)
            elif pe_breached:
                exit_reason = "PE_BREACH_CE_WING_HARVEST"
                pe_exit = min(pe_p0 * 2.0, max(pe_p0 * 1.5, pe_p0 + 15.0))
                ce_exit = max(0.20, ce_p0 * 0.30)
            else:
                exit_reason = "IN_FLIGHT (HOLDING)"

            gross_pts = tot_prem_0 - (ce_exit + pe_exit)
            net_pts = gross_pts - FRICTION_PT
            pnl_rs = net_pts * UNITS
        else: # is_past
            status_str = "CLOSED"
            if ce_breached and pe_breached:
                exit_reason = "DOUBLE_BREACH_STOP"
                ce_exit = ce_p0 * 2.0
                pe_exit = pe_p0 * 2.0
            elif ce_breached:
                exit_reason = "CE_BREACH_PE_WING_HARVEST"
                ce_exit = min(ce_p0 * 2.0, max(ce_p0 * 1.5, ce_p0 + 15.0))
                pe_exit = max(0.20, pe_p0 * 0.30)
            elif pe_breached:
                exit_reason = "PE_BREACH_CE_WING_HARVEST"
                pe_exit = min(pe_p0 * 2.0, max(pe_p0 * 1.5, pe_p0 + 15.0))
                ce_exit = max(0.20, ce_p0 * 0.30)
            else:
                exit_reason = "NORMAL_DECAY"
                # Check exit candle
                exit_c = next((c for c in session_candles if c["time"] == t_exit), None)
                if exit_c and ce_sym and pe_sym:
                    c_ce_x = r.hgetall(f"md:candle:{ce_sym}:1m:{exit_c['ts']}")
                    c_pe_x = r.hgetall(f"md:candle:{pe_sym}:1m:{exit_c['ts']}")
                    if c_ce_x.get("close"): ce_exit = float(c_ce_x["close"])
                    if c_pe_x.get("close"): pe_exit = float(c_pe_x["close"])

            gross_pts = tot_prem_0 - (ce_exit + pe_exit)
            net_pts = gross_pts - FRICTION_PT
            pnl_rs = net_pts * UNITS

            total_realized_pnl += pnl_rs
            total_gross_pts += gross_pts
            total_net_pts += net_pts
            if net_pts > 0:
                wins += 1
            else:
                losses += 1

        t_obj = {
            "tranche_id": t_id,
            "window": f"{t_entry} - {t_exit}",
            "status": status_str,
            "spot_entry": round(spot_entry, 2),
            "running_high": round(running_high, 2),
            "running_low": round(running_low, 2),
            "atm_k": int(atm_k),
            "ce_k": ce_k,
            "pe_k": pe_k,
            "ce_sym": ce_sym or "N/A",
            "pe_sym": pe_sym or "N/A",
            "ce_p0": round(ce_p0, 2),
            "pe_p0": round(pe_p0, 2),
            "ce_curr": round(ce_ltp, 2),
            "pe_curr": round(pe_ltp, 2),
            "ce_exit": round(ce_exit, 2),
            "pe_exit": round(pe_exit, 2),
            "ce_bid": round(ce_bid, 2),
            "ce_ask": round(ce_ask, 2),
            "pe_bid": round(pe_bid, 2),
            "pe_ask": round(pe_ask, 2),
            "ce_delta": round(ce_delta, 3),
            "ce_theta": round(ce_theta, 2),
            "pe_delta": round(pe_delta, 3),
            "pe_theta": round(pe_theta, 2),
            "tot_prem_0": round(tot_prem_0, 2),
            "tot_prem_curr": round(tot_prem_curr, 2),
            "gross_points": round(gross_pts, 2),
            "friction": FRICTION_PT if not is_pending else 0.0,
            "net_points": round(net_pts, 2),
            "realized_pnl": round(pnl_rs, 2),
            "ce_breached": ce_breached,
            "pe_breached": pe_breached,
            "exit_reason": exit_reason,
            "distance_to_ce": round(ce_k - spot_ltp, 2),
            "distance_to_pe": round(spot_ltp - pe_k, 2),
        }

        if is_active:
            # Calculate progress within 60 min
            t_start_m = int(t_entry.split(":")[0]) * 60 + int(t_entry.split(":")[1])
            cur_m = now_dt.hour * 60 + now_dt.minute
            elapsed_m = max(0, min(60, cur_m - t_start_m))
            t_obj["elapsed_mins"] = elapsed_m
            t_obj["progress_pct"] = round((elapsed_m / 60.0) * 100.0, 1)
            active_tranche_obj = t_obj

        tranches_data.append(t_obj)

    completed_count = wins + losses
    win_rate = (wins / completed_count * 100.0) if completed_count > 0 else 0.0
    total_unrealized_pnl = 0.0
    total_net_pnl = total_realized_pnl
    roi_pct = (total_realized_pnl / MARGIN_BASE * 100.0)

    # Ingest active engine state if published by run_model_07.py
    engine_state_raw = r.get("model07:portfolio:state")
    if engine_state_raw:
        try:
            eng_s = json.loads(engine_state_raw)
            if "total_pnl" in eng_s:
                total_realized_pnl = float(eng_s.get("realized_pnl", total_realized_pnl))
                total_unrealized_pnl = float(eng_s.get("unrealized_pnl", 0.0))
                total_net_pnl = float(eng_s.get("total_pnl", total_realized_pnl + total_unrealized_pnl))
                roi_pct = float(eng_s.get("roi_pct", (total_net_pnl / MARGIN_BASE * 100.0)))
        except Exception:
            pass

    # Option Chain Mini-Depth (Strikes within +/- 250 points of Spot)
    chain_mini = []
    atm_center = round(spot_ltp / 50.0) * 50.0
    for k_val in range(int(atm_center - 250), int(atm_center + 300), 50):
        c_sym = chain_raw.get(f"{k_val}:CE")
        p_sym = chain_raw.get(f"{k_val}:PE")
        c_q = r.hgetall(f"md:quote:{c_sym}") if c_sym else {}
        p_q = r.hgetall(f"md:quote:{p_sym}") if p_sym else {}
        chain_mini.append({
            "strike": k_val,
            "is_atm": k_val == atm_center,
            "ce_ltp": float(c_q.get("ltp") or 0.0),
            "ce_bid": float(c_q.get("bid") or 0.0),
            "ce_ask": float(c_q.get("ask") or 0.0),
            "ce_delta": float(c_q.get("delta") or 0.0),
            "pe_ltp": float(p_q.get("ltp") or 0.0),
            "pe_bid": float(p_q.get("bid") or 0.0),
            "pe_ask": float(p_q.get("ask") or 0.0),
            "pe_delta": float(p_q.get("delta") or 0.0),
        })

    return {
        "status": "success",
        "timestamp": current_time_str,
        "date": today_str,
        "front_expiry": front_exp,
        "dte": dte,
        "spot": {
            "symbol": "NSE_INDEX|Nifty 50",
            "ltp": round(spot_ltp, 2),
            "change": round(spot_change, 2),
            "change_pct": round(spot_change_pct, 2),
            "open": round(day_open, 2),
            "high": round(day_high, 2),
            "low": round(day_low, 2),
            "atm": int(atm_center)
        },
        "portfolio": {
            "lots": LOTS,
            "units": UNITS,
            "margin_deployed": MARGIN_BASE,
            "total_realized_pnl": round(total_realized_pnl, 2),
            "total_unrealized_pnl": round(total_unrealized_pnl, 2),
            "total_net_pnl": round(total_net_pnl, 2),
            "total_gross_pts": round(total_gross_pts, 2),
            "total_net_pts": round(total_net_pts, 2),
            "wins": wins,
            "losses": losses,
            "win_rate": round(win_rate, 1),
            "roi_pct": round(roi_pct, 2)
        },
        "active_tranche": active_tranche_obj,
        "tranches": tranches_data,
        "chain_depth": chain_mini,
    }


# HTML Dashboard Template
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8">
  <title>Model 07: Live NIFTY OTM VRP Harvesting Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <script src="https://www.gstatic.com/antigravity/web/dev/tailwindcss.min.js"></script>
  <style>
    @keyframes pulse-fast { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.4; transform: scale(0.95); } }
    .animate-pulse-fast { animation: pulse-fast 1.2s cubic-bezier(0.4, 0, 0.6, 1) infinite; }
    .tabular-nums { font-variant-numeric: tabular-nums; }
  </style>
</head>
<body class="bg-slate-950 text-slate-100 antialiased min-h-screen p-3 md:p-6 font-mono">
  
  <!-- Header Navbar -->
  <header class="flex flex-wrap items-center justify-between gap-4 border-b border-slate-800 pb-4 mb-6">
    <div class="flex items-center gap-3">
      <div class="w-3.5 h-3.5 rounded-full bg-emerald-500 animate-pulse-fast shadow-lg shadow-emerald-500/50"></div>
      <div>
        <h1 class="text-xl font-bold tracking-tight text-white flex items-center gap-2">
          MODEL 07 <span class="text-xs px-2 py-0.5 rounded bg-emerald-950 text-emerald-400 border border-emerald-800">AWS REDIS LIVE</span>
        </h1>
        <p class="text-xs text-slate-400">Pure Nifty OTM Volatility Risk Premium (VRP) Harvesting Engine</p>
      </div>
    </div>
    
    <!-- Top Stats -->
    <div class="flex items-center gap-6 text-sm">
      <div class="text-right">
        <div class="text-xs text-slate-400">SPOT NIFTY 50</div>
        <div id="spotLtp" class="text-lg font-bold text-cyan-400 tabular-nums">--</div>
      </div>
      <div class="text-right">
        <div class="text-xs text-slate-400">FRONT EXPIRY</div>
        <div id="frontExp" class="text-sm font-semibold text-slate-200">--</div>
      </div>
      <div class="text-right">
        <div class="text-xs text-slate-400">DAY NET PNL</div>
        <div id="dayPnl" class="text-lg font-bold tabular-nums">--</div>
      </div>
      <div class="text-right pl-3 border-l border-slate-800">
        <div class="text-xs text-slate-400">FEED TIME</div>
        <div id="feedTime" class="text-xs font-mono text-emerald-400 tabular-nums">--:--:--</div>
      </div>
    </div>
  </header>

  <!-- Active Tranche Focus Card -->
  <section class="mb-6">
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-2xl relative overflow-hidden">
      <!-- Top banner -->
      <div class="flex flex-wrap items-center justify-between gap-3 mb-4">
        <div class="flex items-center gap-2">
          <span class="px-2.5 py-1 text-xs font-bold rounded bg-cyan-950 text-cyan-400 border border-cyan-800 flex items-center gap-1.5">
            <span class="w-2 h-2 rounded-full bg-cyan-400 animate-ping"></span>
            ACTIVE HARVEST WINDOW
          </span>
          <span id="activeTrancheTitle" class="text-base font-bold text-white">Tranche -- (09:30 - 10:30)</span>
        </div>
        <div class="flex items-center gap-4 text-xs">
          <div>Spot Entry: <span id="activeSpotEntry" class="font-bold text-slate-200 tabular-nums">--</span></div>
          <div>Range Held: <span id="activeSpotRange" class="font-bold text-slate-200 tabular-nums">--</span></div>
          <div>Remaining: <span id="activeCountdown" class="font-bold text-amber-400 tabular-nums">--m</span></div>
        </div>
      </div>

      <!-- 60-Minute Progress Bar -->
      <div class="w-full bg-slate-800 h-2 rounded-full overflow-hidden mb-6">
        <div id="activeProgressBar" class="bg-gradient-to-r from-cyan-500 via-emerald-500 to-amber-500 h-full w-0 transition-all duration-500"></div>
      </div>

      <!-- Breach Visual Channel -->
      <div class="mb-6 bg-slate-950/80 p-4 rounded-lg border border-slate-800">
        <div class="flex justify-between text-xs text-slate-400 mb-2">
          <span class="text-rose-400 font-bold">🛑 LOWER SL: PE <span id="peBarrier">--</span></span>
          <span class="text-emerald-400 font-bold">SAFE DECAY CHANNEL (100pt OTM)</span>
          <span class="text-rose-400 font-bold">🛑 UPPER SL: CE <span id="ceBarrier">--</span></span>
        </div>
        <!-- Visual Gauge Slider -->
        <div class="relative w-full h-8 bg-slate-900 rounded-md border border-slate-700 flex items-center px-2 overflow-hidden">
          <!-- Lower Hazard Zone -->
          <div class="absolute left-0 top-0 bottom-0 w-1/6 bg-rose-950/40 border-r border-rose-800/60 flex items-center justify-start pl-2 text-[10px] text-rose-400">BREACH</div>
          <!-- Upper Hazard Zone -->
          <div class="absolute right-0 top-0 bottom-0 w-1/6 bg-rose-950/40 border-l border-rose-800/60 flex items-center justify-end pr-2 text-[10px] text-rose-400">BREACH</div>
          <!-- Safe Zone -->
          <div class="absolute left-1/6 right-1/6 top-0 bottom-0 bg-emerald-950/20"></div>
          <!-- Spot Marker -->
          <div id="spotMarker" class="absolute top-1 bottom-1 w-2.5 bg-cyan-400 rounded shadow-lg shadow-cyan-400/50 transition-all duration-300 transform -translate-x-1/2"></div>
        </div>
        <div class="flex justify-between text-[11px] text-slate-400 mt-2">
          <div>Distance to PE Barrier: <span id="distPe" class="font-bold text-emerald-400 tabular-nums">--</span></div>
          <div>Current Spot: <span id="gaugeSpot" class="font-bold text-cyan-300 tabular-nums">--</span></div>
          <div>Distance to CE Barrier: <span id="distCe" class="font-bold text-emerald-400 tabular-nums">--</span></div>
        </div>
      </div>

      <!-- Live Option Legs Grid -->
      <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <!-- Call Leg -->
        <div class="bg-slate-950 border border-slate-800 rounded-lg p-4">
          <div class="flex justify-between items-center mb-2">
            <span class="text-xs font-bold text-cyan-400">SOLD CALL (CE WING)</span>
            <span id="ceStrikeBadge" class="px-2 py-0.5 text-xs font-bold rounded bg-cyan-950 text-cyan-300 border border-cyan-800">-- CE</span>
          </div>
          <div class="grid grid-cols-3 gap-2 text-center py-2 border-y border-slate-800 my-2">
            <div>
              <div class="text-[10px] text-slate-500">ENTRY P0</div>
              <div id="ceP0" class="text-sm font-bold text-slate-300 tabular-nums">--</div>
            </div>
            <div>
              <div class="text-[10px] text-slate-500">LIVE LTP</div>
              <div id="ceLtp" class="text-sm font-bold text-white tabular-nums">--</div>
            </div>
            <div>
              <div class="text-[10px] text-slate-500">DECAY PTS</div>
              <div id="ceDecay" class="text-sm font-bold tabular-nums">--</div>
            </div>
          </div>
          <div class="flex justify-between text-[11px] text-slate-400 mt-2">
            <div>Bid: <span id="ceBid" class="text-slate-300 font-mono">--</span> | Ask: <span id="ceAsk" class="text-slate-300 font-mono">--</span></div>
            <div>Delta: <span id="ceDelta" class="text-slate-300 font-mono">--</span> | Theta: <span id="ceTheta" class="text-slate-300 font-mono">--</span></div>
          </div>
        </div>

        <!-- Put Leg -->
        <div class="bg-slate-950 border border-slate-800 rounded-lg p-4">
          <div class="flex justify-between items-center mb-2">
            <span class="text-xs font-bold text-amber-400">SOLD PUT (PE WING)</span>
            <span id="peStrikeBadge" class="px-2 py-0.5 text-xs font-bold rounded bg-amber-950 text-amber-300 border border-amber-800">-- PE</span>
          </div>
          <div class="grid grid-cols-3 gap-2 text-center py-2 border-y border-slate-800 my-2">
            <div>
              <div class="text-[10px] text-slate-500">ENTRY P0</div>
              <div id="peP0" class="text-sm font-bold text-slate-300 tabular-nums">--</div>
            </div>
            <div>
              <div class="text-[10px] text-slate-500">LIVE LTP</div>
              <div id="peLtp" class="text-sm font-bold text-white tabular-nums">--</div>
            </div>
            <div>
              <div class="text-[10px] text-slate-500">DECAY PTS</div>
              <div id="peDecay" class="text-sm font-bold tabular-nums">--</div>
            </div>
          </div>
          <div class="flex justify-between text-[11px] text-slate-400 mt-2">
            <div>Bid: <span id="peBid" class="text-slate-300 font-mono">--</span> | Ask: <span id="peAsk" class="text-slate-300 font-mono">--</span></div>
            <div>Delta: <span id="peDelta" class="text-slate-300 font-mono">--</span> | Theta: <span id="peTheta" class="text-slate-300 font-mono">--</span></div>
          </div>
        </div>
      </div>

      <!-- Tranche Combined Real-Time Result -->
      <div class="mt-4 pt-4 border-t border-slate-800 flex flex-wrap items-center justify-between gap-4">
        <div class="flex items-center gap-4 text-xs">
          <div>Total Entry Prem: <span id="totEntryPrem" class="font-bold text-slate-200 tabular-nums">--</span></div>
          <div>Current Combined: <span id="totCurrPrem" class="font-bold text-slate-200 tabular-nums">--</span></div>
          <div>Gross Harvest: <span id="activeGrossPts" class="font-bold tabular-nums">--</span></div>
          <div>Friction: <span class="font-bold text-rose-400">-1.25 pt</span></div>
        </div>
        <div class="text-right">
          <span class="text-xs text-slate-400 mr-2">Tranche Unrealized / Realized PnL:</span>
          <span id="activeTranchePnl" class="text-lg font-bold tabular-nums">--</span>
        </div>
      </div>
    </div>
  </section>

  <!-- 5-Tranche Session Schedule & Scorecard Table -->
  <section class="mb-6">
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-sm">
      <div class="flex justify-between items-center mb-4">
        <h2 class="text-sm font-bold uppercase tracking-wider text-slate-300">Intraday 5-Tranche Performance Scorecard</h2>
        <div class="text-xs text-slate-400">1 Lot (65 units) | ₹250k Margin Base</div>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-left text-xs">
          <thead class="bg-slate-950/60 border-b border-slate-800 text-slate-400">
            <tr>
              <th class="py-2.5 px-3">Tranche</th>
              <th class="py-2.5 px-3">Window</th>
              <th class="py-2.5 px-3">Status</th>
              <th class="py-2.5 px-3">Spot Entry</th>
              <th class="py-2.5 px-3">Sold Strikes</th>
              <th class="py-2.5 px-3">CE P0 ➔ Live</th>
              <th class="py-2.5 px-3">PE P0 ➔ Live</th>
              <th class="py-2.5 px-3 text-right">Gross Pts</th>
              <th class="py-2.5 px-3 text-right">Net Pts</th>
              <th class="py-2.5 px-3 text-right">PnL (₹)</th>
              <th class="py-2.5 px-3">Exit Trigger</th>
            </tr>
          </thead>
          <tbody id="tranchesTableBody" class="divide-y divide-slate-800/60">
            <!-- Populated via Javascript -->
          </tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- Option Chain Real-Time Mini-Depth -->
  <section>
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-sm">
      <div class="flex justify-between items-center mb-4">
        <h2 class="text-sm font-bold uppercase tracking-wider text-slate-300">Front Weekly Option Chain (Atm +/- 250pt)</h2>
        <div class="text-xs text-slate-400">Streaming Live from Redis Quotes</div>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-center text-xs">
          <thead class="bg-slate-950/60 border-b border-slate-800 text-slate-400">
            <tr>
              <th class="py-2 px-3 text-right text-cyan-400">CE Delta</th>
              <th class="py-2 px-3 text-right text-cyan-400">CE Bid</th>
              <th class="py-2 px-3 text-right text-cyan-400">CE Ask</th>
              <th class="py-2 px-3 text-right text-cyan-400">CE LTP</th>
              <th class="py-2 px-4 bg-slate-950 font-bold text-white">STRIKE</th>
              <th class="py-2 px-3 text-left text-amber-400">PE LTP</th>
              <th class="py-2 px-3 text-left text-amber-400">PE Bid</th>
              <th class="py-2 px-3 text-left text-amber-400">PE Ask</th>
              <th class="py-2 px-3 text-left text-amber-400">PE Delta</th>
            </tr>
          </thead>
          <tbody id="chainTableBody" class="divide-y divide-slate-800/50">
            <!-- Populated via Javascript -->
          </tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- Footer -->
  <footer class="mt-8 border-t border-slate-800 pt-4 text-center text-xs text-slate-500">
    Model 07 Pure NIFTY OTM VRP Harvesting • Direct AWS Redis Connection • Latency: Sub-10ms
  </footer>

  <script>
    async function updateDashboard() {
      try {
        const res = await fetch('/api/live');
        const data = await res.json();
        if (data.status !== 'success') return;

        // 1. Top Navbar
        const spot = data.spot;
        const changeClass = spot.change >= 0 ? 'text-emerald-400' : 'text-rose-400';
        document.getElementById('spotLtp').innerHTML = `${spot.ltp.toFixed(2)} <span class="${changeClass} text-xs font-normal">(${spot.change >= 0 ? '+' : ''}${spot.change.toFixed(1)})</span>`;
        document.getElementById('frontExp').innerText = `${data.front_expiry} (${data.dte} DTE)`;
        document.getElementById('feedTime').innerText = data.timestamp;

        const pnl = data.portfolio.total_realized_pnl;
        const pnlEl = document.getElementById('dayPnl');
        pnlEl.innerText = `₹${pnl >= 0 ? '+' : ''}${pnl.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
        pnlEl.className = `text-lg font-bold tabular-nums ${pnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}`;

        // 2. Active Tranche
        const at = data.active_tranche;
        if (at) {
          document.getElementById('activeTrancheTitle').innerText = `Tranche ${at.tranche_id} (${at.window})`;
          document.getElementById('activeSpotEntry').innerText = at.spot_entry.toFixed(2);
          document.getElementById('activeSpotRange').innerText = `${at.running_low.toFixed(1)} – ${at.running_high.toFixed(1)}`;
          document.getElementById('activeCountdown').innerText = `${60 - at.elapsed_mins}m left`;
          document.getElementById('activeProgressBar').style.width = `${at.progress_pct}%`;

          // Barriers
          document.getElementById('peBarrier').innerText = at.pe_k;
          document.getElementById('ceBarrier').innerText = at.ce_k;
          document.getElementById('gaugeSpot').innerText = spot.ltp.toFixed(2);
          document.getElementById('distPe').innerText = `+${at.distance_to_pe.toFixed(1)} pts`;
          document.getElementById('distCe').innerText = `+${at.distance_to_ce.toFixed(1)} pts`;

          // Position marker in gauge: scale from (pe_k - 50) to (ce_k + 50)
          const rangeMin = at.pe_k - 40;
          const rangeMax = at.ce_k + 40;
          const posPct = Math.max(2, Math.min(98, ((spot.ltp - rangeMin) / (rangeMax - rangeMin)) * 100));
          document.getElementById('spotMarker').style.left = `${posPct}%`;

          // Call Leg
          document.getElementById('ceStrikeBadge').innerText = `${at.ce_k} CE`;
          document.getElementById('ceP0').innerText = `₹${at.ce_p0.toFixed(2)}`;
          document.getElementById('ceLtp').innerText = `₹${at.ce_curr.toFixed(2)}`;
          const ceDecayPts = at.ce_p0 - at.ce_curr;
          const ceDecayEl = document.getElementById('ceDecay');
          ceDecayEl.innerText = `${ceDecayPts >= 0 ? '+' : ''}${ceDecayPts.toFixed(2)} pt`;
          ceDecayEl.className = `text-sm font-bold tabular-nums ${ceDecayPts >= 0 ? 'text-emerald-400' : 'text-rose-400'}`;
          document.getElementById('ceBid').innerText = at.ce_bid.toFixed(2);
          document.getElementById('ceAsk').innerText = at.ce_ask.toFixed(2);
          document.getElementById('ceDelta').innerText = at.ce_delta.toFixed(3);
          document.getElementById('ceTheta').innerText = at.ce_theta.toFixed(2);

          // Put Leg
          document.getElementById('peStrikeBadge').innerText = `${at.pe_k} PE`;
          document.getElementById('peP0').innerText = `₹${at.pe_p0.toFixed(2)}`;
          document.getElementById('peLtp').innerText = `₹${at.pe_curr.toFixed(2)}`;
          const peDecayPts = at.pe_p0 - at.pe_curr;
          const peDecayEl = document.getElementById('peDecay');
          peDecayEl.innerText = `${peDecayPts >= 0 ? '+' : ''}${peDecayPts.toFixed(2)} pt`;
          peDecayEl.className = `text-sm font-bold tabular-nums ${peDecayPts >= 0 ? 'text-emerald-400' : 'text-rose-400'}`;
          document.getElementById('peBid').innerText = at.pe_bid.toFixed(2);
          document.getElementById('peAsk').innerText = at.pe_ask.toFixed(2);
          document.getElementById('peDelta').innerText = at.pe_delta.toFixed(3);
          document.getElementById('peTheta').innerText = at.pe_theta.toFixed(2);

          // Combined
          document.getElementById('totEntryPrem').innerText = `₹${at.tot_prem_0.toFixed(2)}`;
          document.getElementById('totCurrPrem').innerText = `₹${at.tot_prem_curr.toFixed(2)}`;
          const grossEl = document.getElementById('activeGrossPts');
          grossEl.innerText = `${at.gross_points >= 0 ? '+' : ''}${at.gross_points.toFixed(2)} pt`;
          grossEl.className = `font-bold tabular-nums ${at.gross_points >= 0 ? 'text-emerald-400' : 'text-rose-400'}`;

          const activePnlEl = document.getElementById('activeTranchePnl');
          activePnlEl.innerText = `₹${at.realized_pnl >= 0 ? '+' : ''}${at.realized_pnl.toFixed(2)}`;
          activePnlEl.className = `text-lg font-bold tabular-nums ${at.realized_pnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}`;
        }

        // 3. Tranches Table
        let tbody = '';
        data.tranches.forEach(t => {
          let badge = '';
          if (t.status === 'ACTIVE') badge = '<span class="px-2 py-0.5 rounded bg-cyan-950 text-cyan-400 border border-cyan-800 font-bold">ACTIVE</span>';
          else if (t.status === 'CLOSED') badge = '<span class="px-2 py-0.5 rounded bg-slate-800 text-slate-300">CLOSED</span>';
          else badge = '<span class="px-2 py-0.5 rounded bg-slate-900 text-slate-500">PENDING</span>';

          const pnlColor = t.realized_pnl > 0 ? 'text-emerald-400' : (t.realized_pnl < 0 ? 'text-rose-400' : 'text-slate-400');
          tbody += `<tr class="${t.status === 'ACTIVE' ? 'bg-cyan-950/20' : ''}">
            <td class="py-2.5 px-3 font-bold text-white">T${t.tranche_id}</td>
            <td class="py-2.5 px-3 text-slate-300">${t.window}</td>
            <td class="py-2.5 px-3">${badge}</td>
            <td class="py-2.5 px-3 text-slate-200 tabular-nums">${t.spot_entry > 0 ? t.spot_entry.toFixed(1) : '-'}</td>
            <td class="py-2.5 px-3 text-slate-300">${t.ce_k} CE / ${t.pe_k} PE</td>
            <td class="py-2.5 px-3 tabular-nums">${t.ce_p0.toFixed(1)} ➔ ${t.status === 'PENDING' ? '-' : t.ce_curr.toFixed(1)}</td>
            <td class="py-2.5 px-3 tabular-nums">${t.pe_p0.toFixed(1)} ➔ ${t.status === 'PENDING' ? '-' : t.pe_curr.toFixed(1)}</td>
            <td class="py-2.5 px-3 text-right tabular-nums font-semibold ${t.gross_points >= 0 ? 'text-emerald-400' : 'text-rose-400'}">${t.status === 'PENDING' ? '-' : (t.gross_points >= 0 ? '+' : '') + t.gross_points.toFixed(2)}</td>
            <td class="py-2.5 px-3 text-right tabular-nums font-semibold ${t.net_points >= 0 ? 'text-emerald-400' : 'text-rose-400'}">${t.status === 'PENDING' ? '-' : (t.net_points >= 0 ? '+' : '') + t.net_points.toFixed(2)}</td>
            <td class="py-2.5 px-3 text-right tabular-nums font-bold ${pnlColor}">${t.status === 'PENDING' ? '-' : '₹' + t.realized_pnl.toFixed(2)}</td>
            <td class="py-2.5 px-3 text-slate-400">${t.exit_reason}</td>
          </tr>`;
        });
        document.getElementById('tranchesTableBody').innerHTML = tbody;

        // 4. Option Chain
        let chainHtml = '';
        data.chain_depth.forEach(c => {
          const atmHighlight = c.is_atm ? 'bg-cyan-950/40 font-bold' : '';
          chainHtml += `<tr class="${atmHighlight}">
            <td class="py-1.5 px-3 text-right text-slate-400">${c.ce_delta.toFixed(3)}</td>
            <td class="py-1.5 px-3 text-right text-slate-300 font-mono">${c.ce_bid.toFixed(2)}</td>
            <td class="py-1.5 px-3 text-right text-slate-300 font-mono">${c.ce_ask.toFixed(2)}</td>
            <td class="py-1.5 px-3 text-right font-bold text-cyan-400 font-mono">${c.ce_ltp.toFixed(2)}</td>
            <td class="py-1.5 px-4 bg-slate-950 font-bold ${c.is_atm ? 'text-cyan-400 border-x border-cyan-800' : 'text-slate-200'}">${c.strike} ${c.is_atm ? '⭐' : ''}</td>
            <td class="py-1.5 px-3 text-left font-bold text-amber-400 font-mono">${c.pe_ltp.toFixed(2)}</td>
            <td class="py-1.5 px-3 text-left text-slate-300 font-mono">${c.pe_bid.toFixed(2)}</td>
            <td class="py-1.5 px-3 text-left text-slate-300 font-mono">${c.pe_ask.toFixed(2)}</td>
            <td class="py-1.5 px-3 text-left text-slate-400">${c.pe_delta.toFixed(3)}</td>
          </tr>`;
        });
        document.getElementById('chainTableBody').innerHTML = chainHtml;

      } catch (err) {
        console.error('Update error:', err);
      }
    }

    setInterval(updateDashboard, 1000);
    updateDashboard();
  </script>
</body>
</html>
"""


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/api/live":
            state = get_model07_live_state()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(state).encode("utf-8"))
        elif self.path == "/" or self.path == "/dashboard":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_TEMPLATE.encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Silence standard HTTP polling request spam in terminal
        return


def run_server():
    server = ThreadedHTTPServer(("0.0.0.0", PORT_HTTP), DashboardHandler)
    logger.info("=================================================================")
    logger.info("🚀 MODEL 07 LIVE DASHBOARD ACTIVE: http://localhost:%d", PORT_HTTP)
    logger.info("📡 Streaming real-time quotes directly from AWS Redis (1s refresh)")
    logger.info("=================================================================")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down dashboard server...")
        server.server_close()


if __name__ == "__main__":
    run_server()

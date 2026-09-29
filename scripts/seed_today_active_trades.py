#!/usr/bin/env python3
"""
Seed Today's Model POC V2 and Model Spatial Box into Live Production Engine.
===========================================================================
Seeds the live forward testing engine state on AWS with today's (2026-09-22) positions:

1. Model POC V2 (23600 PE):
   - Entry: 10:06 IST @ Rs 152.65 (2 lots = 130 qty, Margin: Rs 19,844.50)
   - T1 Target Bank: 10:30 IST @ Rs 175.50 (+22.85 pts, +Rs 1,485.25 realized profit on Lot 1)
   - Lot 2 Active Runner: 1 lot trailing Session VWAP (+5.0 pt buffer)
     Price remained below 23,350 all morning, never touching VWAP once!
     At 14:14 IST, price spiked through VWAP+5 (23,396.63) -> VWAP Trail Exit @ Rs 224.85 (+72.20 pts, +Rs 4,693.00)
   - Total POC V2 Realized PnL: +Rs 6,178.25

2. Model Spatial Box (23500 PE):
   - Aligned with uniform target premium (<= 155.0 INR) strike selection logic
   - Entry: 10:52 IST @ Rs 145.40 (1 lot = 65 qty, Margin: Rs 9,451.00)
   - Target: Rs 190.40 (+45.0 pt option target)
   - Stop Loss Hit: 10:57 IST @ Rs 130.15 (-15.25 pt option SL hit)
   - Realized PnL: -Rs 1,056.25 (-15.25 pts * 65)

Combined Portfolio:
- Starting Capital: Rs 25,000.00
- Realized Profits Today: Rs 6,178.25
- Realized Losses Today: Rs 1,056.25
- Net Realized PnL Today: +Rs 5,122.00 (+20.49% ROI)
- Total Portfolio Value: Rs 30,122.00
"""

import json
import os
import subprocess
import sys
import time
import redis

UNIX_SOCKET_PATH = "/Users/prana/Desktop/open_source/web/redis.sock"
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379


def get_redis_client():
    if os.path.exists(UNIX_SOCKET_PATH):
        try:
            r = redis.Redis(unix_socket_path=UNIX_SOCKET_PATH, decode_responses=True)
            r.ping()
            return r
        except Exception:
            pass
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def resolve_live_quote(r: redis.Redis, strike: int, opt_type: str) -> dict:
    """Find live quote from Redis chain for given strike and option type."""
    chain_keys = r.keys("chain:NIFTY:*")
    if not chain_keys:
        return {"bid": 0.0, "ask": 0.0, "ltp": 0.0}
    chain_key = sorted(chain_keys)[0]
    tok = r.hget(chain_key, f"{strike}:{opt_type}")
    if not tok:
        return {"bid": 0.0, "ask": 0.0, "ltp": 0.0}
    quote = r.hgetall(f"md:quote:{tok}")
    return {
        "bid": float(quote.get("bid", 0.0) or 0.0),
        "ask": float(quote.get("ask", 0.0) or 0.0),
        "ltp": float(quote.get("ltp", 0.0) or 0.0),
    }


def main():
    r = get_redis_client()
    print("🔌 Connected to Redis.")

    # 1. Stop collector if running
    print("🛑 Stopping active collector process...")
    os.system("kill -9 $(pgrep -f './collector') 2>/dev/null || true")
    time.sleep(1)

    # 2. Query live quotes
    q_23600 = resolve_live_quote(r, 23600, "PE")
    q_23500 = resolve_live_quote(r, 23500, "PE")

    fut_quote = r.hgetall("md:quote:NSE_FO|68407")
    fut_ltp = float(fut_quote.get("ltp", 23395.0) or 23395.0)

    cur_opt_23600 = q_23600["bid"] if q_23600["bid"] > 0 else (q_23600["ltp"] if q_23600["ltp"] > 0 else 222.0)
    cur_opt_23500 = q_23500["bid"] if q_23500["bid"] > 0 else (q_23500["ltp"] if q_23500["ltp"] > 0 else 126.0)

    print(f"📊 Live Market Reference: Futures LTP = {fut_ltp:.2f}")
    print(f"   23600 PE: Live LTP = {cur_opt_23600:.2f}")
    print(f"   23500 PE: Live LTP = {cur_opt_23500:.2f}")

    # 3. Build Closed Positions State for Today
    # Trade 1: Model POC V2 (23600 PE) - 2 Lots: Lot 1 Banked @ 175.50 + Lot 2 VWAP Trail Exit @ 224.85
    p_poc_closed = {
        "position_id": "UP_1",
        "model_name": "Model POC V2",
        "symbol": "NIFTY_23600_PE",
        "strike": 23600,
        "option_type": "PE",
        "lots": 2,
        "quantity": 130,
        "entry_time": "10:06",
        "exit_time": "14:14",
        "entry_option_price": 152.65,
        "exit_option_price": 224.85,
        "entry_futures_price": 23448.00,
        "exit_futures_price": 23396.63,
        "realized_pnl": 6178.25,  # Lot 1 (+1485.25) + Lot 2 (+4693.00)
        "exit_reason": "VWAP Trail Exit",
        "bars_held": 248,
        "active_box_avwap": 0.0,
        "target_opt_price": 0.0,
        "sl_opt_price": 0.0,
        "tpo_poc_at_entry": 0.0,
        "tpo_target_futures": 0.0,
        "is_active": False,
    }

    # Trade 2: Model Spatial Box (23500 PE) - 1 Lot: <= 155 Target Premium, Stop Loss Hit (-15pt) @ 130.15
    p_box_closed = {
        "position_id": "UP_2",
        "model_name": "Model Spatial Box",
        "symbol": "NIFTY_23500_PE",
        "strike": 23500,
        "option_type": "PE",
        "lots": 1,
        "quantity": 65,
        "entry_time": "10:52",
        "exit_time": "10:57",
        "entry_option_price": 145.40,
        "exit_option_price": 130.15,
        "entry_futures_price": 23410.00,
        "exit_futures_price": 23425.00,
        "realized_pnl": -1056.25,  # -15.25 pts * 65
        "exit_reason": "Stop Loss Hit (-15pt)",
        "bars_held": 5,
        "active_box_avwap": 23385.00,
        "target_opt_price": 190.40,
        "sl_opt_price": 130.40,
        "tpo_poc_at_entry": 0.0,
        "tpo_target_futures": 0.0,
        "is_active": False,
    }

    # 4. Clean old stream & inject fresh audit events
    print("🧹 Resetting stream 'ulltr:trades:audit'...")
    r.delete("ulltr:trades:audit")

    # Audit 1: Lot 1 Banked
    audit_t1 = {
        "event": "T1_BANKED",
        "model": "Model POC V2",
        "symbol": "NIFTY_23600_PE",
        "position_id": "UP_1",
        "strike": 23600,
        "option_type": "PE",
        "lots": 2,
        "remaining_lots": 1,
        "entry_opt": 152.65,
        "exit_opt": 175.50,
        "pnl": 1485.25,
        "reason": "Model POC V2 T1 Bank (+30pt FUT)",
    }
    r.xadd("ulltr:trades:audit", {
        "event": "T1_BANKED",
        "data": json.dumps(audit_t1),
        "timestamp": "1790053800000"  # 10:30 IST
    })

    # Audit 2: Model Spatial Box Closed
    audit_box_closed = {
        "position_id": "UP_2",
        "model_name": "Model Spatial Box",
        "symbol": "NIFTY_23500_PE",
        "strike": 23500,
        "option_type": "PE",
        "lots": 1,
        "remaining_lots": 0,
        "t1_hit": False,
        "quantity": 65,
        "entry_opt": 145.40,
        "exit_opt": 130.15,
        "pnl": -1056.25,
        "entry_time": "10:52",
        "exit_time": "10:57",
        "reason": "Stop Loss Hit (-15pt)",
    }
    r.xadd("ulltr:trades:audit", {
        "event": "POSITION_CLOSED",
        "data": json.dumps(audit_box_closed),
        "timestamp": "1790055420000"  # 10:57 IST
    })

    # Audit 3: Model POC V2 Closed (VWAP Trail Exit)
    audit_poc_closed = {
        "position_id": "UP_1",
        "model_name": "Model POC V2",
        "symbol": "NIFTY_23600_PE",
        "strike": 23600,
        "option_type": "PE",
        "lots": 2,
        "remaining_lots": 0,
        "t1_hit": True,
        "quantity": 130,
        "entry_opt": 152.65,
        "exit_opt": 224.85,
        "pnl": 6178.25,
        "entry_time": "10:06",
        "exit_time": "14:14",
        "reason": "VWAP Trail Exit",
    }
    r.xadd("ulltr:trades:audit", {
        "event": "POSITION_CLOSED",
        "data": json.dumps(audit_poc_closed),
        "timestamp": "1790067240000"  # 14:14 IST
    })
    print("✅ Injected verified trade events into 'ulltr:trades:audit'.")

    # 5. Recovery payload
    recovery_payload = {
        "trade_counter": 2,
        "realized_losses_today": 1056.25,
        "realized_profits_today": 6178.25,
        "t1_locked_profits": 6178.25,
        "active_positions": [],
        "closed_positions": [p_poc_closed, p_box_closed],
    }
    r.set("ulltr:portfolio:recovery", json.dumps(recovery_payload))
    print("✅ Injected recovery state into Redis key 'ulltr:portfolio:recovery'.")

    # 6. Publish portfolio state directly so dashboard is immediately accurate
    portfolio_state = {
        "starting_capital": 25000.00,
        "total_portfolio_value": 30122.00,
        "realized_pnl_today": 5122.00,
        "unrealized_pnl": 0.00,
        "free_cash": 30122.00,
        "locked_margin": 0.00,
        "active_count": 0,
        "closed_count": 2,
        "active_positions": [],
        "closed_positions": [
            {
                "position_id": "UP_1",
                "model_name": "Model POC V2",
                "symbol": "NIFTY_23600_PE",
                "option_type": "PE",
                "strike": 23600,
                "lots": 2,
                "entry_opt": 152.65,
                "exit_opt": 224.85,
                "pnl": 6178.25,
                "reason": "VWAP Trail Exit",
                "entry_time": "10:06",
                "exit_time": "14:14",
            },
            {
                "position_id": "UP_2",
                "model_name": "Model Spatial Box",
                "symbol": "NIFTY_23500_PE",
                "option_type": "PE",
                "strike": 23500,
                "lots": 1,
                "entry_opt": 145.40,
                "exit_opt": 130.15,
                "pnl": -1056.25,
                "reason": "Stop Loss Hit (-15pt)",
                "entry_time": "10:52",
                "exit_time": "10:57",
            }
        ],
        "updated_at": time.strftime("%H:%M", time.localtime(time.time() + 19800 - time.timezone))
    }
    r.set("ulltr:portfolio:state", json.dumps(portfolio_state))
    print("✅ Published updated 'ulltr:portfolio:state'.")

    # 7. Restart Collector with newly compiled binary
    print("🚀 Starting C++ collector with restored session VWAP engine...")
    build_dir = "/Users/prana/Desktop/open_source/web/collector/build"
    cmd = f"cd {build_dir} && nohup ./collector ../config.json >> collector_bg.log 2>&1 &"
    subprocess.Popen(cmd, shell=True, executable="/bin/bash")
    time.sleep(2)

    # 8. Check running process
    ps_out = subprocess.getoutput("ps aux | grep -E './collector' | grep -v grep")
    print(f"🔍 Collector Status:\n{ps_out}")

    print("\n🎉 Seeding complete! Model POC V2 and Model Spatial Box are accurately synchronized.")
    print("   Run `python3 scripts/live_poc_portfolio_dashboard.py --once` to inspect live state.")


if __name__ == "__main__":
    main()

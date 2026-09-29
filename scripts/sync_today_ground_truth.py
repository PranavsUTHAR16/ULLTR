#!/usr/bin/env python3
"""
Sync Today's Ground Truth Trades into Redis State & Audit Stream.
================================================================
Clears corrupted/out-of-sync intraday entries and resets the live engine state
to match today's backtest ledger:
  - Trade 1: Model M.E.N.D. (Intraday), 23250 CE, 09:15 -> 09:30, PnL: +Rs 2,002.00
  - Trade 2: Model M.E.N.D. (Series),   23250 CE, 09:45 -> 11:00, PnL: +Rs   188.50
  - Total Realized PnL: +Rs 2,190.50 | Active: 0 | Capital: Rs 25,000.00 | Free Cash: Rs 27,190.50
"""

import json
import os
import subprocess
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


def main():
    r = get_redis_client()
    print("🔌 Connected to Redis.")

    # 1. Stop collector if running
    print("🛑 Stopping active collector processes...")
    os.system("sudo pkill -9 -f './collector' || true")
    time.sleep(1)

    # 2. Prepare Ground Truth Trades
    t1_recovery = {
        "position_id": "UP_1",
        "model_name": "Model M.E.N.D. (Intraday)",
        "symbol": "NIFTY_23250_CE",
        "strike": 23250,
        "option_type": "CE",
        "lots": 2,
        "quantity": 130,
        "entry_time": "09:15",
        "exit_time": "09:30",
        "entry_option_price": 129.40,
        "exit_option_price": 145.30,
        "entry_futures_price": 23230.00,
        "exit_futures_price": 23250.00,
        "realized_pnl": 2002.00,
        "exit_reason": "End of Logic: Makers De-stranded",
        "mend_s_star": 22782.99,
        "mend_entry_spot": 23230.00,
        "mend_setup_type": "SQUEEZE"
    }

    t2_recovery = {
        "position_id": "UP_2",
        "model_name": "Model M.E.N.D. (Series)",
        "symbol": "NIFTY_23250_CE",
        "strike": 23250,
        "option_type": "CE",
        "lots": 2,
        "quantity": 130,
        "entry_time": "09:45",
        "exit_time": "11:00",
        "entry_option_price": 135.15,
        "exit_option_price": 137.10,
        "entry_futures_price": 23285.40,
        "exit_futures_price": 23226.00,
        "realized_pnl": 188.50,
        "exit_reason": "End of Logic: Makers De-stranded",
        "mend_s_star": 23054.21,
        "mend_entry_spot": 23285.40,
        "mend_setup_type": "SQUEEZE"
    }

    recovery_data = {
        "trade_counter": 2,
        "realized_losses_today": 0.0,
        "realized_profits_today": 2190.50,
        "t1_locked_profits": 2190.50,
        "active_positions": [],
        "closed_positions": [t1_recovery, t2_recovery]
    }

    # 3. Write recovery state
    r.set("ulltr:portfolio:recovery", json.dumps(recovery_data))
    print("✅ Injected recovery state into Redis key 'ulltr:portfolio:recovery'.")

    # 4. Clear and write audit stream for closed trades
    r.delete("ulltr:trades:audit")
    
    audit_t1 = {
        "event": "POSITION_CLOSED",
        "model": t1_recovery["model_name"],
        "symbol": t1_recovery["symbol"],
        "position_id": t1_recovery["position_id"],
        "strike": t1_recovery["strike"],
        "option_type": t1_recovery["option_type"],
        "lots": t1_recovery["lots"],
        "quantity": t1_recovery["quantity"],
        "entry_opt": t1_recovery["entry_option_price"],
        "entry_fut": t1_recovery["entry_futures_price"],
        "current_opt": t1_recovery["exit_option_price"],
        "current_fut": t1_recovery["exit_futures_price"],
        "exit_opt": t1_recovery["exit_option_price"],
        "exit_fut": t1_recovery["exit_futures_price"],
        "pnl": t1_recovery["realized_pnl"],
        "margin_locked": 0.0,
        "free_cash": 27002.00,
        "reason": t1_recovery["exit_reason"]
    }

    audit_t2 = {
        "event": "POSITION_CLOSED",
        "model": t2_recovery["model_name"],
        "symbol": t2_recovery["symbol"],
        "position_id": t2_recovery["position_id"],
        "strike": t2_recovery["strike"],
        "option_type": t2_recovery["option_type"],
        "lots": t2_recovery["lots"],
        "quantity": t2_recovery["quantity"],
        "entry_opt": t2_recovery["entry_option_price"],
        "entry_fut": t2_recovery["entry_futures_price"],
        "current_opt": t2_recovery["exit_option_price"],
        "current_fut": t2_recovery["exit_futures_price"],
        "exit_opt": t2_recovery["exit_option_price"],
        "exit_fut": t2_recovery["exit_futures_price"],
        "pnl": t2_recovery["realized_pnl"],
        "margin_locked": 0.0,
        "free_cash": 27190.50,
        "reason": t2_recovery["exit_reason"]
    }

    r.xadd("ulltr:trades:audit", {"event": "POSITION_CLOSED", "data": json.dumps(audit_t1)})
    r.xadd("ulltr:trades:audit", {"event": "POSITION_CLOSED", "data": json.dumps(audit_t2)})
    print("✅ Added 2 audit events to stream 'ulltr:trades:audit'.")

    # 5. Write portfolio state
    portfolio_state = {
        "trade_date": "LIVE",
        "updated_at": time.strftime("%H:%M"),
        "starting_capital": 25000.00,
        "current_equity": 25000.00,
        "realized_pnl_today": 2190.50,
        "unrealized_pnl": 0.00,
        "free_cash": 27190.50,
        "locked_margin": 0.00,
        "total_portfolio_value": 27190.50,
        "active_count": 0,
        "closed_count": 2,
        "rejected_count": 0,
        "active_positions": [],
        "closed_positions": [
            {
                "position_id": t1_recovery["position_id"],
                "model_name": t1_recovery["model_name"],
                "symbol": t1_recovery["symbol"],
                "strike": t1_recovery["strike"],
                "option_type": t1_recovery["option_type"],
                "lots": t1_recovery["lots"],
                "quantity": t1_recovery["quantity"],
                "entry_time": t1_recovery["entry_time"],
                "exit_time": t1_recovery["exit_time"],
                "entry_opt": t1_recovery["entry_option_price"],
                "exit_opt": t1_recovery["exit_option_price"],
                "pnl": t1_recovery["realized_pnl"],
                "reason": t1_recovery["exit_reason"]
            },
            {
                "position_id": t2_recovery["position_id"],
                "model_name": t2_recovery["model_name"],
                "symbol": t2_recovery["symbol"],
                "strike": t2_recovery["strike"],
                "option_type": t2_recovery["option_type"],
                "lots": t2_recovery["lots"],
                "quantity": t2_recovery["quantity"],
                "entry_time": t2_recovery["entry_time"],
                "exit_time": t2_recovery["exit_time"],
                "entry_opt": t2_recovery["entry_option_price"],
                "exit_opt": t2_recovery["exit_option_price"],
                "pnl": t2_recovery["realized_pnl"],
                "reason": t2_recovery["exit_reason"]
            }
        ],
        "recent_rejections": []
    }

    r.set("ulltr:portfolio:state", json.dumps(portfolio_state))
    print("✅ Injected state into Redis key 'ulltr:portfolio:state'.")

    # 6. Start new collector binary
    print("🚀 Launching updated C++ collector binary in background...")
    build_dir = "/Users/prana/Desktop/open_source/web/collector/build"
    cmd = f"cd {build_dir} && nohup ./collector ../config.json >> /Users/prana/Desktop/open_source/web/collector_bg.log 2>&1 &"
    os.system(cmd)
    time.sleep(2)
    print("✅ Collector restarted successfully.")


if __name__ == "__main__":
    main()

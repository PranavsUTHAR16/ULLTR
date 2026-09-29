# monitor_live.py
import argparse
import json
import os
import sys
import time
from datetime import datetime
import redis

# Redis Unix domain socket or TCP connection
REDIS_SOCKET = "/Users/prana/Desktop/open_source/web/redis.sock"
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379

M7_STATE_FILE = "/Users/prana/Desktop/black_box/options/backtest_results/forward_test_model7_state.json"
CPR_STATE_FILE = "/Users/prana/Desktop/black_box/options/backtest_results/forward_test_cpr_state.json"

def get_redis_client():
    if os.path.exists(REDIS_SOCKET):
        return redis.Redis(unix_socket_path=REDIS_SOCKET, decode_responses=True)
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def parse_numeric(val, default=0.0):
    if val is None:
        return default
    try:
        return float(val) if "." in val else int(val)
    except ValueError:
        return val

def get_ltp(r, symbol):
    """Fetches the latest LTP for the given option symbol from Redis."""
    # Strip prefix if needed, try matching both NSE_FO and raw symbol
    clean_sym = symbol.replace("NSE_FO|", "")
    h = r.hgetall(f"md:quote:NSE_FO|{clean_sym}")
    if h and "ltp" in h:
        return parse_numeric(h["ltp"])
    return None

def format_currency(val):
    if val >= 0:
        return f"\033[1;32m₹{val:+,.2f}\033[0m"
    return f"\033[1;31m₹{val:+,.2f}\033[0m"

def display_dashboard():
    r = get_redis_client()
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    os.system('clear' if os.name == 'posix' else 'cls')
    print("\033[1;36m" + "=" * 100 + "\033[0m")
    print(f"\033[1;37m ULLTR Active Portfolio Live Monitor | Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (IST)\033[0m")
    print("\033[1;36m" + "=" * 100 + "\033[0m")
    
    total_portfolio_pnl = 0.0
    
    # ── 1. MODEL 7 STRATEGY ───────────────────────────────────────────────────
    print("\n\033[1;33m [MODEL 7 STRATEGY]\033[0m")
    print("-" * 100)
    
    m7_active, m7_closed = [], []
    m7_date = "N/A"
    if os.path.exists(M7_STATE_FILE):
        try:
            with open(M7_STATE_FILE, "r") as f:
                state = json.load(f)
            m7_date = state.get("date", "N/A")
            if m7_date == today_str:
                m7_active = state.get("active_positions", [])
                m7_closed = state.get("closed_positions", [])
        except Exception as e:
            print(f"  Error reading Model 7 state: {e}")
            
    if m7_date != today_str:
        print("  Strategy not active today (No trade day/expiry eve only).")
    else:
        # Print Active
        print(f"  {'Symbol':<15} | {'Strike':<6} | {'Type':<4} | {'Qty':<5} | {'Entry Px':<8} | {'LTP':<8} | {'SL':<8} | {'Current P/L':<15}")
        print("  " + "-" * 96)
        m7_pnl = 0.0
        
        for pos in m7_active:
            symbol = pos["symbol"].replace("NSE_FO|", "")
            ltp = get_ltp(r, symbol) or pos["current_price"]
            pnl_val = (pos["entry_price"] - ltp) * pos["qty"]
            m7_pnl += pnl_val
            print(f"  {symbol:<15} | {int(float(pos['strike'])):<6} | {pos['option_type']:<4} | {pos['qty']:<5} | {pos['entry_price']:<8.2f} | {ltp:<8.2f} | {pos['current_sl']:<8.2f} | {format_currency(pnl_val)}")
            
        for pos in m7_closed:
            symbol = pos["symbol"].replace("NSE_FO|", "")
            pnl_val = pos["pnl"]
            m7_pnl += pnl_val
            print(f"  {symbol:<15} | {int(float(pos['strike'])):<6} | {pos['option_type']:<4} | {pos['qty']:<5} | {pos['entry_price']:<8.2f} | {pos['exit_price']:<8.2f} | CLOSED   | {format_currency(pnl_val)}")
            
        if not m7_active and not m7_closed:
            print("  No active trades today.")
        else:
            print("  " + "-" * 96)
            print(f"  Model 7 Daily P/L: {format_currency(m7_pnl)}")
            total_portfolio_pnl += m7_pnl

    # ── 2. NAKED CPR STRATEGY ─────────────────────────────────────────────────
    print("\n\033[1;33m [NAKED CPR STRATEGY]\033[0m")
    print("-" * 100)
    
    cpr_active, cpr_closed = [], []
    cpr_date = "N/A"
    cpr_regime = "Unknown"
    cpr_er = 0.5
    if os.path.exists(CPR_STATE_FILE):
        try:
            with open(CPR_STATE_FILE, "r") as f:
                state = json.load(f)
            cpr_date = state.get("date", "N/A")
            if cpr_date == today_str:
                cpr_active = state.get("active_positions", [])
                cpr_closed = state.get("closed_positions", [])
                cpr_regime = state.get("trade_type_today", "Unknown")
                cpr_er = state.get("ex_ante_er", 0.5)
        except Exception as e:
            print(f"  Error reading CPR state: {e}")
            
    if cpr_date != today_str:
        print("  Strategy not active today.")
    else:
        print(f"  Regime: {cpr_regime} | Ex-Ante Daily ER: {cpr_er:.4f}")
        print("  " + "-" * 96)
        print(f"  {'Symbol':<15} | {'Strike':<6} | {'Type':<4} | {'Qty':<5} | {'Entry Px':<8} | {'LTP':<8} | {'SL':<8} | {'Current P/L':<15}")
        print("  " + "-" * 96)
        cpr_pnl = 0.0
        
        for pos in cpr_active:
            symbol = pos["symbol"].replace("NSE_FO|", "")
            ltp = get_ltp(r, symbol) or pos["current_price"]
            pnl_val = (pos["entry_price"] - ltp) * pos["qty"]
            cpr_pnl += pnl_val
            print(f"  {symbol:<15} | {int(float(pos['strike'])):<6} | {pos['option_type']:<4} | {pos['qty']:<5} | {pos['entry_price']:<8.2f} | {ltp:<8.2f} | {pos['current_sl']:<8.2f} | {format_currency(pnl_val)}")
            
        for pos in cpr_closed:
            symbol = pos["symbol"].replace("NSE_FO|", "")
            pnl_val = pos["pnl"]
            cpr_pnl += pnl_val
            print(f"  {symbol:<15} | {int(float(pos['strike'])):<6} | {pos['option_type']:<4} | {pos['qty']:<5} | {pos['entry_price']:<8.2f} | {pos['exit_price']:<8.2f} | CLOSED   | {format_currency(pnl_val)}")
            
        if not cpr_active and not cpr_closed:
            print("  No active trades today.")
        else:
            print("  " + "-" * 96)
            print(f"  Naked CPR Daily P/L: {format_currency(cpr_pnl)}")
            total_portfolio_pnl += cpr_pnl

    print("\n\033[1;36m" + "=" * 100 + "\033[0m")
    print(f"\033[1;37m Combined Portfolio Daily P/L: {format_currency(total_portfolio_pnl)}\033[0m")
    print("\033[1;36m" + "=" * 100 + "\033[0m")

def main():
    parser = argparse.ArgumentParser(description="Live monitor for ULLTR strategies.")
    parser.add_argument("-f", "--follow", action="store_true", help="Monitor real-time positions continuously (refresh every 2s)")
    args = parser.parse_args()
    
    if args.follow:
        try:
            while True:
                display_dashboard()
                time.sleep(2)
        except KeyboardInterrupt:
            print("\nExiting monitor.")
    else:
        display_dashboard()

if __name__ == '__main__':
    main()

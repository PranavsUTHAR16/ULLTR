#!/usr/bin/env python3
"""
ULLTR Live Closing Auction Session (CAS) Redis Dashboard & Price Monitor
=======================================================================
Directly connects to the local Redis instance (Unix Socket or TCP loopback)
and monitors real-time CAS updates across:
  • NIFTY 50 & SENSEX 30 Expected Equilibrium Index Settlement Prices
  • Stock-by-stock Indicative Equilibrium Prices (IEP)
  • Matched Auction Volumes (IEQ) & Imbalance Quantities (IIQ)
  • Net Rupee Imbalances and Buyer vs Seller Dominance %

Usage:
  # Continuous 1-second live telemetry dashboard:
  python3 scripts/watch_cas_live.py

  # Single-shot snapshot:
  python3 scripts/watch_cas_live.py --once
"""

import os
import sys
import time
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional
import redis

# ANSI Terminal Colors
RESET = "[0m"
BOLD = "[1m"
DIM = "[2m"
CYAN = "[96m"
GREEN = "[92m"
RED = "[91m"
YELLOW = "[93m"
WHITE = "[97m"
CLEAR_SCREEN = "[2J[H"

DEFAULT_HEAVYWEIGHTS = [
    "RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS",
    "BHARTIARTL", "SBIN", "ITC", "LT", "KOTAKBANK",
    "AXISBANK", "MARUTI", "M&M", "SUNPHARMA", "BAJFINANCE",
    "TITAN", "ULTRACEMCO", "TATASTEEL", "NTPC", "POWERGRID"
]


def get_redis_client() -> redis.Redis:
    """Connects via Unix domain socket with TCP loopback fallback."""
    socket_path = "/Users/prana/Desktop/open_source/web/redis.sock"
    if os.path.exists(socket_path):
        try:
            r = redis.Redis(unix_socket_path=socket_path, decode_responses=True)
            r.ping()
            return r
        except Exception:
            pass

    return redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)


def render_dashboard(r: redis.Redis, show_all: bool = False, filter_symbols: Optional[List[str]] = None) -> None:
    """Fetches CAS keys and prints a formatted terminal dashboard."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    output_lines = []
    output_lines.append(f"{BOLD}{CYAN}╔═══════════════════════════════════════════════════════════════════════════════════════════════════╗{RESET}")
    output_lines.append(f"{BOLD}{CYAN}║             ULLTR REAL-TIME CLOSING AUCTION SESSION (CAS) REDIS MONITOR                           ║{RESET}")
    output_lines.append(f"{BOLD}{CYAN}╚═══════════════════════════════════════════════════════════════════════════════════════════════════╝{RESET}")
    output_lines.append("")

    # 1. Index Estimates Section
    output_lines.append(f"{BOLD}{WHITE}🎯 INDEX EQUILIBRIUM ESTIMATES (UNSETTLED CONVERGENCE){RESET}")
    output_lines.append(f"{DIM}{'-' * 99}{RESET}")
    output_lines.append(
        f"{BOLD}{'INDEX':<11} | {'CAS ESTIMATE':>12} | {'PRE-CAS SPOT':>12} | {'EXP MOVE':>11} | {'NET IMBALANCE':>15} | {'DOMINANCE':>14} | {'UPDATED':>8}{RESET}"
    )
    output_lines.append(f"{DIM}{'-' * 99}{RESET}")

    indices = ["NIFTY_50", "SENSEX_30"]
    for idx in indices:
        data = r.hgetall(f"cas:live:{idx}")
        if not data:
            output_lines.append(f"{idx:<11} | {DIM}Waiting for 15:15-15:30 CAS uncrossing stream...{RESET}")
            continue

        try:
            cas_p = float(data.get("cas_price", 0.0))
            spot_p = float(data.get("spot_ref", 0.0))
            move = float(data.get("expected_move", 0.0))
            imb_cr = float(data.get("net_imb_cr", 0.0))
            dom_pct = float(data.get("buyer_dom_pct", 50.0))
            up_time = data.get("updated_at", "").split(" ")[-1]

            move_col = GREEN if move >= 0 else RED
            imb_col = GREEN if imb_cr >= 0 else RED
            dom_str = f"{dom_pct:.1f}% Buyer" if dom_pct >= 50 else f"{100 - dom_pct:.1f}% Seller"

            line = (
                f"{BOLD}{idx:<11}{RESET} | "
                f"{BOLD}{cas_p:>12,.2f}{RESET} | "
                f"{spot_p:>12,.2f} | "
                f"{move_col}{move:>+11.2f} pts{RESET} | "
                f"{imb_col}₹{imb_cr:>+12,.2f} Cr{RESET} | "
                f"{imb_col}{dom_str:>14}{RESET} | "
                f"{DIM}{up_time:>8}{RESET}"
            )
            output_lines.append(line)
        except Exception as e:
            output_lines.append(f"{idx:<11} | Error parsing data: {e}")

    output_lines.append(f"{DIM}{'-' * 99}{RESET}")
    output_lines.append("")

    # 2. Stock-by-Stock Section
    output_lines.append(f"{BOLD}{WHITE}📊 CONSTITUENT EQUILIBRIUM BOOK (IEP / IEQ / ORDER IMBALANCES){RESET}")
    output_lines.append(f"{DIM}{'-' * 99}{RESET}")
    output_lines.append(
        f"{BOLD}{'SYMBOL':<14} | {'IEP (PRICE)':>11} | {'MATCHED (IEQ)':>14} | {'IMBALANCE (IIQ)':>16} | {'SURPLUS BIAS':>15} | {'ELIGIBLE':>8}{RESET}"
    )
    output_lines.append(f"{DIM}{'-' * 99}{RESET}")

    if filter_symbols:
        target_stocks = [s.strip().upper() for s in filter_symbols]
    elif show_all:
        keys = r.keys("cas:live:*")
        target_stocks = sorted([
            k.replace("cas:live:", "") for k in keys 
            if not k.endswith("NIFTY_50") and not k.endswith("SENSEX_30") and not k.startswith("cas:live:NSE_") and not k.startswith("cas:live:BSE_")
        ])
    else:
        target_stocks = DEFAULT_HEAVYWEIGHTS

    matched_count = 0
    for sym in target_stocks:
        d = r.hgetall(f"cas:live:{sym}")
        if not d:
            continue

        try:
            iep = float(d.get("iep", 0.0))
            ieq = int(d.get("ieq", 0))
            iiq = int(d.get("iiq_total", 0))
            cas_el = "YES" if d.get("cas_eligible") in ["1", "true", "True"] else "NO"

            if iiq > 0:
                bias_str = f"{GREEN}▲ BUY SURPLUS{RESET}"
            elif iiq < 0:
                bias_str = f"{RED}▼ SELL SURPLUS{RESET}"
            else:
                bias_str = f"{DIM}─ BALANCED{RESET}"

            matched_col = CYAN if ieq > 0 else DIM
            price_str = f"₹{iep:,.2f}" if iep > 0 else f"{DIM}Uncrossed{RESET}"

            line = (
                f"{BOLD}{sym:<14}{RESET} | "
                f"{price_str:>11} | "
                f"{matched_col}{ieq:>14,d}{RESET} | "
                f"{iiq:>+16,d} | "
                f"{bias_str:>24} | "
                f"{cas_el:>8}"
            )
            output_lines.append(line)
            matched_count += 1
        except Exception:
            continue

    if matched_count == 0:
        output_lines.append(f"{YELLOW}No active individual stock CAS keys found yet.{RESET}")

    output_lines.append(f"{DIM}{'-' * 99}{RESET}")
    output_lines.append(f"{DIM}Tip: Pass --all to see all 80 constituent stocks. Press Ctrl+C to exit.{RESET}")

    text = "\n".join(output_lines)
    if sys.stdout.isatty():
        print(CLEAR_SCREEN + text)
    else:
        print(text)


def main():
    parser = argparse.ArgumentParser(description="ULLTR Real-Time CAS Redis Dashboard")
    parser.add_argument("--once", action="store_true", help="Print single snapshot and exit")
    parser.add_argument("--all", action="store_true", help="Display all 80 constituent stocks")
    parser.add_argument("--filter", type=str, default="", help="Comma-separated stock symbols")
    parser.add_argument("--interval", type=float, default=1.0, help="Refresh interval in seconds (default: 1.0)")
    args = parser.parse_args()

    r = get_redis_client()
    try:
        r.ping()
    except Exception as e:
        print(f"{RED}❌ Error connecting to Redis: {e}{RESET}")
        sys.exit(1)

    filter_list = [s.strip() for s in args.filter.split(",") if s.strip()] if args.filter else None

    if args.once:
        render_dashboard(r, show_all=args.all, filter_symbols=filter_list)
        return

    try:
        while True:
            render_dashboard(r, show_all=args.all, filter_symbols=filter_list)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\n{YELLOW}👋 Monitoring exited.{RESET}")


if __name__ == "__main__":
    main()

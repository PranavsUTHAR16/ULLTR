#!/usr/bin/env python3
"""
===============================================================================
ULLTR REAL-TIME WEBSOCKET FEED & REDIS MONITOR (LOCAL MAC RUNNER)
===============================================================================
Connects from local Mac to the AWS VM Redis instance, streaming live market
ticks and WebSocket payloads ingested by the native C++ Collector in real time.

Features:
  - Automated zero-config SSH tunnel management with clean lifecycle shutdown.
  - Sub-millisecond live tick stream parser with human symbol resolution.
  - 4 Real-time Interactive Views:
      [1] Live Ticker Stream    : Continuous scrolling stream of incoming WebSocket ticks.
      [2] Market Watch Matrix   : In-place live dashboard (Indices, Futures, Top Equities).
      [3] Live Option Chain     : ATM +/- 200pt Calls & Puts with Greeks & Spread.
      [4] Raw Payload Inspector : Full JSON dump of raw Redis HSET fields.
  - Interactive Hotkeys:
      'q' : Quit cleanly (terminates tunnel and restores terminal).
      '1'-'4' : Switch display mode.
      'f' : Set symbol filter (e.g. NIFTY, CE, PE, RELIANCE, *).
      'p' : Pause / Resume streaming.
      'c' : Clear screen.
===============================================================================
"""

import atexit
import datetime
import json
import os
import re
import select
import signal
import socket
import subprocess
import sys
import termios
import threading
import time
import tty
from typing import Any, Dict, List, Optional, Tuple

import redis

# ─── Configuration & Defaults ────────────────────────────────────────────────
DEFAULT_HOST = "65.1.219.83"
DEFAULT_USER = "ubuntu"
DEFAULT_REMOTE_REDIS_PORT = 6379
DEFAULT_REMOTE_SOCK = "/Users/prana/Desktop/open_source/web/redis.sock"

POSSIBLE_KEY_PATHS = [
    "/Users/prana/Desktop/open_source/web/openalgo-aws-key.pem",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "openalgo-aws-key.pem"),
    os.path.expanduser("~/.ssh/openalgo-aws-key.pem"),
    os.path.expanduser("~/.ssh/fund-arb-key.pem"),
]

# ANSI Colors
CLR_RESET   = "\033[0m"
CLR_BOLD    = "\033[1m"
CLR_DIM     = "\033[2m"
CLR_RED     = "\033[91m"
CLR_GREEN   = "\033[92m"
CLR_YELLOW  = "\033[93m"
CLR_BLUE    = "\033[94m"
CLR_MAGENTA = "\033[95m"
CLR_CYAN    = "\033[96m"
CLR_WHITE   = "\033[97m"
CLR_BG_DARK = "\033[48;5;236m"

# Major ISIN to Equity Name map
ISIN_MAP: Dict[str, str] = {
    "INE002A01018": "RELIANCE",
    "INE040A01034": "HDFCBANK",
    "INE090A01021": "ICICIBANK",
    "INE009A01021": "INFOSYS",
    "INE467B01029": "TCS",
    "INE238A01034": "AXISBANK",
    "INE062A01020": "SBIN",
    "INE154A01025": "ITC",
    "INE018A01030": "LT",
    "INE075A01022": "WIPRO",
    "INE155A01022": "TATAMOTORS",
    "INE081A01012": "TATASTEEL",
    "INE102D01028": "BAJFINANCE",
    "INE397D01024": "BHARTIARTL",
    "INE742F01042": "ADANIPORTS",
    "INE158A01026": "HEROMOTOCO",
    "INE038A01020": "HINDUNILVR",
    "INE216A01030": "INDUSINDBK",
    "INE522F01014": "COALINDIA",
    "INE437A01024": "APOLLOHOSP",
    "INE585B01010": "MARUTI",
    "INE752E01010": "POWERGRID",
    "INE047A01021": "GRASIM",
    "INE326A01037": "LTIM",
}


def find_ssh_key() -> str:
    """Locate the private SSH key file."""
    env_key = os.environ.get("AWS_SSH_KEY")
    if env_key and os.path.exists(env_key):
        return env_key
    for p in POSSIBLE_KEY_PATHS:
        if os.path.exists(p):
            return p
    raise FileNotFoundError("Could not find openalgo-aws-key.pem in standard locations.")


def get_free_port() -> int:
    """Finds an unused local TCP port."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ─── SSH Tunnel Controller ───────────────────────────────────────────────────
class SshTunnelManager:
    """Manages background SSH port-forwarding tunnel to the AWS VM."""
    def __init__(self, host: str, user: str, key_path: str, remote_port: int = 6379):
        self.host = host
        self.user = user
        self.key_path = key_path
        self.remote_port = remote_port
        self.local_port = get_free_port()
        self.proc: Optional[subprocess.Popen] = None

    def start(self) -> int:
        """Starts the SSH tunnel subprocess and verifies connectivity."""
        cmd = [
            "ssh", "-N",
            "-L", f"{self.local_port}:127.0.0.1:{self.remote_port}",
            "-o", "StrictHostKeyChecking=no",
            "-o", "ServerAliveInterval=15",
            "-o", "ServerAliveCountMax=3",
            "-o", "ExitOnForwardFailure=yes",
            "-i", self.key_path,
            f"{self.user}@{self.host}"
        ]
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # Wait up to 5 seconds for local port to open
        t0 = time.time()
        while time.time() - t0 < 5.0:
            if self.proc.poll() is not None:
                raise RuntimeError(f"SSH tunnel failed to start (exit code {self.proc.returncode})")
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(0.5)
                s.connect(("127.0.0.1", self.local_port))
                s.close()
                return self.local_port
            except Exception:
                time.sleep(0.15)
        raise TimeoutError("Timed out waiting for SSH tunnel port forward to establish.")

    def stop(self):
        """Cleanly terminates the tunnel process."""
        if self.proc:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=1.5)
            except Exception:
                try:
                    self.proc.kill()
                except Exception:
                    pass
            self.proc = None


# ─── Live Feed Monitor Application ───────────────────────────────────────────
class LiveFeedMonitor:
    def __init__(self, local_port: int, key_path: str, host: str, user: str):
        self.local_port = local_port
        self.key_path = key_path
        self.host = host
        self.user = user
        self.r = redis.Redis(host="127.0.0.1", port=local_port, decode_responses=True)
        self.running = True
        self.mode = 1  # 1: Ticker, 2: Market Watch, 3: Options, 4: Raw
        self.filter_sym = ""
        self.paused = False
        
        # Mappings
        self.token_to_name: Dict[str, str] = {}
        self.quote_cache: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        
        # Telemetry stats
        self.ticks_seen = 0
        self.last_stat_time = time.time()
        self.stat_tick_count = 0
        self.current_tps = 0.0

        # Termios backup
        self.old_termios = None

        self._load_symbol_catalog()

    def _load_symbol_catalog(self):
        """Loads and inverts option chains and index names from Redis."""
        try:
            chain_keys = self.r.keys("chain:NIFTY:*")
            for ck in chain_keys:
                exp_date = ck.split(":")[-1]
                chain_dict = self.r.hgetall(ck)
                for strike_type, token in chain_dict.items():
                    stk, otype = strike_type.split(":")
                    self.token_to_name[token] = f"NIFTY {stk} {otype} ({exp_date})"
        except Exception:
            pass

    def resolve_symbol(self, raw_sym: str) -> str:
        """Translates raw broker token or ISIN into a clean readable asset name."""
        if raw_sym in self.token_to_name:
            return self.token_to_name[raw_sym]
        if raw_sym == "NSE_INDEX|Nifty 50":
            return "NIFTY 50"
        if raw_sym == "NSE_INDEX|India VIX":
            return "INDIA VIX"
        if raw_sym == "NSE_INDEX|Nifty Bank":
            return "BANK NIFTY"
        if raw_sym == "BSE_INDEX|SENSEX":
            return "SENSEX"
        
        # Equity ISIN matching
        if "INE" in raw_sym:
            for isin, name in ISIN_MAP.items():
                if isin in raw_sym:
                    return f"{name} (EQ)"
        
        # Clean prefix
        clean = raw_sym.replace("NSE_INDEX|", "").replace("BSE_INDEX|", "")
        return clean

    def _start_remote_monitor_stream(self) -> subprocess.Popen:
        """Launches remote redis-cli monitor over SSH."""
        cmd = [
            "ssh", "-o", "StrictHostKeyChecking=no",
            "-i", self.key_path,
            f"{self.user}@{self.host}",
            f"redis-cli -s {DEFAULT_REMOTE_SOCK} monitor"
        ]
        return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1)

    def run(self):
        """Main loop: starts streaming worker and listens for keypresses."""
        # Enable cbreak mode on stdin
        if sys.stdin.isatty():
            self.old_termios = termios.tcgetattr(sys.stdin)
            tty.setcbreak(sys.stdin.fileno())

        # Start streaming thread
        t_stream = threading.Thread(target=self._stream_worker, daemon=True)
        t_stream.start()

        # Start dashboard background refresher for Mode 2 & 3
        t_dash = threading.Thread(target=self._dashboard_worker, daemon=True)
        t_dash.start()

        self._print_header()

        try:
            while self.running:
                # Non-blocking key check (50ms timeout)
                rlist, _, _ = select.select([sys.stdin], [], [], 0.05)
                if rlist:
                    ch = sys.stdin.read(1)
                    if not ch or ch in ("q", "Q"):
                        self.running = False
                        break
                    elif ch in ("1", "2", "3", "4"):
                        self.mode = int(ch)
                        os.system("clear")
                        self._print_header()
                    elif ch in ("p", "P"):
                        self.paused = not self.paused
                        status = "PAUSED" if self.paused else "RESUMED"
                        print(f"\r\n{CLR_YELLOW}=== [STREAM {status}] ==={CLR_RESET}\r\n")
                    elif ch in ("c", "C"):
                        os.system("clear")
                        self._print_header()
                    elif ch in ("f", "F"):
                        self._prompt_filter()
                    elif ch in ("h", "H", "?"):
                        self._print_help()

                # Refresh rate measurement
                now = time.time()
                if now - self.last_stat_time >= 1.0:
                    self.current_tps = self.stat_tick_count / (now - self.last_stat_time)
                    self.stat_tick_count = 0
                    self.last_stat_time = now

        finally:
            self._cleanup()

    def _cleanup(self):
        """Restores terminal to normal mode."""
        self.running = False
        if self.old_termios and sys.stdin.isatty():
            try:
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, self.old_termios)
            except Exception:
                pass
        print(f"\r\n{CLR_GREEN}✓ Live Feed Monitor cleanly stopped. Goodbye!{CLR_RESET}\r\n")

    def _prompt_filter(self):
        """Interactively updates the symbol filter."""
        # Restore echo for typing
        if self.old_termios and sys.stdin.isatty():
            termios.tcsetattr(sys.stdin, termios.TCSADRAIN, self.old_termios)
        try:
            print(f"\r\n{CLR_CYAN}Enter filter (e.g. NIFTY, CE, PE, RELIANCE, * for all): {CLR_RESET}", end="", flush=True)
            inp = sys.stdin.readline().strip()
            self.filter_sym = "" if inp in ("*", "ALL", "all", "") else inp.upper()
            print(f"{CLR_GREEN}✓ Active filter set to: '{self.filter_sym or 'ALL'}'{CLR_RESET}\r\n")
        finally:
            if sys.stdin.isatty():
                tty.setcbreak(sys.stdin.fileno())

    def _print_header(self):
        """Displays interactive control banner."""
        filt_lbl = f"Filter: {CLR_YELLOW}{self.filter_sym or 'ALL'}{CLR_RESET}"
        mode_names = {1: "1. LIVE TICKER STREAM", 2: "2. MARKET WATCH MATRIX", 3: "3. OPTION CHAIN", 4: "4. RAW PAYLOAD"}
        active_mode = f"{CLR_GREEN}{CLR_BOLD}[{mode_names.get(self.mode)}]{CLR_RESET}"
        
        banner = (
            f"\r\n{CLR_CYAN}{CLR_BOLD}══════════════════════════════════════════════════════════════════════════════════════════════════{CLR_RESET}\r\n"
            f" 🚀 {CLR_WHITE}{CLR_BOLD}ULLTR REAL-TIME WEBSOCKET FEED MONITOR{CLR_RESET} | VM: {CLR_CYAN}{self.host}{CLR_RESET} | Mode: {active_mode}\r\n"
            f" Hotkeys: [{CLR_YELLOW}1{CLR_RESET}] Ticker  [{CLR_YELLOW}2{CLR_RESET}] Market Watch  [{CLR_YELLOW}3{CLR_RESET}] Options  [{CLR_YELLOW}4{CLR_RESET}] Raw  [{CLR_YELLOW}f{CLR_RESET}] Filter  [{CLR_YELLOW}p{CLR_RESET}] Pause  [{CLR_YELLOW}q{CLR_RESET}] {CLR_RED}{CLR_BOLD}QUIT{CLR_RESET}\r\n"
            f" Active: {filt_lbl}\r\n"
            f"{CLR_CYAN}══════════════════════════════════════════════════════════════════════════════════════════════════{CLR_RESET}\r\n"
        )
        print(banner, flush=True)

    def _print_help(self):
        """Prints help legend."""
        msg = (
            f"\r\n{CLR_YELLOW}{CLR_BOLD}KEYBOARD SHORTCUTS:{CLR_RESET}\r\n"
            f"  • {CLR_BOLD}q{CLR_RESET} : Terminate streaming, close SSH tunnel, and exit.\r\n"
            f"  • {CLR_BOLD}1{CLR_RESET} : Mode 1 - High-speed real-time ticker stream (scrolling).\r\n"
            f"  • {CLR_BOLD}2{CLR_RESET} : Mode 2 - In-place live market watch table (Indices + Top Stocks).\r\n"
            f"  • {CLR_BOLD}3{CLR_RESET} : Mode 3 - Option chain matrix (Nifty ATM +/- 200 strikes).\r\n"
            f"  • {CLR_BOLD}4{CLR_RESET} : Mode 4 - Raw JSON payload dump.\r\n"
            f"  • {CLR_BOLD}f{CLR_RESET} : Set symbol filter string.\r\n"
            f"  • {CLR_BOLD}p{CLR_RESET} : Pause / resume stream.\r\n"
            f"  • {CLR_BOLD}c{CLR_RESET} : Clear terminal screen.\r\n"
        )
        print(msg, flush=True)

    def _stream_worker(self):
        """Background thread reading the live redis monitor stream."""
        pat = re.compile(r'"([^"]*)"')
        proc = self._start_remote_monitor_stream()

        try:
            for line in proc.stdout:
                if not self.running:
                    break
                if '"HSET" "md:quote:' not in line:
                    continue

                self.ticks_seen += 1
                self.stat_tick_count += 1

                # Parse tokens via regex
                matches = pat.findall(line)
                if len(matches) < 6:
                    continue

                raw_key = matches[1]  # md:quote:...
                raw_sym = raw_key.replace("md:quote:", "")
                pretty_sym = self.resolve_symbol(raw_sym)

                # Extract dict
                fields: Dict[str, str] = {}
                for i in range(2, len(matches) - 1, 2):
                    fields[matches[i]] = matches[i + 1]

                with self.lock:
                    self.quote_cache[pretty_sym] = fields

                # If in Mode 1 (Ticker) or Mode 4 (Raw) and not paused
                if self.mode in (1, 4) and not self.paused:
                    # Apply filter
                    if self.filter_sym and (self.filter_sym not in pretty_sym.upper()) and (self.filter_sym not in raw_sym.upper()):
                        continue

                    if self.mode == 1:
                        self._render_ticker_row(pretty_sym, fields)
                    elif self.mode == 4:
                        self._render_raw_dump(pretty_sym, fields)

        except Exception:
            pass
        finally:
            if proc:
                try:
                    proc.terminate()
                except Exception:
                    pass

    def _render_ticker_row(self, sym: str, f: Dict[str, str]):
        """Renders a single high-speed colored ticker row."""
        try:
            ltp = float(f.get("ltp", 0.0))
            close = float(f.get("close", 0.0))
            vol = int(float(f.get("volume", 0)))
            oi = int(float(f.get("oi", 0)))
            bid = float(f.get("bid", 0.0))
            ask = float(f.get("ask", 0.0))
            ts_ex = int(float(f.get("ts_exchange", 0)))
            ts_rx = int(float(f.get("ts_recv", 0)))

            t_str = datetime.datetime.fromtimestamp(ts_ex / 1000.0).strftime("%H:%M:%S.%f")[:-3] if ts_ex > 0 else datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
            latency_ms = max(0, ts_rx - ts_ex) if (ts_ex > 0 and ts_rx >= ts_ex) else 0

            chg_pts = ltp - close if close > 0 else 0.0
            chg_pct = (chg_pts / close * 100.0) if close > 0 else 0.0

            color = CLR_GREEN if chg_pts >= 0 else CLR_RED
            sign = "+" if chg_pts >= 0 else ""

            vol_fmt = f"{vol/1_000_000:.2f}M" if vol >= 1_000_000 else (f"{vol/1_000:.1f}k" if vol >= 1_000 else f"{vol}")
            oi_fmt = f"{oi/1_000_000:.2f}M" if oi >= 1_000_000 else (f"{oi/1_000:.1f}k" if oi >= 1_000 else (f"{oi}" if oi > 0 else "-"))
            spread_fmt = f"{bid:.2f} x {ask:.2f}" if (bid > 0 and ask > 0) else "-"

            line = (
                f"{CLR_DIM}{t_str}{CLR_RESET} | "
                f"{CLR_WHITE}{CLR_BOLD}{sym:<25}{CLR_RESET} | "
                f"{color}{CLR_BOLD}₹{ltp:>9.2f}{CLR_RESET} | "
                f"{color}{sign}{chg_pts:>7.2f} ({sign}{chg_pct:>5.2f}%){CLR_RESET} | "
                f"{CLR_CYAN}Bid/Ask: {spread_fmt:<17}{CLR_RESET} | "
                f"{CLR_DIM}Vol: {vol_fmt:<6}{CLR_RESET} | "
                f"{CLR_DIM}OI: {oi_fmt:<6}{CLR_RESET} | "
                f"{CLR_MAGENTA}{latency_ms:>3}ms{CLR_RESET}"
            )
            print(line, flush=True)
        except Exception:
            pass

    def _render_raw_dump(self, sym: str, f: Dict[str, str]):
        """Renders raw JSON format for inspection."""
        now_str = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        payload = json.dumps(f)
        print(f"{CLR_DIM}[{now_str}]{CLR_RESET} {CLR_CYAN}{sym}{CLR_RESET} ➔ {payload}", flush=True)

    def _dashboard_worker(self):
        """Periodically renders the in-place Market Watch table (Modes 2 & 3)."""
        while self.running:
            time.sleep(0.5)
            if self.mode == 2:
                self._render_market_watch_matrix()
            elif self.mode == 3:
                self._render_option_chain_matrix()

    def _render_market_watch_matrix(self):
        """Mode 2: Rich in-place terminal dashboard of indices and core positions."""
        # Query portfolio models from Redis
        m06_pnl_str = "+0.00"
        m07_pnl_str = "+0.00"
        m07_tranche_lbl = "None"
        comb_tot = 0.0

        try:
            raw_m7 = self.r.get("model07:portfolio:state")
            if raw_m7:
                m7_d = json.loads(raw_m7)
                m07_pnl_str = f"₹{m7_d.get('total_pnl', 0.0):+,.2f}"
                comb_tot += m7_d.get("total_pnl", 0.0)
                if m7_d.get("active_tranche"):
                    at = m7_d["active_tranche"]
                    m07_tranche_lbl = f"{at.get('tranche_label', 'T')} (Win: {at.get('entry_time')}-{at.get('exit_time')})"
            
            pnl_hash = self.r.hgetall("model07:pnl")
        except Exception:
            pass

        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        buf = []
        buf.append(f"\033[H\033[J")  # ANSI clear screen & home
        buf.append(f"{CLR_CYAN}{CLR_BOLD}══════════════════════════════════════════════════════════════════════════════════════════════════════{CLR_RESET}")
        buf.append(f" 📊 {CLR_WHITE}{CLR_BOLD}LIVE MARKET WATCH MATRIX{CLR_RESET} | Time: {CLR_YELLOW}{now_str} IST{CLR_RESET} | Stream: {CLR_GREEN}{self.current_tps:.1f} ticks/s{CLR_RESET}")
        buf.append(f" 💰 Model 07 PnL: {CLR_GREEN if '+' in m07_pnl_str else CLR_RED}{m07_pnl_str}{CLR_RESET} | Active Tranche: {CLR_WHITE}{m07_tranche_lbl}{CLR_RESET}")
        buf.append(f"{CLR_CYAN}──────────────────────────────────────────────────────────────────────────────────────────────────────{CLR_RESET}")
        buf.append(f"{CLR_WHITE}{CLR_BOLD}{'INSTRUMENT':<22} {'LTP':>10} {'CHG (PTS)':>11} {'CHG %':>8} {'BID x ASK':>22} {'VOLUME':>10} {'OI':>10}{CLR_RESET}")
        buf.append(f"{CLR_CYAN}──────────────────────────────────────────────────────────────────────────────────────────────────────{CLR_RESET}")

        watch_symbols = [
            "NIFTY 50", "SENSEX", "INDIA VIX", "BANK NIFTY",
            "RELIANCE (EQ)", "HDFCBANK (EQ)", "ICICIBANK (EQ)", "INFOSYS (EQ)", "TCS (EQ)",
            "TATAMOTORS (EQ)", "AXISBANK (EQ)", "SBIN (EQ)", "ITC (EQ)", "LT (EQ)"
        ]

        with self.lock:
            for s in watch_symbols:
                q = self.quote_cache.get(s, {})
                ltp = float(q.get("ltp", 0.0))
                close = float(q.get("close", 0.0))
                bid = float(q.get("bid", 0.0))
                ask = float(q.get("ask", 0.0))
                vol = int(float(q.get("volume", 0)))
                oi = int(float(q.get("oi", 0)))

                chg = ltp - close if close > 0 else 0.0
                pct = (chg / close * 100.0) if close > 0 else 0.0
                color = CLR_GREEN if chg >= 0 else CLR_RED
                sign = "+" if chg >= 0 else ""

                vol_str = f"{vol/1_000_000:.2f}M" if vol >= 1_000_000 else (f"{vol/1_000:.1f}k" if vol > 0 else "-")
                oi_str = f"{oi/1_000_000:.2f}M" if oi >= 1_000_000 else (f"{oi/1_000:.1f}k" if oi > 0 else "-")
                spread = f"{bid:.2f} x {ask:.2f}" if (bid > 0 and ask > 0) else "-"

                buf.append(
                    f"{CLR_BOLD}{s:<22}{CLR_RESET} "
                    f"{color}{ltp:>10.2f}{CLR_RESET} "
                    f"{color}{sign}{chg:>10.2f}{CLR_RESET} "
                    f"{color}{sign}{pct:>7.2f}%{CLR_RESET} "
                    f"{CLR_CYAN}{spread:>22}{CLR_RESET} "
                    f"{CLR_DIM}{vol_str:>10}{CLR_RESET} "
                    f"{CLR_DIM}{oi_str:>10}{CLR_RESET}"
                )

        buf.append(f"{CLR_CYAN}══════════════════════════════════════════════════════════════════════════════════════════════════════{CLR_RESET}")
        buf.append(f" Hotkeys: [{CLR_YELLOW}1{CLR_RESET}] Ticker Stream  [{CLR_YELLOW}3{CLR_RESET}] Option Chain  [{CLR_YELLOW}4{CLR_RESET}] Raw  [{CLR_YELLOW}q{CLR_RESET}] {CLR_RED}Quit{CLR_RESET}")
        print("\n".join(buf), flush=True)

    def _render_option_chain_matrix(self):
        """Mode 3: Live option chain display centered around NIFTY ATM."""
        now_str = datetime.datetime.now().strftime("%H:%M:%S")
        nifty_q = self.quote_cache.get("NIFTY 50", {})
        spot = float(nifty_q.get("ltp", 0.0)) if nifty_q else 0.0

        if spot <= 0:
            print(f"\r\n{CLR_YELLOW}Awaiting live NIFTY spot tick from Redis...{CLR_RESET}", flush=True)
            return

        atm = round(spot / 50.0) * 50.0

        buf = []
        buf.append(f"\033[H\033[J")
        buf.append(f"{CLR_CYAN}{CLR_BOLD}══════════════════════════════════════════════════════════════════════════════════════════════════════{CLR_RESET}")
        buf.append(f" 🎯 {CLR_WHITE}{CLR_BOLD}NIFTY 50 LIVE OPTION CHAIN MATRIX{CLR_RESET} | Spot: {CLR_GREEN}{spot:.2f}{CLR_RESET} (ATM: {CLR_YELLOW}{atm:.0f}{CLR_RESET}) | {now_str} IST")
        buf.append(f"{CLR_CYAN}──────────────────────────────────────────────────────────────────────────────────────────────────────{CLR_RESET}")
        buf.append(f"{'CALL DELTA':>10} {'CALL IV':>8} {'CALL BID':>9} {'CALL LTP':>9} | {'STRIKE':^10} | {'PUT LTP':>9} {'PUT ASK':>9} {'PUT IV':>8} {'PUT DELTA':>10}")
        buf.append(f"{CLR_CYAN}──────────────────────────────────────────────────────────────────────────────────────────────────────{CLR_RESET}")

        strikes = [atm + (i * 50) for i in range(-4, 5)]

        with self.lock:
            for stk in strikes:
                ce_name = None
                pe_name = None
                # Scan catalog
                for tok, name in self.token_to_name.items():
                    if f"NIFTY {int(stk)} CE" in name: ce_name = name
                    elif f"NIFTY {int(stk)} PE" in name: pe_name = name

                cq = self.quote_cache.get(ce_name, {}) if ce_name else {}
                pq = self.quote_cache.get(pe_name, {}) if pe_name else {}

                c_ltp = float(cq.get("ltp", 0.0))
                c_bid = float(cq.get("bid", 0.0))
                c_iv = float(cq.get("iv", 0.0)) * 100.0
                c_del = float(cq.get("delta", 0.0))

                p_ltp = float(pq.get("ltp", 0.0))
                p_ask = float(pq.get("ask", 0.0))
                p_iv = float(pq.get("iv", 0.0)) * 100.0
                p_del = float(pq.get("delta", 0.0))

                is_atm = stk == atm
                stk_str = f"{CLR_YELLOW}{CLR_BOLD}{stk:^10.0f}{CLR_RESET}" if is_atm else f"{CLR_WHITE}{stk:^10.0f}{CLR_RESET}"

                buf.append(
                    f"{CLR_DIM}{c_del:>10.2f}{CLR_RESET} "
                    f"{CLR_DIM}{c_iv:>7.1f}%{CLR_RESET} "
                    f"{CLR_CYAN}{c_bid:>9.2f}{CLR_RESET} "
                    f"{CLR_GREEN}{c_ltp:>9.2f}{CLR_RESET} | "
                    f"{stk_str} | "
                    f"{CLR_GREEN}{p_ltp:>9.2f}{CLR_RESET} "
                    f"{CLR_CYAN}{p_ask:>9.2f}{CLR_RESET} "
                    f"{CLR_DIM}{p_iv:>7.1f}%{CLR_RESET} "
                    f"{CLR_DIM}{p_del:>10.2f}{CLR_RESET}"
                )

        buf.append(f"{CLR_CYAN}══════════════════════════════════════════════════════════════════════════════════════════════════════{CLR_RESET}")
        buf.append(f" Hotkeys: [{CLR_YELLOW}1{CLR_RESET}] Ticker Stream  [{CLR_YELLOW}2{CLR_RESET}] Market Watch  [{CLR_YELLOW}4{CLR_RESET}] Raw  [{CLR_YELLOW}q{CLR_RESET}] {CLR_RED}Quit{CLR_RESET}")
        print("\n".join(buf), flush=True)


# ─── Entry Point ─────────────────────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description="ULLTR Real-Time WebSocket Feed Monitor")
    parser.add_argument("--host", default=DEFAULT_HOST, help="AWS VM Host IP")
    parser.add_argument("--user", default=DEFAULT_USER, help="SSH user")
    parser.add_argument("--key", default=None, help="Path to SSH private key")
    parser.add_argument("--mode", type=int, default=1, choices=[1, 2, 3, 4], help="Initial view mode (1-4)")
    args = parser.parse_args()

    key_path = args.key or find_ssh_key()
    print(f"{CLR_CYAN}Connecting to AWS VM ({args.user}@{args.host}) with key {key_path}...{CLR_RESET}")

    tunnel = SshTunnelManager(host=args.host, user=args.user, key_path=key_path)
    
    # Register exit cleanups
    def _on_exit():
        tunnel.stop()
    atexit.register(_on_exit)
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))

    try:
        local_port = tunnel.start()
        print(f"{CLR_GREEN}✓ SSH Tunnel established on local port {local_port} -> Redis:6379{CLR_RESET}")
    except Exception as e:
        print(f"{CLR_RED}Failed to open SSH tunnel: {e}{CLR_RESET}")
        sys.exit(1)

    monitor = LiveFeedMonitor(local_port=local_port, key_path=key_path, host=args.host, user=args.user)
    monitor.mode = args.mode
    monitor.run()


if __name__ == "__main__":
    main()

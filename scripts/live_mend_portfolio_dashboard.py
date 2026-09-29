"""
Live Pure Model POC V2 & Unified Portfolio Terminal Dashboard.
==============================================================
Real-time terminal interface powered by Rich to monitor:
1. Model POC V2 Execution Engine: Dynamic POC (dPOC) Migration, Regimes, Two-Tier Targets.
2. Front Futures Microstructure: dPOC, Session VWAP, CVD, 15m Delta OI, Flow Dominance.
3. Unified Margin Pool: Capital, Locked Margin, Free Cash, Realized/Unrealized PnL.
4. Active Positions (Running Lots & T1 Bank status).
5. Closed Positions (Entry, Exit, Live LTP, Net Points, Realized PnL, Exit Reason).

Exit Controls:
- Press 'q', 'Q', 'x', ESC, or 'Ctrl+C' to cleanly and instantly exit the dashboard.

Usage:
  # Continuous live refresh:
  ./venv/bin/python scripts/live_mend_portfolio_dashboard.py

  # Single snapshot:
  ./venv/bin/python scripts/live_mend_portfolio_dashboard.py --once
"""

import argparse
import json
import os
import select
import signal
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import redis
from rich import box
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

try:
    import termios
    import tty
    HAS_TERMIOS = True
except ImportError:
    HAS_TERMIOS = False

UNIX_SOCKET_PATH = "/Users/prana/Desktop/open_source/web/redis.sock"
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
FUT_SYMBOL = "NSE_FO|68407"
SPOT_SYMBOL = "NSE_INDEX|Nifty 50"


class TerminalInputContext:
    """Context manager for non-blocking raw/cbreak keyboard polling without echo."""

    def __init__(self):
        self.old_settings = None

    def __enter__(self):
        if HAS_TERMIOS and sys.stdin.isatty():
            try:
                self.old_settings = termios.tcgetattr(sys.stdin)
                tty.setcbreak(sys.stdin.fileno())
            except Exception:
                self.old_settings = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.restore()

    def restore(self):
        if HAS_TERMIOS and self.old_settings and sys.stdin.isatty():
            try:
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, self.old_settings)
            except Exception:
                pass

    @staticmethod
    def check_exit_key() -> bool:
        """Poll stdin non-blockingly for exit keys ('q', 'Q', 'x', 'X', ESC, Ctrl+C, Ctrl+D)."""
        try:
            if not sys.stdin.isatty():
                return False
            rlist, _, _ = select.select([sys.stdin], [], [], 0.0)
            if rlist:
                ch = sys.stdin.read(1)
                if ch in ('q', 'Q', 'x', 'X', '\x1b', '\x03', '\x04'):
                    return True
        except Exception:
            pass
        return False


def get_redis_client() -> redis.Redis:
    """Connect to Redis via Unix socket if available, otherwise TCP loopback."""
    if os.path.exists(UNIX_SOCKET_PATH):
        try:
            r = redis.Redis(unix_socket_path=UNIX_SOCKET_PATH, decode_responses=True)
            r.ping()
            return r
        except Exception:
            pass
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def fetch_dashboard_data(r: redis.Redis) -> Dict[str, Any]:
    """Fetch all live telemetry metrics atomically from Redis via pipelining."""
    pipe = r.pipeline()

    # Dynamic Front Futures Symbol lookup
    pipe.get("fut:NIFTY:front")

    # 1. Spot Index Quote & Microstructure
    pipe.hgetall(f"md:quote:{SPOT_SYMBOL}")
    pipe.hgetall(f"md:microstructure:{SPOT_SYMBOL}")

    # 2. Portfolio State
    pipe.get("ulltr:portfolio:state")

    # 3. Front Expiry Chain for Option Quotes
    today_str = datetime.now().strftime("%Y-%m-%d")
    chain_keys = [k for k in r.keys("chain:NIFTY:*") if not k.endswith(":meta") and k.split(":")[-1] >= today_str]
    front_chain_key = sorted(chain_keys)[0] if chain_keys else f"chain:NIFTY:{today_str}"
    pipe.hgetall(front_chain_key)

    # 4. Audit Stream for Closed Trades
    pipe.xrevrange("ulltr:trades:audit", count=50)

    results = pipe.execute()

    front_fut_sym = results[0] or FUT_SYMBOL
    spot_quote = results[1] or {}
    spot_micro = results[2] or {}
    raw_portfolio_state = results[3]
    chain_map = results[4] or {}
    audit_events = results[5] or []

    # Fetch Front Futures Microstructure with resolved symbol
    fut_micro = r.hgetall(f"md:microstructure:{front_fut_sym}") or {}

    # Parse Spot Price
    spot_p = 0.0
    if spot_micro.get("ltp"):
        spot_p = float(spot_micro["ltp"])
    elif spot_quote.get("ltp"):
        spot_p = float(spot_quote["ltp"])

    # Parse Portfolio State
    portfolio = {}
    if raw_portfolio_state:
        try:
            portfolio = json.loads(raw_portfolio_state)
        except Exception:
            portfolio = {}

    # Extract Closed Positions & Query Live LTP
    closed_trades: List[Dict[str, Any]] = []
    seen_pids = set()

    for msg_id, fields in audit_events:
        if fields.get("event") == "POSITION_CLOSED":
            try:
                d = json.loads(fields.get("data", "{}"))
                pid = d.get("position_id")
                if pid and pid not in seen_pids:
                    seen_pids.add(pid)
                    k = d.get("strike")
                    otype = d.get("option_type")
                    tok = chain_map.get(f"{k}:{otype}")
                    cur_ltp = float(r.hget(f"md:quote:{tok}", "ltp") or 0.0) if tok else 0.0
                    d["current_ltp"] = cur_ltp
                    closed_trades.append(d)
            except Exception:
                pass

    if not closed_trades and "closed_positions" in portfolio:
        for p in portfolio["closed_positions"]:
            pid = p.get("position_id")
            if pid and pid not in seen_pids:
                seen_pids.add(pid)
                closed_trades.append(p)

    closed_trades.reverse()

    return {
        "spot": spot_p,
        "fut_micro": fut_micro,
        "portfolio": portfolio,
        "closed_trades": closed_trades
    }


def make_header() -> Panel:
    """Create the top header banner with live timestamp."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S IST")
    header_text = Text()
    header_text.append("⚡ ULLTR QUANTITATIVE ENGINE ", style="bold cyan")
    header_text.append("• ", style="dim")
    header_text.append("SYNCHRONIZED MULTI-HORIZON TRI-MODEL OPTIONS SYSTEM", style="bold white")
    header_text.append(f"  [{now_str}]", style="green")
    return Panel(header_text, style="blue", box=box.ROUNDED)


def make_tri_model_panel(data: Dict[str, Any]) -> Panel:
    """Construct the Synchronized Tri-Model Multi-Horizon Engines display panel."""
    fut = data["fut_micro"]
    ltp = float(fut.get("ltp", 0.0) or 0.0)
    dpoc = float(fut.get("dpoc", 0.0) or 0.0)
    session_vwap = float(fut.get("session_vwap", 0.0) or 0.0)
    cum_cvd = float(fut.get("cum_cvd", 0.0) or 0.0)
    cvd_15m = float(fut.get("cvd_15m", 0.0) or 0.0)
    delta_oi_15m = float(fut.get("delta_oi_15m", 0.0) or 0.0)
    delta_price_15m = float(fut.get("delta_price_15m", 0.0) or 0.0)

    now_dt = datetime.now()
    now_time = now_dt.strftime("%H:%M")

    # 1. Model POC V2 State (09:20 - 10:30 IST)
    is_poc_window = ("09:20" <= now_time <= "10:30")
    poc_diff = (ltp - dpoc) if (ltp > 0 and dpoc > 0) else 0.0
    if is_poc_window:
        if poc_diff >= 10.0:
            if cvd_15m > 0 and delta_price_15m > 0 and delta_oi_15m > 0:
                poc_regime = "[bold green]🚀 TRUE BULL BREAKOUT (CE)[/]"
            else:
                poc_regime = "[bold red]🪤 BULL TRAP FADE (PE)[/]"
        elif poc_diff <= -10.0:
            if cvd_15m > 0 or (delta_price_15m > 0 and delta_oi_15m > 0):
                poc_regime = "[bold green]🛡️ ABSORPTION BOTTOM (CE)[/]"
            else:
                poc_regime = "[bold red]📉 TRUE BEAR BREAKDOWN (PE)[/]"
        else:
            poc_regime = "[cyan]⏳ Scanning (|ΔPOC| < 10)[/]"
        poc_win_str = "[bold green]ACTIVE (09:20-10:30)[/]"
    else:
        poc_regime = "[dim]Closed for Day[/]" if now_time > "10:30" else "[yellow]Waiting (Starts 09:20)[/]"
        poc_win_str = "[dim]CLOSED[/]" if now_time > "10:30" else "[yellow]PRE-WINDOW[/]"

    # 2. Model Spatial Box with AVWAP Arm Gate State (09:20 - 15:00 IST)
    is_box_window = ("09:20" <= now_time <= "15:00")
    box_win_str = "[bold green]ACTIVE (09:20-15:00)[/]" if is_box_window else ("[dim]CLOSED[/]" if now_time > "15:00" else "[yellow]PRE-WINDOW[/]")
    dist_avwap = abs(ltp - session_vwap) if (ltp > 0 and session_vwap > 0) else 0.0
    arm_status = "[bold green]ARMED (Dist <= 15pt)[/]" if dist_avwap <= 15.0 else "[dim yellow]VETOED (Dist > 15pt)[/]"

    # 3. Model TPO Market Profile POC Reversion State (BLOCKED BY USER)
    tpo_win_str = "[bold red]BLOCKED[/]"
    tpo_regime = "[dim red]Trades Disabled by User[/]"
    period_letter = "A"

    table = Table.grid(padding=(0, 1))
    table.add_column("Horizon & Alpha Model", style="bold white", width=22)
    table.add_column("Window", style="bold", width=19)
    table.add_column("Key Microstructure Metric", style="cyan", width=25)
    table.add_column("Live Regime & Signal State", style="white", width=27)

    table.add_row(
        "[bold cyan]1. Model POC V2[/]",
        poc_win_str,
        f"dPOC: [bold yellow]{dpoc:,.1f}[/] (Δ: {poc_diff:+.1f}pt)",
        poc_regime
    )
    table.add_row(
        "[bold magenta]2. Model Spatial Box[/]",
        box_win_str,
        f"AVWAP Gate: {arm_status}",
        f"CVD: {cum_cvd:+,.0f} (+45/-15pt)"
    )
    table.add_row(
        "[dim]3. TPO POC Reversion[/]",
        tpo_win_str,
        "[dim]Disabled / Blocked[/]",
        tpo_regime
    )

    return Panel(table, title="[bold cyan]Synchronized Multi-Horizon Tri-Model Alpha Engines[/]", border_style="cyan", box=box.ROUNDED)


def make_microstructure_panel(data: Dict[str, Any]) -> Panel:
    """Construct the Front Futures Microstructure & dPOC display panel."""
    fut = data["fut_micro"]

    ltp = float(fut.get("ltp", 0.0) or 0.0)
    dpoc = float(fut.get("dpoc", 0.0) or 0.0)
    session_vwap = float(fut.get("session_vwap", 0.0) or 0.0)
    cum_cvd = float(fut.get("cum_cvd", 0.0) or 0.0)
    cvd_15m = float(fut.get("cvd_15m", 0.0) or 0.0)
    lead = fut.get("lead", "NEUTRAL")
    spot_p = data.get("spot", 0.0)

    table = Table.grid(padding=(0, 1))
    table.add_column("Metric", style="bold white", width=16)
    table.add_column("Value", style="magenta", width=22)

    table.add_row("Futures LTP:", f"[bold white]{ltp:,.2f}[/]" if ltp > 0 else "[dim]--[/dim]")
    table.add_row("Underlying Spot:", f"[bold white]{spot_p:,.2f}[/]" if spot_p > 0 else "[dim]--[/dim]")
    table.add_row("Session VWAP:", f"[bold blue]{session_vwap:,.2f}[/bold blue]" if session_vwap > 0 else "[dim]--[/dim]")

    dist_vwap = (ltp - session_vwap) if (ltp > 0 and session_vwap > 0) else 0.0
    vwap_col = "green" if dist_vwap >= 0 else "red"
    table.add_row("VWAP Distance:", f"[{vwap_col}]{dist_vwap:+.2f} pts[/]")

    cvd_color = "green" if cum_cvd >= 0 else "red"
    table.add_row("Cumulative CVD:", f"[{cvd_color}]{cum_cvd:+,.0f}[/]")

    cvd_15m_color = "green" if cvd_15m >= 0 else "red"
    table.add_row("15m Quote CVD:", f"[{cvd_15m_color}]{cvd_15m:+,.0f}[/]")

    lead_color = "green" if "BUYER" in lead else ("red" if "SELLER" in lead else "yellow")
    table.add_row("Orderflow Lead:", f"[{lead_color}]{lead}[/]")

    return Panel(table, title="[bold magenta]NIFTY Futures (Microstructure & Flow)[/]", border_style="magenta", box=box.ROUNDED)


def make_portfolio_panel(data: Dict[str, Any]) -> Panel:
    """Construct the Unified Risk & Margin Pool panel."""
    pf = data["portfolio"]

    start_cap = float(pf.get("starting_capital", 25000.0) or 25000.0)
    tot_val = float(pf.get("total_portfolio_value", start_cap) or start_cap)
    realized_pnl = float(pf.get("realized_pnl_today", 0.0) or 0.0)
    unrealized_pnl = float(pf.get("unrealized_pnl", 0.0) or 0.0)
    free_cash = float(pf.get("free_cash", start_cap) or start_cap)
    locked_margin = float(pf.get("locked_margin", 0.0) or 0.0)
    active_count = int(pf.get("active_count", 0) or 0)
    closed_count = int(pf.get("closed_count", 0) or 0)
    updated_at = pf.get("updated_at", "--:--")

    roi_pct = ((tot_val - start_cap) / start_cap) * 100.0 if start_cap > 0 else 0.0

    active_dir = "NEUTRAL"
    active_positions = pf.get("active_positions", [])
    if active_positions:
        active_dir = active_positions[0].get("option_type", "NEUTRAL")
    dir_color = "bold green" if active_dir == "CE" else ("bold red" if active_dir == "PE" else "dim white")

    table = Table.grid(padding=(0, 2))
    table.add_column("Metric", style="bold white", width=22)
    table.add_column("Value", style="green", width=20)
    table.add_column("Metric2", style="bold white", width=18)
    table.add_column("Value2", style="cyan", width=20)

    tot_color = "bold green" if tot_val >= start_cap else "bold red"
    r_color = "bold green" if realized_pnl >= 0 else "bold red"
    u_color = "green" if unrealized_pnl >= 0 else "red"

    table.add_row("Pool Starting Capital:", f"₹{start_cap:,.2f}", "Locked Margin:", f"₹{locked_margin:,.2f}")
    table.add_row("Total Portfolio Value:", f"[{tot_color}]₹{tot_val:,.2f} ({roi_pct:+5.2f}%)[/]", "Free Cash:", f"₹{free_cash:,.2f}")
    table.add_row("Realized PnL Today:", f"[{r_color}]₹{realized_pnl:+,.2f}[/]", "Position Count:", f"[bold cyan]{active_count} Active[/] ({closed_count} closed)")
    table.add_row("Unrealized MTM:", f"[{u_color}]₹{unrealized_pnl:+,.2f}[/]", "Direction Lock:", f"[{dir_color}]LONG {active_dir}[/] (Aligned Concurrency)")

    title_str = f"Unified ₹{int(start_cap):,} Margin & Risk Pool (Synchronized Tri-Model)"
    return Panel(table, title=f"[bold green]{title_str}[/]", border_style="green", box=box.ROUNDED)


def make_positions_table(data: Dict[str, Any]) -> Panel:
    """Construct the Active Positions table."""
    pf = data["portfolio"]
    active_positions = pf.get("active_positions", [])

    table = Table(expand=True, box=box.SIMPLE_HEAVY)
    table.add_column("ID", style="dim", width=5)
    table.add_column("Model", style="bold yellow", width=16)
    table.add_column("Symbol", style="bold white", width=18)
    table.add_column("Type", width=5)
    table.add_column("Lots", justify="right", width=14)
    table.add_column("Entry Time", justify="center", width=10)
    table.add_column("Entry Opt", justify="right", width=10)
    table.add_column("Current Opt", justify="right", width=11)
    table.add_column("Pts", justify="right", width=8)
    table.add_column("PnL", justify="right", style="bold", width=12)

    if not active_positions:
        table.add_row(
            "-", "[dim]No active[/dim]", "[dim]Scanning POC migrations...[/dim]",
            "-", "-", "-", "-", "-", "-", "[dim]₹0.00[/dim]"
        )
    else:
        for p in active_positions:
            pid = str(p.get("position_id", ""))
            model = str(p.get("model_name", "") or p.get("model", "Model POC V2"))
            sym = str(p.get("symbol", ""))
            otype = str(p.get("option_type", ""))
            lots = p.get("lots", 1)
            rem_lots = p.get("remaining_lots", lots)
            t1_hit = p.get("t1_hit", False)
            etime = str(p.get("entry_time", ""))
            entry_opt = float(p.get("entry_opt", 0.0) or 0.0)
            current_opt = float(p.get("current_opt", entry_opt) or entry_opt)
            pnl = float(p.get("unrealized_pnl", 0.0) or 0.0)
            pts = float(p.get("points", 0.0) or 0.0)

            lot_str = f"{rem_lots}/{lots} lot"
            if t1_hit:
                lot_str += " [green](T1 Banked)[/]"

            otype_style = "green" if otype == "CE" else "red"
            pnl_style = "bold green" if pnl >= 0 else "bold red"
            pts_style = "green" if pts >= 0 else "red"

            table.add_row(
                pid,
                model,
                sym,
                f"[{otype_style}]{otype}[/]",
                lot_str,
                etime,
                f"₹{entry_opt:.2f}",
                f"₹{current_opt:.2f}",
                f"[{pts_style}]{pts:+.2f}[/]",
                f"[{pnl_style}]₹{pnl:+,.2f}[/]"
            )

    return Panel(table, title="[bold white]Active Positions (Synchronized Tri-Model Engines)[/]", border_style="white", box=box.ROUNDED)


def make_closed_positions_table(data: Dict[str, Any]) -> Panel:
    """Construct the Closed Positions history table with Entry, Exit, and Live LTP."""
    closed = data.get("closed_trades", [])

    table = Table(expand=True, box=box.SIMPLE_HEAVY)
    table.add_column("ID", style="dim", width=5)
    table.add_column("Model", style="bold yellow", width=16)
    table.add_column("Symbol", style="bold white", width=18)
    table.add_column("Lots", justify="right", width=6)
    table.add_column("Entry Opt", justify="right", width=10)
    table.add_column("Exit Opt", justify="right", width=10)
    table.add_column("Live LTP", justify="right", width=10)
    table.add_column("Pts", justify="right", width=8)
    table.add_column("Realized PnL", justify="right", style="bold", width=13)
    table.add_column("Exit Reason", style="dim white", width=30)

    if not closed:
        table.add_row(
            "-", "[dim]None[/dim]", "[dim]No closed trades today[/dim]",
            "-", "-", "-", "-", "-", "[dim]₹0.00[/dim]", "-"
        )
    else:
        for t in closed:
            pid = str(t.get("position_id", ""))
            model = str(t.get("model_name", "") or t.get("model", "Model POC V2"))
            sym = str(t.get("symbol", ""))
            lots = t.get("lots", 1)
            entry_opt = float(t.get("entry_opt", 0.0) or 0.0)
            exit_opt = float(t.get("exit_opt", 0.0) or 0.0)
            live_ltp = float(t.get("current_ltp", 0.0) or 0.0)
            pnl = float(t.get("pnl", 0.0) or 0.0)
            reason = str(t.get("reason", ""))
            pts = exit_opt - entry_opt

            pnl_style = "bold green" if pnl >= 0 else "bold red"
            pts_style = "green" if pts >= 0 else "red"

            table.add_row(
                pid,
                model,
                sym,
                f"{lots} lot",
                f"₹{entry_opt:.2f}",
                f"₹{exit_opt:.2f}",
                f"₹{live_ltp:.2f}" if live_ltp > 0 else "[dim]--[/dim]",
                f"[{pts_style}]{pts:+.2f}[/]",
                f"[{pnl_style}]₹{pnl:+,.2f}[/]",
                reason
            )

    return Panel(table, title="[bold yellow]Closed Positions Today (Executed & Settled)[/]", border_style="yellow", box=box.ROUNDED)


def make_footer() -> Panel:
    """Create a minimal bottom status bar with exit hotkey instructions."""
    footer_text = Text()
    footer_text.append("⌨️  Controls: ", style="bold cyan")
    footer_text.append("Press ", style="dim")
    footer_text.append("[ q ]", style="bold white on blue")
    footer_text.append(" or ", style="dim")
    footer_text.append("[ Ctrl+C ]", style="bold white on red")
    footer_text.append(" to cleanly exit dashboard", style="dim")
    return Panel(footer_text, style="dim", box=box.ROUNDED)


def build_full_layout(data: Dict[str, Any]) -> Layout:
    """Compose the Rich Layout grid with all panels."""
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="top_row", size=9),
        Layout(name="middle_row", size=6),
        Layout(name="positions", ratio=1),
        Layout(name="closed", ratio=1),
        Layout(name="footer", size=3)
    )

    layout["header"].update(make_header())

    top_row = Layout()
    top_row.split_row(
        Layout(make_tri_model_panel(data), name="tri_model", ratio=1),
        Layout(make_microstructure_panel(data), name="micro", ratio=1)
    )
    layout["top_row"].update(top_row)
    layout["middle_row"].update(make_portfolio_panel(data))
    layout["positions"].update(make_positions_table(data))
    layout["closed"].update(make_closed_positions_table(data))
    layout["footer"].update(make_footer())

    return layout


def main():
    parser = argparse.ArgumentParser(description="ULLTR Live Synchronized Tri-Model Portfolio Dashboard")
    parser.add_argument("--once", action="store_true", help="Print single snapshot and exit")
    parser.add_argument("--interval", type=float, default=1.0, help="Refresh interval in seconds (default 1.0)")
    parser.add_argument("--no-screen", action="store_true", help="Disable alternate screen buffer (allows scrollback)")
    args = parser.parse_args()

    console = Console()
    r = get_redis_client()

    try:
        r.ping()
    except Exception as e:
        console.print(f"[bold red]❌ Failed connecting to Redis: {e}[/bold red]")
        sys.exit(1)

    if args.once:
        data = fetch_dashboard_data(r)
        console.print(make_header())
        from rich.columns import Columns
        console.print(Columns([make_tri_model_panel(data), make_microstructure_panel(data)], equal=True))
        console.print(make_portfolio_panel(data))
        console.print(make_positions_table(data))
        console.print(make_closed_positions_table(data))
        return

    term_ctx = TerminalInputContext()

    def emergency_exit(signum=None, frame=None):
        term_ctx.restore()
        try:
            console.show_cursor(True)
        except Exception:
            pass
        console.print("\n[bold green]👋 Exited live Tri-Model dashboard cleanly.[/bold green]\n")
        sys.exit(0)

    signal.signal(signal.SIGINT, emergency_exit)
    signal.signal(signal.SIGTERM, emergency_exit)

    console.clear()
    with term_ctx:
        try:
            with Live(console=console, screen=(not args.no_screen), refresh_per_second=4, auto_refresh=True) as live:
                while True:
                    try:
                        data = fetch_dashboard_data(r)
                        layout = build_full_layout(data)
                        live.update(layout)
                    except Exception as e:
                        console.print(f"[bold red]Dashboard update error: {e}[/bold red]")

                    # Responsive sleep loop checking for keypresses and signals every 50ms
                    sleep_deadline = time.time() + args.interval
                    while time.time() < sleep_deadline:
                        if term_ctx.check_exit_key():
                            emergency_exit()
                        time.sleep(0.05)
        except (KeyboardInterrupt, SystemExit):
            emergency_exit()
        finally:
            term_ctx.restore()
            console.show_cursor(True)


if __name__ == "__main__":
    main()

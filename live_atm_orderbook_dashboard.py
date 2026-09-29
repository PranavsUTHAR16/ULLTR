#!/usr/bin/env python3
"""
⚡ ULLTR Real-Time ATM Option Orderbook Terminal Dashboard
==========================================================
High-frequency (30–50 FPS) live 5-level Bid/Ask depth visualizer for ATM options.
Reads streaming market data directly from Redis (local or via SSH tunnel).

Usage:
  1. Direct on AWS VM:
     python3 live_atm_orderbook_dashboard.py --index NIFTY

  2. On Local Mac (auto-tunnels to VM Redis):
     python3 live_atm_orderbook_dashboard.py --remote 65.1.219.83 --key openalgo-aws-key.pem --index NIFTY

  3. Toggle Index:
     --index SENSEX (or NIFTY)
"""

import os
import sys
import time
import argparse
import subprocess
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List

try:
    import redis
except ImportError:
    print("❌ 'redis' package is missing. Install with: pip install redis")
    sys.exit(1)

try:
    from rich.console import Console
    from rich.live import Live
    from rich.table import Table
    from rich.panel import Panel
    from rich.layout import Layout
    from rich.text import Text
    from rich import box
except ImportError:
    print("❌ 'rich' package is missing. Install with: pip install rich")
    sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(description="High-Speed ATM Option Orderbook Dashboard")
    parser.add_argument("--index", type=str, default="NIFTY", choices=["NIFTY", "SENSEX"], help="Underlying Index (default: NIFTY)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Redis Host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=6379, help="Redis Port (default: 6379)")
    parser.add_argument("--fps", type=float, default=30.0, help="Target refresh rate in FPS (default: 30)")
    parser.add_argument("--remote", type=str, default="", help="VM IP for auto SSH tunneling (e.g., 65.1.219.83)")
    parser.add_argument("--key", type=str, default="openalgo-aws-key.pem", help="Path to SSH key for tunneling")
    parser.add_argument("--user", type=str, default="ubuntu", help="SSH user (default: ubuntu)")
    return parser.parse_args()


class SSHRedisTunnel:
    """Manages an automatic SSH background tunnel to the remote Redis instance."""
    def __init__(self, remote_host: str, ssh_key: str, ssh_user: str = "ubuntu", local_port: int = 6380):
        self.remote_host = remote_host
        self.ssh_key = ssh_key
        self.ssh_user = ssh_user
        self.local_port = local_port

    def start(self) -> int:
        cmd = [
            "ssh", "-N", "-f",
            "-L", f"{self.local_port}:localhost:6379",
            "-i", self.ssh_key,
            "-o", "StrictHostKeyChecking=no",
            "-o", "ExitOnForwardFailure=yes",
            f"{self.ssh_user}@{self.remote_host}"
        ]
        try:
            subprocess.run(cmd, check=True, timeout=10)
            time.sleep(0.8)
            return self.local_port
        except Exception as e:
            print(f"⚠️ SSH Tunnel note: {e}")
            return 6379


class LiveOrderbookDashboard:
    def __init__(self, redis_client: redis.Redis, underlying: str = "NIFTY", target_fps: float = 30.0):
        self.r = redis_client
        self.underlying = underlying.upper()
        self.target_fps = target_fps
        self.frame_delay = 1.0 / target_fps
        self.console = Console()
        self.step = 50 if self.underlying == "NIFTY" else 100
        
        # Spot key
        self.spot_sym = self.r.get(f"spot:{self.underlying}") or ("BSE_INDEX|SENSEX" if self.underlying == "SENSEX" else "NSE_INDEX|Nifty 50")
        
        # State tracking
        self.current_atm = 0
        self.expiry = ""
        self.chain_map: Dict[str, str] = {}
        self.ce_token = ""
        self.pe_token = ""
        self.spot = 0.0

        # Immediate contract initialization
        self.init_chain()
        spot_raw = self.r.hget(f"md:quote:{self.spot_sym}", "ltp")
        self.spot = float(spot_raw or 0.0) if spot_raw else (24000.0 if self.underlying == "NIFTY" else 76500.0)
        self.update_atm_contracts(self.spot)

    def init_chain(self):
        """Discovers front expiry and caches option chain mapping."""
        chain_keys = self.r.keys(f"chain:{self.underlying}:*")
        if chain_keys:
            exp_list = sorted([k.split(":")[-1] for k in chain_keys])
            today_str = datetime.now().strftime("%Y-%m-%d")
            self.expiry = next((e for e in exp_list if e >= today_str), exp_list[0])
            self.chain_map = self.r.hgetall(f"chain:{self.underlying}:{self.expiry}")

    def update_atm_contracts(self, spot: float):
        """Updates ATM strike and tokens if spot moves across strike threshold."""
        atm = int(round(spot / self.step) * self.step)
        if atm != self.current_atm or not self.ce_token:
            self.current_atm = atm
            self.ce_token = self.chain_map.get(f"{atm}:CE") or self.chain_map.get(f"{float(atm)}:CE", "")
            self.pe_token = self.chain_map.get(f"{atm}:PE") or self.chain_map.get(f"{float(atm)}:PE", "")

    def fetch_unified_pipeline(self) -> Tuple[float, Dict[str, Any], Dict[str, Any], float]:
        """Pipelined fetch of Spot + ATM CE Depth + ATM PE Depth in 1 single network trip."""
        t0 = time.perf_counter_ns()
        pipe = self.r.pipeline(transaction=False)
        pipe.hget(f"md:quote:{self.spot_sym}", "ltp")
        pipe.hgetall(f"md:quote:{self.ce_token}")
        pipe.hgetall(f"md:quote:{self.pe_token}")
        spot_raw, ce_data, pe_data = pipe.execute()
        t1 = time.perf_counter_ns()
        
        spot = float(spot_raw or 0.0)
        if spot <= 0:
            spot = self.spot if self.spot > 0 else (24000.0 if self.underlying == "NIFTY" else 76500.0)
        self.spot = spot
        
        # Check if ATM changed
        cur_atm = int(round(spot / self.step) * self.step)
        if cur_atm != self.current_atm:
            self.update_atm_contracts(spot)
            
        rtt_us = (t1 - t0) / 1000.0
        return spot, ce_data or {}, pe_data or {}, rtt_us

    def _render_depth_table(self, data: Dict[str, Any], opt_type: str, strike: int) -> Table:
        """Constructs a rich 5-level orderbook table with buy/sell bars and microsecond precision."""
        color = "bright_cyan" if opt_type == "CE" else "bright_magenta"
        token = self.ce_token if opt_type == "CE" else self.pe_token
        title = f"[{color} bold]{self.underlying} {strike} {opt_type}[/{color} bold] [dim]({token})[/dim]"
        
        table = Table(
            title=title,
            box=box.ROUNDED,
            header_style="bold white on navy_blue" if opt_type == "CE" else "bold white on purple4",
            expand=True,
            show_footer=True
        )
        
        table.add_column("Orders", justify="right", style="dim", width=6)
        table.add_column("Bid Qty", justify="right", style="bold green", width=9)
        table.add_column("Bid Price", justify="right", style="bold green", width=10)
        table.add_column("Ask Price", justify="left", style="bold red", width=10)
        table.add_column("Ask Qty", justify="left", style="bold red", width=9)
        table.add_column("Orders", justify="left", style="dim", width=6)
        
        bids = []
        asks = []
        for i in range(1, 6):
            suffix = "" if i == 1 else str(i)
            bp = float(data.get(f"bid{suffix}", 0.0) or 0.0)
            bq = int(float(data.get(f"bid_qty{suffix}", 0) or 0))
            ap = float(data.get(f"ask{suffix}", 0.0) or 0.0)
            aq = int(float(data.get(f"ask_qty{suffix}", 0) or 0))
            bids.append((bp, bq))
            asks.append((ap, aq))
            
        max_q = max(max((b[1] for b in bids), default=1), max((a[1] for a in asks), default=1), 1)

        for i in range(5):
            bp, bq = bids[i]
            ap, aq = asks[i]
            
            b_bar_len = int((bq / max_q) * 6)
            a_bar_len = int((aq / max_q) * 6)
            b_bar = "█" * b_bar_len
            a_bar = "█" * a_bar_len
            
            table.add_row(
                f"{b_bar}",
                f"{bq:,}" if bq > 0 else "-",
                f"₹{bp:.2f}" if bp > 0 else "-",
                f"₹{ap:.2f}" if ap > 0 else "-",
                f"{aq:,}" if aq > 0 else "-",
                f"{a_bar}"
            )
            
        tbq = int(float(data.get("tbq", 0) or 0))
        tsq = int(float(data.get("tsq", 0) or 0))
        tot = tbq + tsq
        b_pct = (tbq / tot * 100.0) if tot > 0 else 50.0
        s_pct = 100.0 - b_pct
        
        table.columns[1].footer = f"[green]TBQ: {tbq:,}[/green]"
        table.columns[2].footer = f"[green]{b_pct:.1f}%[/green]"
        table.columns[3].footer = f"[red]{s_pct:.1f}%[/red]"
        table.columns[4].footer = f"[red]TSQ: {tsq:,}[/red]"
        
        return table

    def _render_greeks_panel(self, data: Dict[str, Any], opt_type: str) -> Panel:
        """Constructs Greeks & Microstructure stats panel."""
        ltp = float(data.get("ltp", 0.0) or 0.0)
        close = float(data.get("close", 0.0) or 0.0)
        chg = ltp - close
        chg_pct = (chg / close * 100.0) if close > 0 else 0.0
        chg_style = "green" if chg >= 0 else "red"
        chg_sign = "+" if chg >= 0 else ""
        
        delta = float(data.get("delta", 0.0) or 0.0)
        theta = float(data.get("theta", 0.0) or 0.0)
        gamma = float(data.get("gamma", 0.0) or 0.0)
        vega = float(data.get("vega", 0.0) or 0.0)
        iv = float(data.get("iv", 0.0) or 0.0) * 100.0
        volume = int(float(data.get("volume", 0) or 0))
        oi = int(float(data.get("oi", 0) or 0))
        
        grid = Table.grid(expand=True)
        grid.add_column(justify="left")
        grid.add_column(justify="right")
        
        grid.add_row("[bold white]LTP[/bold white]", f"[{chg_style} bold]₹{ltp:.2f} ({chg_sign}{chg:.2f} | {chg_sign}{chg_pct:.1f}%)[/{chg_style} bold]")
        grid.add_row("[dim]Delta (Δ)[/dim]", f"[bold yellow]{delta:+.4f}[/bold yellow]")
        grid.add_row("[dim]Theta (θ)[/dim]", f"[bold red]{theta:.2f} ₹/day[/bold red]")
        grid.add_row("[dim]Gamma (Γ)[/dim]", f"[cyan]{gamma:.5f}[/cyan]")
        grid.add_row("[dim]Vega (ν)[/dim]", f"[blue]{vega:.2f}[/blue]")
        grid.add_row("[dim]IV[/dim]", f"[magenta]{iv:.1f}%[/magenta]")
        grid.add_row("[dim]Volume[/dim]", f"[white]{volume:,}[/white]")
        grid.add_row("[dim]Open Int[/dim]", f"[white]{oi:,}[/white]")
        
        return Panel(grid, title=f"[bold]{opt_type} Metrics & Greeks[/bold]", box=box.ROUNDED)

    def build_dashboard(self, spot: float, atm: int, ce_data: Dict[str, Any], pe_data: Dict[str, Any], rtt_us: float, cur_fps: float) -> Layout:
        """Assembles full terminal dashboard layout."""
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body", ratio=1),
            Layout(name="footer", size=3)
        )
        
        # Header
        now_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        header_text = Text()
        header_text.append("⚡ ULLTR HIGH-SPEED ORDERBOOK ENGINE ", style="bold gold1")
        header_text.append(f"| {self.underlying} SPOT: ", style="bold white")
        header_text.append(f"₹{spot:,.2f} ", style="bold cyan")
        header_text.append(f"| ATM STRIKE: ", style="bold white")
        header_text.append(f"{atm} ", style="bold green")
        header_text.append(f"| EXPIRY: ", style="bold white")
        header_text.append(f"{self.expiry} ", style="bold yellow")
        header_text.append(f"| REDIS RTT: ", style="bold white")
        header_text.append(f"{rtt_us:.0f} µs ({rtt_us/1000:.3f} ms) ", style="bold chartreuse1")
        header_text.append(f"| REFRESH: ", style="bold white")
        header_text.append(f"{cur_fps:.1f} FPS ", style="bold magenta")
        header_text.append(f"| {now_time}", style="dim")
        
        layout["header"].update(Panel(header_text, box=box.DOUBLE, style="blue"))
        
        # Body: CE on Left, PE on Right
        layout["body"].split_row(
            Layout(name="ce_col", ratio=1),
            Layout(name="pe_col", ratio=1)
        )
        
        layout["ce_col"].split_column(
            Layout(self._render_depth_table(ce_data, "CE", atm), ratio=2),
            Layout(self._render_greeks_panel(ce_data, "CE"), ratio=1)
        )
        
        layout["pe_col"].split_column(
            Layout(self._render_depth_table(pe_data, "PE", atm), ratio=2),
            Layout(self._render_greeks_panel(pe_data, "PE"), ratio=1)
        )
        
        # Footer
        ce_tbq = int(float(ce_data.get("tbq", 0) or 0))
        pe_tbq = int(float(pe_data.get("tbq", 0) or 0))
        ce_tsq = int(float(ce_data.get("tsq", 0) or 0))
        pe_tsq = int(float(pe_data.get("tsq", 0) or 0))
        
        pcr_vol = (pe_tbq / max(ce_tbq, 1))
        footer_text = Text()
        footer_text.append("📊 ORDERBOOK MICROSTRUCTURE: ", style="bold white")
        footer_text.append(f"CE Imbalance: {((ce_tbq - ce_tsq)/max(ce_tbq+ce_tsq,1)*100):+.1f}% | ", style="cyan")
        footer_text.append(f"PE Imbalance: {((pe_tbq - pe_tsq)/max(pe_tbq+pe_tsq,1)*100):+.1f}% | ", style="magenta")
        footer_text.append(f"Depth PCR (TBQ): {pcr_vol:.2f} | ", style="yellow")
        footer_text.append("Press [Ctrl+C] to Exit", style="bold red")
        
        layout["footer"].update(Panel(footer_text, box=box.ROUNDED, style="dim"))
        return layout

    def run(self):
        """Main ultra-fast rendering loop."""
        print(f"🚀 Initializing High-Speed Orderbook Visualizer for {self.underlying}...")
        self.init_chain()
        
        # Initial spot & ATM contract resolution
        spot_raw = self.r.hget(f"md:quote:{self.spot_sym}", "ltp")
        spot = float(spot_raw or 0.0) if spot_raw else (24000.0 if self.underlying == "NIFTY" else 76500.0)
        self.update_atm_contracts(spot)
        print(f"✅ Initialized: Spot ₹{spot:,.2f} | ATM {self.current_atm} | Expiry: {self.expiry}")
        print(f"   CE Token: {self.ce_token} | PE Token: {self.pe_token}")
        time.sleep(0.5)
        
        last_time = time.perf_counter()
        frame_times: List[float] = []
        
        with Live(console=self.console, refresh_per_second=int(self.target_fps), screen=True) as live:
            try:
                while True:
                    t_loop_start = time.perf_counter()
                    
                    # Ultra-fast unified pipelined fetch
                    spot, ce_data, pe_data, rtt_us = self.fetch_unified_pipeline()
                    
                    # Calculate FPS
                    t_now = time.perf_counter()
                    dt = t_now - last_time
                    last_time = t_now
                    if dt > 0:
                        frame_times.append(1.0 / dt)
                        if len(frame_times) > 30:
                            frame_times.pop(0)
                    cur_fps = sum(frame_times) / len(frame_times) if frame_times else self.target_fps
                    
                    # Render dashboard layout
                    dashboard_view = self.build_dashboard(spot, self.current_atm, ce_data, pe_data, rtt_us, cur_fps)
                    live.update(dashboard_view)
                    
                    # High precision sleep to maintain target FPS
                    elapsed = time.perf_counter() - t_loop_start
                    sleep_time = self.frame_delay - elapsed
                    if sleep_time > 0.001:
                        time.sleep(sleep_time)
                        
            except KeyboardInterrupt:
                pass


def main():
    args = parse_args()
    port = args.port
    
    # Auto-tunnel if remote VM is provided
    if args.remote:
        key_path = os.path.expanduser(args.key)
        if not os.path.exists(key_path):
            alt_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), args.key)
            if os.path.exists(alt_path):
                key_path = alt_path
        print(f"🔒 Setting up SSH Tunnel to Redis on {args.remote} via {key_path}...")
        tunnel = SSHRedisTunnel(remote_host=args.remote, ssh_key=key_path, ssh_user=args.user, local_port=6380)
        port = tunnel.start()
        print(f"✅ SSH Tunnel active on 127.0.0.1:{port}")
        
    try:
        r = redis.Redis(host=args.host, port=port, db=0, decode_responses=True)
        r.ping()
    except Exception as e:
        print(f"❌ Could not connect to Redis at {args.host}:{port}: {e}")
        if not args.remote:
            print("💡 Tip: If connecting to AWS VM from your Mac, run with:")
            print(f"   python3 live_atm_orderbook_dashboard.py --remote 65.1.219.83 --key openalgo-aws-key.pem")
        sys.exit(1)
        
    dashboard = LiveOrderbookDashboard(redis_client=r, underlying=args.index, target_fps=args.fps)
    dashboard.run()


if __name__ == "__main__":
    main()

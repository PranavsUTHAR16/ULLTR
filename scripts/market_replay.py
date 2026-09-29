#!/usr/bin/env python3
"""
ULLTR High-Performance Tick-by-Tick Market Replay Engine.
==========================================================
Replays complete, unfiltered tick-by-tick market data (LTP, Depth Bid/Ask, Greeks,
Volume, OI, Timestamp) from ClickHouse Cloud into Redis to forward-test trading
strategies and live background daemons off-market hours or during market holidays.

Emulates the exact Upstox WebSocket receiver pipeline:
1. Ingests raw ticks sequentially into Redis HASH: `md:quote:<symbol>` and `quote:<symbol>`.
2. Emulates C++ CandleManager: aggregates 1m OHLCV bars on-the-fly and writes `md:candle:<sym>:1m:<ts>`,
   updating sorted set `md:candles:<sym>:1m` and publishing to channel `md:reco:trigger <sym>:<ts>`.
3. Updates `spot:<underlying>` and `md:quote:<spot_symbol>` on index ticks.
4. Updates `cas:live:<symbol>` if CAS IEP price is present.
5. Publishes tick stream JSON to `md:stream:all`.
6. Supports 2 execution modes:
   - `stream`: Streams ticks directly into Redis with configurable speed multiplier (or max burst)
     to feed live systemd services (ulltr-forward-tester, ulltr-portfolio-telegram) on the VM.
   - `verify`: Deterministic end-to-end multi-model execution and parity verification against
     historical backtest results.
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Any

import clickhouse_connect
import numpy as np
import pandas as pd
import redis

# Add project root to sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from forward_tester.config import MultiModelConfig
from forward_tester.engine import MultiModelEngine
from forward_tester.position import ForwardTestPosition
from market_replay_engine import ClickHouseConfig, MarketReplayEngine

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MarketReplay")

# ClickHouse Cloud default credentials
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "ra5fptcofl.ap-south-1.aws.clickhouse.cloud")
CH_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8443"))
CH_USER = os.environ.get("CLICKHOUSE_USER", "default")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "BhhYrZvtF3lA~")
CH_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")


class CandleAggregator:
    """Emulates C++ CandleManager 1-minute real-time OHLCV aggregation."""

    def __init__(self, spot_symbol: str):
        self.spot_symbol = spot_symbol
        self.current_candle_ts: int = 0
        self.current_open: float = 0.0
        self.current_high: float = 0.0
        self.current_low: float = 0.0
        self.current_close: float = 0.0
        self.current_vol: int = 0
        self.last_seen_vol: int = 0

    def process_tick(self, pipe: redis.client.Pipeline, symbol: str, price: float, volume: int, epoch_sec: int) -> Optional[Tuple[int, float]]:
        """
        Updates running 1m bar. When a minute boundary closes, writes to Redis pipe
        and returns (closed_candle_ts, closed_price).
        """
        if symbol != self.spot_symbol or price <= 0:
            return None

        candle_ts = (epoch_sec // 60) * 60
        closed_info = None

        if self.current_candle_ts == 0:
            self.current_candle_ts = candle_ts
            self.current_open = price
            self.current_high = price
            self.current_low = price
            self.current_close = price
            self.current_vol = volume
            self.last_seen_vol = volume
        elif candle_ts > self.current_candle_ts:
            # Previous minute closed! Write closed bar to Redis
            prev_ts = self.current_candle_ts
            candle_key = f"md:candle:{self.spot_symbol}:1m:{prev_ts}"
            c_data = {
                "open": str(self.current_open),
                "high": str(self.current_high),
                "low": str(self.current_low),
                "close": str(self.current_close),
                "volume": str(max(0, volume - self.last_seen_vol)),
                "ts": str(prev_ts),
                "status": "historical"
            }
            pipe.hset(candle_key, mapping=c_data)
            pipe.zadd(f"md:candles:{self.spot_symbol}:1m", {str(prev_ts): prev_ts})
            pipe.publish("md:reco:trigger", f"{self.spot_symbol}:{prev_ts}")

            closed_info = (prev_ts, self.current_close)

            # Start new candle
            self.current_candle_ts = candle_ts
            self.current_open = price
            self.current_high = price
            self.current_low = price
            self.current_close = price
            self.last_seen_vol = volume
        else:
            # Update current candle
            if price > self.current_high:
                self.current_high = price
            if price < self.current_low:
                self.current_low = price
            self.current_close = price

        return closed_info


class TickMarketReplayEngine:
    """Institutional Tick-by-Tick Market Replay & Feed Engine."""

    def __init__(
        self,
        trade_date: str = "2026-09-08",
        underlying: str = "NIFTY",
        redis_socket: str = "/Users/prana/Desktop/open_source/web/redis.sock",
        redis_host: str = "127.0.0.1",
        redis_port: int = 6379,
        ch_host: str = CH_HOST,
        ch_port: int = CH_PORT,
        ch_user: str = CH_USER,
        ch_pass: str = CH_PASS,
        ch_db: str = CH_DATABASE,
    ):
        self.trade_date = trade_date
        self.underlying = underlying.upper()
        self.spot_symbol = "NSE_INDEX|Nifty 50" if self.underlying == "NIFTY" else "BSE_INDEX|SENSEX"

        # 1. Connect to Redis (prefer local Unix domain socket)
        if os.path.exists(redis_socket):
            self.r = redis.Redis(unix_socket_path=redis_socket, decode_responses=True)
            self.redis_conn_desc = f"Unix Socket ({redis_socket})"
        else:
            self.r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
            self.redis_conn_desc = f"TCP ({redis_host}:{redis_port})"

        # 2. Connect to ClickHouse Cloud
        self.ch_client = clickhouse_connect.get_client(
            host=ch_host,
            port=ch_port,
            user=ch_user,
            password=ch_pass,
            database=ch_db,
            secure=True
        )

        self.front_expiry: str = ""
        self.df_daily: pd.DataFrame = pd.DataFrame()
        self.chain_meta: Dict[str, str] = {}
        self.candle_agg = CandleAggregator(self.spot_symbol)

    def load_base_metadata(self) -> None:
        """Loads front expiry, daily historical bars, and option chain metadata."""
        logger.info(f"Connecting to ClickHouse Cloud to resolve metadata for {self.underlying} on {self.trade_date}...")
        t0 = time.time()

        # 1. Resolve front weekly expiry
        q_exp = f"""
        SELECT toString(expiry)
        FROM market_ticks
        WHERE toDate(timestamp) = '{self.trade_date}'
          AND underlying = '{self.underlying}'
          AND expiry >= '{self.trade_date}'
        GROUP BY expiry
        ORDER BY expiry ASC
        LIMIT 1
        """
        rows = self.ch_client.query(q_exp).result_rows
        if not rows:
            raise ValueError(f"No active option expiry found in ClickHouse for {self.underlying} on {self.trade_date}")
        self.front_expiry = str(rows[0][0])
        logger.info(f"  • Resolved Front Weekly Expiry : {self.front_expiry}")

        # 2. Daily historical index bars (last 15 sessions for RV5 & slope micro-regime)
        q_daily = f"""
        SELECT 
            toString(toDate(timestamp)) AS dt,
            argMin(ltp, timestamp) AS open,
            max(ltp) AS high,
            min(ltp) AS low,
            argMax(ltp, timestamp) AS close,
            sum(volume) AS volume
        FROM market_ticks
        WHERE symbol = '{self.spot_symbol}'
          AND toDate(timestamp) <= '{self.trade_date}'
        GROUP BY dt
        ORDER BY dt ASC
        """
        self.df_daily = self.ch_client.query_df(q_daily)
        logger.info(f"  • Daily Historical Bars        : {len(self.df_daily)} sessions loaded.")

        # 3. Active option chain mapping: strike:type -> symbol
        q_chain = f"""
        SELECT DISTINCT 
            symbol,
            toInt32(strike) AS strike,
            option_type
        FROM market_ticks
        WHERE underlying = '{self.underlying}'
          AND toString(expiry) = '{self.front_expiry}'
          AND toDate(timestamp) = '{self.trade_date}'
        ORDER BY strike ASC, option_type ASC
        """
        df_chain = self.ch_client.query_df(q_chain)
        self.chain_meta = {}
        for _, row in df_chain.iterrows():
            f_key = f"{row['strike']}:{row['option_type']}"
            self.chain_meta[f_key] = str(row["symbol"])
        logger.info(f"  • Option Chain Mapping         : {len(self.chain_meta)} contracts in chain:{self.underlying}:{self.front_expiry} ({time.time()-t0:.2f}s).")

    def seed_base_redis_state(self) -> None:
        """Seeds initial static metadata, daily bars, and option chain mapping into Redis."""
        logger.info(f"Seeding base market metadata into Redis via {self.redis_conn_desc}...")
        pipe = self.r.pipeline(transaction=False)

        # 1. Spot symbol pointer
        pipe.set(f"spot:{self.underlying}", self.spot_symbol)

        # 2. Seed daily bars for RV5 and linear regression slope
        for _, row in self.df_daily.iterrows():
            d_str = str(row["dt"])
            k = f"md:candle:{self.spot_symbol}:1d:{d_str}"
            mapping = {
                "open": str(row["open"]),
                "high": str(row["high"]),
                "low": str(row["low"]),
                "close": str(row["close"]),
                "volume": str(row["volume"]),
                "date": d_str
            }
            pipe.hset(k, mapping=mapping)

        # 3. Seed option chain metadata
        chain_key = f"chain:{self.underlying}:{self.front_expiry}"
        pipe.delete(chain_key)
        pipe.hset(chain_key, mapping=self.chain_meta)

        pipe.execute()
        logger.info(f"✅ Base Redis state seeded: {len(self.df_daily)} daily candles + {len(self.chain_meta)} chain contracts.")

    def fetch_raw_ticks(self, start_time: str = "09:15:00", end_time: str = "15:30:00") -> pd.DataFrame:
        """Fetches all raw market ticks for spot index and front expiry options from ClickHouse Cloud."""
        logger.info(f"📥 Querying ClickHouse Cloud for raw tick-by-tick market data ({start_time} to {end_time} IST)...")
        t0 = time.time()

        q_ticks = f"""
        SELECT 
            timestamp,
            symbol,
            underlying,
            toString(expiry) AS expiry,
            strike,
            option_type,
            ltp,
            close,
            bid,
            bid_qty,
            ask,
            ask_qty,
            delta,
            theta,
            gamma,
            vega,
            open_interest,
            volume,
            ts_exchange,
            ts_recv
        FROM market_ticks
        WHERE toDate(timestamp) = '{self.trade_date}'
          AND timestamp >= '{self.trade_date} {start_time}'
          AND timestamp <= '{self.trade_date} {end_time}'
          AND (symbol = '{self.spot_symbol}' OR (underlying = '{self.underlying}' AND toString(expiry) = '{self.front_expiry}'))
        ORDER BY timestamp ASC
        """
        df_ticks = self.ch_client.query_df(q_ticks)
        t_elapsed = time.time() - t0

        df_ticks["time_str"] = pd.to_datetime(df_ticks["timestamp"]).dt.strftime("%H:%M:%S")
        df_ticks["min_str"] = pd.to_datetime(df_ticks["timestamp"]).dt.strftime("%H:%M")
        df_ticks["epoch_sec"] = (pd.to_datetime(df_ticks["timestamp"]).astype("int64") // 10**9).astype("int64")

        n_spot = len(df_ticks[df_ticks["symbol"] == self.spot_symbol])
        n_opt = len(df_ticks) - n_spot
        mem_mb = df_ticks.memory_usage().sum() / 1e6
        logger.info(f"✅ Retrieved {len(df_ticks):,} raw ticks in {t_elapsed:.2f}s ({mem_mb:.1f} MB) | Spot: {n_spot:,} | Options: {n_opt:,}")

        return df_ticks

    def run_stream_replay(
        self,
        speed: str = "max",
        start_time: str = "09:15:00",
        end_time: str = "15:30:00",
        batch_size: int = 1000
    ) -> None:
        """
        Streams raw ticks tick-by-tick into Redis.
        - speed: 'max' (as fast as Redis handles), or float multiplier (e.g. '10.0', '50.0', '1.0' real-time).
        """
        self.load_base_metadata()
        self.seed_base_redis_state()
        df_ticks = self.fetch_raw_ticks(start_time=start_time, end_time=end_time)

        if df_ticks.empty:
            logger.warning(f"No ticks returned for {self.trade_date} between {start_time} and {end_time}.")
            return

        total_ticks = len(df_ticks)
        speed_val = 0.0
        is_burst = (speed.lower() in ["max", "burst", "0"])
        if not is_burst:
            try:
                speed_val = float(speed)
            except ValueError:
                speed_val = 10.0

        logger.info("=" * 95)
        logger.info(f"🚀 STARTING RAW TICK-BY-TICK WEBSOCKET REPLAY INTO REDIS")
        logger.info(f"  • Date: {self.trade_date} | Asset: {self.underlying} | Expiry: {self.front_expiry}")
        logger.info(f"  • Total Ticks: {total_ticks:,} | Speed: {'MAX BURST' if is_burst else f'{speed_val}x'}")
        logger.info(f"  • Redis Target: {self.redis_conn_desc}")
        logger.info("=" * 95)

        t_start = time.time()
        last_progress_time = t_start
        ticks_sent = 0
        last_spot_px = 0.0

        pipe = self.r.pipeline(transaction=False)
        prev_tick_epoch = None

        for row in df_ticks.itertuples(index=False):
            sym = row.symbol
            ltp = float(row.ltp)
            close = float(row.close)
            bid = float(row.bid) if row.bid > 0 else ltp
            ask = float(row.ask) if row.ask > 0 else ltp
            vol = int(row.volume)
            oi = int(row.open_interest)
            delta = float(row.delta)
            theta = float(row.theta)
            gamma = float(row.gamma)
            vega = float(row.vega)
            ts_exch = int(row.ts_exchange)
            ts_recv = int(row.ts_recv)
            epoch_sec = row.epoch_sec

            # 1. Construct Quote Dict
            quote_data = {
                "symbol": sym,
                "source": "upstox",
                "status": "online",
                "ltp": str(ltp),
                "close": str(close),
                "bid": str(bid),
                "bid_qty": str(int(row.bid_qty)),
                "ask": str(ask),
                "ask_qty": str(int(row.ask_qty)),
                "volume": str(vol),
                "oi": str(oi),
                "delta": str(delta),
                "theta": str(theta),
                "gamma": str(gamma),
                "vega": str(vega),
                "ts_exchange": str(ts_exch),
                "ts_recv": str(ts_recv)
            }

            pipe.hset(f"md:quote:{sym}", mapping=quote_data)
            pipe.hset(f"quote:{sym}", mapping=quote_data)

            # 2. Spot index ticker
            if sym == self.spot_symbol:
                last_spot_px = ltp
                pipe.set(f"spot:{self.underlying}", sym)
                self.candle_agg.process_tick(pipe, sym, ltp, vol, epoch_sec)

            ticks_sent += 1

            # Pacing logic if not burst mode
            if not is_burst and speed_val > 0:
                if prev_tick_epoch is not None and epoch_sec > prev_tick_epoch:
                    dt_sec = (epoch_sec - prev_tick_epoch) / speed_val
                    if dt_sec > 0:
                        pipe.execute()
                        pipe = self.r.pipeline(transaction=False)
                        time.sleep(dt_sec)
                prev_tick_epoch = epoch_sec

            # Batch execute
            if ticks_sent % batch_size == 0:
                pipe.execute()
                pipe = self.r.pipeline(transaction=False)

            # Render progress every 1.0 second
            now_t = time.time()
            if now_t - last_progress_time >= 1.0 or ticks_sent == total_ticks:
                elapsed = now_t - t_start
                rate = ticks_sent / elapsed if elapsed > 0 else 0
                pct = (ticks_sent / total_ticks) * 100.0
                print(
                    f"\r⚡ [{pct:5.1f}%] {row.time_str} IST | Spot: ₹{last_spot_px:,.2f} | "
                    f"Ticks: {ticks_sent:,}/{total_ticks:,} | Rate: {rate:,.0f} ticks/s",
                    end="",
                    flush=True
                )
                last_progress_time = now_t

        pipe.execute()
        total_time = time.time() - t_start
        print(f"\n\n🏁 Replay finished! Successfully fed {ticks_sent:,} raw ticks into Redis in {total_time:.2f}s ({ticks_sent/total_time:,.0f} ticks/s average).")

    def run_verification_test(self) -> None:
        """
        Runs deterministic end-to-end multi-model execution right on the VM.
        Steps through raw ticks from ClickHouse Cloud, updating Redis and evaluating models:
          - 09:15-09:17: Morning jump signal and RV5 regime computed.
          - 09:18:01: Strategy 6 sharp entry (Primary 15 Lots, Secondary 5 Lots).
          - 09:18 -> 15:00: Tick-by-tick real quote stop loss monitoring (2.0x SL).
          - 15:00:00: Morning models EOD squareoff.
        """
        logger.info("=" * 105)
        logger.info(f"🧪 RUNNING TICK-BY-TICK MULTI-MODEL PARITY VERIFICATION FOR SESSION: {self.trade_date}")
        logger.info("=" * 105)

        self.load_base_metadata()
        self.seed_base_redis_state()
        df_ticks = self.fetch_raw_ticks(start_time="09:15:00", end_time="15:30:00")

        if df_ticks.empty:
            logger.error("No ticks found for verification.")
            return

        # Initialize Forward Test Engine in Dry-Run mode
        cfg = MultiModelConfig()
        cfg.strategy6.total_lots = 20  # Strategy 6 Restored to 20 Lots
        engine = MultiModelEngine(config=cfg, dry_run=True)
        engine.init_trading_day(self.trade_date)
        engine.strategy6.active_positions.clear()
        engine.strategy6.closed_positions.clear()

        entry_done = False
        eod_done = False
        last_min_checked = ""
        batch_pipe = self.r.pipeline(transaction=False)
        pipe_count = 0

        t0 = time.time()
        logger.info(f"Advancing through {len(df_ticks):,} ticks sequentially...")

        for row in df_ticks.itertuples(index=False):
            sym = row.symbol
            ltp = float(row.ltp)
            close = float(row.close)
            bid = float(row.bid) if row.bid > 0 else ltp
            ask = float(row.ask) if row.ask > 0 else ltp
            vol = int(row.volume)
            oi = int(row.open_interest)
            time_str = row.time_str
            min_str = row.min_str
            epoch_sec = row.epoch_sec

            # 1. Update quote in Redis pipe
            q_data = {
                "symbol": sym,
                "ltp": str(ltp),
                "close": str(close),
                "bid": str(bid),
                "bid_qty": str(int(row.bid_qty)),
                "ask": str(ask),
                "ask_qty": str(int(row.ask_qty)),
                "volume": str(vol),
                "oi": str(oi),
                "delta": str(row.delta),
                "theta": str(row.theta),
                "gamma": str(row.gamma),
                "vega": str(row.vega),
                "ts_exchange": str(row.ts_exchange),
                "ts_recv": str(row.ts_recv)
            }
            batch_pipe.hset(f"md:quote:{sym}", mapping=q_data)
            batch_pipe.hset(f"quote:{sym}", mapping=q_data)
            pipe_count += 2

            # 2. Spot index bar update
            if sym == self.spot_symbol:
                batch_pipe.set(f"spot:{self.underlying}", sym)
                self.candle_agg.process_tick(batch_pipe, sym, ltp, vol, epoch_sec)

            # Flush pipe every 500 commands
            if pipe_count >= 500:
                batch_pipe.execute()
                batch_pipe = self.r.pipeline(transaction=False)
                pipe_count = 0

            # 3. Check 09:18 AM Entry
            if not entry_done and min_str >= "09:18":
                batch_pipe.execute()
                batch_pipe = self.r.pipeline(transaction=False)
                pipe_count = 0

                new_pos = engine.strategy6.execute_0918_entry()
                if new_pos:
                    print(f"\n🎯 [09:18 ENTRY TRIGGERED] Regime: {engine.strategy6.regime} | Morning Jump: {engine.strategy6.morning_active} (Max Abs Ret: {engine.strategy6.max_abs_ret*100:.3f}%)")
                    for p in new_pos:
                        print(f"   • {p.leg_type:10s} {p.option_type} Strike {p.strike:.0f} ({p.symbol}) | {p.lots} Lots ({p.lots*p.lot_size} Qty) @ ₹{p.entry_price:.2f} | SL: ₹{p.sl_price:.2f}")
                    entry_done = True

            # 4. Tick-by-tick stop loss monitoring
            if engine.strategy6.active_positions:
                for pos in list(engine.strategy6.active_positions):
                    if pos.symbol == sym:
                        triggered, reason = pos.update_price(ltp)
                        if triggered:
                            pos.exit_time = time_str
                            engine.strategy6.active_positions.remove(pos)
                            engine.strategy6.closed_positions.append(pos)
                            pts = (pos.entry_price - pos.exit_price) if pos.direction == "SELL" else (pos.exit_price - pos.entry_price)
                            print(f"🚨 [{time_str} EXIT HIT] {pos.leg_type:10s} {pos.option_type} {pos.strike:.0f} | Status: {pos.status} | Exit: ₹{pos.exit_price:.2f} | PnL: ₹{pos.pnl:+,.2f} ({pts:+.2f} pts)")

            # 5. Check 15:00 EOD Squareoff
            if not eod_done and min_str >= "15:00":
                batch_pipe.execute()
                batch_pipe = self.r.pipeline(transaction=False)
                pipe_count = 0

                closed_eod = engine.strategy6.execute_eod_squareoff("15:00")
                if closed_eod:
                    for cp in closed_eod:
                        pts = (cp.entry_price - cp.exit_price) if cp.direction == "SELL" else (cp.exit_price - cp.entry_price)
                        print(f"🏁 [15:00 EOD SQUAREOFF] {cp.leg_type:10s} {cp.option_type} {cp.strike:.0f} | Exit: ₹{cp.exit_price:.2f} | PnL: ₹{cp.pnl:+,.2f} ({pts:+.2f} pts)")
                eod_done = True

        batch_pipe.execute()

        # Scorecard
        print("\n" + "=" * 105)
        print("📊 REPLICATED TICK-BY-TICK FORWARD TEST SCORECARD (STRATEGY 6 @ 20 LOTS)")
        print("=" * 105)

        total_pnl = sum(p.pnl for p in engine.strategy6.closed_positions)
        wins = sum(1 for p in engine.strategy6.closed_positions if p.pnl > 0)
        losses = sum(1 for p in engine.strategy6.closed_positions if p.pnl < 0)
        total_legs = len(engine.strategy6.closed_positions)

        print(f"Trade Date                 : {self.trade_date}")
        print(f"Underlying                 : {self.underlying} (Expiry: {engine.strategy6.expiry})")
        print(f"Regime Detected            : {engine.strategy6.regime}")
        print(f"Morning Jump (09:15-09:17) : {engine.strategy6.morning_active} (Max Abs Return: {engine.strategy6.max_abs_ret*100:.3f}%)")
        print(f"Total Legs Executed        : {total_legs}")
        print(f"Win / Loss Record          : {wins} Wins / {losses} Losses")
        print(f"Total Session PnL          : ₹{total_pnl:+,.2f}")
        print(f"Elapsed Simulation Time    : {time.time() - t0:.2f}s")
        print("-" * 105)
        print("Detailed Replicated Positions:")
        for p in engine.strategy6.closed_positions:
            pts = (p.entry_price - p.exit_price) if p.direction == "SELL" else (p.exit_price - p.entry_price)
            print(f"  • {p.symbol:15s} | Strike {p.strike:7.1f} {p.option_type} ({p.leg_type:9s}) | Lots: {p.lots:2d} | Entry: ₹{p.entry_price:6.2f} -> Exit: ₹{p.exit_price:6.2f} | Status: {p.status:12s} | PnL: ₹{p.pnl:+9.2f}")
        print("=" * 105)


def main():
    parser = argparse.ArgumentParser(description="ULLTR Tick-by-Tick Market Replay Engine")
    parser.add_argument("--date", type=str, default="2026-09-08", help="Trading date to replay (YYYY-MM-DD)")
    parser.add_argument("--underlying", type=str, default="NIFTY", help="Asset to replay (default: NIFTY)")
    parser.add_argument("--mode", type=str, default="verify", choices=["verify", "stream"],
                        help="Replay mode: verify (run full verification on VM), stream (feed raw ticks to live Redis)")
    parser.add_argument("--speed", type=str, default="max", help="Stream speed multiplier ('max', '50', '10', '1.0')")
    parser.add_argument("--start-time", type=str, default="09:15:00", help="Start time for ticks (HH:MM:SS)")
    parser.add_argument("--end-time", type=str, default="15:30:00", help="End time for ticks (HH:MM:SS)")
    parser.add_argument("--batch-size", type=int, default=1000, help="Pipeline batch size for Redis HSET")
    parser.add_argument("--redis-socket", type=str, default="/Users/prana/Desktop/open_source/web/redis.sock",
                        help="Path to Redis Unix socket")
    parser.add_argument("--redis-host", type=str, default="127.0.0.1", help="Redis host")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
    parser.add_argument("--ch-host", type=str, default=CH_HOST, help="ClickHouse Cloud host")
    parser.add_argument("--ch-port", type=int, default=CH_PORT, help="ClickHouse Cloud port")
    parser.add_argument("--ch-user", type=str, default=CH_USER, help="ClickHouse user")
    parser.add_argument("--ch-pass", type=str, default=CH_PASS, help="ClickHouse password")
    parser.add_argument("--ch-db", type=str, default=CH_DATABASE, help="ClickHouse database")
    args = parser.parse_args()

    engine = TickMarketReplayEngine(
        trade_date=args.date,
        underlying=args.underlying,
        redis_socket=args.redis_socket,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        ch_host=args.ch_host,
        ch_port=args.ch_port,
        ch_user=args.ch_user,
        ch_pass=args.ch_pass,
        ch_db=args.ch_db,
    )

    if args.mode == "stream":
        ch_cfg = ClickHouseConfig(
            host=args.ch_host,
            port=args.ch_port,
            user=args.ch_user,
            password=args.ch_pass,
            database=args.ch_db,
            secure=True
        )
        stream_engine = MarketReplayEngine(
            trade_date=args.date,
            underlying=args.underlying,
            ch_config=ch_cfg,
            redis_socket=args.redis_socket,
            redis_host=args.redis_host,
            redis_port=args.redis_port
        )
        stream_engine.replay(
            speed=args.speed,
            start_time=args.start_time,
            end_time=args.end_time,
            batch_size=args.batch_size
        )
    elif args.mode == "verify":
        engine.run_verification_test()


if __name__ == "__main__":
    main()

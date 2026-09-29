#!/usr/bin/env python3
"""
ULLTR Market Replay Engine.
===========================
High-performance, pure in-memory market replay engine designed to stream tick-by-tick
market data (Spot Index, Option Chains, Greeks, and L2 Depth) directly from ClickHouse Cloud
into a local Redis database (Unix Domain Socket / TCP).

Replicates the exact Upstox WebSocket receiver + C++ Collector pipeline so that downstream
strategies, forward testers, and web APIs can execute off-market hours or on historical sessions
with zero code modifications.
"""

from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
import json
try:
    import orjson
    def fast_dumps(obj):
        return orjson.dumps(obj).decode("utf-8")
except ImportError:
    def fast_dumps(obj):
        return json.dumps(obj)
import logging
import os
import sys
import time
from typing import Dict, Generator, List, Optional, Set, Tuple, Any

import clickhouse_connect
import pandas as pd
import redis

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("MarketReplayEngine")


@dataclass
class ClickHouseConfig:
    """ClickHouse Cloud Connection Credentials and Settings."""
    host: str = "ra5fptcofl.ap-south-1.aws.clickhouse.cloud"
    user: str = "default"
    password: str = "BhhYrZvtF3lA~"
    port: int = 8443
    database: str = "default"
    secure: bool = True


@dataclass
class CandleBar:
    """Represents an active or closed OHLCV candle bar."""
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: int
    status: str = "live"

    def to_redis_mapping(self) -> Dict[str, str]:
        return {
            "open": f"{self.open:.2f}",
            "high": f"{self.high:.2f}",
            "low": f"{self.low:.2f}",
            "close": f"{self.close:.2f}",
            "volume": str(self.volume),
            "status": self.status
        }


class MultiTimeframeCandleAggregator:
    """
    On-the-fly multi-timeframe candle aggregator emulating C++ CandleManager.
    Builds 1m, 3m, 5m, 15m, and 30m OHLCV bars from incoming raw ticks
    and pipelines updates to Redis hashes and ZSET indexes.
    """

    TIMEFRAMES = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
    }

    def __init__(self, symbols_to_aggregate: Optional[Set[str]] = None):
        # symbols_to_aggregate: None means aggregate for all passed symbols
        self.symbols_to_aggregate = symbols_to_aggregate
        # key: (symbol, timeframe) -> CandleBar
        self.active_bars: Dict[Tuple[str, str], CandleBar] = {}
        # key: symbol -> last cumulative volume seen today
        self.last_seen_volumes: Dict[str, int] = {}

    def process_tick(
        self,
        pipe: redis.client.Pipeline,
        symbol: str,
        price: float,
        cumulative_volume: int,
        epoch_sec: int,
        publish_reco: bool = True
    ) -> List[Tuple[str, str, int]]:
        """
        Updates running candles for all configured timeframes.
        Returns a list of closed bars: [(symbol, timeframe, closed_ts_epoch), ...].
        """
        if price <= 0:
            return []

        if self.symbols_to_aggregate is not None and symbol not in self.symbols_to_aggregate:
            return []

        last_vol = self.last_seen_volumes.get(symbol, 0)
        inc_vol = 0 if last_vol == 0 else max(0, cumulative_volume - last_vol)
        self.last_seen_volumes[symbol] = cumulative_volume

        closed_candles: List[Tuple[str, str, int]] = []

        for tf, duration in self.TIMEFRAMES.items():
            candle_start_ts = (epoch_sec // duration) * duration
            bar_key = (symbol, tf)
            active_bar = self.active_bars.get(bar_key)

            if active_bar is None:
                # First tick in interval
                self.active_bars[bar_key] = CandleBar(
                    timestamp=candle_start_ts,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=inc_vol,
                    status="live"
                )
                k = f"md:candle:{symbol}:{tf}:{candle_start_ts}"
                pipe.hset(k, mapping=self.active_bars[bar_key].to_redis_mapping())
                pipe.zadd(f"md:candles:{symbol}:{tf}", {str(candle_start_ts): candle_start_ts})

            elif candle_start_ts > active_bar.timestamp:
                # Interval closed!
                prev_bar = active_bar
                prev_bar.status = "historical"
                prev_k = f"md:candle:{symbol}:{tf}:{prev_bar.timestamp}"
                pipe.hset(prev_k, mapping=prev_bar.to_redis_mapping())
                pipe.zadd(f"md:candles:{symbol}:{tf}", {str(prev_bar.timestamp): prev_bar.timestamp})

                closed_candles.append((symbol, tf, prev_bar.timestamp))

                if tf == "1m" and publish_reco:
                    pipe.publish("md:reco:trigger", f"{symbol}:{prev_bar.timestamp}")

                # Start fresh interval bar
                self.active_bars[bar_key] = CandleBar(
                    timestamp=candle_start_ts,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=inc_vol,
                    status="live"
                )
                new_k = f"md:candle:{symbol}:{tf}:{candle_start_ts}"
                pipe.hset(new_k, mapping=self.active_bars[bar_key].to_redis_mapping())
                pipe.zadd(f"md:candles:{symbol}:{tf}", {str(candle_start_ts): candle_start_ts})

            else:
                # Update existing bar
                active_bar.high = max(active_bar.high, price)
                active_bar.low = min(active_bar.low, price)
                active_bar.close = price
                active_bar.volume += inc_vol

                k = f"md:candle:{symbol}:{tf}:{candle_start_ts}"
                pipe.hset(k, mapping=active_bar.to_redis_mapping())

        return closed_candles


class MarketReplayEngine:
    """
    Core Replay Engine. Streams tick-by-tick ClickHouse data into Redis with
    configurable speed pacing, option chain discovery, and candle aggregation.
    """

    def __init__(
        self,
        trade_date: str = "2026-09-08",
        underlying: str = "NIFTY",
        ch_config: Optional[ClickHouseConfig] = None,
        redis_socket: str = "/Users/prana/Desktop/open_source/web/redis.sock",
        redis_host: str = "127.0.0.1",
        redis_port: int = 6379,
        strikes_range: Optional[int] = None,
        publish_stream: bool = True
    ):
        self.trade_date = trade_date
        self.underlying = underlying.upper()
        self.spot_symbol = "NSE_INDEX|Nifty 50" if self.underlying == "NIFTY" else "BSE_INDEX|SENSEX"
        self.ch_config = ch_config or ClickHouseConfig()
        self.strikes_range = strikes_range
        self.publish_stream = publish_stream

        # 1. Connect to Redis (prefer Unix socket)
        if os.path.exists(redis_socket):
            self.r = redis.Redis(unix_socket_path=redis_socket, decode_responses=True)
            self.redis_conn_desc = f"Unix Socket ({redis_socket})"
        else:
            self.r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
            self.redis_conn_desc = f"TCP ({redis_host}:{redis_port})"

        # 2. Connect to ClickHouse Cloud
        self.ch_client = clickhouse_connect.get_client(
            host=self.ch_config.host,
            port=self.ch_config.port,
            user=self.ch_config.user,
            password=self.ch_config.password,
            database=self.ch_config.database,
            secure=self.ch_config.secure
        )

        self.front_expiry: str = ""
        self.chain_meta: Dict[str, str] = {}
        self.selected_symbols: Set[str] = set()
        self.df_daily: pd.DataFrame = pd.DataFrame()
        self.aggregator = MultiTimeframeCandleAggregator()

    def load_metadata(self) -> None:
        """Discovers front expiry, strikes, and loads historical daily candles."""
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
        logger.info(f"  • Front Weekly Expiry Resolved: {self.front_expiry}")

        # 1b. Resolve front futures symbol
        q_fut = f"""
        SELECT symbol
        FROM market_ticks
        WHERE toDate(timestamp) = '{self.trade_date}'
          AND underlying = '{self.underlying}'
          AND option_type = 'FUT'
        GROUP BY symbol
        ORDER BY min(expiry) ASC
        LIMIT 1
        """
        fut_rows = self.ch_client.query(q_fut).result_rows
        self.front_futures_symbol = str(fut_rows[0][0]) if fut_rows else ""
        if self.front_futures_symbol:
            logger.info(f"  • Front Futures Symbol Resolved: {self.front_futures_symbol}")

        # 2. Option Chain Discovery
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
        all_chain_symbols = set()
        for _, row in df_chain.iterrows():
            f_key = f"{row['strike']}:{row['option_type']}"
            sym = str(row["symbol"])
            self.chain_meta[f_key] = sym
            all_chain_symbols.add(sym)

        # Filter strikes if range is specified
        if self.strikes_range is not None and self.strikes_range > 0:
            # Query approximate opening spot price to center strikes
            q_spot_open = f"""
            SELECT ltp FROM market_ticks
            WHERE symbol = '{self.spot_symbol}'
              AND toDate(timestamp) = '{self.trade_date}'
            ORDER BY timestamp ASC
            LIMIT 1
            """
            r_sp = self.ch_client.query(q_spot_open).result_rows
            open_spot = float(r_sp[0][0]) if r_sp else 24000.0
            increment = 100 if self.underlying == "SENSEX" else 50
            atm_strike = int(round(open_spot / increment) * increment)
            min_strike = atm_strike - (self.strikes_range * increment)
            max_strike = atm_strike + (self.strikes_range * increment)

            filtered_chain = {}
            for k, sym in self.chain_meta.items():
                stk = int(k.split(":")[0])
                if min_strike <= stk <= max_strike:
                    filtered_chain[k] = sym
            self.chain_meta = filtered_chain
            self.selected_symbols = set(filtered_chain.values())
            logger.info(f"  • Filtered Chain (ATM ±{self.strikes_range} strikes @ {atm_strike}): {len(self.chain_meta)} contracts")
        else:
            self.selected_symbols = all_chain_symbols

        self.selected_symbols.add(self.spot_symbol)
        if self.front_futures_symbol:
            self.selected_symbols.add(self.front_futures_symbol)
        self.aggregator.symbols_to_aggregate = self.selected_symbols

        # 3. Load daily historical candles (last 15 sessions for baseline models)
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
        logger.info(f"  • Daily Bars Loaded           : {len(self.df_daily)} sessions ({time.time()-t0:.2f}s)")

    def seed_initial_redis_state(self) -> None:
        """Seeds initial spot mapping, option chain map, and daily candles into Redis."""
        logger.info(f"Seeding base Redis state via {self.redis_conn_desc}...")
        pipe = self.r.pipeline(transaction=False)

        # 1. Spot Pointer & Futures
        pipe.set(f"spot:{self.underlying}", self.spot_symbol)
        pipe.set("spot:VIX", "NSE_INDEX|India VIX")
        if self.front_futures_symbol:
            pipe.set(f"fut:{self.underlying}:front", self.front_futures_symbol)

        # Clear stale candles and ZSETs for clean replay
        for k in self.r.keys(f"md:candle:{self.spot_symbol}:*"):
            self.r.delete(k)
        for k in self.r.keys(f"md:candles:{self.spot_symbol}:*"):
            self.r.delete(k)
        if self.front_futures_symbol:
            for k in self.r.keys(f"md:candle:{self.front_futures_symbol}:*"):
                self.r.delete(k)
            for k in self.r.keys(f"md:candles:{self.front_futures_symbol}:*"):
                self.r.delete(k)

        # 2. Daily Candles
        for _, row in self.df_daily.iterrows():
            d_str = str(row["dt"])
            k = f"md:candle:{self.spot_symbol}:1d:{d_str}"
            mapping = {
                "open": str(row["open"]),
                "high": str(row["high"]),
                "low": str(row["low"]),
                "close": str(row["close"]),
                "volume": str(row["volume"]),
                "date": d_str,
                "status": "historical"
            }
            pipe.hset(k, mapping=mapping)

        # 3. Option Chain Hash
        for old_key in self.r.keys(f"chain:{self.underlying}:*"):
            self.r.delete(old_key)
        chain_key = f"chain:{self.underlying}:{self.front_expiry}"
        if self.chain_meta:
            pipe.hset(chain_key, mapping=self.chain_meta)

        pipe.execute()
        logger.info(f"✅ Base Redis state seeded: {len(self.chain_meta)} chain contracts mapped to {chain_key}.")

    def stream_ticks_chunked(
        self,
        start_time: str = "09:15:00",
        end_time: str = "15:30:00",
        chunk_minutes: int = 30
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Generator streaming ticks in time-sliced chunks from ClickHouse Cloud
        to minimize peak memory usage and allow streaming to start immediately.
        """
        start_dt = datetime.strptime(f"{self.trade_date} {start_time}", "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(f"{self.trade_date} {end_time}", "%Y-%m-%d %H:%M:%S")

        current_slice_start = start_dt
        while current_slice_start < end_dt:
            current_slice_end = min(current_slice_start + timedelta(minutes=chunk_minutes), end_dt)
            t_s_str = current_slice_start.strftime("%Y-%m-%d %H:%M:%S")
            t_e_str = current_slice_end.strftime("%Y-%m-%d %H:%M:%S")

            q = f"""
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
                rho,
                open_interest,
                volume,
                ts_exchange,
                ts_recv
            FROM market_ticks
            WHERE toDate(timestamp) = '{self.trade_date}'
              AND timestamp >= '{t_s_str}'
              AND timestamp < '{t_e_str}'
              AND (symbol = '{self.spot_symbol}' OR symbol = '{self.front_futures_symbol}' OR (underlying = '{self.underlying}' AND toString(expiry) = '{self.front_expiry}'))
            ORDER BY timestamp ASC
            """
            t0 = time.time()
            df_chunk = self.ch_client.query_df(q)
            q_time = time.time() - t0

            if not df_chunk.empty:
                if self.selected_symbols:
                    df_chunk = df_chunk[df_chunk["symbol"].isin(self.selected_symbols)]

                if not df_chunk.empty:
                    df_chunk["epoch_ms"] = (pd.to_datetime(df_chunk["timestamp"]).astype("int64") // 10**6).astype("int64")
                    df_chunk["epoch_sec"] = df_chunk["epoch_ms"] // 1000
                    df_chunk["time_str"] = pd.to_datetime(df_chunk["timestamp"]).dt.strftime("%H:%M:%S")
                    logger.info(f"📥 Loaded slice [{t_s_str[-8:]} - {t_e_str[-8:]}] : {len(df_chunk):,} ticks in {q_time:.2f}s")
                    yield df_chunk

            current_slice_start = current_slice_end

    def replay(
        self,
        speed: str = "max",
        start_time: str = "09:15:00",
        end_time: str = "15:30:00",
        batch_size: int = 1000
    ) -> None:
        """
        Executes real-time market replay with drift-free pacing into Redis.
        """
        self.load_metadata()
        self.seed_initial_redis_state()

        is_burst = (str(speed).lower() in ["max", "burst", "0"])
        speed_factor = 0.0 if is_burst else float(speed)

        logger.info("=" * 90)
        logger.info(f"🚀 LAUNCHING ULLTR MARKET REPLAY ENGINE")
        logger.info(f"  • Date: {self.trade_date} | Underlying: {self.underlying} | Expiry: {self.front_expiry}")
        logger.info(f"  • Speed: {'MAX BURST (Unthrottled)' if is_burst else f'{speed_factor}x Real-Time Pacing'}")
        logger.info(f"  • Target: {self.redis_conn_desc}")
        logger.info("=" * 90)

        t_sim_start_ms: Optional[int] = None
        t_wall_start: Optional[float] = None
        ticks_sent = 0
        last_progress_time = time.perf_counter()
        last_spot_px = 0.0

        pipe = self.r.pipeline(transaction=False)

        for df_chunk in self.stream_ticks_chunked(start_time=start_time, end_time=end_time):
            for row in df_chunk.itertuples(index=False):
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
                rho = float(row.rho)
                ts_exch = int(row.ts_exchange)
                ts_recv = int(row.ts_recv)
                epoch_ms = int(row.epoch_ms)
                epoch_sec = int(row.epoch_sec)

                # Initialize pacing anchors on first tick
                if t_sim_start_ms is None:
                    t_sim_start_ms = epoch_ms
                    t_wall_start = time.perf_counter()

                # 1. Update In-Memory Redis Quote Hashes
                quote_data = {
                    "symbol": sym,
                    "source": "replay",
                    "status": "online",
                    "ltp": f"{ltp:.2f}",
                    "close": f"{close:.2f}",
                    "bid": f"{bid:.2f}",
                    "bid_qty": str(int(row.bid_qty)),
                    "ask": f"{ask:.2f}",
                    "ask_qty": str(int(row.ask_qty)),
                    "volume": str(vol),
                    "oi": str(oi),
                    "delta": f"{delta:.4f}",
                    "theta": f"{theta:.4f}",
                    "gamma": f"{gamma:.6f}",
                    "vega": f"{vega:.4f}",
                    "rho": f"{rho:.4f}",
                    "ts_exchange": str(ts_exch),
                    "ts_recv": str(ts_recv)
                }

                pipe.hset(f"md:quote:{sym}", mapping=quote_data)
                pipe.hset(f"quote:{sym}", mapping=quote_data)

                # 2. Publish to Pub/Sub md:stream:all
                if self.publish_stream:
                    pipe.publish("md:stream:all", fast_dumps(quote_data))

                # 3. Spot index ticker & multi-timeframe candles
                if sym == self.spot_symbol:
                    last_spot_px = ltp
                    pipe.set(f"spot:{self.underlying}", sym)

                self.aggregator.process_tick(pipe, sym, ltp, vol, epoch_sec, publish_reco=True)

                ticks_sent += 1

                # 4. Drift-Free Wall Clock Pacing (if not burst)
                if not is_burst and speed_factor > 0:
                    sim_elapsed_ms = epoch_ms - t_sim_start_ms
                    desired_wall_sec = (sim_elapsed_ms / 1000.0) / speed_factor
                    actual_wall_sec = time.perf_counter() - t_wall_start
                    delay_sec = desired_wall_sec - actual_wall_sec

                    if delay_sec > 0.002:  # Ahead by > 2 milliseconds
                        pipe.execute()
                        pipe = self.r.pipeline(transaction=False)
                        time.sleep(delay_sec)

                # Flush pipeline at batch boundaries
                if ticks_sent % batch_size == 0:
                    pipe.execute()
                    pipe = self.r.pipeline(transaction=False)

                # Display progress
                now = time.perf_counter()
                if now - last_progress_time >= 1.0:
                    elapsed = now - (t_wall_start or now)
                    rate = ticks_sent / elapsed if elapsed > 0 else 0.0
                    print(
                        f"\r⚡ {row.time_str} IST | Spot: ₹{last_spot_px:,.2f} | "
                        f"Ticks Replayed: {ticks_sent:,} | Rate: {rate:,.0f} ticks/s",
                        end="",
                        flush=True
                    )
                    last_progress_time = now

        if self.publish_stream:
            pipe.publish("md:stream:all", fast_dumps({"status": "REPLAY_COMPLETE"}))
        pipe.execute()
        total_time = time.perf_counter() - (t_wall_start or time.perf_counter())
        avg_rate = ticks_sent / total_time if total_time > 0 else 0
        print(f"\n\n🏁 Market Replay Completed: {ticks_sent:,} ticks processed in {total_time:.2f}s ({avg_rate:,.0f} ticks/s).")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="ULLTR Tick-by-Tick Market Replay Engine")
    parser.add_argument("--date", type=str, default="2026-09-08", help="Trading date (YYYY-MM-DD)")
    parser.add_argument("--underlying", type=str, default="NIFTY", help="Asset: NIFTY or SENSEX")
    parser.add_argument("--speed", type=str, default="max", help="Speed: 'max' or float (1.0, 5.0, 10.0)")
    parser.add_argument("--start-time", type=str, default="09:15:00", help="Start time (HH:MM:SS)")
    parser.add_argument("--end-time", type=str, default="15:30:00", help="End time (HH:MM:SS)")
    parser.add_argument("--batch-size", type=int, default=1000, help="Redis pipeline batch size")
    parser.add_argument("--strikes-range", type=int, default=None, help="ATM +/- count strikes to replay (None=all)")
    parser.add_argument("--no-pubsub", action="store_true", help="Disable tick Pub/Sub publishing")
    parser.add_argument("--redis-socket", type=str, default="/Users/prana/Desktop/open_source/web/redis.sock", help="Unix socket")
    parser.add_argument("--redis-host", type=str, default="127.0.0.1", help="TCP Host")
    parser.add_argument("--redis-port", type=int, default=6379, help="TCP Port")

    args = parser.parse_args()

    engine = MarketReplayEngine(
        trade_date=args.date,
        underlying=args.underlying,
        redis_socket=args.redis_socket,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        strikes_range=args.strikes_range,
        publish_stream=(not args.no_pubsub)
    )

    engine.replay(
        speed=args.speed,
        start_time=args.start_time,
        end_time=args.end_time,
        batch_size=args.batch_size
    )


if __name__ == "__main__":
    main()

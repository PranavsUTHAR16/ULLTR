#!/usr/bin/env python3
"""
ULLTR Historical Redis Feeder.
================================
Drop-in replacement for the Upstox WebSocket receiver during holidays and off-hours.
Pulls complete historical tick & quote data from ClickHouse Cloud and streams it into
local Redis on the server, replicating the exact data structures and PubSub events
that the live Upstox WebSocket feed delivers.

Allows the FULL Forward-Testing System (C++ StrategyEngine & Python Daemons) to run
and execute trades against historical data without any code modifications.

Keys Updated:
  • `spot:NIFTY` and `spot:SENSEX`
  • `chain:NIFTY:<expiry>`
  • `md:quote:<symbol>` and `quote:<symbol>`
  • `md:candle:<symbol>:1m:<ts>`
  • `md:candles:<symbol>:1m`
  • `cas:live:<symbol>` (during 15:20-15:30 CAS)
  • PubSub: `md:stream:all` and `md:reco:trigger`
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

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("HistoricalFeeder")

# ClickHouse Cloud default credentials
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "ra5fptcofl.ap-south-1.aws.clickhouse.cloud")
CH_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8443"))
CH_USER = os.environ.get("CLICKHOUSE_USER", "default")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "BhhYrZvtF3lA~")
CH_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")


class HistoricalRedisFeeder:
    """Streams historical market data into Redis, emulating Upstox WebSocket feed."""

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
        self.df_spot_1m: pd.DataFrame = pd.DataFrame()
        self.df_opts_1m: pd.DataFrame = pd.DataFrame()

    def prepare_session_data(self) -> None:
        """Loads all required market data for the session from ClickHouse Cloud."""
        logger.info(f"Connecting to ClickHouse Cloud for {self.underlying} on {self.trade_date}...")
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
        logger.info(f"  • Front Expiry : {self.front_expiry}")

        # 2. Daily historical index bars (last 15 sessions for RV5 & slope)
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
        logger.info(f"  • Daily Bars   : {len(self.df_daily)} sessions loaded.")

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
        logger.info(f"  • Option Chain : {len(self.chain_meta)} contracts mapped in chain:{self.underlying}:{self.front_expiry}.")

        # 4. Intraday 1-minute spot bars
        q_spot_1m = f"""
        SELECT 
            toStartOfInterval(timestamp, INTERVAL 1 MINUTE) AS bar_1m,
            argMin(ltp, timestamp) AS open,
            max(ltp) AS high,
            min(ltp) AS low,
            argMax(ltp, timestamp) AS close,
            sum(volume) AS volume
        FROM market_ticks
        WHERE symbol = '{self.spot_symbol}'
          AND toDate(timestamp) = '{self.trade_date}'
          AND timestamp >= '{self.trade_date} 09:15:00'
          AND timestamp <= '{self.trade_date} 15:30:00'
        GROUP BY bar_1m
        ORDER BY bar_1m ASC
        """
        self.df_spot_1m = self.ch_client.query_df(q_spot_1m)
        self.df_spot_1m["time_str"] = pd.to_datetime(self.df_spot_1m["bar_1m"]).dt.strftime("%H:%M")
        self.df_spot_1m["epoch_sec"] = (pd.to_datetime(self.df_spot_1m["bar_1m"]).astype("int64") // 10**9).astype("int64")
        logger.info(f"  • Spot 1M Bars : {len(self.df_spot_1m)} bars (09:15 to 15:30).")

        # 5. Intraday 1-minute aggregated option quotes with Greeks & Depth
        q_opts = f"""
        SELECT 
            toStartOfInterval(timestamp, INTERVAL 1 MINUTE) AS bar_1m,
            symbol,
            toInt32(strike) AS strike,
            option_type,
            toFloat64(argMax(ltp, timestamp)) AS ltp,
            toFloat64(argMax(bid, timestamp)) AS bid,
            toUInt32(argMax(bid_qty, timestamp)) AS bid_qty,
            toFloat64(argMax(ask, timestamp)) AS ask,
            toUInt32(argMax(ask_qty, timestamp)) AS ask_qty,
            toFloat64(argMax(delta, timestamp)) AS delta,
            toFloat64(argMax(theta, timestamp)) AS theta,
            toFloat64(argMax(gamma, timestamp)) AS gamma,
            toFloat64(argMax(vega, timestamp)) AS vega,
            toFloat64(argMax(iv, timestamp)) AS iv,
            toUInt64(argMax(open_interest, timestamp)) AS oi,
            toUInt64(sum(volume)) AS volume,
            toFloat64(argMax(close, timestamp)) AS close,
            toInt64(argMax(ts_exchange, timestamp)) AS ts_exchange,
            toInt64(argMax(ts_recv, timestamp)) AS ts_recv
        FROM market_ticks
        WHERE underlying = '{self.underlying}'
          AND toString(expiry) = '{self.front_expiry}'
          AND toDate(timestamp) = '{self.trade_date}'
          AND timestamp >= '{self.trade_date} 09:15:00'
          AND timestamp <= '{self.trade_date} 15:30:00'
        GROUP BY bar_1m, symbol, strike, option_type
        ORDER BY bar_1m ASC
        """
        self.df_opts_1m = self.ch_client.query_df(q_opts)
        self.df_opts_1m["time_str"] = pd.to_datetime(self.df_opts_1m["bar_1m"]).dt.strftime("%H:%M")
        logger.info(f"  • Option 1M    : {len(self.df_opts_1m):,} snapshots across {len(self.df_spot_1m)} intervals in {time.time()-t0:.2f}s.")

    def seed_initial_metadata(self) -> None:
        """Seeds base symbol pointers, daily candles, and option chain metadata into Redis."""
        logger.info(f"Seeding base Redis state via {self.redis_conn_desc}...")
        pipe = self.r.pipeline(transaction=False)

        # 1. Spot pointer
        pipe.set(f"spot:{self.underlying}", self.spot_symbol)

        # 2. Daily candles for RV5 & slope
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

        # 3. Active option chain metadata
        chain_key = f"chain:{self.underlying}:{self.front_expiry}"
        pipe.delete(chain_key)
        pipe.hset(chain_key, mapping=self.chain_meta)

        pipe.execute()
        logger.info(f"✅ Base Redis state seeded: {len(self.df_daily)} daily candles + {len(self.chain_meta)} chain contracts.")

    def inject_minute_frame(self, time_str: str) -> Dict[str, Any]:
        """
        Injects an entire 1-minute market frame into Redis:
        - Spot index quote (`md:quote:<spot>`)
        - Spot 1m candle (`md:candle:<spot>:1m:<ts>`, sorted set `md:candles:<spot>:1m`)
        - PubSub candle closed trigger (`md:reco:trigger <spot>:<ts>`)
        - All 50+ active option quotes (`md:quote:<token>` and `quote:<token>`) with Greeks & Depth
        - PubSub tick stream (`md:stream:all`)
        """
        pipe = self.r.pipeline(transaction=False)
        spot_close = 0.0

        # 1. Find and inject Spot bar
        spot_rows = self.df_spot_1m[self.df_spot_1m["time_str"] == time_str]
        if not spot_rows.empty:
            s_row = spot_rows.iloc[0]
            spot_close = float(s_row["close"])
            epoch_sec = int(s_row["epoch_sec"])

            # 1m candle HSET
            candle_key = f"md:candle:{self.spot_symbol}:1m:{epoch_sec}"
            candle_data = {
                "open": str(s_row["open"]),
                "high": str(s_row["high"]),
                "low": str(s_row["low"]),
                "close": str(spot_close),
                "volume": str(s_row["volume"]),
                "ts": str(epoch_sec),
                "status": "historical"
            }
            pipe.hset(candle_key, mapping=candle_data)
            pipe.zadd(f"md:candles:{self.spot_symbol}:1m", {str(epoch_sec): epoch_sec})

            # Spot quote HSET
            spot_quote = {
                "symbol": self.spot_symbol,
                "source": "upstox",
                "status": "online",
                "ltp": str(spot_close),
                "close": str(spot_close),
                "volume": str(s_row["volume"]),
                "ts_exchange": str(epoch_sec * 1000),
                "ts_recv": str(int(time.time() * 1000))
            }
            pipe.hset(f"md:quote:{self.spot_symbol}", mapping=spot_quote)
            pipe.publish("md:reco:trigger", f"{self.spot_symbol}:{epoch_sec}")

        # 2. Inject all option quotes for this minute
        opt_rows = self.df_opts_1m[self.df_opts_1m["time_str"] == time_str]
        for _, o_row in opt_rows.iterrows():
            sym = str(o_row["symbol"])
            ltp = float(o_row["ltp"])
            bid = float(o_row["bid"]) if o_row["bid"] > 0 else ltp
            ask = float(o_row["ask"]) if o_row["ask"] > 0 else ltp

            quote_data = {
                "symbol": sym,
                "source": "upstox",
                "status": "online",
                "ltp": str(ltp),
                "close": str(o_row["close"]),
                "bid": str(bid),
                "bid_qty": str(int(o_row["bid_qty"])),
                "ask": str(ask),
                "ask_qty": str(int(o_row["ask_qty"])),
                "volume": str(int(o_row["volume"])),
                "oi": str(int(o_row["oi"])),
                "delta": str(float(o_row["delta"])),
                "theta": str(float(o_row["theta"])),
                "gamma": str(float(o_row["gamma"])),
                "vega": str(float(o_row["vega"])),
                "iv": str(float(o_row["iv"])),
                "ts_exchange": str(int(o_row["ts_exchange"])),
                "ts_recv": str(int(time.time() * 1000))
            }
            pipe.hset(f"md:quote:{sym}", mapping=quote_data)
            pipe.hset(f"quote:{sym}", mapping=quote_data)

        # 3. Compute Maker Delta-Neutral Center S* from Greeks and set in Redis
        if not opt_rows.empty:
            oi_arr = opt_rows["oi"].values.astype(float)
            delta_arr = np.abs(opt_rows["delta"].values.astype(float))
            strikes_arr = opt_rows["strike"].values.astype(float)
            weights = oi_arr * delta_arr
            tot_w = np.sum(weights)
            if tot_w > 0:
                s_star_val = round((np.sum(strikes_arr * weights) / tot_w) / 25.0) * 25.0
                pipe.set("mend:s_star", str(s_star_val))

        pipe.execute()
        return {"time_str": time_str, "spot": spot_close, "options_count": len(opt_rows)}

    def run_feed(
        self,
        speed: str = "max",
        start_time: str = "09:15",
        end_time: str = "15:30",
        delay_sec: float = 0.05
    ) -> None:
        """
        Runs the continuous feed loop into Redis.
        - speed: 'max' (instant burst), or 'realtime' (60s sleep), or float multiplier (e.g. '10.0', '50.0').
        """
        self.prepare_session_data()
        self.seed_initial_metadata()

        minutes = sorted(self.df_spot_1m["time_str"].unique())
        filtered_mins = [m for m in minutes if start_time <= m <= end_time]

        logger.info("=" * 95)
        logger.info(f"🚀 STARTING HISTORICAL REDIS FEEDER: {self.trade_date} ({start_time} -> {end_time})")
        logger.info(f"  • Total Minute Intervals : {len(filtered_mins)}")
        logger.info(f"  • Pacing Mode            : {'MAX BURST' if speed.lower() in ['max', 'burst', '0'] else f'{speed}x'}")
        logger.info(f"  • Redis Target Socket    : {self.redis_conn_desc}")
        logger.info("=" * 95)

        t_start = time.time()
        for idx, m in enumerate(filtered_mins, 1):
            res = self.inject_minute_frame(m)
            pct = (idx / len(filtered_mins)) * 100.0
            print(
                f"\r⚡ [{pct:5.1f}%] [{idx:03d}/{len(filtered_mins):03d}] {m} IST | "
                f"Spot: ₹{res['spot']:,.2f} | Options Updated: {res['options_count']:2d}",
                end="",
                flush=True
            )

            # Pacing
            if speed.lower() in ["max", "burst", "0"]:
                if delay_sec > 0:
                    time.sleep(delay_sec)
            elif speed.lower() == "realtime":
                time.sleep(60.0)
            else:
                try:
                    mult = float(speed)
                    sleep_time = 60.0 / mult
                    time.sleep(sleep_time)
                except ValueError:
                    time.sleep(delay_sec)

        total_sec = time.time() - t_start
        print(f"\n\n🏁 Feeder complete! Streamed {len(filtered_mins)} market intervals into Redis in {total_sec:.2f}s.")


def main():
    parser = argparse.ArgumentParser(description="ULLTR Historical Redis Feeder")
    parser.add_argument("--date", type=str, default="2026-09-08", help="Trading date to feed (YYYY-MM-DD)")
    parser.add_argument("--underlying", type=str, default="NIFTY", help="Underlying asset (default: NIFTY)")
    parser.add_argument("--speed", type=str, default="max", help="Pacing speed ('max', 'realtime', '10', '50')")
    parser.add_argument("--start-time", type=str, default="09:15", help="Start time (HH:MM)")
    parser.add_argument("--end-time", type=str, default="15:30", help="End time (HH:MM)")
    parser.add_argument("--delay", type=float, default=0.01, help="Delay in seconds between intervals in max mode")
    parser.add_argument("--redis-socket", type=str, default="/Users/prana/Desktop/open_source/web/redis.sock",
                        help="Path to Redis Unix socket")
    args = parser.parse_args()

    feeder = HistoricalRedisFeeder(
        trade_date=args.date,
        underlying=args.underlying,
        redis_socket=args.redis_socket
    )
    feeder.run_feed(
        speed=args.speed,
        start_time=args.start_time,
        end_time=args.end_time,
        delay_sec=args.delay
    )


if __name__ == "__main__":
    main()

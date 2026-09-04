# clickhouse_streamer.py
import os
import sys
import time
import json
import logging
from datetime import datetime, date
from typing import Dict, List, Any, Optional
import redis
import clickhouse_connect

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ClickHouseStreamer")

# ClickHouse Configuration
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "libz0hxoze.ap-south-1.aws.clickhouse.cloud")
CH_USER = os.environ.get("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "BhhYrZvtF3lA~")
CH_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8443"))
CH_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")

# Redis Configuration
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

BATCH_SIZE = 5000
FLUSH_INTERVAL_SEC = 2.5

class ClickHouseTickStreamer:
    """
    Streams all real-time tick-by-tick market data (LTP, Depth Bid/Ask, Greeks, OI, Volume)
    from local Redis to ClickHouse Cloud in optimized micro-batches.
    """
    def __init__(self):
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.ch_client = None
        self.connect_clickhouse()
        
        self.symbol_meta: Dict[str, Dict[str, Any]] = {}
        self.last_seen_ts: Dict[str, int] = {}
        self.buffer: List[List[Any]] = []
        self.last_flush_time = time.time()
        self.total_inserted = 0
        self.last_meta_refresh = 0.0

        self.columns = [
            "timestamp", "symbol", "underlying", "expiry", "strike", "option_type",
            "ltp", "close", "bid", "bid_qty", "ask", "ask_qty", "volume", "open_interest",
            "iv", "delta", "theta", "gamma", "vega", "rho", "ts_exchange", "ts_recv",
            "bid2", "bid_qty2", "ask2", "ask_qty2",
            "bid3", "bid_qty3", "ask3", "ask_qty3",
            "bid4", "bid_qty4", "ask4", "ask_qty4",
            "bid5", "bid_qty5", "ask5", "ask_qty5",
            "tbq", "tsq"
        ]

    def connect_clickhouse(self):
        """Initializes connection to ClickHouse Cloud."""
        while True:
            try:
                logger.info(f"Connecting to ClickHouse Cloud at {CH_HOST}...")
                self.ch_client = clickhouse_connect.get_client(
                    host=CH_HOST,
                    user=CH_USER,
                    password=CH_PASSWORD,
                    port=CH_PORT,
                    database=CH_DATABASE,
                    secure=True
                )
                res = self.ch_client.query("SELECT 1").result_set[0][0]
                logger.info(f"✅ Successfully connected to ClickHouse Cloud! (Heartbeat: {res})")
                self.ensure_schema()
                break
            except Exception as e:
                logger.error(f"❌ ClickHouse connection failed: {e}. Retrying in 5 seconds...")
                time.sleep(5.0)

    def ensure_schema(self):
        """Ensures the market_ticks table exists with optimal column compression codecs and 5-level depth."""
        schema_sql = """
        CREATE TABLE IF NOT EXISTS market_ticks (
            timestamp DateTime64(3, 'Asia/Kolkata') CODEC(DoubleDelta, LZ4),
            symbol LowCardinality(String) CODEC(ZSTD(1)),
            underlying LowCardinality(String) CODEC(ZSTD(1)),
            expiry Date CODEC(DoubleDelta, LZ4),
            strike Float32 CODEC(Gorilla, ZSTD(1)),
            option_type LowCardinality(String) CODEC(ZSTD(1)),
            ltp Float32 CODEC(Gorilla, ZSTD(1)),
            close Float32 CODEC(Gorilla, ZSTD(1)),
            bid Float32 CODEC(Gorilla, ZSTD(1)),
            bid_qty UInt32 CODEC(T64, ZSTD(1)),
            ask Float32 CODEC(Gorilla, ZSTD(1)),
            ask_qty UInt32 CODEC(T64, ZSTD(1)),
            volume UInt64 CODEC(T64, ZSTD(1)),
            open_interest UInt64 CODEC(T64, ZSTD(1)),
            iv Float32 CODEC(Gorilla, ZSTD(1)),
            delta Float32 CODEC(Gorilla, ZSTD(1)),
            theta Float32 CODEC(Gorilla, ZSTD(1)),
            gamma Float32 CODEC(Gorilla, ZSTD(1)),
            vega Float32 CODEC(Gorilla, ZSTD(1)),
            rho Float32 CODEC(Gorilla, ZSTD(1)),
            ts_exchange Int64 CODEC(DoubleDelta, LZ4),
            ts_recv Int64 CODEC(DoubleDelta, LZ4),
            bid2 Float32 CODEC(Gorilla, ZSTD(1)),
            bid_qty2 UInt32 CODEC(T64, ZSTD(1)),
            ask2 Float32 CODEC(Gorilla, ZSTD(1)),
            ask_qty2 UInt32 CODEC(T64, ZSTD(1)),
            bid3 Float32 CODEC(Gorilla, ZSTD(1)),
            bid_qty3 UInt32 CODEC(T64, ZSTD(1)),
            ask3 Float32 CODEC(Gorilla, ZSTD(1)),
            ask_qty3 UInt32 CODEC(T64, ZSTD(1)),
            bid4 Float32 CODEC(Gorilla, ZSTD(1)),
            bid_qty4 UInt32 CODEC(T64, ZSTD(1)),
            ask4 Float32 CODEC(Gorilla, ZSTD(1)),
            ask_qty4 UInt32 CODEC(T64, ZSTD(1)),
            bid5 Float32 CODEC(Gorilla, ZSTD(1)),
            bid_qty5 UInt32 CODEC(T64, ZSTD(1)),
            ask5 Float32 CODEC(Gorilla, ZSTD(1)),
            ask_qty5 UInt32 CODEC(T64, ZSTD(1)),
            tbq UInt64 CODEC(T64, ZSTD(1)),
            tsq UInt64 CODEC(T64, ZSTD(1))
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (underlying, expiry, strike, option_type, symbol, timestamp)
        SETTINGS index_granularity = 8192;
        """
        self.ch_client.command(schema_sql)
        logger.info("✅ Verified ClickHouse schema for 'market_ticks'")

    def refresh_symbol_metadata(self):
        """Loads and maps metadata (Underlying, Expiry, Strike, OptionType) for all symbols."""
        meta = {}
        # 1. Spot Indices
        meta["NSE_INDEX|Nifty 50"] = {"underlying": "NIFTY", "expiry": "1970-01-01", "strike": 0.0, "option_type": "INDEX"}
        meta["BSE_INDEX|SENSEX"] = {"underlying": "SENSEX", "expiry": "1970-01-01", "strike": 0.0, "option_type": "INDEX"}
        meta["NSE_INDEX|Nifty Bank"] = {"underlying": "BANKNIFTY", "expiry": "1970-01-01", "strike": 0.0, "option_type": "INDEX"}

        # 2. From nifty_option_symbols.json if present
        symbols_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nifty_option_symbols.json")
        if os.path.exists(symbols_file):
            try:
                with open(symbols_file, "r") as f:
                    cfg_data = json.load(f)
                    if isinstance(cfg_data, list):
                        for item in cfg_data:
                            sym = item.get("instrument_key")
                            if sym:
                                meta[sym] = {
                                    "underlying": item.get("underlying_symbol", "NIFTY"),
                                    "expiry": item.get("expiry", "1970-01-01"),
                                    "strike": float(item.get("strike", 0.0)),
                                    "option_type": item.get("instrument_type", "OPT")
                                }
            except Exception as e:
                logger.warning(f"Error parsing nifty_option_symbols.json: {e}")

        # 2b. From equity_symbols.json if present
        equity_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "equity_symbols.json")
        if os.path.exists(equity_file):
            try:
                with open(equity_file, "r") as f:
                    eq_data = json.load(f)
                    for sym, item in eq_data.items():
                        meta[sym] = {
                            "underlying": item.get("symbol", "EQ"),
                            "expiry": "1970-01-01",
                            "strike": 0.0,
                            "option_type": "EQ"
                        }
            except Exception as e:
                logger.warning(f"Error parsing equity_symbols.json: {e}")

        # 3. From Redis chain keys
        try:
            chain_keys = self.r.keys("chain:*")
            for ck in chain_keys:
                # Format: chain:<UNDERLYING>:<EXPIRY>
                parts = ck.split(":")
                if len(parts) == 3:
                    und = parts[1]
                    exp = parts[2]
                    chain_map = self.r.hgetall(ck)
                    for strike_type, sym in chain_map.items():
                        if ":" in strike_type:
                            stk_str, otype = strike_type.split(":")
                            meta[sym] = {
                                "underlying": und,
                                "expiry": exp,
                                "strike": float(stk_str),
                                "option_type": otype
                            }
        except Exception as e:
            logger.warning(f"Error parsing Redis chains: {e}")

        # 4. From Redis meta:* keys (Futures & other custom instruments)
        try:
            meta_keys = self.r.keys("meta:*")
            for mk in meta_keys:
                sym = mk.replace("meta:", "")
                h = self.r.hgetall(mk)
                if h:
                    meta[sym] = {
                        "underlying": h.get("underlying", "NIFTY"),
                        "expiry": h.get("expiry", "1970-01-01"),
                        "strike": float(h.get("strike", 0.0)),
                        "option_type": h.get("option_type", "FUT")
                    }
        except Exception as e:
            logger.warning(f"Error parsing Redis meta:* keys: {e}")

        try:
            self.quote_keys = self.r.keys("md:quote:*")
        except Exception:
            self.quote_keys = []

        self.symbol_meta = meta
        self.last_meta_refresh = time.time()
        logger.info(f"📋 Symbol metadata refreshed: {len(self.symbol_meta)} instruments, {len(self.quote_keys)} quote keys mapped.")

    def flush_buffer(self):
        """Flushes buffered tick records into ClickHouse."""
        if not self.buffer:
            return

        rows_to_insert = self.buffer
        self.buffer = []
        self.last_flush_time = time.time()

        try:
            self.ch_client.insert("market_ticks", rows_to_insert, column_names=self.columns)
            self.total_inserted += len(rows_to_insert)
            logger.info(f"⚡ Flushed {len(rows_to_insert):,} ticks to ClickHouse Cloud (Total: {self.total_inserted:,})")
        except Exception as e:
            logger.error(f"❌ Failed to insert batch into ClickHouse: {e}. Reconnecting...")
            # Put back rows into buffer to prevent data loss
            self.buffer = rows_to_insert + self.buffer
            self.connect_clickhouse()

    def run(self):
        """Main real-time tick streaming loop."""
        self.run_streamer()

    def run_streamer(self):
        """Main streaming loop: scans Redis quotes and pushes ticks to ClickHouse Cloud."""
        logger.info(f"Starting ULLTR ClickHouse Real-time Streamer (Batch size: {BATCH_SIZE}, Flush interval: {FLUSH_INTERVAL_SEC}s)...")
        
        self.refresh_symbol_metadata()

        last_stats_log = time.time()
        ticks_in_interval = 0

        while True:
            try:
                # Refresh metadata periodically (every 5 minutes)
                if time.time() - self.last_meta_refresh >= 300.0:
                    self.refresh_symbol_metadata()

                quote_keys = getattr(self, "quote_keys", None) or self.r.keys("md:quote:*")
                if not quote_keys:
                    time.sleep(0.5)
                    continue

                # Pipelined fetch of all quote hashes
                pipe = self.r.pipeline(transaction=False)
                for k in quote_keys:
                    pipe.hgetall(k)
                quotes = pipe.execute()

                now_ts = datetime.now()

                for k, q in zip(quote_keys, quotes):
                    if not q:
                        continue

                    sym = q.get("symbol", k.replace("md:quote:", ""))
                    ts_recv = int(float(q.get("ts_recv", 0)))
                    ts_exch = int(float(q.get("ts_exchange", 0)))
                    
                    # Deduplication check: only insert if exchange/recv timestamp or quote is fresh
                    tick_id_ts = ts_recv if ts_recv > 0 else ts_exch
                    if tick_id_ts > 0 and self.last_seen_ts.get(sym) == tick_id_ts:
                        continue

                    if tick_id_ts > 0:
                        self.last_seen_ts[sym] = tick_id_ts

                    # Resolve metadata
                    m = self.symbol_meta.get(sym, {
                        "underlying": "SENSEX" if "SENSEX" in sym or "BSE" in sym else "NIFTY",
                        "expiry": "1970-01-01",
                        "strike": 0.0,
                        "option_type": "CE" if "CE" in sym else ("PE" if "PE" in sym else "INDEX")
                    })

                    # Calculate precise datetime timestamp from ms
                    if ts_exch > 0:
                        tick_dt = datetime.fromtimestamp(ts_exch / 1000.0)
                    elif ts_recv > 0:
                        tick_dt = datetime.fromtimestamp(ts_recv / 1000.0)
                    else:
                        tick_dt = now_ts

                    exp_str = m["expiry"]
                    try:
                        exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date() if exp_str != "1970-01-01" else date(1970, 1, 1)
                    except Exception:
                        exp_date = date(1970, 1, 1)

                    row = [
                        tick_dt,
                        sym,
                        m["underlying"],
                        exp_date,
                        float(m["strike"]),
                        m["option_type"],
                        float(q.get("ltp", 0.0)),
                        float(q.get("close", 0.0)),
                        float(q.get("bid", 0.0)),
                        int(float(q.get("bid_qty", 0))),
                        float(q.get("ask", 0.0)),
                        int(float(q.get("ask_qty", 0))),
                        int(float(q.get("volume", 0))),
                        int(float(q.get("oi", 0))),
                        float(q.get("iv", 0.0)),
                        float(q.get("delta", 0.0)),
                        float(q.get("theta", 0.0)),
                        float(q.get("gamma", 0.0)),
                        float(q.get("vega", 0.0)),
                        float(q.get("rho", 0.0)),
                        ts_exch,
                        ts_recv,
                        float(q.get("bid2", 0.0)),
                        int(float(q.get("bid_qty2", 0))),
                        float(q.get("ask2", 0.0)),
                        int(float(q.get("ask_qty2", 0))),
                        float(q.get("bid3", 0.0)),
                        int(float(q.get("bid_qty3", 0))),
                        float(q.get("ask3", 0.0)),
                        int(float(q.get("ask_qty3", 0))),
                        float(q.get("bid4", 0.0)),
                        int(float(q.get("bid_qty4", 0))),
                        float(q.get("ask4", 0.0)),
                        int(float(q.get("ask_qty4", 0))),
                        float(q.get("bid5", 0.0)),
                        int(float(q.get("bid_qty5", 0))),
                        float(q.get("ask5", 0.0)),
                        int(float(q.get("ask_qty5", 0))),
                        int(float(q.get("tbq", 0))),
                        int(float(q.get("tsq", 0)))
                    ]

                    self.buffer.append(row)
                    ticks_in_interval += 1

                # Flush buffer if limit reached or timeout passed
                if len(self.buffer) >= BATCH_SIZE or (time.time() - self.last_flush_time >= FLUSH_INTERVAL_SEC):
                    self.flush_buffer()

                # Log stats every 10 seconds
                if time.time() - last_stats_log >= 10.0:
                    rate = ticks_in_interval / (time.time() - last_stats_log)
                    logger.info(f"📊 Streaming Throughput: {rate:,.1f} ticks/sec | Total Flushed: {self.total_inserted:,} | Buffer: {len(self.buffer)}")
                    last_stats_log = time.time()
                    ticks_in_interval = 0

                time.sleep(0.20)

            except Exception as e:
                logger.error(f"⚠️ Error in streaming loop: {e}")
                time.sleep(1.0)

if __name__ == "__main__":
    streamer = ClickHouseTickStreamer()
    streamer.run()

#!/usr/bin/env python3
"""
ULLTR Standalone Stock Order Book Depth (full_d30) Dual-Connection Collector
==========================================================================
Collects deep 30-level order book data for all 50 NIFTY 50 and 30 SENSEX constituent stocks
using separate dedicated WebSocket connections under the Upstox Plus 5-connection allowance.

Architecture:
- Connection 1: 50 NIFTY 50 Stocks (NSE_EQ) in 'full_d30' mode (strict <=50 keys limit)
- Connection 2: 30 SENSEX 30 Stocks (BSE_EQ) in 'full_d30' mode (strict <=50 keys limit)
- F&O Options Collection: Completely separate C++ collector process (unaffected & independent)
Flushes micro-batches asynchronously to ClickHouse Cloud (default.stock_orderbook_depth).
"""

import sys
import os
import time
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta
import ssl
import websockets
import clickhouse_connect

# Ensure api directory is in python path for protobuf
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(CURRENT_DIR, "api"))
import MarketDataFeedV3_pb2 as pb

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("StockDepthCollector")

IST = timezone(timedelta(hours=5, minutes=30))

# ClickHouse Cloud Credentials
CH_HOST = "libz0hxoze.ap-south-1.aws.clickhouse.cloud"
CH_USER = "default"
CH_PASS = "BhhYrZvtF3lA~"
CH_PORT = 8443

BATCH_SIZE = 2500
FLUSH_INTERVAL_SEC = 2.0


class StockDepthCollector:
    def __init__(self):
        self.access_token = None
        self.ch_client = None
        self.buffer = []
        self.last_flush = time.time()
        self.nse_instruments = []
        self.bse_instruments = []
        self.metadata = {}  # symbol -> {underlying, exchange}
        self.total_flushed = 0
        self.running = True
        self.lock = asyncio.Lock()

    def load_token(self):
        token_paths = [
            os.path.join(CURRENT_DIR, "login", "access_token.json"),
            os.path.join(CURRENT_DIR, "access_token.json")
        ]
        for p in token_paths:
            if os.path.exists(p):
                try:
                    with open(p) as f:
                        data = json.load(f)
                        self.access_token = data.get("access_token")
                        if self.access_token:
                            logger.info(f"✅ Loaded Upstox access token from {p}")
                            return True
                except Exception as e:
                    logger.error(f"Error reading token {p}: {e}")
        logger.error("❌ Access token not found!")
        return False

    def load_instruments(self):
        eq_file = os.path.join(CURRENT_DIR, "equity_symbols.json")
        if not os.path.exists(eq_file):
            logger.error(f"❌ {eq_file} not found!")
            return False
        with open(eq_file) as f:
            eqs = json.load(f)

        # Connection 1: 50 NIFTY 50 stocks (strictly <= 50 keys for full_d30 compliance)
        self.nse_instruments = [k for k, v in eqs.items() if v.get("index") == "NIFTY_50" or k.startswith("NSE_EQ")][:50]

        # Connection 2: 30 SENSEX 30 stocks (strictly <= 50 keys for full_d30 compliance)
        self.bse_instruments = [k for k, v in eqs.items() if v.get("index") == "SENSEX_30" or k.startswith("BSE_EQ")][:30]

        self.metadata = {k: {"underlying": v["symbol"], "exchange": v["exchange"]} for k, v in eqs.items()}
        logger.info(f"📋 Loaded {len(self.nse_instruments)} NSE stocks (Conn 1) + {len(self.bse_instruments)} BSE stocks (Conn 2) for full_d30 depth collection.")
        return True

    def init_clickhouse(self):
        try:
            self.ch_client = clickhouse_connect.get_client(
                host=CH_HOST,
                user=CH_USER,
                password=CH_PASS,
                port=CH_PORT,
                secure=True
            )
            self.ch_client.command("""
            CREATE TABLE IF NOT EXISTS default.stock_orderbook_depth (
                timestamp DateTime64(3, 'Asia/Kolkata') CODEC(DoubleDelta, LZ4),
                symbol LowCardinality(String) CODEC(ZSTD(1)),
                underlying LowCardinality(String) CODEC(ZSTD(1)),
                exchange LowCardinality(String) CODEC(ZSTD(1)),
                ltp Float64 CODEC(Gorilla, ZSTD(1)),
                close Float64 CODEC(Gorilla, ZSTD(1)),
                volume Int64 CODEC(T64, ZSTD(1)),
                total_buy_qty Int64 CODEC(T64, ZSTD(1)),
                total_sell_qty Int64 CODEC(T64, ZSTD(1)),
                bid1 Float64 CODEC(Gorilla, ZSTD(1)),
                bid_qty1 Int64 CODEC(T64, ZSTD(1)),
                ask1 Float64 CODEC(Gorilla, ZSTD(1)),
                ask_qty1 Int64 CODEC(T64, ZSTD(1)),
                bids_price Array(Float64) CODEC(ZSTD(1)),
                bids_qty Array(Int64) CODEC(ZSTD(1)),
                asks_price Array(Float64) CODEC(ZSTD(1)),
                asks_qty Array(Int64) CODEC(ZSTD(1)),
                market_buy_qty Int64 CODEC(T64, ZSTD(1)),
                market_sell_qty Int64 CODEC(T64, ZSTD(1))
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(timestamp)
            ORDER BY (underlying, symbol, timestamp);
            """)
            logger.info("✅ Connected to ClickHouse Cloud and verified stock_orderbook_depth table.")
            return True
        except Exception as e:
            logger.error(f"❌ ClickHouse initialization error: {e}")
            return False

    def flush_buffer(self):
        if not self.buffer:
            return
        try:
            columns = [
                'timestamp', 'symbol', 'underlying', 'exchange',
                'ltp', 'close', 'volume', 'total_buy_qty', 'total_sell_qty',
                'bid1', 'bid_qty1', 'ask1', 'ask_qty1',
                'bids_price', 'bids_qty', 'asks_price', 'asks_qty',
                'market_buy_qty', 'market_sell_qty'
            ]
            self.ch_client.insert('default.stock_orderbook_depth', self.buffer, column_names=columns)
            count = len(self.buffer)
            self.total_flushed += count
            self.buffer.clear()
            self.last_flush = time.time()
            logger.info(f"💾 Flushed {count} depth rows to ClickHouse Cloud (Total: {self.total_flushed:,})")
        except Exception as e:
            logger.error(f"❌ Failed to flush depth buffer to ClickHouse: {e}")

    async def get_authorized_ws_url(self):
        import requests
        auth_url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json"
        }
        for attempt in range(5):
            try:
                r = requests.get(auth_url, headers=headers, timeout=10)
                if r.status_code == 200:
                    return r.json().get("data", {}).get("authorizedRedirectUri")
                logger.error(f"Auth API returned status {r.status_code}: {r.text}")
            except Exception as e:
                logger.error(f"Auth API request exception: {e}")
            await asyncio.sleep(3)
            self.load_token()
            headers["Authorization"] = f"Bearer {self.access_token}"
        return None

    async def stream_worker(self, segment_name, instruments):
        if not instruments:
            logger.warning(f"[{segment_name}] No instruments specified. Worker terminating.")
            return

        while self.running:
            try:
                ws_url = await self.get_authorized_ws_url()
                if not ws_url:
                    logger.error(f"[{segment_name}] Failed to obtain authorized WebSocket URI. Retrying in 5s...")
                    await asyncio.sleep(5)
                    continue

                logger.info(f"[{segment_name}] Connecting to Upstox Depth WebSocket for {len(instruments)} instruments...")
                ssl_ctx = ssl.create_default_context()
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE

                async with websockets.connect(ws_url, ssl=ssl_ctx, max_size=10_000_000, ping_interval=20) as ws:
                    sub_payload = {
                        "guid": f"stock_depth_{segment_name.lower()}_{int(time.time()*1000)}",
                        "method": "sub",
                        "data": {
                            "mode": "full_d30",
                            "instrumentKeys": instruments
                        }
                    }
                    await ws.send(json.dumps(sub_payload).encode('utf-8'))
                    logger.info(f"🚀 [{segment_name}] Subscribed to {len(instruments)} stocks in 'full_d30' (30-level depth) mode!")

                    msg_count = 0
                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                            if isinstance(msg, bytes):
                                feed_resp = pb.FeedResponse()
                                feed_resp.ParseFromString(msg)
                                msg_count += 1
                                if msg_count % 100 == 1:
                                    logger.info(f"📥 [{segment_name}] Frame #{msg_count} containing {len(feed_resp.feeds)} feeds")

                                now_dt = datetime.now(IST)
                                new_rows = []

                                for sym_key, feed in feed_resp.feeds.items():
                                    meta = self.metadata.get(sym_key, {"underlying": sym_key, "exchange": f"{segment_name}_EQ"})
                                    underlying = meta["underlying"]
                                    exchange = meta["exchange"]

                                    ltp = 0.0
                                    cp = 0.0
                                    vol = 0
                                    tbq = 0
                                    tsq = 0
                                    bids_p = []
                                    bids_q = []
                                    asks_p = []
                                    asks_q = []

                                    if feed.HasField("fullFeed") and feed.fullFeed.HasField("marketFF"):
                                        mff = feed.fullFeed.marketFF
                                        ltp = float(mff.ltpc.ltp)
                                        cp = float(mff.ltpc.cp)
                                        vol = int(mff.vtt)
                                        tbq = int(mff.tbq)
                                        tsq = int(mff.tsq)

                                        # Extract all depth levels (up to 30 levels)
                                        if mff.HasField("marketLevel"):
                                            for q in mff.marketLevel.bidAskQuote:
                                                if q.bidQ > 0:
                                                    bids_p.append(float(q.bidP))
                                                    bids_q.append(int(q.bidQ))
                                                if q.askQ > 0:
                                                    asks_p.append(float(q.askP))
                                                    asks_q.append(int(q.askQ))

                                    elif feed.HasField("firstLevelWithGreeks"):
                                        flg = feed.firstLevelWithGreeks
                                        ltp = float(flg.ltpc.ltp)
                                        cp = float(flg.ltpc.cp)
                                        vol = int(flg.vtt)
                                        if flg.HasField("firstDepth"):
                                            if flg.firstDepth.bidQ > 0:
                                                bids_p.append(float(flg.firstDepth.bidP))
                                                bids_q.append(int(flg.firstDepth.bidQ))
                                            if flg.firstDepth.askQ > 0:
                                                asks_p.append(float(flg.firstDepth.askP))
                                                asks_q.append(int(flg.firstDepth.askQ))

                                    elif feed.HasField("ltpc"):
                                        ltp = float(feed.ltpc.ltp)
                                        cp = float(feed.ltpc.cp)

                                    bid1 = bids_p[0] if bids_p else 0.0
                                    bid_qty1 = bids_q[0] if bids_q else 0
                                    ask1 = asks_p[0] if asks_p else 0.0
                                    ask_qty1 = asks_q[0] if asks_q else 0

                                    mkt_buy_qty = sum(q for p, q in zip(bids_p, bids_q) if p == 0.0)
                                    mkt_sell_qty = sum(q for p, q in zip(asks_p, asks_q) if p == 0.0)

                                    if ltp > 0 or bids_p or asks_p:
                                        new_rows.append([
                                            now_dt, sym_key, underlying, exchange,
                                            ltp, cp, vol, tbq, tsq,
                                            bid1, bid_qty1, ask1, ask_qty1,
                                            bids_p, bids_q, asks_p, asks_q,
                                            mkt_buy_qty, mkt_sell_qty
                                        ])

                                if new_rows:
                                    async with self.lock:
                                        self.buffer.extend(new_rows)
                                        if len(self.buffer) >= BATCH_SIZE:
                                            self.flush_buffer()

                        except asyncio.TimeoutError:
                            continue

            except Exception as e:
                logger.error(f"⚠️ [{segment_name}] WebSocket error: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3)

    async def flush_loop(self):
        while self.running:
            await asyncio.sleep(FLUSH_INTERVAL_SEC)
            async with self.lock:
                if self.buffer and (time.time() - self.last_flush >= FLUSH_INTERVAL_SEC or len(self.buffer) >= BATCH_SIZE):
                    self.flush_buffer()

    async def run(self):
        if not self.load_token() or not self.load_instruments() or not self.init_clickhouse():
            return

        logger.info(
            f"🌟 Starting Dual-Connection 30-Depth Collector:\n"
            f"   📡 Connection 1: NSE NIFTY 50 ({len(self.nse_instruments)} stocks)\n"
            f"   📡 Connection 2: BSE SENSEX 30 ({len(self.bse_instruments)} stocks)\n"
            f"   ⚡ F&O Options Collector: Unaffected (Independent C++ process)"
        )

        task_nse = asyncio.create_task(self.stream_worker("NSE", self.nse_instruments))
        task_bse = asyncio.create_task(self.stream_worker("BSE", self.bse_instruments))
        task_flush = asyncio.create_task(self.flush_loop())

        await asyncio.gather(task_nse, task_bse, task_flush)


if __name__ == "__main__":
    collector = StockDepthCollector()
    try:
        asyncio.run(collector.run())
    except KeyboardInterrupt:
        logger.info("Collector stopped by user.")

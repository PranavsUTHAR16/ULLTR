import os
import sys
import time
import json
import urllib.parse
import redis
import requests
import logging
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/Users/prana/Desktop/open_source/web/reconciler.log')
    ]
)

UNIX_SOCKET_PATH = "/Users/prana/Desktop/open_source/web/redis.sock"
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
TOKEN_PATH = "/Users/prana/Desktop/open_source/web/login/access_token.json"
SYMBOLS_PATH = "/Users/prana/Desktop/open_source/web/nifty_option_symbols.json"

class RateLimiter:
    """Thread-safe rate limiter using a sliding delay to prevent API rate limit breaches."""
    def __init__(self, rate_per_sec: float):
        self.delay = 1.0 / rate_per_sec
        self.last_call = 0.0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
            self.last_call = time.time()

class StandaloneReconciler:
    def __init__(self):
        # 1. Connect to Redis safely
        if os.path.exists(UNIX_SOCKET_PATH):
            self.r = redis.Redis(unix_socket_path=UNIX_SOCKET_PATH, decode_responses=True)
            logging.info(f"Connected to Redis via Unix socket: {UNIX_SOCKET_PATH}")
        else:
            self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            logging.info(f"Connected to Redis via TCP loopback: {REDIS_HOST}:{REDIS_PORT}")
            
        try:
            self.r.ping()
        except Exception as e:
            logging.critical(f"Failed to connect to Redis: {e}")
            sys.exit(1)

        # 2. State configuration variables
        self.token = ""
        self.instruments = []
        self.reco_wait_times = {}  # ts -> bool (True if wait completed for this timestamp)
        
        self.load_token()
        self.load_symbols()
        
        # 3. Thread Pool Executor (10 concurrent threads) & Conservative Rate Limiter (5.0 requests/second max)
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.limiter = RateLimiter(5.0)

    def load_token(self):
        """Loads access token from access_token.json."""
        try:
            if os.path.exists(TOKEN_PATH):
                with open(TOKEN_PATH) as f:
                    tk = json.load(f)
                    self.token = tk.get("access_token", "")
                    logging.info("Access token successfully loaded.")
            else:
                logging.warning(f"Access token file not found at: {TOKEN_PATH}")
        except Exception as e:
            logging.error(f"Error loading access token: {e}")

    def load_symbols(self):
        """Loads instruments from nifty_option_symbols.json."""
        try:
            if os.path.exists(SYMBOLS_PATH):
                with open(SYMBOLS_PATH) as f:
                    data = json.load(f)
                    self.instruments = []
                    if "index_key" in data:
                        self.instruments = [data["index_key"]] + data["symbols"]
                    else:
                        for underlying, info in data.items():
                            self.instruments.append(info["index_key"])
                            self.instruments.extend(info["symbols"])
                    logging.info(f"Loaded {len(self.instruments)} instruments for reconciliation.")
            else:
                logging.warning(f"Symbols path not found at: {SYMBOLS_PATH}")
        except Exception as e:
            logging.error(f"Error loading symbols: {e}")

    def parse_iso_timestamp(self, ts_str: str) -> int:
        """Parses Upstox ISO-8601 string to integer Unix epoch seconds."""
        try:
            return int(datetime.fromisoformat(ts_str).timestamp())
        except Exception as e:
            logging.error(f"Failed to parse timestamp {ts_str}: {e}")
            return 0

    def trigger_token_refresh(self) -> bool:
        """Executes auth.py script to refresh token."""
        logging.info("Triggering automated token refresh using auth.py...")
        if os.path.exists(TOKEN_PATH):
            try:
                os.remove(TOKEN_PATH)
            except Exception as ex:
                logging.warning(f"Failed to remove stale token file: {ex}")
        auth_script = "/Users/prana/Desktop/open_source/web/login/auth.py"
        import subprocess
        ret = subprocess.run(
            [sys.executable, auth_script],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(auth_script)
        )
            
        if ret.returncode == 0:
            self.load_token()
            return True
        logging.error(f"Automated token refresh failed. Error: {ret.stderr}")
        return False

    def propagate_parent_recalculations(self, symbol: str, ts_closed: int):
        """Recalculates higher timeframe parent candles natively in Redis using pipelining."""
        parent_timeframes = [
            ("3m", 180),
            ("5m", 300),
            ("15m", 900),
            ("30m", 1800)
        ]
        
        for tf, duration in parent_timeframes:
            ts_parent = (ts_closed // duration) * duration
            
            # Fetch all 1m candles inside parent block in a single pipeline
            pipe = self.r.pipeline()
            for t in range(ts_parent, ts_parent + duration, 60):
                pipe.hgetall(f"md:candle:{symbol}:1m:{t}")
            
            raw_candles = pipe.execute()
            valid_candles = []
            
            for t, c_data in zip(range(ts_parent, ts_parent + duration, 60), raw_candles):
                if c_data:
                    try:
                        valid_candles.append({
                            "timestamp": t,
                            "open": float(c_data["open"]),
                            "high": float(c_data["high"]),
                            "low": float(c_data["low"]),
                            "close": float(c_data["close"]),
                            "volume": int(c_data["volume"])
                        })
                    except (ValueError, TypeError):
                        pass
            
            if not valid_candles:
                continue
                
            # Aggregate values
            o = valid_candles[0]["open"]
            c = valid_candles[-1]["close"]
            h = max(x["high"] for x in valid_candles)
            l = min(x["low"] for x in valid_candles)
            v = sum(x["volume"] for x in valid_candles)
            
            parent_key = f"md:candle:{symbol}:{tf}:{ts_parent}"
            zset_key = f"md:candles:{symbol}:{tf}"
            
            pipe = self.r.pipeline()
            pipe.hset(parent_key, mapping={
                "open": str(o),
                "high": str(h),
                "low": str(l),
                "close": str(c),
                "volume": str(v),
                "status": "reconciled"
            })
            pipe.zadd(zset_key, {str(ts_parent): ts_parent})
            pipe.execute()
            logging.debug(f"Propagated aggregation for {symbol} {tf} at {ts_parent}")

    def reconcile_symbol(self, symbol: str, ts_closed: int, attempt: int = 1):
        """Processes Upstox API retrieval and performs self-healing for a single symbol."""
        # 1. Enforce rate limiting (5 req/sec) before issuing request
        self.limiter.wait()

        encoded_symbol = urllib.parse.quote(symbol, safe='')
        url = f"https://api.upstox.com/v3/historical-candle/intraday/{encoded_symbol}/minutes/1"
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.token}'
        }
        
        try:
            resp = requests.get(url, headers=headers, timeout=6)
            
            # Handle rate limiting with sequential exponential backoff retry
            if resp.status_code == 429:
                if attempt < 4:
                    backoff = 2 ** attempt
                    logging.warning(f"⚠️ Upstox API 429 Rate Limited for {symbol}. Backing off for {backoff}s...")
                    time.sleep(backoff)
                    self.reconcile_symbol(symbol, ts_closed, attempt + 1)
                else:
                    logging.error(f"❌ Exceeded max retries (429) for {symbol} at {ts_closed}")
                return

            if resp.status_code == 401 and attempt < 2:
                logging.warning("Upstox API 401 Unauthorized. Refreshing token...")
                if self.trigger_token_refresh():
                    self.reconcile_symbol(symbol, ts_closed, attempt + 1)
                return
                
            if resp.status_code != 200:
                logging.error(f"Upstox API error ({resp.status_code}) for {symbol}")
                return
                
            body = resp.json()
            if body.get("status") != "success":
                logging.error(f"API status failure for {symbol}: {body}")
                return
                
            candles = body["data"]["candles"]
            matched_candle = None
            for c in candles:
                ts_api = self.parse_iso_timestamp(c[0])
                if ts_api == ts_closed:
                    matched_candle = c
                    break
                    
            if not matched_candle:
                # If fresh (within 3 mins), re-queue once to allow broker indexing latency
                if int(time.time()) - ts_closed < 180 and attempt < 2:
                    logging.info(f"Candle {ts_closed} not found in Upstox for {symbol}. Re-scheduling...")
                    time.sleep(1.0)
                    self.reconcile_symbol(symbol, ts_closed, attempt + 1)
                return
                
            # Parsed API Candle
            api_o = float(matched_candle[1])
            api_h = float(matched_candle[2])
            api_l = float(matched_candle[3])
            api_c = float(matched_candle[4])
            api_v = int(matched_candle[5])
            
            # Fetch local candle from Redis HASH
            candle_key = f"md:candle:{symbol}:1m:{ts_closed}"
            cur = self.r.hgetall(candle_key)
            
            discrepancy = False
            is_major = False
            diff_details = ""
            if not cur:
                discrepancy = True
                is_major = True
                diff_details = "Local candle not found in Redis (GUEST GAP)"
            else:
                try:
                    cur_o = float(cur.get("open", 0.0))
                    cur_h = float(cur.get("high", 0.0))
                    cur_l = float(cur.get("low", 0.0))
                    cur_c = float(cur.get("close", 0.0))
                    cur_v = int(cur.get("volume", 0))
                    
                    o_diff = abs(cur_o - api_o)
                    h_diff = abs(cur_h - api_h)
                    l_diff = abs(cur_l - api_l)
                    c_diff = abs(cur_c - api_c)
                    v_diff = abs(cur_v - api_v)
                    
                    # 1. Any variation is a discrepancy that we will heal in Redis to match Upstox ground truth
                    if (o_diff > 0.01 or h_diff > 0.01 or l_diff > 0.01 or c_diff > 0.01 or v_diff > 0):
                        discrepancy = True
                        
                        # 2. Determine if this discrepancy is a significant "Major Gap" or just a "Minor Deviation"
                        # Major price discrepancy: diff is > 0.50 rupees AND represents > 0.5% of the broker price
                        price_major = False
                        for p_diff, p_api in [(o_diff, api_o), (h_diff, api_h), (l_diff, api_l), (c_diff, api_c)]:
                            if p_diff > 0.50 and p_api > 0 and (p_diff / p_api) > 0.005:
                                price_major = True
                                break
                        
                        # Major volume discrepancy: volume difference is > 10,000 AND > 15% of the broker volume
                        vol_major = False
                        if v_diff > 10000 and api_v > 0 and (v_diff / api_v) > 0.15:
                            vol_major = True
                            
                        if price_major or vol_major:
                            is_major = True
                            
                        diff_details = (f"O: local={cur_o:.2f}/api={api_o:.2f} (diff={o_diff:.2f}), "
                                        f"H: local={cur_h:.2f}/api={api_h:.2f} (diff={h_diff:.2f}), "
                                        f"L: local={cur_l:.2f}/api={api_l:.2f} (diff={l_diff:.2f}), "
                                        f"C: local={cur_c:.2f}/api={api_c:.2f} (diff={c_diff:.2f}), "
                                        f"V: local={cur_v}/api={api_v} (diff={v_diff})")
                except Exception as ex:
                    discrepancy = True
                    is_major = True
                    diff_details = f"Exception comparing values: {ex}"
                    
            if discrepancy:
                if is_major:
                    logging.info(f"⚠️ [Self-Healing] Significant Discrepancy for {symbol} 1m at {ts_closed}! Details: {diff_details}. Updating Redis...")
                else:
                    logging.debug(f"[Self-Healing] Minor physical timing deviation resolved for {symbol} at {ts_closed}. Details: {diff_details}")
                
                self.r.hset(candle_key, mapping={
                    "open": str(api_o),
                    "high": str(api_h),
                    "low": str(api_l),
                    "close": str(api_c),
                    "volume": str(api_v),
                    "status": "reconciled"
                })
                self.propagate_parent_recalculations(symbol, ts_closed)
            else:
                self.r.hset(candle_key, "status", "reconciled")
                
        except Exception as e:
            logging.error(f"Error in reconcile_symbol for {symbol} at {ts_closed}: {e}")

    def wait_sync(self, ts_closed: int):
        """Implements a single 5-second broker synchronization wait per closed timestamp."""
        now_time = int(time.time())
        elapsed = now_time - ts_closed
        
        # Adaptive: Only wait if the candle closed < 5 seconds ago
        if elapsed < 5:
            wait_sec = 5.0 - elapsed
            logging.info(f"⏳ Waiting {wait_sec:.1f}s for broker API synchronization on timestamp {ts_closed}...")
            time.sleep(wait_sec)

    def process_item(self, symbol: str, ts_closed: int):
        """Pre-processes synchronizations and dispatches parallel executor threads."""
        if ts_closed not in self.reco_wait_times:
            self.reco_wait_times[ts_closed] = True
            self.wait_sync(ts_closed)
            
        self.executor.submit(self.reconcile_symbol, symbol, ts_closed)

    def run_catchup(self):
        """Startup catch-up scan. Scans all 'live' candles from the last 2 minutes and heals them."""
        logging.info("🏃 Starting dynamic boot-up catch-up scan...")
        live_count = 0
        now_epoch = int(time.time())
        
        # Scan Redis ZSET indexes for all instruments to find recent 'live' status keys
        for symbol in self.instruments:
            zset_key = f"md:candles:{symbol}:1m"
            
            # Fetch candle timestamps from the last 2 minutes (120 seconds)
            ts_list = self.r.zrangebyscore(zset_key, now_epoch - 120, now_epoch)
            if not ts_list:
                continue
                
            pipe = self.r.pipeline()
            for ts in ts_list:
                pipe.hget("md:candle:" + symbol + ":1m:" + ts, "status")
            statuses = pipe.execute()
            
            for ts, status in zip(ts_list, statuses):
                if status == "live":
                    ts_closed = int(ts)
                    self.executor.submit(self.reconcile_symbol, symbol, ts_closed)
                    live_count += 1
                    
        if live_count > 0:
            logging.info(f"✅ Boot-up catch-up dispatched {live_count} recent 'live' candles for parallel rate-limited reconciliation.")
        else:
            logging.info("✅ No recent un-reconciled 'live' candles found. System is clean!")

    def start(self):
        """Starts the main Pub/Sub subscription event loop daemon."""
        # 1. Run startup catch-up scan
        self.run_catchup()
        
        # 2. Subscribe to Redis md:reco:trigger
        pubsub = self.r.pubsub()
        pubsub.subscribe("md:reco:trigger")
        logging.info("📡 Subscribed to Redis channel 'md:reco:trigger'. Listening for closed minutes...")
        
        # Clean obsolete keys from self.reco_wait_times periodically to prevent memory leak
        last_cleanup = time.time()
        
        for msg in pubsub.listen():
            if msg["type"] == "message":
                try:
                    payload = msg["data"]
                    if ":" in payload:
                        symbol, ts_str = payload.split(":", 1)
                        ts_closed = int(ts_str)
                        self.process_item(symbol, ts_closed)
                except Exception as e:
                    logging.error(f"Error handling Pub/Sub message: {e}")
                    
            # Memory cleanup of tracking keys older than 1 hour
            if time.time() - last_cleanup > 1800:
                now_epoch = int(time.time())
                self.reco_wait_times = {k: v for k, v in self.reco_wait_times.items() if now_epoch - k < 3600}
                last_cleanup = time.time()

if __name__ == "__main__":
    reconciler = StandaloneReconciler()
    try:
        reconciler.start()
    except KeyboardInterrupt:
        logging.info("👋 Standalone Reconciler cleanly terminated on keyboard interrupt.")
        sys.exit(0)

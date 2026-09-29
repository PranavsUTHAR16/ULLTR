#!/usr/bin/env python
# scratch/seed_option_candles.py

import os
import json
import requests
import redis
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Global rate limiter state to respect Upstox API limit of 240 reqs/min (4 reqs/sec)
last_req_time = 0.0
rate_limit_lock = threading.Lock()

def rate_limit():
    """Global rate limiter ensuring at most 3 API requests per second across all threads."""
    global last_req_time
    with rate_limit_lock:
        now = time.time()
        elapsed = now - last_req_time
        delay = 0.35 - elapsed
        if delay > 0:
            time.sleep(delay)
        last_req_time = time.time()

def seed_symbol_candles(symbol, headers, r, to_date, from_date):
    """Fetches and seeds candles for a single option symbol."""
    try:
        # 1. Fetch Intraday 1m candles
        rate_limit()
        url_intra = f"https://api.upstox.com/v3/historical-candle/intraday/{symbol}/minutes/1"
        res_intra = requests.get(url_intra, headers=headers, timeout=5)
        intra_candles = []
        if res_intra.status_code == 200:
            intra_candles = res_intra.json().get("data", {}).get("candles", [])
        
        # 2. Fetch Historical 1m candles
        rate_limit()
        url_hist = f"https://api.upstox.com/v3/historical-candle/{symbol}/minutes/1/{to_date}/{from_date}"
        res_hist = requests.get(url_hist, headers=headers, timeout=5)
        hist_candles = []
        if res_hist.status_code == 200:
            hist_candles = res_hist.json().get("data", {}).get("candles", [])
            
        all_raw = hist_candles + intra_candles
        if not all_raw:
            return False, "No raw candle data returned"
            
        # Parse and deduplicate
        candles_dict = {}
        for c in all_raw:
            ts_str = c[0]
            try:
                dt = datetime.fromisoformat(ts_str)
                ts = int(dt.timestamp())
                candles_dict[ts] = {
                    "timestamp": ts,
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": int(c[5])
                }
            except Exception:
                pass
                
        if not candles_dict:
            return False, "Failed to parse any candles"
            
        sorted_ts = sorted(candles_dict.keys())
        candles_1m = [candles_dict[ts] for ts in sorted_ts]
        
        # Seed multiple timeframes (1m, 3m, 5m, 15m, 30m) to Redis
        tf_intervals = {
            "1m": 60,
            "3m": 180,
            "5m": 300,
            "15m": 900,
            "30m": 1800
        }
        
        for tf, interval_sec in tf_intervals.items():
            groups = {}
            for c in candles_1m:
                ts_parent = (c["timestamp"] // interval_sec) * interval_sec
                if ts_parent not in groups:
                    groups[ts_parent] = []
                groups[ts_parent].append(c)
                
            candles_agg = []
            for ts_parent in sorted(groups.keys()):
                group = groups[ts_parent]
                candles_agg.append({
                    "timestamp": ts_parent,
                    "open": group[0]["open"],
                    "high": max(x["high"] for x in group),
                    "low": min(x["low"] for x in group),
                    "close": group[-1]["close"],
                    "volume": sum(x["volume"] for x in group)
                })
                
            zset_key = f"md:candles:{symbol}:{tf}"
            
            # FAST OPTIMIZATION: Delete only the ZSET key. 
            # We completely omit the slow, blocking KEYS command to query individual candle keys.
            r.delete(zset_key)
            
            pipe = r.pipeline()
            for c in candles_agg:
                candle_key = f"md:candle:{symbol}:{tf}:{c['timestamp']}"
                pipe.hset(candle_key, mapping={
                    "open": f"{c['open']:.2f}",
                    "high": f"{c['high']:.2f}",
                    "low": f"{c['low']:.2f}",
                    "close": f"{c['close']:.2f}",
                    "volume": str(c['volume']),
                    "status": "historical"
                })
                pipe.zadd(zset_key, {str(c['timestamp']): c['timestamp']})
            pipe.execute()
            
        return True, f"Successfully seeded {len(candles_1m)} 1m candles"
    except Exception as e:
        return False, f"Exception occurred: {e}"

def main():
    token_path = "/Users/prana/Desktop/open_source/web/login/access_token.json"
    if not os.path.exists(token_path):
        print(f"❌ Error: access_token.json not found at {token_path}")
        return
        
    with open(token_path) as f:
        token = json.load(f)["access_token"]
        
    symbols_path = "/Users/prana/Desktop/open_source/web/nifty_option_symbols.json"
    if not os.path.exists(symbols_path):
        print(f"❌ Error: nifty_option_symbols.json not found at {symbols_path}")
        return
        
    with open(symbols_path) as f:
        sym_data = json.load(f)
        
    symbols = []
    if "symbols" in sym_data:
        symbols = sym_data["symbols"]
    else:
        for underlying, info in sym_data.items():
            symbols.extend(info.get("symbols", []))
            
    print(f"Loaded {len(symbols)} option symbols from configuration.")
    
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    socket_path = '/Users/prana/Desktop/open_source/web/redis.sock'
    if os.path.exists(socket_path):
        r = redis.Redis(unix_socket_path=socket_path, decode_responses=True)
        print(f"Connecting to Redis via Unix Socket: {socket_path}")
    else:
        r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
        print("Connecting to Redis via TCP loopback")
        
    # Calculate dates for historical lookup (last 8 days to cover 5 trading days)
    today = datetime.now()
    from_date = (today - timedelta(days=8)).strftime("%Y-%m-%d")
    to_date = today.strftime("%Y-%m-%d")
    
    print(f"Fetching and seeding historical ({from_date} to {to_date}) and intraday option candles from Upstox API...")
    success_count = 0
    start_time = time.time()
    
    # Run symbol seeding tasks concurrently in a ThreadPool
    max_workers = 8
    print(f"Starting ThreadPoolExecutor with {max_workers} workers...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(seed_symbol_candles, symbol, headers, r, to_date, from_date): symbol
            for symbol in symbols
        }
        
        for idx, future in enumerate(as_completed(futures)):
            symbol = futures[future]
            try:
                success, msg = future.result()
                if success:
                    success_count += 1
                else:
                    # Silent skip or log warnings for expired/empty contracts
                    pass
            except Exception as exc:
                print(f"⚠️ Thread exception for {symbol}: {exc}")
                
            if (idx + 1) % 20 == 0 or (idx + 1) == len(symbols):
                elapsed = time.time() - start_time
                print(f"⏳ [{idx+1}/{len(symbols)}] Seeding in progress... Success: {success_count} | Elapsed: {elapsed:.2f}s")
                
    total_time = time.time() - start_time
    print(f"\n🎉 Option candles seeding completed successfully for {success_count}/{len(symbols)} symbols in {total_time:.2f} seconds!")

if __name__ == "__main__":
    main()

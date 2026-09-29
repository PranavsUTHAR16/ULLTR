import os
import json
import requests
import redis
import time
from datetime import datetime, timedelta

def seed_index_candles(r, token, symbol, underlying_key):
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    # 1. Fetch Intraday 1m candles
    url_intra = f"https://api.upstox.com/v3/historical-candle/intraday/{requests.utils.quote(symbol)}/minutes/1"
    print(f"\nFetching intraday 1m candles for {symbol}...")
    res = requests.get(url_intra, headers=headers)
    intra_candles = []
    if res.status_code == 200:
        intra_candles = res.json().get("data", {}).get("candles", [])
        print(f"Fetched {len(intra_candles)} intraday 1m candles.")
    else:
        print(f"⚠️ Failed to fetch intraday candles: {res.text}")
        
    # 2. Fetch Historical 1m candles for last 8 days (to cover 5 trading days)
    today = datetime.now()
    from_date = (today - timedelta(days=8)).strftime("%Y-%m-%d")
    to_date = today.strftime("%Y-%m-%d")
    url_hist = f"https://api.upstox.com/v3/historical-candle/{requests.utils.quote(symbol)}/minutes/1/{to_date}/{from_date}"
    print(f"Fetching historical 1m candles: {url_hist}")
    res_hist = requests.get(url_hist, headers=headers)
    hist_candles = []
    if res_hist.status_code == 200:
        hist_candles = res_hist.json().get("data", {}).get("candles", [])
        print(f"Fetched {len(hist_candles)} historical 1m candles.")
    else:
        print(f"⚠️ Failed to fetch historical candles: {res_hist.text}")
        
    all_raw = hist_candles + intra_candles
    if not all_raw:
        print(f"❌ Error: Could not retrieve any candle data for {symbol} from Upstox API.")
        return
        
    # 3. Parse and Deduplicate
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
        except Exception as e:
            print(f"⚠️ Parse error on candle {c}: {e}")
            
    # Sort chronologically
    sorted_ts = sorted(candles_dict.keys())
    candles_1m = [candles_dict[ts] for ts in sorted_ts]
    print(f"Merged and unique 1m candles count for {symbol}: {len(candles_1m)}")
    
    if not candles_1m:
        print(f"⚠️ No parsed candles for {symbol}. Skipping.")
        return

    # Seed the spot index quote to the latest candle close price
    latest_candle = candles_1m[-1]
    r.set(f"spot:{underlying_key}", symbol)
    r.hset(f"md:quote:{symbol}", mapping={
        "symbol": symbol,
        "ltp": f"{latest_candle['close']:.2f}",
        "close": f"{latest_candle['close']:.2f}",
        "volume": str(latest_candle['volume']),
        "ts_exchange": str(latest_candle['timestamp'] * 1000)
    })
    print(f"Seeded {underlying_key} Spot Quote LTP: {latest_candle['close']} (Timestamp: {datetime.fromtimestamp(latest_candle['timestamp']).strftime('%Y-%m-%d %H:%M:%S')})")
    
    # 5. Seed Multi-timeframe Candles to Redis
    tf_intervals = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "15m": 900,
        "30m": 1800
    }
    
    for tf, duration in tf_intervals.items():
        zset_key = f"md:candles:{symbol}:{tf}"
        
        # Aggregate candles
        aggregated_dict = {}
        for c in candles_1m:
            ts_closed = c["timestamp"]
            ts_bucket = (ts_closed // duration) * duration
            
            if ts_bucket not in aggregated_dict:
                aggregated_dict[ts_bucket] = []
            aggregated_dict[ts_bucket].append(c)
            
        print(f"Seeding {len(aggregated_dict)} aggregated {tf} candles for {symbol}...")
        
        pipe = r.pipeline()
        for ts_bucket, bucket_candles in aggregated_dict.items():
            # Sort bucket candles chronologically
            bucket_candles = sorted(bucket_candles, key=lambda x: x["timestamp"])
            
            o = bucket_candles[0]["open"]
            c = bucket_candles[-1]["close"]
            h = max(x["high"] for x in bucket_candles)
            l = min(x["low"] for x in bucket_candles)
            v = sum(x["volume"] for x in bucket_candles)
            
            candle_key = f"md:candle:{symbol}:{tf}:{ts_bucket}"
            
            pipe.hset(candle_key, mapping={
                "open": str(o),
                "high": str(h),
                "low": str(l),
                "close": str(c),
                "volume": str(v),
                "status": "historical"
            })
            pipe.zadd(zset_key, {str(ts_bucket): ts_bucket})
            
        pipe.execute()
        print(f"✅ Seeding and aggregation successfully complete for {symbol} ({tf}).")

def main():
    token_path = "/Users/prana/Desktop/open_source/web/login/access_token.json"
    if not os.path.exists(token_path):
        print(f"❌ Error: access_token.json not found at {token_path}")
        return
        
    with open(token_path) as f:
        token = json.load(f)["access_token"]
        
    socket_path = '/Users/prana/Desktop/open_source/web/redis.sock'
    if os.path.exists(socket_path):
        r = redis.Redis(unix_socket_path=socket_path, decode_responses=True)
        print(f"Connecting to Redis via Unix Socket: {socket_path}")
    else:
        r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
        print("Connecting to Redis via TCP loopback")

    indices = [
        {"symbol": "NSE_INDEX|Nifty 50", "key": "NIFTY"},
        {"symbol": "BSE_INDEX|SENSEX", "key": "SENSEX"}
    ]

    for index in indices:
        try:
            seed_index_candles(r, token, index["symbol"], index["key"])
            time.sleep(0.5) # respect rate limit
        except Exception as e:
            print(f"❌ Error seeding {index['symbol']}: {e}")

if __name__ == "__main__":
    main()

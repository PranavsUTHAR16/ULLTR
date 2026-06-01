import os
import redis
import json
from datetime import datetime

def main():
    socket_path = '/Users/prana/Desktop/open_source/web/redis.sock'
    if os.path.exists(socket_path):
        r = redis.Redis(unix_socket_path=socket_path, decode_responses=True)
    else:
        r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
        
    symbol = "NSE_INDEX|Nifty 50"
    timeframes = ["1m", "3m", "5m", "15m", "30m"]
    
    print("📋 Redis Candle Seeding Verification")
    print("====================================")
    
    for tf in timeframes:
        zset_key = f"md:candles:{symbol}:{tf}"
        size = r.zcard(zset_key)
        print(f"\n⚡ Timeframe '{tf}' | Total candles in ZSET index: {size}")
        
        if size > 0:
            # Get latest 3 candles
            latest_ts = r.zrange(zset_key, -3, -1)
            print("   Latest 3 candles:")
            for ts in latest_ts:
                candle_key = f"md:candle:{symbol}:{tf}:{ts}"
                data = r.hgetall(candle_key)
                local_time = datetime.fromtimestamp(int(ts))
                print(f"   - {local_time} (Epoch {ts}) -> OHLCV: {data.get('open')}/{data.get('high')}/{data.get('low')}/{data.get('close')} | Vol: {data.get('volume')} | Status: {data.get('status')}")

if __name__ == "__main__":
    main()

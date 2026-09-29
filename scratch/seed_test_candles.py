import os
import redis
import time
from datetime import datetime, timedelta

def main():
    socket_path = '/Users/prana/Desktop/open_source/web/redis.sock'
    if os.path.exists(socket_path):
        r = redis.Redis(unix_socket_path=socket_path, decode_responses=True)
        print(f"Seeding via Unix Socket: {socket_path}")
    else:
        r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
        print("Seeding via TCP loopback")

    symbol = "NSE_INDEX|Nifty 50"
    timeframes = ["1m", "3m", "5m", "15m", "30m"]
    tf_minutes = {
        "1m": 1,
        "3m": 3,
        "5m": 5,
        "15m": 15,
        "30m": 30
    }
    
    # 1. Seed Spot keys
    r.set("spot:NIFTY", symbol)
    r.hset(f"md:quote:{symbol}", mapping={
        "symbol": symbol,
        "ltp": "23500.00",
        "close": "23480.00",
        "volume": "1500000",
        "ts_exchange": str(int(time.time() * 1000))
    })

    print("Generating and seeding 50 dummy candles for each timeframe...")
    
    for tf in timeframes:
        minutes = tf_minutes[tf]
        zset_key = f"md:candles:{symbol}:{tf}"
        r.delete(zset_key)
        
        # Cover enough historical bars for warm-up
        base_time = datetime.now() - timedelta(minutes=minutes * 50)
        base_price = 23450.0
        
        pipe = r.pipeline()
        for i in range(50):
            candle_time = base_time + timedelta(minutes=minutes * i)
            ts = int(candle_time.timestamp())
            
            # Build progressive up-trend with small variance
            o = base_price + (i * 1.5)
            h = o + 5.0
            l = o - 2.0
            c = o + 2.0
            v = 10000 + (i * 200)

            candle_key = f"md:candle:{symbol}:{tf}:{ts}"
            
            # Seed HSET
            pipe.hset(candle_key, mapping={
                "open": f"{o:.2f}",
                "high": f"{h:.2f}",
                "low": f"{l:.2f}",
                "close": f"{c:.2f}",
                "volume": str(v),
                "status": "historical"
            })
            
            # Add to ZSET index
            pipe.zadd(zset_key, {str(ts): ts})
            
        pipe.execute()
        print(f"✅ Seeding complete for '{tf}'! Added 50 candles to ZSET index '{zset_key}'.")

if __name__ == "__main__":
    main()

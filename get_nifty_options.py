import pandas as pd
import redis
import json
import sys
import os
from datetime import datetime

def main():
    print("📈 Upstox Nifty 50 Option Chain Finder")
    print("=======================================")
    
    # 1. Connect to Redis (prefer Unix domain socket for performance)
    socket_path = '/Users/prana/Desktop/open_source/web/redis.sock'
    try:
        if os.path.exists(socket_path):
            print(f"Connecting to Redis via Unix Domain Socket: {socket_path}...")
            r = redis.Redis(unix_socket_path=socket_path, decode_responses=True)
        else:
            print("Connecting to Redis via TCP loopback (127.0.0.1:6379)...")
            r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
            
        # Get Nifty 50 Index LTP from Redis HASH
        spot_data = r.hgetall("md:quote:NSE_INDEX|Nifty 50")
        if spot_data:
            spot_price = float(spot_data.get("ltp", 0.0))
            print(f"🔥 Live Nifty 50 Index Spot Price from Redis HASH: {spot_price}")
        else:
            print("⚠️ Nifty 50 Index Spot Quote not found in Redis HASH cache (is the collector running?)")
            spot_price = 22800.0 # Standard fallback
            print(f"Using default fallback spot price: {spot_price}")
    except Exception as e:
        print(f"⚠️ Redis connection failed ({e}). Using default spot price fallback: 22800.0")
        spot_price = 22800.0
        
    # 2. Load instruments list from online source
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    print(f"Loading instruments list from {url}...")
    try:
        nfo = pd.read_csv(url)
    except Exception as e:
        print(f"❌ Error downloading instruments list: {e}")
        sys.exit(1)
        
    # Filter Nifty 50 Index Options (OPTIDX)
    nifty_options = nfo[(nfo['name'] == 'NIFTY') & (nfo['instrument_type'] == 'OPTIDX')].copy()
    nifty_options['expiry_dt'] = pd.to_datetime(nifty_options['expiry'])
    
    # Sort and get unique expiries
    expiries = sorted(nifty_options['expiry_dt'].unique())
    
    # Filter out expiries that are already in the past or have expired today (passed 15:45)
    now = datetime.now()
    today = now.date()
    current_time_str = now.strftime("%H:%M")
    
    valid_expiries = []
    for exp in expiries:
        exp_date = pd.Timestamp(exp).date()
        if exp_date < today:
            continue
        elif exp_date == today:
            # If today is the expiry date and we are at or after 15:45, it is expired!
            if current_time_str >= "15:45":
                print(f"⚠️ Earliest expiry {exp_date} expired today. Shifting forward.")
                continue
        valid_expiries.append(exp)
        
    if len(valid_expiries) < 2:
        print(f"❌ Error: Found less than 2 valid expiries in NFO list after filtering out stale ones!")
        sys.exit(1)
        
    expiry_1 = pd.Timestamp(valid_expiries[0])
    expiry_2 = pd.Timestamp(valid_expiries[1])
    
    print(f"\n✅ Front Expiry Date Found: {expiry_1.strftime('%Y-%m-%d')}")
    print(f"✅ Second Expiry Date Found: {expiry_2.strftime('%Y-%m-%d')}")
    
    # 3. Filter strikes within +/- 3% around Spot price to keep list optimized and highly liquid
    lower_bound = spot_price * 0.97
    upper_bound = spot_price * 1.03
    print(f"Strikes Range Filter: {lower_bound:.1f} to {upper_bound:.1f} (Spot +/- 3%)")
    
    # Expiry 1 Option Keys
    exp1_opts = nifty_options[
        (nifty_options['expiry_dt'] == expiry_1) & 
        (nifty_options['strike'] >= lower_bound) & 
        (nifty_options['strike'] <= upper_bound)
    ]
    
    # Expiry 2 Option Keys
    exp2_opts = nifty_options[
        (nifty_options['expiry_dt'] == expiry_2) & 
        (nifty_options['strike'] >= lower_bound) & 
        (nifty_options['strike'] <= upper_bound)
    ]
    
    keys_1 = exp1_opts['instrument_key'].tolist()
    keys_2 = exp2_opts['instrument_key'].tolist()
    
    print(f"\nFront Expiry options count: {len(keys_1)}")
    print(f"Second Expiry options count: {len(keys_2)}")
    
    total_options = keys_1 + keys_2
    print(f"Total options selected: {len(total_options)}")
    
    # 4. Save to a JSON file so the C++ collector or other scripts can read it directly!
    output_path = 'nifty_option_symbols.json'
    with open(output_path, 'w') as out:
        json.dump({
            "index_key": "NSE_INDEX|Nifty 50",
            "expiry_1": expiry_1.strftime('%Y-%m-%d'),
            "expiry_2": expiry_2.strftime('%Y-%m-%d'),
            "spot_price": spot_price,
            "symbols": total_options
        }, out, indent=2)
        
    print(f"\n💾 Saved selected option symbols to: {output_path}")
    
    # 5. Seed Option Chain Maps & Spot Key directly to Redis
    try:
        print("\n🌱 Seeding option chain metadata maps to Redis...")
        expiry_1_str = expiry_1.strftime('%Y-%m-%d')
        expiry_2_str = expiry_2.strftime('%Y-%m-%d')
        
        # Seed spot index mapping
        r.set("spot:NIFTY", "NSE_INDEX|Nifty 50")
        
        # Map Expiry 1 (strike:CE/PE -> instrument_key)
        exp1_map = {}
        for _, row in exp1_opts.iterrows():
            strike_val = int(row['strike'])
            opt_type = row['option_type']
            exp1_map[f"{strike_val}:{opt_type}"] = row['instrument_key']
            
        r.delete(f"chain:NIFTY:{expiry_1_str}")
        if exp1_map:
            r.hset(f"chain:NIFTY:{expiry_1_str}", mapping=exp1_map)
            
        # Map Expiry 2
        exp2_map = {}
        for _, row in exp2_opts.iterrows():
            strike_val = int(row['strike'])
            opt_type = row['option_type']
            exp2_map[f"{strike_val}:{opt_type}"] = row['instrument_key']
            
        r.delete(f"chain:NIFTY:{expiry_2_str}")
        if exp2_map:
            r.hset(f"chain:NIFTY:{expiry_2_str}", mapping=exp2_map)
            
        print(f"✅ Successfully seeded `spot:NIFTY` and chain hashes:")
        print(f"   - `chain:NIFTY:{expiry_1_str}` ({len(exp1_map)} entries)")
        print(f"   - `chain:NIFTY:{expiry_2_str}` ({len(exp2_map)} entries)")
        
    except Exception as e:
        print(f"⚠️ Failed to seed Redis option chain mapping: {e}")

if __name__ == "__main__":
    main()

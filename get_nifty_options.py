import pandas as pd
import redis
import json
import sys
import os
import requests
from datetime import datetime

def get_spot_price(r, token, instrument_key, default_fallback):
    spot_price = None

    # Tier A: Try Redis HASH cache
    if r:
        try:
            spot_data = r.hgetall(f"md:quote:{instrument_key}")
            if spot_data and "ltp" in spot_data:
                spot_price = float(spot_data.get("ltp"))
                print(f"🔥 Live Spot Price for {instrument_key} from Redis HASH: {spot_price}")
        except Exception as e:
            print(f"⚠️ Redis spot quote check failed for {instrument_key}: {e}")

    # Tier B: Try Upstox API LTP Quote fallback
    if not spot_price and token:
        print(f"🔍 Spot Quote for {instrument_key} not found in Redis. Fetching live LTP from Upstox API...")
        try:
            url = f"https://api.upstox.com/v2/market-quote/ltp?instrument_key={requests.utils.quote(instrument_key)}"
            headers = {
                'Accept': 'application/json',
                'Authorization': f'Bearer {token}'
            }
            res = requests.get(url, headers=headers, timeout=5)
            if res.status_code == 200:
                res_data = res.json()
                data_dict = res_data.get("data", {})
                quote_data = data_dict.get(instrument_key) or data_dict.get(instrument_key.replace("|", ":"))
                spot_price = float(quote_data.get("last_price", 0.0)) if quote_data else 0.0
                if spot_price > 0:
                    print(f"🔥 Live Spot Price for {instrument_key} from Upstox API: {spot_price}")
                else:
                    spot_price = None
        except Exception as e:
            print(f"⚠️ Failed to fetch live spot price for {instrument_key} from Upstox API: {e}")

    # Tier C: Default fallback
    if not spot_price:
        spot_price = default_fallback
        print(f"⚠️ Redis and API lookup failed for {instrument_key}. Using default fallback: {spot_price}")

    return spot_price

def main():
    print("📈 Upstox Option Chain Finder (NIFTY & SENSEX)")
    print("=============================================")
    
    # Connect to Redis
    socket_path = '/Users/prana/Desktop/open_source/web/redis.sock'
    r = None
    try:
        if os.path.exists(socket_path):
            print(f"Connecting to Redis via Unix Domain Socket: {socket_path}...")
            r = redis.Redis(unix_socket_path=socket_path, decode_responses=True)
        else:
            print("Connecting to Redis via TCP loopback (127.0.0.1:6379)...")
            r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
        r.ping()
    except Exception as e:
        print(f"⚠️ Redis connection failed or inactive: {e}")

    # Load Access Token
    token = None
    token_path = '/Users/prana/Desktop/open_source/web/login/access_token.json'
    if os.path.exists(token_path):
        try:
            with open(token_path) as f:
                token = json.load(f).get('access_token')
        except Exception as e:
            print(f"⚠️ Failed to read access token: {e}")

    # 1. Resolve Spot Prices
    nifty_spot = get_spot_price(r, token, "NSE_INDEX|Nifty 50", 23500.0)
    sensex_spot = get_spot_price(r, token, "BSE_INDEX|SENSEX", 73500.0)

    # 2. Download Upstox complete instruments CSV
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    print(f"\nLoading instruments list from {url}...")
    try:
        nfo = pd.read_csv(url)
    except Exception as e:
        print(f"❌ Error downloading instruments list: {e}")
        sys.exit(1)

    underlyings = {
        "NIFTY": {
            "name": "NIFTY",
            "exchange": "NSE_FO",
            "index_key": "NSE_INDEX|Nifty 50",
            "spot_price": nifty_spot,
            "strike_increment": 50
        },
        "SENSEX": {
            "name": "SENSEX",
            "exchange": "BSE_FO",
            "index_key": "BSE_INDEX|SENSEX",
            "spot_price": sensex_spot,
            "strike_increment": 100
        }
    }

    output_data = {}
    now = datetime.now()
    today = now.date()
    current_time_str = now.strftime("%H:%M")

    for key, config in underlyings.items():
        print(f"\n--- Processing {key} Options ---")
        
        # Filter matching options
        options_df = nfo[
            (nfo['name'] == config['name']) & 
            (nfo['exchange'] == config['exchange']) & 
            (nfo['instrument_type'] == 'OPTIDX')
        ].copy()
        
        if options_df.empty:
            print(f"⚠️ No instruments found for underlying {key}!")
            continue

        options_df['expiry_dt'] = pd.to_datetime(options_df['expiry'])
        expiries = sorted(options_df['expiry_dt'].unique())

        # Filter out stale expiries
        valid_expiries = []
        for exp in expiries:
            exp_date = pd.Timestamp(exp).date()
            if exp_date < today:
                continue
            elif exp_date == today:
                if current_time_str >= "15:45":
                    print(f"   ⚠️ Expiry {exp_date} expired today at 15:45. Skipping.")
                    continue
            valid_expiries.append(exp)

        if len(valid_expiries) < 2:
            print(f"❌ Error: Less than 2 valid expiries for {key}!")
            continue

        expiry_1 = pd.Timestamp(valid_expiries[0])
        expiry_2 = pd.Timestamp(valid_expiries[1])
        expiry_1_str = expiry_1.strftime('%Y-%m-%d')
        expiry_2_str = expiry_2.strftime('%Y-%m-%d')

        print(f"   Front Expiry: {expiry_1_str}")
        print(f"   Second Expiry: {expiry_2_str}")

        # Filter strikes within +/- 3% around Spot price
        lower_bound = config['spot_price'] * 0.97
        upper_bound = config['spot_price'] * 1.03
        print(f"   Strikes Range (+/- 3%): {lower_bound:.1f} to {upper_bound:.1f}")

        exp1_opts = options_df[
            (options_df['expiry_dt'] == expiry_1) & 
            (options_df['strike'] >= lower_bound) & 
            (options_df['strike'] <= upper_bound)
        ]
        exp2_opts = options_df[
            (options_df['expiry_dt'] == expiry_2) & 
            (options_df['strike'] >= lower_bound) & 
            (options_df['strike'] <= upper_bound)
        ]

        keys_1 = exp1_opts['instrument_key'].tolist()
        keys_2 = exp2_opts['instrument_key'].tolist()
        total_symbols = keys_1 + keys_2
        print(f"   Selected option symbols: {len(total_symbols)} (Exp1: {len(keys_1)}, Exp2: {len(keys_2)})")

        output_data[key] = {
            "index_key": config['index_key'],
            "expiry_1": expiry_1_str,
            "expiry_2": expiry_2_str,
            "spot_price": config['spot_price'],
            "symbols": total_symbols
        }

        # Seed option chain metadata directly to Redis
        if r:
            try:
                # Seed spot index mapping
                r.set(f"spot:{key}", config['index_key'])
                r.set("spot:VIX", "NSE_INDEX|India VIX")

                # Map Expiry 1 (strike:CE/PE -> instrument_key)
                exp1_map = {}
                for _, row in exp1_opts.iterrows():
                    strike_val = int(row['strike'])
                    opt_type = row['option_type']
                    exp1_map[f"{strike_val}:{opt_type}"] = row['instrument_key']
                
                r.delete(f"chain:{key}:{expiry_1_str}")
                if exp1_map:
                    r.hset(f"chain:{key}:{expiry_1_str}", mapping=exp1_map)

                # Map Expiry 2
                exp2_map = {}
                for _, row in exp2_opts.iterrows():
                    strike_val = int(row['strike'])
                    opt_type = row['option_type']
                    exp2_map[f"{strike_val}:{opt_type}"] = row['instrument_key']

                r.delete(f"chain:{key}:{expiry_2_str}")
                if exp2_map:
                    r.hset(f"chain:{key}:{expiry_2_str}", mapping=exp2_map)

                print(f"   🌱 Seeded `spot:{key}` and chain maps for expiries: {expiry_1_str}, {expiry_2_str}")
            except Exception as e:
                print(f"   ⚠️ Redis seeding failed for {key}: {e}")

    # 4. Save output_data dictionary to JSON file
    output_path = 'nifty_option_symbols.json'
    try:
        with open(output_path, 'w') as out:
            json.dump(output_data, out, indent=2)
        print(f"\n💾 Saved selected options config to: {output_path}")
    except Exception as e:
        print(f"❌ Failed to save config to JSON: {e}")

if __name__ == "__main__":
    main()

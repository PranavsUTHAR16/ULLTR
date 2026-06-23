# forward_tester_twap/seed_clickhouse.py
import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta, date
import clickhouse_connect

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load database config
from config import CLICKHOUSE_CONFIG

def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_CONFIG['host'],
        port=CLICKHOUSE_CONFIG['port'],
        username=CLICKHOUSE_CONFIG['username'],
        password=CLICKHOUSE_CONFIG['password'],
        database=CLICKHOUSE_CONFIG['database']
    )

def fetch_nifty_candles(token, start_date, end_date):
    """Fetches Nifty 50 1m candles in blocks of 7 days to avoid payload limits."""
    symbol = "NSE_INDEX|Nifty 50"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    current_end = end_date
    all_candles = []
    
    while current_end > start_date:
        current_start = max(start_date, current_end - timedelta(days=7))
        from_str = current_start.strftime("%Y-%m-%d")
        to_str = current_end.strftime("%Y-%m-%d")
        
        url = f"https://api.upstox.com/v3/historical-candle/{requests.utils.quote(symbol)}/minutes/1/{to_str}/{from_str}"
        print(f"Fetching Nifty candles from {from_str} to {to_str}...")
        
        try:
            res = requests.get(url, headers=headers)
            if res.status_code == 200:
                candles = res.json().get("data", {}).get("candles", [])
                all_candles.extend(candles)
                print(f"  Retrieved {len(candles)} candles.")
            else:
                print(f"  ⚠️ Error fetching candles: {res.text}")
        except Exception as e:
            print(f"  ⚠️ Request failed: {e}")
            
        current_end = current_start - timedelta(days=1)
        
    return all_candles

def fetch_vix_candles(token, start_date, end_date):
    """Fetches daily India VIX candles."""
    symbol = "NSE_INDEX|India VIX"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    from_str = start_date.strftime("%Y-%m-%d")
    to_str = end_date.strftime("%Y-%m-%d")
    url = f"https://api.upstox.com/v3/historical-candle/{requests.utils.quote(symbol)}/days/1/{to_str}/{from_str}"
    
    print(f"Fetching Daily India VIX from {from_str} to {to_str}...")
    try:
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            candles = res.json().get("data", {}).get("candles", [])
            print(f"  Retrieved {len(candles)} VIX daily records.")
            return candles
        else:
            print(f"  ⚠️ Error fetching VIX: {res.text}")
    except Exception as e:
        print(f"  ⚠️ Request failed: {e}")
    return []

def main():
    token_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "login", "access_token.json")
    if not os.path.exists(token_path):
        print(f"❌ Error: access_token.json missing at {token_path}")
        return
        
    with open(token_path, "r") as f:
        creds = json.load(f)
    token = creds.get("access_token")
    if not token:
        print("❌ Error: access_token field missing in JSON.")
        return
        
    ch_client = get_clickhouse_client()
    print("🚀 Connected to ClickHouse.")
    
    # 1. Setup tables
    ch_client.command("""
        CREATE TABLE IF NOT EXISTS nifty (
            timestamp DateTime,
            open Float64,
            high Float64,
            low Float64,
            close Float64
        ) ENGINE = MergeTree()
        ORDER BY timestamp
    """)
    
    ch_client.command("""
        CREATE TABLE IF NOT EXISTS vix (
            date Date,
            vix_close Float64,
            vix_1d_change Float64
        ) ENGINE = MergeTree()
        ORDER BY date
    """)
    
    today = datetime.now()
    start_date = today - timedelta(days=90)
    
    # 2. Fetch and Insert Nifty
    nifty_raw = fetch_nifty_candles(token, start_date, today)
    if nifty_raw:
        rows = []
        seen = set()
        for c in nifty_raw:
            ts_str = c[0]
            # Upstox returns ISO time e.g., '2026-06-23T14:03:00+05:30'
            try:
                # Truncate offset for standard parsing
                dt = datetime.fromisoformat(ts_str.split("+")[0])
                if dt in seen:
                    continue
                seen.add(dt)
                rows.append({
                    'timestamp': dt,
                    'open': float(c[1]),
                    'high': float(c[2]),
                    'low': float(c[3]),
                    'close': float(c[4])
                })
            except Exception as e:
                print(f"Error parsing candle {c}: {e}")
                
        df_nifty = pd.DataFrame(rows).sort_values('timestamp').reset_index(drop=True)
        # Clear existing to prevent duplicate seeding
        min_ts = df_nifty['timestamp'].min().strftime('%Y-%m-%d %H:%M:%S')
        ch_client.command(f"ALTER TABLE nifty DELETE WHERE timestamp >= '{min_ts}'")
        ch_client.insert('nifty', df_nifty)
        print(f"✅ Successfully inserted {len(df_nifty)} Nifty 1m candles into ClickHouse.")
        
    # 3. Fetch and Insert VIX
    vix_raw = fetch_vix_candles(token, start_date, today)
    if vix_raw:
        rows = []
        for c in vix_raw:
            ts_str = c[0]
            try:
                dt = datetime.fromisoformat(ts_str.split("+")[0]).date()
                rows.append({
                    'date': dt,
                    'vix_close': float(c[4])
                })
            except Exception as e:
                print(f"Error parsing VIX {c}: {e}")
                
        df_vix = pd.DataFrame(rows).sort_values('date').reset_index(drop=True)
        df_vix['vix_1d_change'] = df_vix['vix_close'].diff().fillna(0.0)
        
        # Clear existing to prevent duplicate seeding
        min_date = df_vix['date'].min().strftime('%Y-%m-%d')
        ch_client.command(f"ALTER TABLE vix DELETE WHERE date >= '{min_date}'")
        ch_client.insert('vix', df_vix)
        print(f"✅ Successfully inserted {len(df_vix)} VIX daily records into ClickHouse.")

if __name__ == "__main__":
    main()

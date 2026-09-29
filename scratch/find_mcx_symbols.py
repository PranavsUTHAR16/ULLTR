import pandas as pd
import gzip
import io
import requests

def main():
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    print(f"Loading Upstox instrument master from {url}...")
    
    response = requests.get(url)
    with gzip.open(io.BytesIO(response.content), 'rt') as f:
        df = pd.read_csv(f)
        
    print(f"Total instruments loaded: {len(df)}")
    
    # Filter for Crude Oil (with space) in MCX_FO
    futures = df[
        (df['exchange'] == 'MCX_FO') & 
        (df['name'] == 'CRUDE OIL') & 
        (df['instrument_type'] == 'FUTCOM')
    ]
    
    print("\n🔥 Deployed MCX Crude Oil Futures (FUTCOM):")
    print("===========================================")
    
    # Sort chronologically by expiry to get the front contract
    futures = futures.copy()
    futures['expiry_dt'] = pd.to_datetime(futures['expiry'])
    futures = futures.sort_values('expiry_dt')
    
    for idx, row in futures.iterrows():
        print(f"Key: {row['instrument_key']} | Symbol: {row['tradingsymbol']} | Expiry: {row['expiry']} | Type: {row['instrument_type']}")

if __name__ == "__main__":
    main()

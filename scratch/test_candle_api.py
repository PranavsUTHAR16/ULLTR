import os
import json
import requests
import pandas as pd

def main():
    token_path = "/Users/prana/Desktop/open_source/web/login/access_token.json"
    with open(token_path) as f:
        token = json.load(f)["access_token"]
        
    symbol = "NSE_INDEX|Nifty 50" # Let's try Spot Nifty index
    
    # Try fetching intraday
    url = f"https://api.upstox.com/v3/historical-candle/intraday/{symbol}/minutes/1"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    print(f"Calling: {url}")
    response = requests.get(url, headers=headers)
    print(f"Status Code: {response.status_code}")
    if response.status_code == 200:
        res = response.json()
        print(f"Success! Status: {res.get('status')}")
        candles = res.get("data", {}).get("candles", [])
        print(f"Total intraday candles fetched: {len(candles)}")
        
    # Try fetching historical for last week
    url_hist = f"https://api.upstox.com/v3/historical-candle/{symbol}/minutes/1/2026-05-29/2026-05-25"
    print(f"Calling: {url_hist}")
    response_hist = requests.get(url_hist, headers=headers)
    print(f"Status Code: {response_hist.status_code}")
    if response_hist.status_code == 200:
        res = response_hist.json()
        print(f"Success! Status: {res.get('status')}")
        candles = res.get("data", {}).get("candles", [])
        print(f"Total historical candles fetched: {len(candles)}")
        if candles:
            print("First 3 candles:")
            for c in candles[:3]:
                print(c)
            print("Last 3 candles:")
            for c in candles[-3:]:
                print(c)
    else:
        print(response.text)

if __name__ == "__main__":
    main()

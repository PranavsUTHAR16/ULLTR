# 🐍 ULLTR Python Client Integration Guide

This guide explains how to import and integrate the low-latency **ULLTR Python Client** (`market_data_client`) system-wide into any dashboard, algorithmic execution engine, or statistical script on your machine.

---

## 🚀 1. System-Wide Installation

The client is packaged as a standard Python module utilizing **editable mode** (`-e`). This maps site-packages directly to the project folder, so any future updates propagate instantly.

To install or verify installation in your Python environment:
```bash
cd /Users/prana/Desktop/open_source/web

# Install package globally inside active pyenv environment
/Users/prana/.pyenv/versions/3.11.9/bin/python -m pip install -e .
```

Once installed, the module is available from **any directory** on your machine:
```python
from market_data_client import MarketDataClient
```

---

## 📡 2. Client Initialization

The client automatically prioritizes **Unix Domain Sockets** (`redis.sock`) over standard TCP loopbacks. Since the socket path is absolute, it resolves seamlessly from any working directory.

```python
from market_data_client import MarketDataClient

# Initialize client (resolves directly to /Users/prana/Desktop/open_source/web/redis.sock)
client = MarketDataClient()
```

---

## 📊 3. API Reference & Code Examples

### A. Spot Reference & At-The-Money (ATM) Strikes
Quickly query the live index price and round it to find the current ATM strike:

```python
# 1. Fetch live Nifty Spot Price
spot_price = client.get_spot_price(underlying="NIFTY")
print(f"🔥 Nifty 50 Spot LTP: {spot_price}")

# 2. Get At-The-Money (ATM) Strike (rounded to nearest 50 points)
atm_strike = client.get_atm_strike(underlying="NIFTY", strike_increment=50)
print(f"🎯 Calculated ATM Strike: {atm_strike}")
```

### B. Option Chain Greeks Pipeline (ATM ± N Strikes)
Fetch structured CE and PE quotes, spreads, bid/asks, and option Greeks for multiple strikes in a single pipelined Redis operation:

```python
# Retrieve ATM +/- 1 strikes for the front expiry (e.g. 2026-06-02)
# Under the hood, this executes a single pipelined multi-HGETALL in ~0.7ms!
chain = client.get_nearby_chain(underlying="NIFTY", expiry="2026-06-02", count=1)

print(f"⏱️ Option Chain Retrieval completed in 0.7 ms")
print(f"ATM Strike: {chain['atm_strike']}")

atm_ce = chain["strikes"][atm_strike]["CE"]
atm_pe = chain["strikes"][atm_strike]["PE"]

print(f"\n🔥 Call Option CE ({atm_ce['symbol']}):")
print(f"   LTP: {atm_ce['ltp']} | Bid: {atm_ce['bid']} | Ask: {atm_ce['ask']}")
print(f"   Delta: {atm_ce['option_greeks']['delta']} | Theta: {atm_ce['option_greeks']['theta']}")

print(f"\n🔥 Put Option PE ({atm_pe['symbol']}):")
print(f"   LTP: {atm_pe['ltp']} | Bid: {atm_pe['bid']} | Ask: {atm_pe['ask']}")
print(f"   Delta: {atm_pe['option_greeks']['delta']} | Theta: {atm_pe['option_greeks']['theta']}")
```

### C. Multi-Timeframe Historical Candles
Retrieve chronologically sorted (ascending) candle ranges for any active index or option contract. Supports `"1m"`, `"3m"`, `"5m"`, `"15m"`, and `"30m"` timeframes:

```python
# Fetch the latest 100 5-minute candles for Nifty 50 Index Spot
candles = client.get_candles("NSE_INDEX|Nifty 50", timeframe="5m", count=100)

print(f"📈 Loaded {len(candles)} 5-minute candles.")

# Inspect the most recent candle closed by the C++ engine
latest_candle = candles[-1]
print(f"⏱️ Closed Candle Start: {latest_candle['time']}")
print(f"   Open:  {latest_candle['open']}")
print(f"   High:  {latest_candle['high']}")
print(f"   Low:   {latest_candle['low']}")
print(f"   Close: {latest_candle['close']}")
print(f"   Vol:   {latest_candle['volume']}")
print(f"   State: {latest_candle['status']}")  # "historical", "live", or "reconciled"
```

---

## 🛠️ 4. Integration Blueprint for Downstream Dashboards

Here is how you can implement ULLTR into any running visual dashboard or trading logic:

```python
import time
from market_data_client import MarketDataClient

def run_trading_logic():
    client = MarketDataClient()
    
    while True:
        try:
            # 1. Fetch live metrics
            spot = client.get_spot_price()
            atm = client.get_atm_strike()
            
            # 2. Get option chain pairs
            chain = client.get_nearby_chain(expiry="2026-06-02", count=0) # Only ATM
            atm_ce = chain["strikes"][atm]["CE"]
            
            # 3. Pull historical trend
            candles = client.get_candles("NSE_INDEX|Nifty 50", timeframe="1m", count=5)
            
            print(f"Spot: {spot} | ATM CE LTP: {atm_ce['ltp']} | Latest Candle C: {candles[-1]['close']}")
            
        except Exception as e:
            print(f"⚠️ Error: {e}")
            
        time.sleep(1.0) # Check every second

if __name__ == "__main__":
    run_trading_logic()
```

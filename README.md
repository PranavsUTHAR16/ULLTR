# ⚡ ULLTR: Ultra-Low Latency Ticks & Option Reconciliation

An ultra-low latency, pure in-memory, decoupled market-data platform designed for Indian index & options algorithmic trading. By isolating broker-specific protocols, the platform achieves sub-millisecond local data retrieval using a C++ native WebSocket collector, Protobuf decoding, and a direct Unix Domain Socket pipeline to a custom-configured Redis database.

---

## 📡 1. High-Performance Architecture

```text
                               ┌────────────────────────┐
                               │ Upstox Market Feed WS  │
                               └───────────┬────────────┘
                                           │ (Protobuf Stream)
                                           ▼
                      ┌─────────────────────────────────────────┐
                      │    C++ Ingestion Ingestor (collector)    │
                      ├─────────────────────────────────────────┤
                      │ • Decodes Protobuf V3 Greeks & Ticks    │
                      │ • C++ Candle Resampler (1m to 30m)      │
                      │ • C++ Self-Healing Reconciliation Loop  │
                      └────────────────────┬────────────────────┘
                                           │
                        (Unix Socket Local IPC / redis.sock)
                                           │
                                           ▼
                      ┌─────────────────────────────────────────┐
                      │    In-Memory Pure Cache (Redis)         │
                      ├─────────────────────────────────────────┤
                      │ • Ticks & option Greeks (HASH)          │
                      │ • Chain strike maps & Spot references   │
                      │ • Multi-timeframe OHLCV candles (HASH)  │
                      │ • Chronological epoch index (ZSET)      │
                      └────────────┬───────────────────────┬────┘
                                   │                       │
           (Sub-600µs Direct Read) │                       │ (Pub/Sub Stream)
                                   ▼                       ▼
            ┌─────────────────────────────┐        ┌──────────────────────────────┐
            │   Python Client Client      │        │ FastAPI Gateway Server (API) │
            │    (MarketDataClient)       │        │  (FastAPI WebSocket / REST)  │
            └─────────────────────────────┘        └──────────────────────────────┘
```

---

## 📂 2. Repository Architecture & Layout

This project follows a decoupled, modular design to ensure strict separation of concerns, high stability, and maintainability:

```text
market-data-platform/
  ├── collector/                      # Native C++ Ingestion Ingestor
  │     ├── src/
  │     │    ├── main.cpp             # Ingestor class, WebSocket loops, Protobuf parsing
  │     │    ├── candle_manager.hpp   # C++ Candle structures & class headers
  │     │    └── candle_manager.cpp   # Seeding resampler, HTTPS client, Reconciliation thread
  │     ├── proto/
  │     │    ├── MarketDataFeedV3.proto # Official Upstox V3 Feed schema
  │     │    └── MarketDataFeedV3.pb.cc # Compiled Protobuf classes
  │     ├── CMakeLists.txt            # CMake compilation specifications
  │     └── config.json               # Ingestor configuration (symbols, Redis ports, token file)
  │
  ├── login/                          # TOTP-Based Playwright Authentication
  │     ├── auth.py                   # Playwright automation script to get OAuth tokens
  │     ├── .env                      # API Credentials (TOTP keys, PIN, client secret)
  │     └── access_token.json         # Single source of truth Bearer token (JSON format)
  │
  ├── api/                            # Downstream Gateway Gateway
  │     ├── app/
  │     │    └── main.py              # FastAPI async server (REST quotes, batch lists, Pub/Sub WS)
  │     └── requirements.txt          # Python FastAPI server requirements
  │
  ├── redis.conf                      # In-memory performance-optimized Redis socket configuration
  ├── get_nifty_options.py            # Extracts active ATM Nifty 50 CE/PE strikes (+/- 3%)
  ├── expiry_manager.py               # Expiry rollover daemon & morning setup calendar scheduler
  ├── market_data_client.py           # Sub-millisecond direct Unix socket Python client
  └── README.md                       # Comprehensive system documentation (this file)
```

---

## 💾 3. Pure In-Memory Redis Schema

All data structures are stored as highly optimized Redis keys to ensure lock-free concurrent access at raw speed:

### A. Spot Reference
- **Key**: `spot:{underlying}` (e.g. `spot:NIFTY`)
- **Type**: `string`
- **Value**: `"NSE_INDEX|Nifty 50"`

### B. Option Chain Expiry Map
- **Key**: `chain:{underlying}:{expiry}` (e.g. `chain:NIFTY:2026-06-02`)
- **Type**: `HASH`
- **Fields**: Mappings of `strike:option_type` -> `instrument_key`
  - `"23500:CE"` -> `"NSE_FO|57012"`
  - `"23500:PE"` -> `"NSE_FO|57013"`

### C. Live In-Memory Quote Snapshot
- **Key**: `md:quote:{instrument_key}` (e.g. `md:quote:NSE_FO|57032`)
- **Type**: `HASH`
- **Fields & Values**:
  - `symbol`: `"NSE_FO|57032"`
  - `ltp` / `close`: Float strings (e.g. `"243.15"`, `"241.05"`)
  - `volume` / `oi` / `iv`: Raw metric numeric values
  - `bid` / `bid_qty` / `ask` / `ask_qty`: Best bid-ask spread parameters
  - `ts_exchange` / `ts_recv`: Unix epoch timestamps in milliseconds
  - `delta` / `theta` / `gamma` / `vega` / `rho`: Decoded Protobuf Option Greeks

### D. Multi-Timeframe OHLCV Candles
- **Key**: `md:candle:{symbol}:{timeframe}:{ts_epoch}` (e.g. `md:candle:NSE_INDEX|Nifty 50:5m:1780048620`)
- **Type**: `HASH`
- **Fields**:
  - `open`, `high`, `low`, `close`: Float strings
  - `volume`: Incremental trade volume in this interval
  - `status`: Lifecycle flags (`"historical"`, `"live"`, or `"reconciled"`)
- **Chronological Index**: `md:candles:{symbol}:{timeframe}` (type: `ZSET`), containing timestamps as both members and scores. Used for instant sliding range queries.

---

## ⚡ 4. Native C++ Candle Pipeline

The candle pipeline operates natively inside the **C++ Ingestor** to guarantee zero I/O latency:

1. **Historical Seeding (Catch-up)**:
   - On startup, queries the Upstox V3 historical and intraday HTTPS APIs using a high-performance **Boost.Beast HTTPS Client** directly in C++.
   - Implements a timezone-independent UTC epoch parser and URL encoder to download `1m` candle data for exactly the last **5 trading days** (automatically adjusting for holidays/weekends).
   - Dynamically resamples/aggregates `1m` data into `3m`, `5m`, `15m`, and `30m` candles in C++ and pipes them to Redis in multi-exec batches.
2. **Real-Time Aggregation**:
   - Listens to decoded WebSocket ticks. Calculates timeframe floor boundaries.
   - Computes exact incremental volumes: `inc_vol = current_tick_volume - last_volume`.
   - Modifies active candles in-memory and executes O(1) pipelined updates to Redis.
3. **Self-Healing Reconciliation**:
   - A background C++ thread monitors closed 1m candles, waits for broker API sync, fetches the official Upstox API minute candle, and checks it against Redis.
   - If a discrepancy (dropped tick/missed volume) is detected, it overwrites the Redis keys and cascades recalculations to correct the parent `3m`, `5m`, `15m`, and `30m` candles.

---

## ⚙️ 5. Compilation, Configuration & Running

### Prerequisites
- macOS/Linux environment
- Boost (Asio, Beast), hiredis, Protobuf, OpenSSL development libraries
- Python (with `redis`, `pandas`, `playwright`, `requests`)

### A. Compilation (C++ Ingestor)
Ensure you are inside the `collector/build` folder:
```bash
cd collector
mkdir -p build && cd build
cmake ..
make -j4
```

### B. High-Performance Redis Startup
Shut down standard default Redis server configurations and launch using our optimal memory-only config:
```bash
# Shutdown running redis instances
redis-cli shutdown || true

# Start local Unix Socket optimized daemon
redis-server redis.conf --daemonize yes
```

### C. Live Token Generation (Playwright)
Run the automated authenticator inside the `login` folder:
```bash
cd login
# If running for the first time, install playwright chromium browsers
playwright install chromium
python auth.py
```
This saves a fresh token file to `login/access_token.json` which is referenced by the C++ config and python utilities.

### D. Running the Expiry & Setup Daemon
The Expiry Daemon runs in the background. It updates option strike mappings every day at **08:45 AM**, and triggers contract rollover automatically at **15:45 PM** on expiry days:
```bash
python expiry_manager.py
```

### E. Launching the C++ Ingestor
Run the compiled binary passing the path of the config:
```bash
./collector ../config.json
```

---

## 🐍 6. Pipelined Python Client Usage

The platform provides a high-level programmatic client `MarketDataClient` in [market_data_client.py](file:///Users/prana/Desktop/open_source/web/market_data_client.py) that bypasses network sockets and resolves data over Unix Domain sockets:

```python
from market_data_client import MarketDataClient

# Connect to Unix domain socket
client = MarketDataClient()

# 1. Fetch live index spot price
spot = client.get_spot_price("NIFTY")
print(f"Index Spot: {spot}")

# 2. Get At-The-Money (ATM) strike price
atm = client.get_atm_strike("NIFTY", strike_increment=50)
print(f"ATM Strike: {atm}")

# 3. Retrieve pipelined quotes & Greeks for ATM +/- 1 strikes in 0.5 milliseconds
chain = client.get_nearby_chain(underlying="NIFTY", expiry="2026-06-02", count=1)
print(chain["strikes"][atm]["CE"]) # Options quote + Greeks

# 4. Retrieve chronological historical candle series
candles_5m = client.get_candles("NSE_INDEX|Nifty 50", timeframe="5m", count=100)
for candle in candles_5m[-3:]:
    print(f"{candle['time']} | O: {candle['open']} | H: {candle['high']} | C: {candle['close']}")
```

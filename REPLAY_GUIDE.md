# 🎬 ULLTR Market Replay System Guide

A comprehensive guide to running the **ULLTR Tick-by-Tick Market Replay Engine** from ClickHouse Cloud into your local active Redis instance for backtesting, strategy validation, and forward-testing off-market hours.

---

## 📡 1. Overview & Connection Architecture

The Market Replay Engine acts as a **virtual live exchange feeder**. It streams historical tick-by-tick market data (Spot Index, Option Chains, Greeks, and L2 Depth) from **ClickHouse Cloud** directly into your **local Redis instance**.

Any downstream client (strategy engines, `ForwardTestDataClient`, WebSockets, dashboards) connects to Redis exactly as it would during live market hours—**with zero code changes**.

```text
 ┌────────────────────────────────────────────────────────┐
 │        ClickHouse Cloud (market_ticks table)           │
 │     154M+ ticks (Spot, Options, Greeks, 5-tier Depth)  │
 └───────────────────────────┬────────────────────────────┘
                             │ Chunked Chronological Stream
                             ▼
 ┌────────────────────────────────────────────────────────┐
 │            ULLTR Market Replay Engine                  │
 │  (market_replay_engine.py / scripts/market_replay.py)  │
 │  • Drift-Free Wall-Clock Pacer (1x, 5x, 10x, Max Burst)│
 │  • Multi-Timeframe Candle Aggregator (1m, 3m, 5m, 15m) │
 └───────────────────────────┬────────────────────────────┘
                             │ Local IPC (Auto-detected)
                             ▼
 ┌────────────────────────────────────────────────────────┐
 │         Current Active Local Redis Instance            │
 │  • Unix Socket: /Users/prana/Desktop/open_source/      │
 │                 web/redis.sock (Sub-300µs IPC)         │
 │  • TCP Fallback: 127.0.0.1:6379 (Active local server)  │
 └──────┬──────────────────────┬───────────────────┬──────┘
        │                      │                   │
        ▼                      ▼                   ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────┐
│ Forward Tester  │   │  C++ Strategy   │   │ Dashboards  │
│(forward_tester/)│   │ Engine & Models │   │  & Web APIs │
└─────────────────┘   └─────────────────┘   └─────────────┘
```

### ✅ Does this play into your currently active local Redis?
**Yes!**
- The engine automatically inspects the local machine:
  1. If the Unix domain socket `/Users/prana/Desktop/open_source/web/redis.sock` exists, it uses it for sub-millisecond local IPC.
  2. Otherwise, it automatically connects to your active local TCP Redis server at **`127.0.0.1:6379`**.
- Downstream tools (`market_data_client.py`, `forward_tester/data_client.py`, etc.) follow the exact same fallback order, guaranteeing that the replay engine and your strategies are always talking to the **same local Redis database**.

---

## 💾 2. What Data Is Populated into Redis During Replay?

| Key Pattern | Redis Type | Contents & Fields |
| :--- | :--- | :--- |
| `spot:{underlying}` | `STRING` | Pointer to spot symbol (e.g., `spot:NIFTY` $\rightarrow$ `NSE_INDEX\|Nifty 50`). |
| `chain:{underlying}:{expiry}` | `HASH` | Full option chain mapping: `strike:type` $\rightarrow$ `instrument_key` (e.g. `23700:CE` $\rightarrow$ `NSE_FO\|42627`). |
| `md:quote:{sym}` & `quote:{sym}` | `HASH` | Real-time quote snapshot: `ltp`, `close`, `bid`, `bid_qty`, `ask`, `ask_qty`, `volume`, `oi`, `delta`, `theta`, `gamma`, `vega`, `rho`, `ts_exchange`, `ts_recv`. |
| `md:candle:{sym}:{tf}:{ts}` | `HASH` | OHLCV bar for `1m`, `3m`, `5m`, `15m`, `30m`: `open`, `high`, `low`, `close`, `volume`, `status` (`historical` / `live`). |
| `md:candles:{sym}:{tf}` | `ZSET` | Chronological epoch timestamp index for instant range queries. |
| `md:stream:all` | `Pub/Sub` | JSON stream of every incoming tick (for real-time WebSocket clients). |
| `md:reco:trigger` | `Pub/Sub` | Fired when a 1-minute bar closes (`<symbol>:<timestamp>`), signaling strategy recalculations. |

---

## 🚀 3. How to Use: Command Reference

The replay engine is located at [market_replay_engine.py](file:///Users/prana/Desktop/open_source/web/market_replay_engine.py) and wrapped by [scripts/market_replay.py](file:///Users/prana/Desktop/open_source/web/scripts/market_replay.py).

### A. Accelerated Replay (10x Speed - Recommended for testing)
Plays back market data at 10 times real-time speed (e.g., a 1-hour session replays in 6 minutes):
```bash
python3 market_replay_engine.py --date 2026-09-08 --underlying NIFTY --speed 10
```

### B. True 1x Real-Time Playback
Simulates real live market speed with microsecond precision:
```bash
python3 market_replay_engine.py --date 2026-09-08 --underlying NIFTY --speed 1.0
```

### C. Maximum Burst Speed (Fastest Possible Backtest)
Unthrottles playback to process thousands of ticks per second as fast as Redis can ingest:
```bash
python3 market_replay_engine.py --date 2026-09-08 --underlying NIFTY --speed max
```

### D. Specific Time Window
Replay only the market open volatility (e.g. 09:15 to 10:00 IST):
```bash
python3 market_replay_engine.py --date 2026-09-08 --start-time 09:15:00 --end-time 10:00:00 --speed 5
```

### E. Lightweight ATM Strike Filtering
To replay only ATM $\pm 5$ strikes instead of all contracts across the chain:
```bash
python3 market_replay_engine.py --date 2026-09-08 --underlying NIFTY --strikes-range 5 --speed 10
```

### F. Replaying SENSEX
```bash
python3 market_replay_engine.py --date 2026-09-08 --underlying SENSEX --speed 10
```

---

## 🛠️ 4. Full Dual-Terminal Workflow: Testing Strategies

To run and observe your algorithmic trading strategies or daemons against the replayed market:

### Step 1: In Terminal 1 — Start the Replay Feeder
```bash
cd /Users/prana/Desktop/open_source/web
git checkout ulltr-replay

# Start replaying a historical session (e.g. September 8, 2026) at 10x speed
python3 market_replay_engine.py --date 2026-09-08 --underlying NIFTY --speed 10
```

### Step 2: In Terminal 2 — Run Forward-Tester, Strategy, or Dashboard
While Terminal 1 is streaming ticks, run any downstream consumer:

#### Option A: Run Forward-Tester Models
```bash
cd /Users/prana/Desktop/open_source/web
python3 forward_tester/run.py
```

#### Option B: Run End-to-End Parity Verification
```bash
cd /Users/prana/Desktop/open_source/web
python3 scripts/market_replay.py --date 2026-09-08 --mode verify
```

#### Option C: Live Inspection via Python Client
```python
from forward_tester.data_client import ForwardTestDataClient

client = ForwardTestDataClient()

# Check live spot
spot = client.get_spot_price("NIFTY")
print(f"Index Spot: ₹{spot:,.2f}")

# Check ATM strike & Greeks
atm = client.get_atm_strike("NIFTY")
exp = client.get_front_expiry("NIFTY")
chain = client.get_option_chain_quotes("NIFTY", exp, count=1)
call_opt = chain["strikes"][atm]["CE"]
print(f"ATM Call: {call_opt['symbol']} | LTP: ₹{call_opt['ltp']} | Delta: {call_opt['option_greeks']['delta']}")

# Inspect multi-timeframe candles generated on the fly
candles_5m = client.client.get_candles("NSE_INDEX|Nifty 50", timeframe="5m", count=5)
for c in candles_5m:
    print(f"{c['time']} | O: {c['open']} | H: {c['high']} | L: {c['low']} | C: {c['close']}")
```

---

## 🔍 5. Verifying Live Redis Activity

You can monitor Redis in real time while the replay is active:

```bash
# Check key count growth
redis-cli dbsize

# Monitor spot price updates live
redis-cli get spot:NIFTY
redis-cli hgetall "md:quote:NSE_INDEX|Nifty 50"

# Inspect the latest 1-minute candle
redis-cli zrevrange "md:candles:NSE_INDEX|Nifty 50:1m" 0 0

# Listen to the live Pub/Sub tick stream
redis-cli subscribe md:stream:all
```

---

## ⚙️ 6. Command-Line Arguments Reference

| Argument | Default | Description |
| :--- | :--- | :--- |
| `--date` | `2026-09-08` | Trading date to replay (`YYYY-MM-DD`). |
| `--underlying` | `NIFTY` | Underlying index (`NIFTY` or `SENSEX`). |
| `--speed` | `max` | Playback speed (`1.0` for real-time, `5.0`, `10.0`, `50.0`, or `max` for burst). |
| `--start-time` | `09:15:00` | Start time of replay session (`HH:MM:SS`). |
| `--end-time` | `15:30:00` | End time of replay session (`HH:MM:SS`). |
| `--batch-size` | `1000` | Number of Redis commands executed per pipeline flush. |
| `--strikes-range` | `None` (All) | Filter option chain to ATM $\pm N$ strikes. |
| `--no-pubsub` | `False` | Disables publishing to `md:stream:all` to maximize raw throughput. |
| `--redis-socket` | `.../redis.sock` | Path to Unix domain socket. |
| `--redis-host` | `127.0.0.1` | Local Redis TCP host. |
| `--redis-port` | `6379` | Local Redis TCP port. |

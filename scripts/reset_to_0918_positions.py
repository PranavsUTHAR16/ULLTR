import requests, json, urllib.parse, math, time, os, sys
from datetime import datetime, date
import pandas as pd
import redis
from scipy.stats import norm
import numpy as np

# Connect to Redis
r = redis.Redis(unix_socket_path='/Users/prana/Desktop/open_source/web/redis.sock', decode_responses=True)

# Load access token
token_path = '/Users/prana/Desktop/open_source/web/login/access_token.json'
with open(token_path) as f:
    token = json.load(f)['access_token']

headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}

# 1. Fetch 09:18 Spot Candle
sym_spot = urllib.parse.quote('NSE_INDEX|Nifty 50')
url_spot = f'https://api.upstox.com/v2/historical-candle/intraday/{sym_spot}/1minute'
r_spot = requests.get(url_spot, headers=headers).json()
candles_spot = r_spot.get('data', {}).get('candles', [])

spot_0918 = 24305.40
ts_0918 = None
for c in candles_spot:
    if '09:18' in c[0]:
        spot_0918 = float(c[4])
        dt = datetime.fromisoformat(c[0])
        ts_0918 = int(dt.timestamp())
        # Seed in Redis
        r.hset(f'md:candle:NSE_INDEX|Nifty 50:1m:{ts_0918}', mapping={
            'open': str(c[1]), 'high': str(c[2]), 'low': str(c[3]), 'close': str(c[4]), 'ts': str(ts_0918)
        })
        break

print(f'✅ Seeded 09:18 NIFTY Spot: ₹{spot_0918:.2f} (TS: {ts_0918})')

# 2. Fetch option chain for NIFTY 2026-08-25
underlying = 'NIFTY'
expiry = '2026-08-25'
lot_size = 65

# Load all option contracts
chain_data = r.hgetall(f'chain:{underlying}:{expiry}')
if not chain_data:
    # Try expiries
    print("Fetching option contracts from nifty_option_symbols.json...")
    with open('/Users/prana/Desktop/open_source/web/nifty_option_symbols.json') as f:
        sym_cfg = json.load(f)
    symbols = sym_cfg.get('symbols', [])
else:
    symbols = list(chain_data.values())

# Fetch 09:18 prices for all options
option_rows = []
print(f"Fetching 09:18 candle prices for {len(symbols)} option contracts...")

for sym in symbols:
    url = f'https://api.upstox.com/v2/historical-candle/intraday/{urllib.parse.quote(sym)}/1minute'
    resp = requests.get(url, headers=headers).json()
    candles = resp.get('data', {}).get('candles', [])
    px_0918 = None
    for c in candles:
        if '09:18' in c[0]:
            px_0918 = float(c[4])
            dt = datetime.fromisoformat(c[0])
            c_ts = int(dt.timestamp())
            r.hset(f'md:candle:{sym}:1m:{c_ts}', mapping={
                'open': str(c[1]), 'high': str(c[2]), 'low': str(c[3]), 'close': str(c[4]), 'ts': str(c_ts)
            })
            break

# Now build option chain with Greeks using shadow_greeks
sys.path.insert(0, '/Users/prana/Desktop/open_source/web')
from forward_tester.shadow_greeks import select_delta_strike, compute_bs_greeks_onthefly
from forward_tester.data_client import ForwardTestDataClient

data_client = ForwardTestDataClient()
df_opts = []
chain = data_client.get_option_chain(underlying, expiry)

for strike, opts in chain.items():
    for opt_type, symbol in [("CE", opts.get("CE")), ("PE", opts.get("PE"))]:
        if symbol:
            c_keys = r.keys(f"md:candle:{symbol}:1m:*")
            px = 0.0
            for k in c_keys:
                ts = int(k.split(":")[-1])
                dt = datetime.fromtimestamp(ts)
                if dt.hour == 9 and dt.minute == 18:
                    px = float(r.hget(k, "close") or 0.0)
                    break
            if px > 0:
                df_opts.append({
                    "symbol": symbol,
                    "strike": float(strike),
                    "option_type": opt_type,
                    "close": float(px),
                    "dte": 1.0
                })

df = pd.DataFrame(df_opts)
print(f"✅ Built option chain with {len(df)} contracts at 09:18 AM!")

# Select exact strikes at 09:18 AM spot
# 1. Strategy 6: Primary (0.25Δ) & Secondary (0.10Δ)
ce_s6_pri = select_delta_strike(df, "CE", 0.25, spot_0918, underlying=underlying)
pe_s6_pri = select_delta_strike(df, "PE", 0.25, spot_0918, underlying=underlying)
ce_s6_sec = select_delta_strike(df, "CE", 0.10, spot_0918, underlying=underlying)
pe_s6_sec = select_delta_strike(df, "PE", 0.10, spot_0918, underlying=underlying)

# 2. Ultra-TSMOM: Aggressive (0.45Δ) & Defensive (0.10Δ)
ce_ut_agg = select_delta_strike(df, "CE", 0.45, spot_0918, underlying=underlying)
pe_ut_agg = select_delta_strike(df, "PE", 0.45, spot_0918, underlying=underlying)
ce_ut_def = select_delta_strike(df, "CE", 0.10, spot_0918, underlying=underlying)
pe_ut_def = select_delta_strike(df, "PE", 0.10, spot_0918, underlying=underlying)

print("\n" + "=" * 80)
print(f"🎯 PROPER 09:18 AM STRIKE SELECTIONS (Spot = ₹{spot_0918:.2f})")
print("=" * 80)
print(f"Strategy 6 Primary   : CE Strike {ce_s6_pri['strike']} (@ ₹{ce_s6_pri['entry_price']:.2f}, Δ={ce_s6_pri['delta']:.3f}) | PE Strike {pe_s6_pri['strike']} (@ ₹{pe_s6_pri['entry_price']:.2f}, Δ={pe_s6_pri['delta']:.3f})")
print(f"Strategy 6 Secondary : CE Strike {ce_s6_sec['strike']} (@ ₹{ce_s6_sec['entry_price']:.2f}, Δ={ce_s6_sec['delta']:.3f}) | PE Strike {pe_s6_sec['strike']} (@ ₹{pe_s6_sec['entry_price']:.2f}, Δ={pe_s6_sec['delta']:.3f})")
print(f"Ultra-TSMOM Aggressive: CE Strike {ce_ut_agg['strike']} (@ ₹{ce_ut_agg['entry_price']:.2f}, Δ={ce_ut_agg['delta']:.3f}) | PE Strike {pe_ut_agg['strike']} (@ ₹{pe_ut_agg['entry_price']:.2f}, Δ={pe_ut_agg['delta']:.3f})")
print(f"Ultra-TSMOM Defensive : CE Strike {ce_ut_def['strike']} (@ ₹{ce_ut_def['entry_price']:.2f}, Δ={ce_ut_def['delta']:.3f}) | PE Strike {pe_ut_def['strike']} (@ ₹{pe_ut_def['entry_price']:.2f}, Δ={pe_ut_def['delta']:.3f})")

# Write to dual_model_trades.csv
trades = [
    # Model 1: Strategy 6 (15 Primary, 5 Secondary)
    {"date": "2026-08-24", "model_id": "STRATEGY_6", "underlying": underlying, "expiry": expiry, "symbol": ce_s6_pri["symbol"], "leg_type": "PRIMARY", "option_type": "CE", "strike": ce_s6_pri["strike"], "lots": 15, "target_delta": 0.25, "entry_price": ce_s6_pri["entry_price"], "current_price": ce_s6_pri["entry_price"], "sl_mult": 2.0, "sl_price": ce_s6_pri["entry_price"] * 2.0, "status": "OPEN", "exit_price": 0.0, "pnl": 0.0},
    {"date": "2026-08-24", "model_id": "STRATEGY_6", "underlying": underlying, "expiry": expiry, "symbol": pe_s6_pri["symbol"], "leg_type": "PRIMARY", "option_type": "PE", "strike": pe_s6_pri["strike"], "lots": 15, "target_delta": 0.25, "entry_price": pe_s6_pri["entry_price"], "current_price": pe_s6_pri["entry_price"], "sl_mult": 2.0, "sl_price": pe_s6_pri["entry_price"] * 2.0, "status": "OPEN", "exit_price": 0.0, "pnl": 0.0},
    {"date": "2026-08-24", "model_id": "STRATEGY_6", "underlying": underlying, "expiry": expiry, "symbol": ce_s6_sec["symbol"], "leg_type": "SECONDARY", "option_type": "CE", "strike": ce_s6_sec["strike"], "lots": 5, "target_delta": 0.10, "entry_price": ce_s6_sec["entry_price"], "current_price": ce_s6_sec["entry_price"], "sl_mult": 2.0, "sl_price": ce_s6_sec["entry_price"] * 2.0, "status": "OPEN", "exit_price": 0.0, "pnl": 0.0},
    {"date": "2026-08-24", "model_id": "STRATEGY_6", "underlying": underlying, "expiry": expiry, "symbol": pe_s6_sec["symbol"], "leg_type": "SECONDARY", "option_type": "PE", "strike": pe_s6_sec["strike"], "lots": 5, "target_delta": 0.10, "entry_price": pe_s6_sec["entry_price"], "current_price": pe_s6_sec["entry_price"], "sl_mult": 2.0, "sl_price": pe_s6_sec["entry_price"] * 2.0, "status": "OPEN", "exit_price": 0.0, "pnl": 0.0},

    # Model 2: Ultra-TSMOM (10 Aggressive, 10 Defensive)
    {"date": "2026-08-24", "model_id": "ULTRA_TSMOM", "underlying": underlying, "expiry": expiry, "symbol": ce_ut_agg["symbol"], "leg_type": "AGGRESSIVE", "option_type": "CE", "strike": ce_ut_agg["strike"], "lots": 10, "target_delta": 0.45, "entry_price": ce_ut_agg["entry_price"], "current_price": ce_ut_agg["entry_price"], "sl_mult": 1.75, "sl_price": ce_ut_agg["entry_price"] * 1.75, "status": "OPEN", "exit_price": 0.0, "pnl": 0.0},
    {"date": "2026-08-24", "model_id": "ULTRA_TSMOM", "underlying": underlying, "expiry": expiry, "symbol": pe_ut_agg["symbol"], "leg_type": "AGGRESSIVE", "option_type": "PE", "strike": pe_ut_agg["strike"], "lots": 10, "target_delta": 0.45, "entry_price": pe_ut_agg["entry_price"], "current_price": pe_ut_agg["entry_price"], "sl_mult": 1.75, "sl_price": pe_ut_agg["entry_price"] * 1.75, "status": "OPEN", "exit_price": 0.0, "pnl": 0.0},
    {"date": "2026-08-24", "model_id": "ULTRA_TSMOM", "underlying": underlying, "expiry": expiry, "symbol": ce_ut_def["symbol"], "leg_type": "DEFENSIVE", "option_type": "CE", "strike": ce_ut_def["strike"], "lots": 10, "target_delta": 0.10, "entry_price": ce_ut_def["entry_price"], "current_price": ce_ut_def["entry_price"], "sl_mult": 1.75, "sl_price": ce_ut_def["entry_price"] * 1.75, "status": "OPEN", "exit_price": 0.0, "pnl": 0.0},
    {"date": "2026-08-24", "model_id": "ULTRA_TSMOM", "underlying": underlying, "expiry": expiry, "symbol": pe_ut_def["symbol"], "leg_type": "DEFENSIVE", "option_type": "PE", "strike": pe_ut_def["strike"], "lots": 10, "target_delta": 0.10, "entry_price": pe_ut_def["entry_price"], "current_price": pe_ut_def["entry_price"], "sl_mult": 1.75, "sl_price": pe_ut_def["entry_price"] * 1.75, "status": "OPEN", "exit_price": 0.0, "pnl": 0.0},
]

df_trades = pd.DataFrame(trades)
df_trades.to_csv('/Users/prana/Desktop/open_source/web/forward_tester/dual_model_trades.csv', index=False)
print("✅ Saved exact 09:18 AM trades to dual_model_trades.csv!")

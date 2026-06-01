import os
import redis

class MarketDataClient:
    """
    Ultra-low latency, direct-access Python client for real-time market data
    using a pure in-memory Redis Unix domain socket pipeline.
    """
    def __init__(self, unix_socket_path="/Users/prana/Desktop/open_source/web/redis.sock", host="127.0.0.1", port=6379):
        # Prefer Unix Socket for sub-millisecond local IPC latency
        if os.path.exists(unix_socket_path):
            self.r = redis.Redis(unix_socket_path=unix_socket_path, decode_responses=True)
        else:
            self.r = redis.Redis(host=host, port=port, decode_responses=True)

    def _parse_numeric(self, val, default=0.0):
        """Helper to safely convert Redis string values to numeric types."""
        if val is None:
            return default
        try:
            if "." in val:
                return float(val)
            return int(val)
        except ValueError:
            return val

    def _parse_hash_quote(self, h):
        """Helper to convert a flat Redis HASH dict to typed numeric structures."""
        if not h:
            return None
            
        typed = {}
        # Str fields
        for f in ["symbol", "source", "status"]:
            if f in h:
                typed[f] = h[f]
                
        # Numeric fields
        for f in ["ltp", "close", "volume", "oi", "iv", "bid", "bid_qty", "ask", "ask_qty", "ts_exchange", "ts_recv"]:
            if f in h:
                typed[f] = self._parse_numeric(h[f])
                
        # Nest Option Greeks
        greeks = {}
        for g in ["delta", "theta", "gamma", "vega", "rho"]:
            if g in h:
                greeks[g] = self._parse_numeric(h[g])
        if greeks:
            typed["option_greeks"] = greeks
            
        return typed

    def get_spot_key(self, underlying="NIFTY"):
        """Resolve the spot instrument key (e.g., 'spot:NIFTY' -> 'NSE_INDEX|Nifty 50')."""
        return self.r.get(f"spot:{underlying.upper()}") or "NSE_INDEX|Nifty 50"

    def get_spot_quote(self, underlying="NIFTY"):
        """Fetch the latest spot quote snapshot directly from the Redis HASH."""
        key = self.get_spot_key(underlying)
        raw_hash = self.r.hgetall(f"md:quote:{key}")
        return self._parse_hash_quote(raw_hash)

    def get_spot_price(self, underlying="NIFTY", fallback=23500.0):
        """Fetch just the spot price LTP as a float."""
        quote = self.get_spot_quote(underlying)
        if quote and "ltp" in quote:
            return quote["ltp"]
        return fallback

    def get_atm_strike(self, underlying="NIFTY", strike_increment=50):
        """Calculate the At-The-Money (ATM) strike price rounded to the nearest increment."""
        spot = self.get_spot_price(underlying)
        return int(round(spot / strike_increment) * strike_increment)

    def get_nearby_chain(self, underlying="NIFTY", expiry=None, count=2, strike_increment=50):
        """
        Fetch quotes and option Greeks for At-The-Money (ATM) and nearby strikes in a single
        highly optimized Redis pipelined exchange.
        
        Args:
            underlying (str): Asset name (default NIFTY)
            expiry (str): Expiry date string (YYYY-MM-DD)
            count (int): Number of strikes above/below ATM to retrieve (ATM +/- count strikes)
            strike_increment (int): Strike interval (default 50 for Nifty)
            
        Returns:
            dict: Structured options chain map centered around ATM
        """
        if not expiry:
            raise ValueError("Expiry date (YYYY-MM-DD) must be specified")
            
        # 1. Resolve Spot Index Price & ATM strike
        spot_price = self.get_spot_price(underlying)
        atm_strike = int(round(spot_price / strike_increment) * strike_increment)
        
        # 2. Build target strikes list (ATM +/- count)
        strikes = [atm_strike + (i * strike_increment) for i in range(-count, count + 1)]
        
        # 3. Read option metadata map for this expiry (chain:NIFTY:<expiry>)
        chain_meta = self.r.hgetall(f"chain:NIFTY:{expiry}")
        if not chain_meta:
            return {
                "underlying": underlying,
                "spot_price": spot_price,
                "atm_strike": atm_strike,
                "expiry": expiry,
                "error": f"Metadata chain mapping for expiry '{expiry}' not seeded in Redis",
                "strikes": {}
            }
            
        # 4. Resolve CE/PE instrument keys and map them
        strike_keys = {}
        target_symbols = []
        for strike in strikes:
            ce_field = f"{strike}:CE"
            pe_field = f"{strike}:PE"
            
            ce_key = chain_meta.get(ce_field)
            pe_key = chain_meta.get(pe_field)
            
            strike_keys[strike] = {"CE": ce_key, "PE": pe_key}
            if ce_key:
                target_symbols.append(ce_key)
            if pe_key:
                target_symbols.append(pe_key)
                
        # 5. Pipelined multi-HGETALL (one single network exchange)
        pipe = self.r.pipeline()
        for sym in target_symbols:
            pipe.hgetall(f"md:quote:{sym}")
            
        raw_quotes = pipe.execute()
        
        # Map raw quotes back to their symbols
        symbol_quotes = {}
        for sym, raw_q in zip(target_symbols, raw_quotes):
            symbol_quotes[sym] = self._parse_hash_quote(raw_q)
            
        # 6. Construct structured output
        chain_quotes = {}
        for strike in strikes:
            ce_key = strike_keys[strike]["CE"]
            pe_key = strike_keys[strike]["PE"]
            
            ce_quote = symbol_quotes.get(ce_key) if ce_key else {"error": "Symbol mapping missing"}
            pe_quote = symbol_quotes.get(pe_key) if pe_key else {"error": "Symbol mapping missing"}
            
            chain_quotes[strike] = {
                "CE": ce_quote,
                "PE": pe_quote
            }
            
        return {
            "underlying": underlying,
            "spot_price": spot_price,
            "atm_strike": atm_strike,
            "expiry": expiry,
            "strikes": chain_quotes
        }

    def get_candles(self, symbol, timeframe="1m", count=100):
        """
        Fetch the latest N candles for a symbol and timeframe.
        Returns a chronologically sorted (ascending) list of candles.
        """
        zset_key = f"md:candles:{symbol}:{timeframe}"
        ts_list = self.r.zrevrange(zset_key, 0, count - 1)
        if not ts_list:
            return []
            
        pipe = self.r.pipeline()
        for ts in ts_list:
            pipe.hgetall(f"md:candle:{symbol}:{timeframe}:{ts}")
            
        raw_candles = pipe.execute()
        
        # Import datetime locally to prevent any namespace conflicts
        from datetime import datetime
        
        candles = []
        for ts, h in zip(ts_list, raw_candles):
            if h:
                candles.append({
                    "timestamp": int(ts),
                    "time": datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S"),
                    "open": float(h.get("open", 0)),
                    "high": float(h.get("high", 0)),
                    "low": float(h.get("low", 0)),
                    "close": float(h.get("close", 0)),
                    "volume": int(h.get("volume", 0)),
                    "status": h.get("status", "unknown")
                })
                
        return sorted(candles, key=lambda x: x["timestamp"])

if __name__ == "__main__":
    # Standard check / manual test
    client = MarketDataClient()
    spot = client.get_spot_price()
    atm = client.get_atm_strike()
    print("📈 Low-Latency Client Test Snapshot:")
    print(f"   Spot price: {spot}")
    print(f"   ATM strike: {atm}")
    
    # Try fetching Nifty front expiry ATM +/- 1 strikes
    print("\n⚡ Pipelining ATM +/- 1 strikes chain quotes...")
    import time
    start = time.perf_counter()
    chain = client.get_nearby_chain(expiry="2026-06-02", count=1)
    end = time.perf_counter()
    
    print(f"⏱️ Fetch completed in {(end - start) * 1000.0:.3f} ms!")
    
    # Print formatted ATM pair
    atm_ce = chain["strikes"][atm]["CE"]
    atm_pe = chain["strikes"][atm]["PE"]
    print(f"\n🔥 ATM Strike CE ({atm_ce.get('symbol') if atm_ce else 'N/A'}):")
    print(f"   LTP: {atm_ce.get('ltp')} | Bid: {atm_ce.get('bid')} | Ask: {atm_ce.get('ask')}")
    print(f"   Delta: {atm_ce.get('option_greeks', {}).get('delta')} | Theta: {atm_ce.get('option_greeks', {}).get('theta')}")
    
    print(f"\n🔥 ATM Strike PE ({atm_pe.get('symbol') if atm_pe else 'N/A'}):")
    print(f"   LTP: {atm_pe.get('ltp')} | Bid: {atm_pe.get('bid')} | Ask: {atm_pe.get('ask')}")
    print(f"   Delta: {atm_pe.get('option_greeks', {}).get('delta')} | Theta: {atm_pe.get('option_greeks', {}).get('theta')}")

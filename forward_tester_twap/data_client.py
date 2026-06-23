# forward_tester_twap/data_client.py
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import clickhouse_connect

# Add parent directory to sys.path to import market_data_client and config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from market_data_client import MarketDataClient
from config import CLICKHOUSE_CONFIG

class ForwardTestDataClient:
    """
    Extends MarketDataClient with ClickHouse historical loading/saving 
    and option strike discovery needed for the TWAP forward tester.
    """
    def __init__(self):
        self.client = MarketDataClient()
        self.ch_client = None
        self._init_clickhouse()

    def _init_clickhouse(self):
        """Establish ClickHouse client connection using config parameters."""
        try:
            self.ch_client = clickhouse_connect.get_client(
                host=CLICKHOUSE_CONFIG['host'],
                port=CLICKHOUSE_CONFIG['port'],
                username=CLICKHOUSE_CONFIG['username'],
                password=CLICKHOUSE_CONFIG['password'],
                database=CLICKHOUSE_CONFIG['database']
            )
            print("🚀 Successfully connected to ClickHouse server on AWS.")
        except Exception as e:
            print(f"⚠️ Warning: ClickHouse connection failed ({e}). Running in fallback mode.")
            self.ch_client = None

    # ─── Live Market Data Queries (Redis) ───

    def get_spot_price(self, underlying: str = "NIFTY") -> float:
        """Fetch live spot price for NIFTY or SENSEX from Redis."""
        return self.client.get_spot_price(underlying)

    def get_atm_strike(self, underlying: str = "NIFTY") -> int:
        """Get current ATM strike for NIFTY or SENSEX from Redis."""
        return self.client.get_atm_strike(underlying)

    def get_front_expiry(self, underlying: str = "NIFTY") -> str:
        """Find the front weekly expiry date for the underlying currently seeded in Redis (returns YYYY-MM-DD)."""
        keys = self.client.r.keys(f"chain:{underlying.upper()}:*")
        if not keys:
            return None
        expiries = sorted([k.split(":")[-1] for k in keys])
        today_str = date.today().strftime("%Y-%m-%d")
        for exp in expiries:
            if exp >= today_str:
                return exp
        return expiries[0] if expiries else None

    def get_monthly_expiry(self, underlying: str = "NIFTY") -> str:
        """Find the monthly expiry date (last expiry of the current month) currently seeded in Redis."""
        keys = self.client.r.keys(f"chain:{underlying.upper()}:*")
        if not keys:
            return None
        expiries = sorted([k.split(":")[-1] for k in keys])
        today_str = date.today().strftime("%Y-%m-%d")
        
        # Filter expiries >= today
        valid_expiries = [exp for exp in expiries if exp >= today_str]
        if not valid_expiries:
            return expiries[-1] if expiries else None
            
        # Take the last expiry of the first available month group (current month)
        front_month = valid_expiries[0][:7]  # YYYY-MM
        month_expiries = [e for e in valid_expiries if e.startswith(front_month)]
        return month_expiries[-1]

    def get_option_chain_quotes(self, underlying: str, expiry: str, count: int = 15) -> dict:
        """Get option chain quotes around ATM strike for the given expiry."""
        return self.client.get_nearby_chain(underlying=underlying, expiry=expiry, count=count)

    def get_live_candles(self, symbol: str = "NSE_INDEX|Nifty 50", timeframe: str = "1m", count: int = 100) -> list:
        """Fetch the latest closed candles from Redis."""
        return self.client.get_candles(symbol, timeframe=timeframe, count=count)

    # ─── Historical Data Queries (ClickHouse) ───

    def load_historical_nifty(self, days: int = 60) -> pd.DataFrame:
        """Loads historical 1m Nifty Spot data from ClickHouse for indicator seeding."""
        if not self.ch_client:
            print("⚠️ ClickHouse client not initialized. Cannot load historical Nifty.")
            return pd.DataFrame()

        start_date = (date.today() - timedelta(days=days)).strftime('%Y-%m-%d')
        query = f"""
            SELECT timestamp, open, high, low, close
            FROM nifty
            WHERE toDate(timestamp) >= '{start_date}'
              AND toTime(timestamp) >= toTime(toDateTime('1970-01-02 09:15:00'))
              AND toTime(timestamp) <= toTime(toDateTime('1970-01-02 15:30:00'))
            ORDER BY timestamp
        """
        try:
            res = self.ch_client.query(query)
            df = pd.DataFrame(res.result_rows, columns=res.column_names)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            if df['timestamp'].dt.tz is not None:
                df['timestamp'] = df['timestamp'].dt.tz_localize(None)
            df['td'] = df['timestamp'].dt.date
            df['tt'] = df['timestamp'].dt.time
            print(f"✅ Loaded {len(df)} rows of historical 1m Nifty data from ClickHouse.")
            return df
        except Exception as e:
            print(f"❌ Failed to load historical Nifty from ClickHouse: {e}")
            return pd.DataFrame()

    def load_historical_vix(self, days: int = 90) -> pd.DataFrame:
        """Loads historical India VIX close data from ClickHouse."""
        if not self.ch_client:
            print("⚠️ ClickHouse client not initialized. Cannot load VIX history.")
            return pd.DataFrame()

        start_date = (date.today() - timedelta(days=days)).strftime('%Y-%m-%d')
        query = f"""
            SELECT date, vix_close, vix_1d_change
            FROM vix
            WHERE date >= '{start_date}'
            ORDER BY date
        """
        try:
            res = self.ch_client.query(query)
            df = pd.DataFrame(res.result_rows, columns=res.column_names)
            df['date'] = pd.to_datetime(df['date']).dt.date
            print(f"✅ Loaded {len(df)} rows of historical VIX data from ClickHouse.")
            return df
        except Exception as e:
            print(f"⚠️ VIX table missing or failed to query ClickHouse: {e}. Generating default VIX table.")
            # Default fallback vix dataframe
            dates = [date.today() - timedelta(days=i) for i in range(days, -1, -1)]
            df = pd.DataFrame({'date': dates, 'vix_close': 15.0, 'vix_1d_change': 0.0})
            return df

    def save_today_nifty(self, df_1m: pd.DataFrame):
        """Inserts today's 1-minute Nifty Spot candles into ClickHouse."""
        if not self.ch_client:
            print("⚠️ ClickHouse client not initialized. Cannot save today's Nifty.")
            return
        if df_1m.empty:
            print("⚠️ No Nifty data to save.")
            return
            
        try:
            # Drop extra helper columns to match database schema
            df_to_save = df_1m[['timestamp', 'open', 'high', 'low', 'close']].copy()
            
            # Ensure proper typing
            df_to_save['open'] = df_to_save['open'].astype(float)
            df_to_save['high'] = df_to_save['high'].astype(float)
            df_to_save['low'] = df_to_save['low'].astype(float)
            df_to_save['close'] = df_to_save['close'].astype(float)
            
            # Ensure ClickHouse table exists
            self.ch_client.command("""
                CREATE TABLE IF NOT EXISTS nifty (
                    timestamp DateTime,
                    open Float64,
                    high Float64,
                    low Float64,
                    close Float64
                ) ENGINE = MergeTree()
                ORDER BY timestamp
            """)
            
            self.ch_client.insert('nifty', df_to_save)
            print(f"✅ Saved {len(df_to_save)} Nifty 1m candles to ClickHouse.")
        except Exception as e:
            print(f"❌ Failed to save today's Nifty to ClickHouse: {e}")

    def save_today_vix(self, vix_close: float):
        """Saves today's daily VIX close to ClickHouse."""
        if not self.ch_client:
            return
            
        try:
            today_dt = date.today()
            
            # Ensure VIX table exists
            self.ch_client.command("""
                CREATE TABLE IF NOT EXISTS vix (
                    date Date,
                    vix_close Float64,
                    vix_1d_change Float64
                ) ENGINE = MergeTree()
                ORDER BY date
            """)
            
            # Calculate 1d change by querying yesterday's close
            res = self.ch_client.query(f"SELECT vix_close FROM vix WHERE date < '{today_dt.strftime('%Y-%m-%d')}' ORDER BY date DESC LIMIT 1")
            prev_vix = res.result_rows[0][0] if res.result_rows else vix_close
            vix_1d_change = vix_close - prev_vix
            
            df_vix = pd.DataFrame([{
                'date': today_dt,
                'vix_close': float(vix_close),
                'vix_1d_change': float(vix_1d_change)
            }])
            
            # Delete today's entry first to prevent duplicate entries if re-run
            self.ch_client.command(f"ALTER TABLE vix DELETE WHERE date = '{today_dt.strftime('%Y-%m-%d')}'")
            
            self.ch_client.insert('vix', df_vix)
            print(f"✅ Saved today's VIX ({vix_close}) to ClickHouse.")
        except Exception as e:
            print(f"❌ Failed to save today's VIX to ClickHouse: {e}")

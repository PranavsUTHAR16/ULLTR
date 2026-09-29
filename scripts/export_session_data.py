#!/usr/bin/env python3
"""
Session Data Exporter for C++ Strategy Engine Market Replay.

Queries ClickHouse Cloud directly to extract:
1. 1-minute futures bars with developing POC, session VWAP, cumulative CVD, and 15m delta metrics.
2. 1-minute option quotes with Greeks, bid/ask depth, and open interest for the active front weekly expiry.
"""

import argparse
import logging
import os
import sys
import time
from typing import Optional

import clickhouse_connect
import pandas as pd

# Add tick_data project root to path
TICK_DATA_DIR = "/Users/prana/Desktop/black_box/tick_data"
if os.path.exists(TICK_DATA_DIR) and TICK_DATA_DIR not in sys.path:
    sys.path.insert(0, TICK_DATA_DIR)

from config import ClickHouseConfig, ModelPOCV2Config
from models.model_poc_v2 import ModelPOCV2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SessionDataExporter")

CH_HOST = os.environ.get("CLICKHOUSE_HOST", "ra5fptcofl.ap-south-1.aws.clickhouse.cloud")
CH_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8443"))
CH_USER = os.environ.get("CLICKHOUSE_USER", "default")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "BhhYrZvtF3lA~")
CH_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")


def get_clickhouse_client() -> clickhouse_connect.driver.client.Client:
    """Establish authenticated connection to ClickHouse Cloud."""
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASS,
        database=CH_DATABASE,
        secure=True,
    )


def export_futures_bars(client: clickhouse_connect.driver.client.Client, trade_date: str, output_path: str) -> pd.DataFrame:
    """
    Compute 1-minute futures bars, dPOC, and session VWAP via ModelPOCV2.
    
    Parameters
    ----------
    client : clickhouse_connect client
    trade_date : str (YYYY-MM-DD)
    output_path : str
    
    Returns
    -------
    pd.DataFrame
    """
    logger.info("Computing 1m futures bars, dPOC, and VWAP for %s...", trade_date)
    m_poc = ModelPOCV2(ModelPOCV2Config())
    m_poc._ch_client = client
    df_fut = m_poc.fetch_futures_and_profile(trade_date)
    
    if df_fut.empty:
        raise RuntimeError(f"No futures bars retrieved for {trade_date}")
        
    df_fut.to_csv(output_path, index=False)
    logger.info("Successfully exported %d futures bars to %s", len(df_fut), output_path)
    return df_fut


def export_options_quotes(client: clickhouse_connect.driver.client.Client, trade_date: str, output_path: str) -> pd.DataFrame:
    """
    Query 1-minute option quotes with Greeks and order book depth for the active front expiry.
    
    Parameters
    ----------
    client : clickhouse_connect client
    trade_date : str (YYYY-MM-DD)
    output_path : str
    
    Returns
    -------
    pd.DataFrame
    """
    # 1. Resolve front weekly expiry
    q_exp = f"""
    SELECT min(expiry) 
    FROM market_ticks 
    WHERE underlying = 'NIFTY' 
      AND option_type IN ('CE', 'PE') 
      AND toDate(timestamp) = '{trade_date}' 
      AND expiry >= '{trade_date}'
    """
    exp_res = client.query(q_exp).result_rows
    if not exp_res or not exp_res[0][0]:
        raise RuntimeError(f"No active option expiry found for {trade_date}")
        
    opt_expiry = str(exp_res[0][0])
    logger.info("Active front expiry for %s is %s", trade_date, opt_expiry)

    # 2. Extract 1-minute option bars
    t0 = time.time()
    q_opts = f"""
    SELECT 
        toStartOfInterval(timestamp, INTERVAL 1 MINUTE) AS bar_1m,
        symbol,
        toInt32(strike) AS strike,
        option_type,
        toFloat64(argMax(ltp, timestamp)) AS ltp,
        toFloat64(argMax(bid, timestamp)) AS bid,
        toUInt32(argMax(bid_qty, timestamp)) AS bid_qty,
        toFloat64(argMax(ask, timestamp)) AS ask,
        toUInt32(argMax(ask_qty, timestamp)) AS ask_qty,
        toFloat64(argMax(delta, timestamp)) AS delta,
        toFloat64(argMax(theta, timestamp)) AS theta,
        toFloat64(argMax(gamma, timestamp)) AS gamma,
        toFloat64(argMax(vega, timestamp)) AS vega,
        toFloat64(argMax(iv, timestamp)) AS iv,
        toUInt64(argMax(open_interest, timestamp)) AS oi,
        toUInt64(sum(volume)) AS volume,
        toFloat64(argMax(close, timestamp)) AS close
    FROM market_ticks
    WHERE underlying = 'NIFTY'
      AND toString(expiry) = '{opt_expiry}'
      AND toDate(timestamp) = '{trade_date}'
      AND timestamp >= '{trade_date} 09:15:00'
      AND timestamp <= '{trade_date} 15:30:00'
    GROUP BY bar_1m, symbol, strike, option_type
    ORDER BY bar_1m ASC, strike ASC, option_type ASC
    """
    logger.info("Querying option quotes from ClickHouse Cloud...")
    df_opts = client.query_df(q_opts)
    if df_opts.empty:
        raise RuntimeError(f"No option quotes found for {trade_date} (expiry: {opt_expiry})")

    df_opts['time_str'] = pd.to_datetime(df_opts['bar_1m']).dt.strftime('%H:%M')
    n_mins = df_opts['time_str'].nunique()
    df_opts.to_csv(output_path, index=False)
    logger.info(
        "Exported %d option rows across %d distinct minutes in %.2fs to %s",
        len(df_opts), n_mins, time.time() - t0, output_path
    )
    return df_opts


def export_mend_progression(client: clickhouse_connect.driver.client.Client, trade_date: str, output_path: str) -> pd.DataFrame:
    """
    Compute 15-minute Black-Scholes Maker Equilibrium Center (S*) and Net Maker Delta
    directly from raw ClickHouse market_ticks on-the-fly without any pre-computed CSV files.
    """
    t0 = time.time()
    logger.info("Computing dynamic M.E.N.D. Black-Scholes progression for %s from raw ticks...", trade_date)
    
    # 1. Front weekly expiry
    q_exp = f"""
    SELECT toString(min(expiry)) FROM market_ticks 
    WHERE underlying = 'NIFTY' AND option_type IN ('CE', 'PE') AND toDate(timestamp) = '{trade_date}' AND expiry >= toDate('{trade_date}')
    """
    exp = str(client.query(q_exp).result_rows[0][0])
    
    # 2. 15m Spot index bars for target date
    q_spot = f"""
    SELECT toDateTime(toStartOfInterval(timestamp, INTERVAL 15 MINUTE)) AS bar_time, argMax(toFloat64(ltp), timestamp) AS spot_close
    FROM market_ticks WHERE symbol = 'NSE_INDEX|Nifty 50' AND toDate(timestamp) = '{trade_date}'
      AND toHour(timestamp)*60 + toMinute(timestamp) >= 9*60 + 15 AND toHour(timestamp)*60 + toMinute(timestamp) <= 15*60 + 30
    GROUP BY bar_time ORDER BY bar_time ASC
    """
    df_spot = client.query_df(q_spot)
    
    # 3. Series order flow from start of expiry cycle (up to last 7 days)
    q_opts = f"""
    WITH raw AS (
        SELECT 
            toDateTime(toStartOfInterval(timestamp, INTERVAL 15 MINUTE)) AS bar_time,
            timestamp, strike, option_type,
            toFloat64(ltp) AS ltp, toFloat64(bid) AS bid, toFloat64(ask) AS ask, toFloat64(iv) AS iv,
            toFloat64(greatest(0, volume - lagInFrame(volume, 1, volume) OVER (PARTITION BY symbol ORDER BY timestamp))) AS ltq,
            (bid + ask) / 2.0 AS midpoint,
            if(ltp > midpoint, 1, if(ltp < midpoint, -1, 0)) AS lr_sign
        FROM market_ticks
        WHERE underlying = 'NIFTY' AND option_type IN ('CE', 'PE') AND expiry = toDate('{exp}') 
          AND toDate(timestamp) BETWEEN subtractDays(toDate('{trade_date}'), 7) AND toDate('{trade_date}')
          AND toDate(timestamp) >= toDate('2026-08-01')
          AND toHour(timestamp)*60 + toMinute(timestamp) >= 9*60 + 15 AND toHour(timestamp)*60 + toMinute(timestamp) <= 15*60 + 30
    )
    SELECT bar_time, strike, option_type,
        sum(if(lr_sign > 0, ltq, if(lr_sign == 0, ltq * 0.5, 0.0))) AS taker_buy,
        sum(if(lr_sign < 0, ltq, if(lr_sign == 0, ltq * 0.5, 0.0))) AS taker_sell,
        argMax(iv, timestamp) AS iv
    FROM raw GROUP BY bar_time, strike, option_type ORDER BY bar_time ASC, strike ASC
    """
    df_opts = client.query_df(q_opts)
    
    expiry_dt = pd.to_datetime(f"{exp} 15:30:00")
    opts_by_bar = {t: grp for t, grp in df_opts.groupby('bar_time')}
    all_bar_times = sorted(df_opts['bar_time'].unique())
    target_dt_bars = set(df_spot['bar_time'].unique())
    
    import numpy as np
    import scipy.optimize as so
    import scipy.stats as si
    
    series_inv = {}
    r = 0.065
    def calc_bs_delta(S, K, otype, T, sigma):
        if T <= 1e-5:
            return 1.0 if (otype == 'CE' and S > K) else (-1.0 if (otype == 'PE' and S < K) else 0.0)
        sigma_eff = max(0.05, min(0.60, sigma))
        d1 = (np.log(S / K) + (r + 0.5 * sigma_eff**2) * T) / (sigma_eff * np.sqrt(T))
        return si.norm.cdf(d1) if otype == 'CE' else si.norm.cdf(d1) - 1.0
        
    records = []
    spot_map = dict(zip(df_spot['bar_time'], df_spot['spot_close']))
    for b_time in all_bar_times:
        bar_opts = opts_by_bar.get(b_time, pd.DataFrame())
        for _, opt_row in bar_opts.iterrows():
            k = int(opt_row['strike'])
            otype = opt_row['option_type']
            maker_flow = -(opt_row['taker_buy'] - opt_row['taker_sell'])
            iv = opt_row['iv'] if opt_row['iv'] > 0 else 0.12
            if (k, otype) not in series_inv:
                series_inv[(k, otype)] = {'pos': 0.0, 'iv': iv}
            series_inv[(k, otype)]['pos'] += maker_flow
            series_inv[(k, otype)]['iv'] = iv
        
        if b_time in target_dt_bars:
            spot = spot_map[b_time]
            rem_seconds = max(60, (expiry_dt - b_time.replace(tzinfo=None)).total_seconds())
            T = rem_seconds / (365.25 * 24 * 3600)
            
            def series_delta_fn(S):
                return sum(item['pos'] * calc_bs_delta(S, strk, otype, T, item['iv']) for (strk, otype), item in series_inv.items())
            
            s_star = ""
            grid = np.linspace(spot - 600, spot + 600, 25)
            vals = [series_delta_fn(s) for s in grid]
            for i in range(len(vals) - 1):
                if vals[i] * vals[i+1] <= 0:
                    try:
                        s_star = round(so.brentq(series_delta_fn, grid[i], grid[i+1]), 2)
                        break
                    except Exception:
                        pass
            net_delta = round(series_delta_fn(spot), 2)
            t_str = b_time.strftime('%H:%M')
            bar_ts_full = b_time.strftime('%Y-%m-%d %H:%M:%S')
            records.append({
                'bar_time': bar_ts_full,
                'trade_date': trade_date,
                'spot': spot,
                'spot_high': spot,
                'spot_low': spot,
                's_star_series': s_star,
                's_star_session': s_star,
                'ser_k_center': spot,
                'maker_net_delta': net_delta,
                'total_maker_inv': 0.0
            })
            
    df_out = pd.DataFrame(records)
    df_out.to_csv(output_path, index=False)
    logger.info("Exported %d dynamic MEND progression rows in %.2fs to %s", len(df_out), time.time() - t0, output_path)
    return df_out


def main():
    parser = argparse.ArgumentParser(description="Export session bars, option quotes, and dynamic MEND progression for C++ engine replay.")
    parser.add_argument("--date", type=str, required=True, help="Trade date in YYYY-MM-DD format (e.g. 2026-09-11)")
    parser.add_argument("--output-dir", type=str, default="/Users/prana/Desktop/open_source/web/collector", help="Output directory")
    args = parser.parse_args()

    trade_date = args.date
    date_clean = trade_date.replace("-", "")
    os.makedirs(args.output_dir, exist_ok=True)

    bars_csv = os.path.join(args.output_dir, f"session_bars_{date_clean}.csv")
    opts_csv = os.path.join(args.output_dir, f"session_options_{date_clean}.csv")
    mend_csv = os.path.join(args.output_dir, f"session_mend_{date_clean}.csv")

    client = get_clickhouse_client()
    export_futures_bars(client, trade_date, bars_csv)
    export_options_quotes(client, trade_date, opts_csv)
    export_mend_progression(client, trade_date, mend_csv)
    logger.info("Export complete for %s:\n  Bars: %s\n  Options: %s\n  MEND: %s", trade_date, bars_csv, opts_csv, mend_csv)


if __name__ == "__main__":
    main()

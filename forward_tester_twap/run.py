# forward_tester_twap/run.py
import argparse
import sys
import os
import time
from datetime import datetime, time as dtime

# Add parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from forward_tester_twap.engine import TWAPForwardTestEngine
from forward_tester_twap.twap_config import EXIT_TIME
from expiry_manager import is_market_open_today

def run_live(engine: TWAPForwardTestEngine):
    """Runs the live forward test execution loop during market hours."""
    print("Starting TWAP Breakout Forward Tester in LIVE mode...")
    
    # 1. Check if NSE market is open today
    if not is_market_open_today():
        print("🔴 NSE Exchange is CLOSED today. TWAP Forward Tester will not trade. Exiting.")
        sys.exit(0)
        
    print("🟢 NSE Exchange is OPEN today. Initiating execution loop...")
    exit_h, exit_m, exit_s = EXIT_TIME
    exit_triggered = False
    
    while True:
        try:
            now = datetime.now().time()
            
            # Run one strategy tick (updates candles, checks breakouts, monitors positions)
            engine.run_tick()
            
            # Send periodic Telegram P/L update (every 60 seconds)
            if time.time() - engine.last_tg_update_time >= 60:
                engine.send_periodic_pnl_update()
                
            # Render terminal dashboard
            engine.render_dashboard()
            
            # Check EOD Exit (3:25 PM IST)
            if not exit_triggered and now >= dtime(exit_h, exit_m, exit_s):
                engine.execute_eod_pipeline()
                exit_triggered = True
                print("\n🏁 EOD Execution completed. Exiting forward test loop.")
                break
                
            # Sleep for 5 seconds between ticks
            time.sleep(5.0)
            
        except KeyboardInterrupt:
            print("\n👋 Keyboard interrupt received. Exiting gracefully...")
            break
        except Exception as e:
            print(f"⚠️ Error in execution loop: {e}")
            time.sleep(10.0)

def run_dry_run(engine: TWAPForwardTestEngine):
    """Simulates a fast-forward day to test indicator calculations, strike selection, and logs."""
    print("=" * 80)
    print("🏃 RUNNING TWAP BREAKOUT DRY-RUN SIMULATION (FAST-FORWARD)")
    print("=" * 80)
    
    time.sleep(1.0)
    
    print("\nStep 1: Fetching current Nifty Spot price and calculating ATM option strike...")
    spot = engine.client.get_spot_price("NIFTY")
    atm = engine.client.get_atm_strike("NIFTY")
    print(f"  Live Nifty Spot: {spot:.2f} | ATM Strike: {atm}")
    
    time.sleep(1.0)
    
    print("\nStep 2: Checking live 1-minute Nifty Spot candles in Redis...")
    live_candles = engine.client.get_live_candles("NSE_INDEX|Nifty 50", timeframe="1m", count=10)
    if live_candles:
        print(f"  Successfully retrieved {len(live_candles)} live candles from Redis.")
        print(f"  Latest candle close: {live_candles[-1]['close']} at {datetime.fromtimestamp(live_candles[-1]['timestamp'])}")
    else:
        print("  ⚠️ Redis candle cache is empty! Ensure ULLTR C++ ingestion engine is running.")
    
    time.sleep(1.0)
    
    print("\nStep 3: Calculating simulated feature extraction and scoring LightGBM model...")
    # Mocking features list for prediction verification
    mock_features = {
        'vol_park_20': 0.012, 'vol_park_60': 0.011, 'vol_gk_20': 0.013, 'vol_gk_60': 0.011,
        'atr_14': 25.0, 'atr_ratio': 1.1, 'vol_regime': 1.05, 'twap_slope': 0.5, 'rsi_14': 58.0,
        'ema_fast_slope': 0.002, 'close_vs_ema20': 0.005, 'momentum_5': 0.003, 'momentum_15': 0.008,
        'bar_body_pct': 0.6, 'upper_wick_pct': 0.2, 'lower_wick_pct': 0.2, 'close_streak': 3.0,
        'intraday_return': 0.004, 'session_high_pct': 0.8, 'session_range_pct': 0.7,
        'cum_bars_above_twap': 12.0, 'realized_vol_today': 0.009, 'prior_range': 220.0,
        'opening_gap_pct': 0.15, 'open_pos_relative': 0.6, 'days_to_weekly_expiry': 2.0,
        'days_to_monthly_expiry': 16.0, 'vix_close': 14.5, 'vix_1d_change': -0.2,
        'vix_5d_change': 0.5, 'vix_z60d': 0.1, 'atm_iv': 0.14, 'iv_skew': 0.012,
        'iv_term_struct': 1.02, 'pcr_oi': 0.98, 'atm_delta_spread': 0.015, 'atm_iv_z60d': -0.1,
        'atm_iv_5d_change': 0.002, 'mins_since_920': 180.0, 'day_of_week': 2.0,
        'band_width': 150.0, 'band_width_pctile': 0.65, 'band_expansion_rate': 0.2,
        'penetration_depth': 0.12, 'touch_count_today': 2.0, 'signal_rank_today': 1.0
    }
    
    prob_win = engine.check_ml_confidence(mock_features)
    print(f"  Simulated LightGBM prediction score: {prob_win:.4f}")
    
    time.sleep(1.0)
    
    print("\nStep 4: Executing a simulated LONG breakout option sale...")
    spot_entry = spot
    sl_spot = spot_entry - 50.0
    tp_spot = spot_entry + 50.0
    engine.execute_option_entry(
        direction="LONG",
        spot_price=spot_entry,
        sl_spot=sl_spot,
        tp_spot=tp_spot,
        confidence=prob_win
    )
    
    time.sleep(1.5)
    engine.render_dashboard()
    
    time.sleep(1.0)
    
    print("\nStep 5: Simulating Stop-Loss breach (Option premium spike)...")
    if engine.active_position:
        p = engine.active_position
        print(f"  Artificially spiking premium of {p.symbol} from {p.entry_price} to {p.premium_sl * 1.05}...")
        p.update_state(current_premium=p.premium_sl * 1.05, current_spot=spot)
        engine.update_and_monitor()
        
    time.sleep(1.5)
    engine.render_dashboard()
    
    time.sleep(1.0)
    
    print("\nStep 6: Executing EOD Square-off and ClickHouse caching pipeline...")
    engine.execute_eod_pipeline()
    
    print("\n" + "=" * 80)
    print("✅ TWAP BREAKOUT DRY-RUN SIMULATION COMPLETED SUCCESSFULLY!")
    print(f"  Trades logged to: {engine.log_file}")
    print("=" * 80)

def main():
    parser = argparse.ArgumentParser(description="TWAP Band Breakout Forward Tester")
    parser.add_argument("--dry-run", action="store_true", help="Run a quick simulation of indicators and order executions")
    parser.add_argument("--disable-ml", action="store_true", help="Bypasses LightGBM ML confidence filter and executes all breakouts")
    args = parser.parse_args()
    
    engine = TWAPForwardTestEngine(dry_run=args.dry_run, disable_ml=args.disable_ml)
    
    if args.dry_run:
        run_dry_run(engine)
    else:
        run_live(engine)

if __name__ == "__main__":
    main()

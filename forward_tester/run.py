# forward_tester/run.py
import argparse
import sys
import os
import time
from datetime import datetime, time as dtime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from forward_tester.engine import MultiModelEngine
from forward_tester.config import MultiModelConfig
from expiry_manager import is_market_open_today

def run_live(engine: MultiModelEngine):
    """Runs the live Multi-Model execution loop during market hours."""
    print("Starting Forward Tester (Model 1: Strategy 6 [20 Lots]) in LIVE mode...")
    
    if not is_market_open_today():
        print("🔴 NSE Exchange is CLOSED today. Forward Tester will not trade. Exiting.")
        sys.exit(0)
        
    print("🟢 NSE Exchange is OPEN today. Initiating execution loop...")
    
    entry_h, entry_m, entry_s = engine.config.strategy6.entry_time  # 09:18:01
    exit_h, exit_m, exit_s = 15, 0, 5                                # Runs through 15:00:05 for Strategy 6 squareoff
    
    # BUG-25 FIX: Only block re-entry if there are currently ACTIVE positions.
    # Closed positions from a prior session or earlier that day must not prevent
    # new entries (false positive from position recovery on restart).
    entry_triggered = len(engine.strategy6.active_positions) > 0

    eod_squareoff_done = False
    last_render_time = 0.0
    last_telegram_time = time.time()
    last_heartbeat_hour = -1
    market_open_refreshed = False
    
    while True:
        try:
            now_dt = datetime.now()
            now = now_dt.time()
            
            # 1. Market Open Re-initialization & Morning Telegram Broadcast at 09:15 AM
            if not market_open_refreshed and now >= dtime(9, 15, 0) and now < dtime(exit_h, exit_m, exit_s):
                print("🔔 Market Open detected (09:15 IST). Refreshing option chains and day state from Redis...")
                engine.init_trading_day(now_dt.strftime("%Y-%m-%d"))
                engine.load_saved_positions()
                engine.send_telegram_morning_heartbeat()
                market_open_refreshed = True

            # 2. Check 09:18 AM Strategy 6 Entry
            if not entry_triggered and now >= dtime(entry_h, entry_m, entry_s) and now < dtime(15, 0, 0):
                if not engine.strategy6.expiry:
                    engine.init_trading_day(now_dt.strftime("%Y-%m-%d"))
                engine.execute_0918_dual_model_entry()
                if len(engine.strategy6.active_positions) > 0:
                    entry_triggered = True
                
            # 3. Monitor active positions & trailing stops
            engine.update_and_monitor()
                
            # 4. Render terminal status dashboard once per second
            if time.time() - last_render_time >= 1.0:
                engine.render_dashboard()
                last_render_time = time.time()

            # 5. Send 15-second Telegram model updates while positions are active
            if (engine.active_positions or engine.closed_positions) and (time.time() - last_telegram_time >= 15.0):
                engine.send_telegram_model_periodic_updates()
                last_telegram_time = time.time()

            # 6. Send Hourly Telegram Heartbeat when idle (10:00, 11:00, 12:00, 13:00, 14:00)
            if not engine.active_positions and now_dt.minute == 0 and now_dt.hour != last_heartbeat_hour and 9 <= now_dt.hour <= 15:
                engine.send_telegram_periodic_heartbeat()
                last_heartbeat_hour = now_dt.hour

            # 7. Check 15:00:00 Squareoff for Strategy 6 & EOD Broadcast
            if not eod_squareoff_done and now >= dtime(15, 0, 0):
                engine.execute_eod_squareoff()
                eod_squareoff_done = True
                print("\n🏁 15:00:00 Strategy 6 squared off and EOD broadcast sent.")

            # 8. Check 15:00:05 Session Exit
            if now >= dtime(exit_h, exit_m, exit_s):
                print("\n🏁 15:00:05 Trading session completed. Exiting forward test loop.")
                break
                
            time.sleep(0.005)
            
        except KeyboardInterrupt:
            print("\n👋 Keyboard interrupt received. Exiting gracefully...")
            if engine.active_positions:
                engine.execute_eod_squareoff()
            break
        except Exception as e:
            print(f"⚠️ Error in execution loop: {e}")
            time.sleep(5.0)

def run_dry_run(engine: MultiModelEngine):
    """Simulates Strategy 6 strike selection, allocation, and stop-loss monitoring."""
    s6_lots = engine.config.strategy6.total_lots
    print("=" * 85)
    print(f"🏃 RUNNING FORWARD TESTER DRY-RUN SIMULATION (STRATEGY 6 @ {s6_lots} LOTS)")
    print(f"  • Model 1: STRATEGY_6 (Vol-Adaptive Regime Engine @ {s6_lots} Lots / ₹50L Cap)")
    print("=" * 85)
    
    print(f"\nStep 1: Simulating Strategy 6 09:18 AM Entry ({s6_lots} Lots)...")
    engine.execute_0918_dual_model_entry()
    
    time.sleep(1.0)
    engine.render_dashboard()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Forward Tester (Model 1: Strategy 6 [20 Lots])")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry-run simulation mode")
    parser.add_argument("--live", action="store_true", help="Run in live continuous polling mode")
    args = parser.parse_args()
    
    config = MultiModelConfig()
    engine = MultiModelEngine(config=config, dry_run=args.dry_run)
    
    if args.dry_run:
        run_dry_run(engine)
    else:
        run_live(engine)

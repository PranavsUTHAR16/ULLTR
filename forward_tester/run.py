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
    print("Starting MULTI-MODEL Forward Tester (Model 1: Strategy 6 [10 Lots] | Model 2: 0216 Model [10 Lots]) in LIVE mode...")
    
    if not is_market_open_today():
        print("🔴 NSE Exchange is CLOSED today. Forward Tester will not trade. Exiting.")
        sys.exit(0)
        
    print("🟢 NSE Exchange is OPEN today. Initiating execution loop...")
    
    entry_h, entry_m, entry_s = engine.config.strategy6.entry_time  # 09:18:01
    exit_h, exit_m, exit_s = 15, 25, 0                               # Extended to 15:25:00 for CAS execution
    
    entry_triggered = len(engine.strategy6.active_positions) > 0 or len(engine.strategy6.closed_positions) > 0
    morning_exit_triggered = False
    cas_armed = False
    cas_triggered = False
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
                engine.send_telegram_morning_heartbeat()
                market_open_refreshed = True

            # 2. Check 09:18 AM Strategy 6 Entry
            if not entry_triggered and now >= dtime(entry_h, entry_m, entry_s) and now < dtime(15, 0, 0):
                if not engine.strategy6.expiry:
                    engine.init_trading_day(now_dt.strftime("%Y-%m-%d"))
                engine.execute_0918_dual_model_entry()
                if len(engine.strategy6.active_positions) > 0:
                    entry_triggered = True
                
            # 3. Monitor active positions & evaluate 5-minute candle signals
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

            # 7. Check 15:00 PM Squareoff for Morning Models (Strategy 6 & 0216)
            if not morning_exit_triggered and now >= dtime(15, 0, 0):
                engine.strategy6.execute_eod_squareoff("15:00")
                engine.model_0216.execute_eod_squareoff("15:00")
                morning_exit_triggered = True
                print("\n🏁 15:00:00 Morning models squared off. Entering CAS Monitoring Phase...")

            # 8. Check 15:15 PM CAS Arming
            if not cas_armed and now >= dtime(15, 15, 0):
                engine.arm_cas_session()
                cas_armed = True

            # 9. Check 15:20:01 PM CAS Entry Execution (Sub-1ms Real Order Placement)
            if not cas_triggered and now >= dtime(15, 20, 1):
                engine.execute_cas_entry()
                cas_triggered = True

            # 10. Check 15:25 PM Final EOD Exit
            if now >= dtime(exit_h, exit_m, exit_s):
                engine.execute_eod_squareoff()
                print("\n🏁 15:25:00 Full session completed. Exiting forward test loop.")
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
    """Simulates Dual-Model strike selection, allocation, and stop-loss monitoring."""
    print("=" * 85)
    print("🏃 RUNNING MULTI-MODEL FORWARD TESTER DRY-RUN SIMULATION (20 LOTS PORTFOLIO CAP)")
    print("  • Model 1: STRATEGY_6 (Vol-Adaptive Regime Engine @ 10 Lots)")
    print("  • Model 2: 0216_MODEL (Master Derivatives Engine @ 10 Lots / 5m Candle Boundary)")
    print("=" * 85)
    
    print("\nStep 1: Simulating Strategy 6 09:18 AM Entry (10 Lots)...")
    engine.execute_0918_dual_model_entry()
    
    print("\nStep 2: Simulating 0216 Model 5-Minute Candle Signal Evaluation...")
    engine.evaluate_5m_boundary()
    
    print("\nStep 3: Simulating CAS Sub-1ms Equilibrium & Real Order Placement...")
    engine.arm_cas_session()
    engine.execute_cas_entry()
    
    time.sleep(1.0)
    engine.render_dashboard()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multi-Model Forward Tester (Strategy 6 + 0216 Model)")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry-run simulation mode")
    parser.add_argument("--live", action="store_true", help="Run in live continuous polling mode")
    args = parser.parse_args()
    
    config = MultiModelConfig()
    engine = MultiModelEngine(config=config, dry_run=args.dry_run)
    
    if args.dry_run:
        run_dry_run(engine)
    else:
        run_live(engine)

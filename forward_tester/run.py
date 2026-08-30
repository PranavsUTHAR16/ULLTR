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
    exit_h, exit_m, exit_s = engine.config.strategy6.exit_time      # 15:00:00
    
    entry_triggered = len(engine.strategy6.active_positions) > 0 or len(engine.strategy6.closed_positions) > 0
    exit_triggered = False
    last_render_time = 0.0
    
    while True:
        try:
            now = datetime.now().time()
            
            # Check 09:18 AM Strategy 6 Entry
            if not entry_triggered and now >= dtime(entry_h, entry_m, entry_s) and now < dtime(exit_h, exit_m, exit_s):
                engine.execute_0918_dual_model_entry()
                if len(engine.strategy6.active_positions) > 0:
                    entry_triggered = True
                
            # Monitor active positions & evaluate 0216 Model 5-minute candle signals
            engine.update_and_monitor()
                
            # Render terminal status dashboard once per second
            if time.time() - last_render_time >= 1.0:
                engine.render_dashboard()
                last_render_time = time.time()

            # Check 15:00 PM EOD Exit
            if not exit_triggered and now >= dtime(exit_h, exit_m, exit_s):
                engine.execute_eod_squareoff()
                exit_triggered = True
                print("\n🏁 EOD Execution completed. Exiting forward test loop.")
                break
                
            time.sleep(0.5)
            
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

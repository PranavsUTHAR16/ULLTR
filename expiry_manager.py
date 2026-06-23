import time
import os
import sys
import json
import redis
import subprocess
from datetime import datetime
import upstox_client
from upstox_client.rest import ApiException

def is_market_open_today():
    """Queries Upstox API to verify if the NSE exchange is open today, with a weekend fallback."""
    token_path = '/Users/prana/Desktop/open_source/web/login/access_token.json'
    
    # Force daily refresh: run auth.py if token file doesn't exist or is not from today
    run_needed = False
    if not os.path.exists(token_path):
        run_needed = True
    else:
        from datetime import datetime, date
        mtime_date = datetime.fromtimestamp(os.path.getmtime(token_path)).date()
        if mtime_date < date.today():
            run_needed = True
            
    if run_needed:
        print("🔄 Token is missing or not from today. Running automated token refresh (auth.py) before market check...")
        auth_script = "/Users/prana/Desktop/open_source/web/login/auth.py"
        run_auth = subprocess.run(
            [sys.executable, auth_script],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(auth_script)
        )
        if run_auth.returncode != 0:
            print(f"❌ auth.py failed (exit code {run_auth.returncode}). Error output: {run_auth.stderr}")

    try:
        if not os.path.exists(token_path):
            raise FileNotFoundError(f"Access token file not found at {token_path}")
            
        with open(token_path) as f:
            token = json.load(f)['access_token']
            
        configuration = upstox_client.Configuration()
        configuration.access_token = token
        api_instance = upstox_client.MarketHolidaysAndTimingsApi(upstox_client.ApiClient(configuration))
        try:
            api_response = api_instance.get_market_status('NSE')
        except ApiException as ae:
            if ae.status == 401:
                print("⚠️ Expiry Manager API Unauthorized (401). Deleting stale token and triggering automated token refresh...")
                if os.path.exists(token_path):
                    try:
                        os.remove(token_path)
                    except Exception as ex:
                        print(f"⚠️ Failed to remove stale token file: {ex}")
                
                # Run auth.py
                auth_script = "/Users/prana/Desktop/open_source/web/login/auth.py"
                run_auth = subprocess.run(
                    [sys.executable, auth_script],
                    capture_output=True,
                    text=True,
                    cwd=os.path.dirname(auth_script)
                )
                
                # Reload token
                with open(token_path) as f:
                    token = json.load(f)['access_token']
                configuration.access_token = token
                api_instance = upstox_client.MarketHolidaysAndTimingsApi(upstox_client.ApiClient(configuration))
                api_response = api_instance.get_market_status('NSE')
            else:
                raise
                
        status_data = getattr(api_response, 'data', None)
        status = getattr(status_data, 'status', 'CLOSED').upper() if status_data else 'CLOSED'
        print(f"🔍 Upstox Market Status for NSE today: {status}")
        
        # If the status is CLOSED, the market is definitely closed today
        if status == 'CLOSED':
            return False
        return True
    except Exception as e:
        print(f"⚠️ Failed to get live market status from Upstox API ({e}). Using weekend fallback.")
        # Fallback: check if weekend (Saturday=5, Sunday=6)
        day = datetime.now().weekday()
        if day in [5, 6]:
            print("🔴 Weekend fallback triggered: Today is a weekend. Market is CLOSED.")
            return False
        print("🟢 Weekend fallback triggered: Today is a weekday. Assuming market is OPEN.")
        return True

def restart_collector():
    """Stops the active C++ collector and reconciler, and restarts them in the background."""
    print("🔄 Terminating active C++ Ingestion Collector & Reconciler...")
    # Kill existing collector and reconciler binaries
    subprocess.run(["pkill", "-f", "./collector"], capture_output=True)
    subprocess.run(["pkill", "-f", "reconciler.py"], capture_output=True)
    
    build_dir = "/Users/prana/Desktop/open_source/web/collector/build"
    log_file = "/Users/prana/Desktop/open_source/web/collector_bg.log"
    
    print(f"🔄 Launching C++ Collector in the background (logs: {log_file})...")
    with open(log_file, "a") as log:
        # Spawn C++ collector as a detached background process group
        subprocess.Popen(
            ["./collector", "../config.json"],
            cwd=build_dir,
            stdout=log,
            stderr=log,
            preexec_fn=os.setpgrp # Detach process group so it runs independently of daemon
        )
        
    print("🔄 Launching Standalone Reconciler in the background...")
    reco_log_file = "/Users/prana/Desktop/open_source/web/reconciler_stdout.log"
    with open(reco_log_file, "a") as log:
        subprocess.Popen(
            [sys.executable, "reconciler.py"],
            cwd="/Users/prana/Desktop/open_source/web",
            stdout=log,
            stderr=log,
            preexec_fn=os.setpgrp
        )
        
    print("✅ C++ Collector and Reconciler successfully started in background!")

def restart_strategy_services():
    """Strategy services are disabled/removed."""
    pass

def main():
    print("⏰ Upstox Market Data Expiry & Daily Manager Daemon Started")
    print("==========================================================")
    
    # 1. Connect to Redis (prefer Unix domain socket)
    socket_path = '/Users/prana/Desktop/open_source/web/redis.sock'
    try:
        if os.path.exists(socket_path):
            print(f"Connecting to Redis via Unix Domain Socket: {socket_path}...")
            r = redis.Redis(unix_socket_path=socket_path, decode_responses=True)
        else:
            print("Connecting to Redis via TCP loopback (127.0.0.1:6379)...")
            r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
        print("Connected to Redis successfully.")
    except Exception as e:
        print(f"❌ Failed to connect to Redis: {e}")
        sys.exit(1)
        
    check_interval = 60 # Check every minute for precise time alignment
    print(f"Daemon active. Checking daily and expiry trigger conditions every {check_interval} seconds...\n")
    
    while True:
        try:
            # 2. Load the current active symbols config to fetch front expiry
            symbols_path = "/Users/prana/Desktop/open_source/web/nifty_option_symbols.json"
            if not os.path.exists(symbols_path):
                print(f"⚠️ Symbols file '{symbols_path}' not found. Waiting...")
                time.sleep(30)
                continue
                
            with open(symbols_path) as f:
                sym_data = json.load(f)
                
            # Support both flat and nested configurations
            expiry_dates = {}
            if "expiry_1" in sym_data:
                expiry_1_str = sym_data.get("expiry_1")
                if expiry_1_str:
                    expiry_dates["NIFTY"] = datetime.strptime(expiry_1_str, "%Y-%m-%d").date()
            else:
                for underlying, info in sym_data.items():
                    exp_str = info.get("expiry_1")
                    if exp_str:
                        expiry_dates[underlying] = datetime.strptime(exp_str, "%Y-%m-%d").date()

            if not expiry_dates:
                print("⚠️ No front expiry dates found in nifty_option_symbols.json. Waiting...")
                time.sleep(30)
                continue
            
            # 3. Get current time parameters
            now = datetime.now()
            current_date = now.date()
            current_time_str = now.strftime("%H:%M")
            
            # --- TRIGGER 1: Daily Morning Setup at 08:45 AM (or catch-up on boot) ---
            daily_processed = r.get(f"daily:processed:{current_date}")
            if current_time_str >= "08:45" and not daily_processed:
                print(f"\n☀️ Morning Check Triggered at {current_time_str}...")
                
                # Check if market is open today
                if is_market_open_today():
                    print("🟢 Market is OPEN today. Running morning setup...")
                    print("🔄 Running get_nifty_options.py...")
                    run_opt = subprocess.run(
                        [sys.executable, "get_nifty_options.py"],
                        capture_output=True,
                        text=True,
                        cwd="/Users/prana/Desktop/open_source/web"
                    )
                    print(run_opt.stdout)
                    
                    if run_opt.returncode == 0:
                        # Merge newly selected symbols into C++ collector config
                        print("🔄 Merging updated instruments into C++ configuration...")
                        merge_cmd = """
import json, redis
with open('nifty_option_symbols.json') as f:
    opts = json.load(f)
with open('collector/config.json') as f:
    cfg = json.load(f)
expected = []
if 'index_key' in opts:
    expected = [opts['index_key']] + opts['symbols']
else:
    for underlying, info in opts.items():
        expected.append(info['index_key'])
        expected.extend(info['symbols'])
# Append India VIX to the instruments list
expected.append("NSE_INDEX|India VIX")
cfg['instruments'] = expected
with open('collector/config.json', 'w') as f:
    json.dump(cfg, f, indent=2)

# Seed spot:VIX in Redis
try:
    r = redis.Redis(unix_socket_path="/Users/prana/Desktop/open_source/web/redis.sock", decode_responses=True)
    r.set("spot:VIX", "NSE_INDEX|India VIX")
except:
    pass
"""
                        run_merge = subprocess.run(
                            [sys.executable, "-c", merge_cmd],
                            capture_output=True,
                            text=True,
                            cwd="/Users/prana/Desktop/open_source/web"
                        )
                        print(run_merge.stdout)
                        
                        # Run seeding script to pull historical candles
                        print("🔄 Seeding historical spot candles for index features...")
                        subprocess.run(
                            [sys.executable, "scratch/seed_real_candles.py"],
                            capture_output=True,
                            text=True,
                            cwd="/Users/prana/Desktop/open_source/web"
                        )
                        
                        # Run seeding script to pull historical options candles
                        print("🔄 Seeding historical options candles for catch-up entries...")
                        subprocess.run(
                            [sys.executable, "scratch/seed_option_candles.py"],
                            capture_output=True,
                            text=True,
                            cwd="/Users/prana/Desktop/open_source/web"
                        )
                        
                        # Start C++ Ingestor
                        restart_collector()
                        restart_strategy_services()
                        r.set(f"daily:processed:{current_date}", "open")
                        print(f"🎉 Morning setup and startup successfully completed at {now.strftime('%Y-%m-%d %H:%M:%S')}!\n")
                    else:
                        print(f"❌ Failed to run options finder: {run_opt.stderr}")
                        print("Will retry in the next check cycle...\n")
                else:
                    print("🔴 Market is CLOSED (Holiday/Weekend) today. Stopping collector to conserve resources...")
                    # Stop active collector if running
                    subprocess.run(["pkill", "-f", "./collector"], capture_output=True)
                    r.set(f"daily:processed:{current_date}", "closed")
                    print(f"🎉 Daily morning check processed. Collector stopped. Day marked as closed.\n")
            
            # --- TRIGGER 3: Market Open Collector Fresh Restart at 09:14:58 AM IST ---
            market_restart_processed = r.get(f"daily:market_restart:{current_date}")
            if current_time_str == "09:14" and not market_restart_processed:
                # Double-check if market is open today
                if is_market_open_today():
                    # Sleep until second is exactly 58
                    now_seconds = datetime.now().second
                    sleep_needed = 58 - now_seconds
                    if sleep_needed > 0:
                        print(f"⏳ Market Open approaching. Sleeping {sleep_needed}s to hit exactly 09:14:58 IST...")
                        time.sleep(sleep_needed)
                        
                    print(f"\n🔔 Market Open Restart Triggered at {datetime.now().strftime('%H:%M:%S')} IST...")
                    restart_collector()
                    restart_strategy_services()
                    r.set(f"daily:market_restart:{current_date}", "processed")
                    print(f"🎉 Collector successfully restarted for live market hours at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}!\n")
            
            # --- TRIGGER 2: Afternoon Expiry Rollover at 15:45 PM ---
            is_expiry_day_passed_time = False
            is_expiry_date_stale = False
            for underlying, exp_date in expiry_dates.items():
                if current_date == exp_date and current_time_str >= "15:45":
                    is_expiry_day_passed_time = True
                    print(f"Rollover condition (expiry reached) met for {underlying} (expiry: {exp_date})")
                if current_date > exp_date:
                    is_expiry_date_stale = True
                    print(f"Rollover condition (stale expiry) met for {underlying} (expiry: {exp_date})")
            
            if is_expiry_day_passed_time or is_expiry_date_stale:
                # Retrieve last updated date from Redis cache to prevent double-running
                last_updated = r.get("expiry:last_updated_date")
                
                if last_updated != str(current_date):
                    reason = "Expiry Date Reached at 15:45" if is_expiry_day_passed_time else "Stale Expiry Date (System Offline Catch-up)"
                    print(f"🚨 ROLLOVER TRIGGERED: {reason}")
                    print(f"   Current Date: {current_date} | Expiry Dates: {expiry_dates} | Time: {current_time_str}")
                    
                    # Step A: Run options finder to fetch new instruments & seed Redis maps
                    print("🔄 Running get_nifty_options.py...")
                    run_opt = subprocess.run(
                        [sys.executable, "get_nifty_options.py"],
                        capture_output=True,
                        text=True,
                        cwd="/Users/prana/Desktop/open_source/web"
                    )
                    print(run_opt.stdout)
                    
                    if run_opt.returncode == 0:
                        # Step B: Merge newly selected symbols into C++ collector config
                        print("🔄 Merging updated instruments into C++ configuration...")
                        merge_cmd = """
import json
with open('nifty_option_symbols.json') as f:
    opts = json.load(f)
with open('collector/config.json') as f:
    cfg = json.load(f)
expected = []
if 'index_key' in opts:
    expected = [opts['index_key']] + opts['symbols']
else:
    for underlying, info in opts.items():
        expected.append(info['index_key'])
        expected.extend(info['symbols'])
cfg['instruments'] = expected
with open('collector/config.json', 'w') as f:
    json.dump(cfg, f, indent=2)
"""
                        run_merge = subprocess.run(
                            [sys.executable, "-c", merge_cmd],
                            capture_output=True,
                            text=True,
                            cwd="/Users/prana/Desktop/open_source/web"
                        )
                        print(run_merge.stdout)
                        
                        # Step C: Restart C++ Ingestor detached
                        restart_collector()
                        restart_strategy_services()
                        
                        # Set successfully updated date in Redis to block double-triggering
                        r.set("expiry:last_updated_date", str(current_date))
                        print(f"🎉 Expiry rollover successfully completed at {now.strftime('%Y-%m-%d %H:%M:%S')}!\n")
                    else:
                        print(f"❌ Failed to run options finder: {run_opt.stderr}")
                        print("Will retry in the next check cycle...\n")
            
            # --- TRIGGER 4: Daily After-Market Trades Sync at 15:35 PM IST ---
            if current_time_str >= "15:35":
                history_sync_processed = r.get(f"daily:history_sync:{current_date}")
                if not history_sync_processed:
                    print(f"\n🔔 After-Market Trades Sync Triggered at {current_time_str}...")
                    detailed_csv = "/Users/prana/Desktop/black_box/Rho_Phi_Nifty/data/live_trades_detailed.csv"
                    historical_csv = "/Users/prana/Desktop/black_box/Rho_Phi_Nifty/data/live_trades_historical.csv"
                    
                    if os.path.exists(detailed_csv):
                        try:
                            import pandas as pd
                            df_t = pd.read_csv(detailed_csv)
                            today_str = current_date.strftime("%Y-%m-%d")
                            today_df = df_t[df_t["timestamp"].astype(str).str.startswith(today_str)]
                            
                            if not today_df.empty:
                                if os.path.exists(historical_csv):
                                    df_hist = pd.read_csv(historical_csv)
                                    combined = pd.concat([df_hist, today_df]).drop_duplicates(subset=["timestamp", "action", "position_type"])
                                else:
                                    combined = today_df
                                os.makedirs(os.path.dirname(historical_csv), exist_ok=True)
                                combined.to_csv(historical_csv, index=False)
                                print(f"✅ Successfully synced {len(today_df)} of today's trades into {historical_csv}")
                            else:
                                print("No trades executed today to sync.")
                                
                            tracker_script = "/Users/prana/Desktop/black_box/Rho_Phi_Nifty/live_portfolio_tracker.py"
                            if os.path.exists(tracker_script):
                                print(f"🔄 Executing live_portfolio_tracker.py (EOD report update)...")
                                run_tracker = subprocess.run(
                                    [sys.executable, tracker_script],
                                    capture_output=True,
                                    text=True,
                                    cwd="/Users/prana/Desktop/black_box/Rho_Phi_Nifty"
                                )
                                print(run_tracker.stdout)
                                if run_tracker.returncode != 0:
                                    print(f"⚠️ Tracker warning/error: {run_tracker.stderr}")
                                    
                            r.set(f"daily:history_sync:{current_date}", "processed")
                            print(f"🎉 After-Market Sync successfully completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}!\n")
                        except Exception as sync_ex:
                            print(f"⚠️ Failed to sync trades / aggregate portfolio: {sync_ex}")
                    else:
                        print(f"⚠️ Active trades detailed CSV not found at {detailed_csv}")
            
        except Exception as e:
            print(f"⚠️ Error in daemon check cycle: {e}")
            
        time.sleep(check_interval)

if __name__ == "__main__":
    main()

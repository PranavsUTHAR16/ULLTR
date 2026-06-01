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
                print("⚠️ Expiry Manager API Unauthorized (401). Triggering automated token refresh using auth.py...")
                # Run auth.py
                auth_script = "/Users/prana/Desktop/open_source/web/login/auth.py"
                run_auth = subprocess.run(["python", auth_script], capture_output=True, text=True)
                if run_auth.returncode != 0:
                    subprocess.run(["python3", auth_script], capture_output=True, text=True)
                
                # Reload token
                with open(token_path) as f:
                    token = json.load(f)['access_token']
                configuration.access_token = token
                api_instance = upstox_client.MarketHolidaysAndTimingsApi(upstox_client.ApiClient(configuration))
                api_response = api_instance.get_market_status('NSE')
            else:
                raise
                
        status_data = api_response.get('data', {})
        status = status_data.get('status', 'CLOSED').upper()
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
    """Stops the active C++ collector and restarts it detached in the background."""
    print("🔄 Terminating active C++ Ingestion Collector...")
    # Kill the existing collector binary
    subprocess.run(["pkill", "-f", "./collector"], capture_output=True)
    
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
    print("✅ C++ Collector successfully started in background!")

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
                
            expiry_1_str = sym_data.get("expiry_1") # YYYY-MM-DD
            if not expiry_1_str:
                print("⚠️ No front expiry found in nifty_option_symbols.json. Waiting...")
                time.sleep(30)
                continue
                
            expiry_1_date = datetime.strptime(expiry_1_str, "%Y-%m-%d").date()
            
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
                        ["python", "get_nifty_options.py"],
                        capture_output=True,
                        text=True,
                        cwd="/Users/prana/Desktop/open_source/web"
                    )
                    print(run_opt.stdout)
                    
                    if run_opt.returncode == 0:
                        # Merge newly selected symbols into C++ collector config
                        print("🔄 Merging updated instruments into C++ configuration...")
                        merge_cmd = """
import json
with open('nifty_option_symbols.json') as f:
    opts = json.load(f)
with open('collector/config.json') as f:
    cfg = json.load(f)
cfg['instruments'] = [opts['index_key']] + opts['symbols']
with open('collector/config.json', 'w') as f:
    json.dump(cfg, f, indent=2)
"""
                        run_merge = subprocess.run(
                            ["python", "-c", merge_cmd],
                            capture_output=True,
                            text=True,
                            cwd="/Users/prana/Desktop/open_source/web"
                        )
                        print(run_merge.stdout)
                        
                        # Start C++ Ingestor
                        restart_collector()
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
            
            # --- TRIGGER 2: Afternoon Expiry Rollover at 15:45 PM ---
            # Case A: Today is expiry day and time is >= 15:45 (market close catch-up)
            is_expiry_day_passed_time = (current_date == expiry_1_date and current_time_str >= "15:45")
            
            # Case B: Expiry has passed entirely (handling downtime / system catch-up)
            is_expiry_date_stale = (current_date > expiry_1_date)
            
            if is_expiry_day_passed_time or is_expiry_date_stale:
                # Retrieve last updated date from Redis cache to prevent double-running
                last_updated = r.get("expiry:last_updated_date")
                
                if last_updated != str(current_date):
                    reason = "Expiry Date Reached at 15:45" if is_expiry_day_passed_time else "Stale Expiry Date (System Offline Catch-up)"
                    print(f"🚨 ROLLOVER TRIGGERED: {reason}")
                    print(f"   Current Date: {current_date} | Front Expiry: {expiry_1_date} | Time: {current_time_str}")
                    
                    # Step A: Run options finder to fetch new instruments & seed Redis maps
                    print("🔄 Running get_nifty_options.py...")
                    run_opt = subprocess.run(
                        ["python", "get_nifty_options.py"],
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
cfg['instruments'] = [opts['index_key']] + opts['symbols']
with open('collector/config.json', 'w') as f:
    json.dump(cfg, f, indent=2)
"""
                        run_merge = subprocess.run(
                            ["python", "-c", merge_cmd],
                            capture_output=True,
                            text=True,
                            cwd="/Users/prana/Desktop/open_source/web"
                        )
                        print(run_merge.stdout)
                        
                        # Step C: Restart C++ Ingestor detached
                        restart_collector()
                        
                        # Set successfully updated date in Redis to block double-triggering
                        r.set("expiry:last_updated_date", str(current_date))
                        print(f"🎉 Expiry rollover successfully completed at {now.strftime('%Y-%m-%d %H:%M:%S')}!\n")
                    else:
                        print(f"❌ Failed to run options finder: {run_opt.stderr}")
                        print("Will retry in the next check cycle...\n")
            
        except Exception as e:
            print(f"⚠️ Error in daemon check cycle: {e}")
            
        time.sleep(check_interval)

if __name__ == "__main__":
    main()

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
    """
    Comprehensive Market Status & Holiday Verifier using Upstox API.
    Handles:
    1. Weekends (Saturday / Sunday) -> Market CLOSED.
    2. Official Trading Holidays (via get_holidays()) -> Market CLOSED.
    3. Live Exchange Status (via get_market_status('NSE')) ->
       - NORMAL_OPEN, PRE_OPEN_*, CAS_* -> Market ACTIVE.
       - HOLIDAY, CLOSED -> Market CLOSED.
       - NORMAL_CLOSE, CLOSING_END, CLOSING_START:
         If current time is past 15:30 IST, market is CLOSED for the day.
    """
    now = datetime.now()
    today_date = now.date()
    current_time_str = now.strftime("%H:%M")

    # 1. Immediate Weekend Check (Saturday=5, Sunday=6)
    if now.weekday() in [5, 6]:
        print("🔴 Weekend check: Today is a weekend (Saturday/Sunday). Market is CLOSED.")
        return False

    # 2. Check if current time is past market hours (after 15:30 IST)
    if current_time_str >= "15:30":
        print(f"🔴 Off-market hours: Current time is {current_time_str} IST (past 15:30). Market is CLOSED.")
        return False

    token_path = '/Users/prana/Desktop/open_source/web/login/access_token.json'
    if not os.path.exists(token_path) and os.path.exists('/Users/prana/Desktop/open_source/web/access_token.json'):
        token_path = '/Users/prana/Desktop/open_source/web/access_token.json'

    # Check token age / existence
    run_needed = False
    if not os.path.exists(token_path):
        run_needed = True
    else:
        mtime_date = datetime.fromtimestamp(os.path.getmtime(token_path)).date()
        if mtime_date < today_date:
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
            if os.path.exists('/Users/prana/Desktop/open_source/web/access_token.json'):
                token_path = '/Users/prana/Desktop/open_source/web/access_token.json'
            else:
                raise FileNotFoundError(f"Access token file not found at {token_path}")
            
        with open(token_path) as f:
            token = json.load(f)['access_token']
            
        configuration = upstox_client.Configuration()
        configuration.access_token = token
        api_instance = upstox_client.MarketHolidaysAndTimingsApi(upstox_client.ApiClient(configuration))

        # 3. Check Upstox Official Holiday Calendar
        try:
            holidays_resp = api_instance.get_holidays()
            if holidays_resp and hasattr(holidays_resp, 'data') and holidays_resp.data:
                for h in holidays_resp.data:
                    d = getattr(h, '_date', None) or getattr(h, 'date', None)
                    if hasattr(d, 'date'):
                        d = d.date()
                    elif isinstance(d, str):
                        d = datetime.strptime(d[:10], '%Y-%m-%d').date()
                    if d == today_date:
                        h_type = getattr(h, 'holiday_type', 'TRADING_HOLIDAY')
                        closed_ex = getattr(h, 'closed_exchanges', []) or []
                        if h_type == 'TRADING_HOLIDAY' and (not closed_ex or any(ex in closed_ex for ex in ['NSE', 'NFO', 'BSE', 'BFO'])):
                            h_desc = getattr(h, 'description', 'Official Exchange Holiday')
                            print(f"🔴 Upstox Holiday Detected: Today ({today_date}) is {h_desc}. Market is CLOSED.")
                            return False
        except Exception as he:
            print(f"⚠️ Warning querying Upstox holiday calendar: {he}")

        # 4. Check Upstox Live Market Status for NSE
        try:
            api_response = api_instance.get_market_status('NSE')
        except ApiException as ae:
            if ae.status == 401:
                print("⚠️ Expiry Manager API Unauthorized (401). Refreshing token...")
                auth_script = "/Users/prana/Desktop/open_source/web/login/auth.py"
                subprocess.run(
                    [sys.executable, auth_script],
                    capture_output=True,
                    text=True,
                    cwd=os.path.dirname(auth_script)
                )
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
        
        # Definitive Closed Statuses
        if status in ['HOLIDAY', 'CLOSED']:
            print(f"🔴 Market status is {status}. Market is CLOSED.")
            return False

        if status in ['NORMAL_CLOSE', 'CLOSING_END', 'CLOSING_START']:
            if current_time_str >= "15:30":
                print(f"🔴 Market has concluded for the day (Status: {status} at {current_time_str} IST). Market is CLOSED.")
                return False

        # Active trading statuses
        active_statuses = [
            'NORMAL_OPEN', 'PRE_OPEN_START', 'PRE_OPEN_END', 'PRE_OPEN_M_END',
            'CAS_LM_START', 'CAS_M_STOP', 'CAS_STOP', 'CTS_CLOSE'
        ]
        if status in active_statuses:
            print(f"🟢 Market is actively open (Status: {status}).")
            return True

        if "08:30" <= current_time_str <= "15:30":
            print(f"🟡 Market status is {status} during trading hours (08:30-15:30). Assuming OPEN.")
            return True

        return False
    except Exception as e:
        print(f"⚠️ Failed to get live market status from Upstox API ({e}). Using time/weekday fallback.")
        if now.weekday() in [5, 6] or current_time_str >= "15:30":
            print(f"🔴 Fallback: Weekend or after 15:30 IST ({current_time_str}). Market is CLOSED.")
            return False
        print(f"🟢 Fallback: Weekday during trading hours ({current_time_str}). Assuming market is OPEN.")
        return True

def clear_premarket_logs():
    """Truncates log files each morning during premarket activity to keep disk I/O and memory ultra-fast."""
    log_files = [
        "/Users/prana/Desktop/open_source/web/collector_bg.log",
        "/Users/prana/Desktop/open_source/web/reconciler_stdout.log",
        "/Users/prana/Desktop/open_source/web/reconciler.log",
    ]
    for lf in log_files:
        try:
            if os.path.exists(lf):
                with open(lf, "w") as f:
                    f.write(f"=== Premarket log reset for {date.today()} ===\n")
                print(f"🧹 Truncated premarket log: {lf}")
        except Exception as e:
            print(f"⚠️ Could not clear log {lf}: {e}")

def restart_collector(clear_logs: bool = False):
    """Stops the active C++ collector and reconciler, and restarts them in the background."""
    print("🔄 Terminating active C++ Ingestion Collector & Reconciler...")
    if clear_logs:
        clear_premarket_logs()
        
    # Check if systemd manages ulltr-collector
    res = subprocess.run(["systemctl", "is-active", "ulltr-collector.service"], capture_output=True, text=True)
    if res.returncode == 0:
        print("🔄 Restarting ulltr-collector.service via systemctl...")
        subprocess.run(["sudo", "systemctl", "restart", "ulltr-collector.service"], capture_output=True)
    else:
        # Kill existing collector binaries cleanly
        subprocess.run(["pkill", "-9", "-f", "collector/build/collector"], capture_output=True)
        subprocess.run(["pkill", "-9", "-f", "./collector"], capture_output=True)
        time.sleep(0.5)
        
        build_dir = "/Users/prana/Desktop/open_source/web/collector/build"
        log_file = "/Users/prana/Desktop/open_source/web/collector_bg.log"
        
        print(f"🔄 Launching C++ Collector in the background (logs: {log_file})...")
        with open(log_file, "a") as log:
            subprocess.Popen(
                ["./collector", "../config.json"],
                cwd=build_dir,
                stdout=log,
                stderr=log,
                preexec_fn=os.setpgrp
            )
        
    subprocess.run(["pkill", "-f", "reconciler.py"], capture_output=True)
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
                        run_merge = subprocess.run(
                            [sys.executable, "scripts/update_instruments.py"],
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
            
            # --- TRIGGER 3.5: Scrape India 10-Year Bond Yield at 09:17:30 AM IST ---
            bond_yield_processed = r.get(f"daily:bond_yield:{current_date}")
            if current_time_str == "09:17" and not bond_yield_processed:
                print(f"\n📈 Premarket 09:17 AM Trigger: Scraping India 10Y Bond Yield from Investing.com...")
                try:
                    subprocess.run(
                        [sys.executable, "scrape_bond_yield.py"],
                        capture_output=True,
                        text=True,
                        cwd="/Users/prana/Desktop/open_source/web"
                    )
                    r.set(f"daily:bond_yield:{current_date}", "processed")
                    print(f"✅ Bond yield scraped and saved for dynamic Greeks calculation.")
                except Exception as e:
                    print(f"⚠️ Error running bond yield scraper: {e}")
            
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
                        run_merge = subprocess.run(
                            [sys.executable, "scripts/update_instruments.py"],
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

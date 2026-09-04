#!/usr/bin/env python3
import os
import sys
import subprocess
import time
import json
from datetime import datetime

# Define base paths
BASE_DIR = "/Users/prana/Desktop/open_source/web"
REDIS_SOCK = os.path.join(BASE_DIR, "redis.sock")
TOKEN_PATH = os.path.join(BASE_DIR, "login/access_token.json")
SYMBOLS_PATH = os.path.join(BASE_DIR, "nifty_option_symbols.json")
COLLECTOR_BUILD_DIR = os.path.join(BASE_DIR, "collector/build")
COLLECTOR_BIN = os.path.join(COLLECTOR_BUILD_DIR, "collector")

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def check_redis_running():
    try:
        import redis
        if os.path.exists(REDIS_SOCK):
            r = redis.Redis(unix_socket_path=REDIS_SOCK, socket_timeout=2)
            if r.ping():
                return True
        r_tcp = redis.Redis(host="127.0.0.1", port=6379, socket_timeout=2)
        if r_tcp.ping():
            return True
    except Exception:
        pass
    return False

def start_redis():
    log("⚠️ Redis is not running. Attempting to start Redis server...")
    redis_conf = os.path.join(BASE_DIR, "redis.conf")
    
    # Try macOS path
    macos_redis = "/opt/homebrew/bin/redis-server"
    if os.path.exists(macos_redis):
        cmd = [macos_redis, redis_conf, "--daemonize", "yes"]
    else:
        cmd = ["redis-server", redis_conf, "--daemonize", "yes"]
        
    try:
        subprocess.run(cmd, check=True)
        time.sleep(2)  # Wait for boot
        if check_redis_running():
            log("✅ Redis server started successfully.")
            return True
        else:
            log("❌ Redis server started but is still unreachable.")
            return False
    except Exception as e:
        log(f"❌ Failed to start Redis server: {e}")
        return False

def get_redis_client():
    import redis
    if os.path.exists(REDIS_SOCK):
        return redis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=True)
    return redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

def is_process_running(proc_name_pattern):
    try:
        # Use pgrep to find matching processes
        output = subprocess.run(["pgrep", "-f", proc_name_pattern], capture_output=True, text=True)
        pids = output.stdout.strip().split()
        # Filter out current script PID
        my_pid = str(os.getpid())
        pids = [p for p in pids if p != my_pid]
        return len(pids) > 0
    except Exception:
        # Fallback to ps aux grep
        try:
            output = subprocess.run(["ps", "aux"], capture_output=True, text=True)
            count = 0
            for line in output.stdout.splitlines():
                if proc_name_pattern in line and "grep" not in line and str(os.getpid()) not in line:
                    count += 1
            return count > 0
        except Exception:
            return False

def recover_services():
    log("🚀 Starting ULLTR services recovery check...")
    
    # 1. Ensure Redis is up
    if not check_redis_running():
        if not start_redis():
            log("❌ Critical Error: Redis is offline and cannot be started. Exiting.")
            sys.exit(1)
    else:
        log("✅ Redis is online.")
        
    r = get_redis_client()
    
    # 2. Check token validity
    if not os.path.exists(TOKEN_PATH):
        log("⚠️ Access token missing. Running login/auth.py...")
        try:
            subprocess.run([sys.executable, os.path.join(BASE_DIR, "login/auth.py")], check=True, cwd=BASE_DIR)
        except Exception as e:
            log(f"❌ Failed to refresh token: {e}")
            
    # Check if we have token today
    try:
        with open(TOKEN_PATH) as f:
            token_data = json.load(f)
            # Simple check of token field
            if "access_token" not in token_data:
                raise ValueError("No access_token field in token file")
    except Exception as e:
        log(f"⚠️ Access token is invalid or unreadable: {e}. Running login/auth.py...")
        try:
            subprocess.run([sys.executable, os.path.join(BASE_DIR, "login/auth.py")], check=True, cwd=BASE_DIR)
        except Exception as auth_err:
            log(f"❌ Failed to refresh token: {auth_err}")
            
    # 3. Check Option Chain Metadata
    today_str = datetime.now().strftime("%Y-%m-%d")
    spot_quote = r.hgetall("md:quote:NSE_INDEX|Nifty 50")
    spot_quote_sensex = r.hgetall("md:quote:BSE_INDEX|SENSEX")
    
    # Check if spot and option chains metadata are seeded in Redis
    chain_keys = r.keys("chain:NIFTY:*")
    chain_keys_sensex = r.keys("chain:SENSEX:*")
    if not chain_keys or not chain_keys_sensex or not spot_quote or not spot_quote_sensex:
        log("⚠️ Option chain metadata/spot quote missing in Redis. Seeding options configuration...")
        try:
            subprocess.run([sys.executable, os.path.join(BASE_DIR, "get_nifty_options.py")], check=True, cwd=BASE_DIR)
            # Reload chain keys
            chain_keys = r.keys("chain:NIFTY:*")
        except Exception as e:
            log(f"❌ Failed to seed options configuration: {e}")
            
    # 4. Check/Merge symbols into collector config
    if os.path.exists(SYMBOLS_PATH):
        try:
            with open(SYMBOLS_PATH) as f:
                opts = json.load(f)
            collector_config_path = os.path.join(BASE_DIR, "collector/config.json")
            if os.path.exists(collector_config_path):
                with open(collector_config_path) as f:
                    cfg = json.load(f)
                
                # Check if they are matched
                expected_instruments = []
                if "index_key" in opts:
                    expected_instruments = [opts['index_key']] + opts['symbols']
                else:
                    for underlying, info in opts.items():
                        expected_instruments.append(info['index_key'])
                        expected_instruments.extend(info['symbols'])
                        
                if cfg.get('instruments') != expected_instruments:
                    log("🔄 Merging updated instruments into C++ configuration...")
                    cfg['instruments'] = expected_instruments
                    with open(collector_config_path, 'w') as f:
                        json.dump(cfg, f, indent=2)
                    log("✅ Instruments merged successfully.")
        except Exception as e:
            log(f"⚠️ Failed to verify/merge collector configuration: {e}")
            
    # 5. Check if candles are seeded
    candles_keys_count = len(r.keys("md:candles:*"))
    if candles_keys_count < 10:  # If we have very few option candle keys, trigger seeding
        log(f"⚠️ Only {candles_keys_count} options candles keys found in Redis. Seeding historical options and spot candles...")
        try:
            log("🔄 Seeding historical spot index candles...")
            subprocess.run([sys.executable, os.path.join(BASE_DIR, "scratch/seed_real_candles.py")], check=True, cwd=BASE_DIR)
            
            log("🔄 Seeding option candles (this takes a moment)...")
            subprocess.run([sys.executable, os.path.join(BASE_DIR, "scratch/seed_option_candles.py")], check=True, cwd=BASE_DIR)
            log("✅ Seeding completed successfully.")
        except Exception as e:
            log(f"❌ Seeding process encountered an error: {e}")
    else:
        log(f"✅ Option candles are seeded in Redis ({candles_keys_count} keys).")
        
    # 6. Ensure C++ Ingestor (Collector) is running
    is_collector_running = is_process_running("./collector")
    if not is_collector_running:
        log("⚠️ C++ Ingestor (Collector) is NOT running. Launching...")
        log_file = os.path.join(BASE_DIR, "collector_bg.log")
        # Ensure build directory and binary exist
        if not os.path.exists(COLLECTOR_BIN):
            log("❌ Error: C++ collector binary not found at build directory. Attempting to build...")
            try:
                os.makedirs(COLLECTOR_BUILD_DIR, exist_ok=True)
                subprocess.run(["cmake", ".."], check=True, cwd=COLLECTOR_BUILD_DIR)
                subprocess.run(["make"], check=True, cwd=COLLECTOR_BUILD_DIR)
                log("✅ C++ collector compiled successfully.")
            except Exception as e:
                log(f"❌ Failed to compile C++ collector: {e}")
                
        if os.path.exists(COLLECTOR_BIN):
            try:
                with open(log_file, "a") as log_fh:
                    subprocess.Popen(
                        ["./collector", "../config.json"],
                        cwd=COLLECTOR_BUILD_DIR,
                        stdout=log_fh,
                        stderr=log_fh,
                        preexec_fn=os.setpgrp
                    )
                log("✅ C++ Ingestor (Collector) started.")
            except Exception as e:
                log(f"❌ Failed to start C++ collector: {e}")
    else:
        log("✅ C++ Ingestor (Collector) is running.")
        
    # 7. Ensure Python Reconciler is running
    is_reconciler_running = is_process_running("reconciler.py")
    if not is_reconciler_running:
        log("⚠️ Python Reconciler is NOT running. Launching...")
        reco_log_file = os.path.join(BASE_DIR, "reconciler_stdout.log")
        try:
            with open(reco_log_file, "a") as log_fh:
                subprocess.Popen(
                    [sys.executable, "reconciler.py"],
                    cwd=BASE_DIR,
                    stdout=log_fh,
                    stderr=log_fh,
                    preexec_fn=os.setpgrp
                )
            log("✅ Python Reconciler started.")
        except Exception as e:
            log(f"❌ Failed to start reconciler: {e}")
    else:
        log("✅ Python Reconciler is running.")
        
    # 8. Ensure Expiry Manager Daemon is running
    is_daemon_running = is_process_running("expiry_manager.py")
    if not is_daemon_running:
        log("⚠️ Expiry Manager Daemon is NOT running. Launching...")
        
        # Set processed flag for today to prevent re-setup loop
        today_processed_key = f"daily:processed:{today_str}"
        if not r.get(today_processed_key):
            r.set(today_processed_key, "open")
            log(f"Marked {today_processed_key} as open to prevent duplicate morning setup.")
            
        daemon_stdout_log = os.path.join(BASE_DIR, "expiry_manager_stdout.log")
        try:
            with open(daemon_stdout_log, "a") as log_fh:
                subprocess.Popen(
                    [sys.executable, "-u", "expiry_manager.py"],
                    cwd=BASE_DIR,
                    stdout=log_fh,
                    stderr=log_fh,
                    preexec_fn=os.setpgrp
                )
            log("✅ Expiry Manager Daemon started.")
        except Exception as e:
            log(f"❌ Failed to start Expiry Manager Daemon: {e}")
    else:
        log("✅ Expiry Manager Daemon is running.")
        
    log("🎉 Recovery check complete.")

if __name__ == "__main__":
    recover_services()

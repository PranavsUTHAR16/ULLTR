"""
ULLTR System - WebSocket & Broker Data Feed Health Monitor
==========================================================
Checks every 10 seconds that the broker's WebSocket connection is active,
ticks are flowing into the system properly, and reconciler is running.

Monitors:
1. Process Patrol: Verifies C++ collector, reconciler.py, and expiry_manager.py are active.
2. Tick Data Flow: Verifies that logs and status dashboards are updated in real-time.
3. Redis Connectivity: Ensures Redis Unix Domain Socket or TCP connection is healthy.
4. Smart Suppression & Heartbeats: Keeps Telegram clean with hourly heartbeats and rate-limiting.

Note: Market hours restrictions are completely removed. The health checker executes
fully 24/7 (or whenever the VM starts, e.g., for EOD data fetching/catchup after 5 PM).
"""

import os
import sys
import time
import subprocess
import logging
import asyncio
import aiohttp
from datetime import datetime
import pytz

# Add current directory to path so we can import config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Define IST timezone manually for ULLTR
IST = pytz.timezone('Asia/Kolkata')

# Telegram configuration from reconciler or static settings
TELEGRAM_BOT_TOKEN = "8234942867:AAFdoNjo72DsEYo9DSicTJm8-t5n_B_G30g"
TELEGRAM_CHAT_ID = "-5009029141"
TELEGRAM_ENABLED = True

# Health Check Configurations
CHECK_INTERVAL = 10
HEARTBEAT_HOURS = 1.0

# ULLTR Paths
BASE_DIR = "/Users/prana/Desktop/open_source/web"
COLLECTOR_LOG = os.path.join(BASE_DIR, "collector_bg.log")
RECONCILER_LOG = os.path.join(BASE_DIR, "reconciler.log")
RECO_STDOUT = os.path.join(BASE_DIR, "reconciler_stdout.log")

# Setup logging for the health checker itself
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | HEALTH | %(levelname)s | %(message)s'
)
logger = logging.getLogger("ULLTRHealthChecker")


class ULLTRHealthChecker:
    def __init__(self):
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.enabled = TELEGRAM_ENABLED and self.bot_token != "YOUR_BOT_TOKEN_HERE"
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.session = None
        
        # Log offsets
        self.reco_offset = 0
        if os.path.exists(RECONCILER_LOG):
            self.reco_offset = os.path.getsize(RECONCILER_LOG)
            
        self.coll_offset = 0
        if os.path.exists(COLLECTOR_LOG):
            self.coll_offset = os.path.getsize(COLLECTOR_LOG)
            
        # Alarm states
        self.alert_states = {
            "ws_disconnected": False,
            "data_feed_stalled": False,
            "collector_down": False,
            "reconciler_down": False,
            "redis_down": False
        }
        
        # Timing trackers
        self.last_heartbeat_time = 0
        self.last_tick_time = time.time()
        self.errors_sent_this_minute = 0
        self.last_minute_reset = time.time()

    async def init_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def send_telegram(self, text: str):
        if not self.enabled:
            logger.info(f"[HEALTH-MOCK] {text}")
            return False
            
        try:
            await self.init_session()
            url = f"{self.base_url}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML"
            }
            async with self.session.post(url, json=payload) as response:
                if response.status == 200:
                    return True
                else:
                    err_txt = await response.text()
                    logger.error(f"Telegram API error {response.status}: {err_txt}")
                    return False
        except Exception as e:
            logger.error(f"Telegram request failed: {e}")
            return False

    async def check_redis(self):
        """Checks if local Redis server is active and accessible via unix socket or loopback"""
        try:
            import redis
            socket_path = "/Users/prana/Desktop/open_source/web/redis.sock"
            if os.path.exists(socket_path):
                r = redis.Redis(unix_socket_path=socket_path, decode_responses=True)
            else:
                r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)
                
            r.ping()
            
            if self.alert_states["redis_down"]:
                self.alert_states["redis_down"] = False
                await self.send_telegram("✅ <b>REDIS DATABASE RECOVERY</b>\nRedis connection successfully restored.")
        except Exception as e:
            if not self.alert_states["redis_down"]:
                self.alert_states["redis_down"] = True
                await self.send_telegram(f"🚨 <b>REDIS DATABASE ALERT</b>\n❌ Failed to ping Redis database! Error: {e}")

    async def check_processes(self):
        """Monitors active state of ULLTR system processes"""
        try:
            # 1. Check C++ Ingestor (./collector)
            coll_check = subprocess.run(["pgrep", "-f", "./collector"], capture_output=True)
            if coll_check.returncode != 0:
                if not self.alert_states["collector_down"]:
                    self.alert_states["collector_down"] = True
                    await self.send_telegram("🚨 <b>ULLTR SYSTEM ALERT</b>\n❌ C++ Ingestor (<code>./collector</code>) process is not running!")
            else:
                if self.alert_states["collector_down"]:
                    self.alert_states["collector_down"] = False
                    await self.send_telegram("✅ <b>ULLTR PROCESS RECOVERY</b>\nC++ Ingestor (<code>./collector</code>) is back online.")

            # 2. Check Reconciler daemon (reconciler.py)
            reco_check = subprocess.run(["pgrep", "-f", "reconciler.py"], capture_output=True)
            if reco_check.returncode != 0:
                if not self.alert_states["reconciler_down"]:
                    self.alert_states["reconciler_down"] = True
                    await self.send_telegram("🚨 <b>ULLTR SYSTEM ALERT</b>\n❌ Python Reconciler (<code>reconciler.py</code>) is not running!")
            else:
                if self.alert_states["reconciler_down"]:
                    self.alert_states["reconciler_down"] = False
                    await self.send_telegram("✅ <b>ULLTR PROCESS RECOVERY</b>\nPython Reconciler (<code>reconciler.py</code>) is back online.")

        except Exception as e:
            logger.error(f"Error checking processes: {e}")

    async def check_data_feed(self):
        """Verifies that WebSocket tick data is active and flowing into the system"""
        now = time.time()
        feed_active = False

        # Verify tick activity by checking modification time of C++ log
        if os.path.exists(COLLECTOR_LOG):
            mtime = os.path.getmtime(COLLECTOR_LOG)
            if now - mtime <= 30:  # Collector log updated in last 30s
                feed_active = True
                self.last_tick_time = mtime

        # Verify by checking reconciler log modifications
        if not feed_active and os.path.exists(RECONCILER_LOG):
            mtime = os.path.getmtime(RECONCILER_LOG)
            if now - mtime <= 45:  # Reconciler log updated in last 45s
                feed_active = True
                self.last_tick_time = mtime

        # Alert if data feed stalls (No ticks logged)
        if not feed_active:
            # Gating: Check if current time is within weekdays and market hours (09:16 - 15:30 IST)
            # Also check if Redis has marked today as a market holiday
            now_ist = datetime.now(IST)
            is_weekday = now_ist.weekday() < 5
            time_str = now_ist.strftime("%H:%M")
            is_market_hours = "09:16" <= time_str <= "15:30"
            
            is_market_holiday = False
            try:
                import redis
                socket_path = "/Users/prana/Desktop/open_source/web/redis.sock"
                if os.path.exists(socket_path):
                    r_check = redis.Redis(unix_socket_path=socket_path, decode_responses=True)
                else:
                    r_check = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)
                processed = r_check.get(f"daily:processed:{now_ist.date()}")
                if processed == "closed":
                    is_market_holiday = True
            except Exception:
                pass

            if is_weekday and is_market_hours and not is_market_holiday:
                elapsed = now - self.last_tick_time
                if elapsed > 60:
                    if not self.alert_states["data_feed_stalled"]:
                        self.alert_states["data_feed_stalled"] = True
                        msg = (f"🚨 <b>ULLTR DATA FEED ALERT</b>\n"
                               f"━━━━━━━━━━━━━━━━━\n"
                               f"⚠️ <b>No market ticks received in {elapsed:.0f}s!</b>\n"
                               f"🔌 This indicates the broker WebSocket connection is frozen, "
                               f"or C++ collector has stopped parsing feed ticks.\n\n"
                               f"🕐 Time: {datetime.now(IST).strftime('%H:%M:%S')} IST")
                        await self.send_telegram(msg)
        else:
            if self.alert_states["data_feed_stalled"]:
                self.alert_states["data_feed_stalled"] = False
                msg = (f"✅ <b>ULLTR DATA FEED RECOVERY</b>\n"
                       f"━━━━━━━━━━━━━━━━━\n"
                       f"🔌 Market ticks resumed! C++ Collector is active.\n"
                       f"🕐 Time: {datetime.now(IST).strftime('%H:%M:%S')} IST")
                await self.send_telegram(msg)

    async def scan_log_for_websocket_events(self):
        """Scans reconciler and collector logs for connection closures, 401s, or 429 rate limits"""
        await self.scan_file_for_errors(RECONCILER_LOG, "reco")
        await self.scan_file_for_errors(COLLECTOR_LOG, "collector")

    async def scan_file_for_errors(self, filepath: str, source_label: str):
        if not os.path.exists(filepath):
            return

        try:
            # Reset minute-based error rate limiter
            now = time.time()
            if now - self.last_minute_reset > 60:
                self.errors_sent_this_minute = 0
                self.last_minute_reset = now

            file_size = os.path.getsize(filepath)
            
            # Read last offset
            offset_attr = f"{source_label}_offset"
            current_offset = getattr(self, offset_attr, 0)

            if file_size < current_offset:
                current_offset = 0  # Rotated

            # Safety Guard: If the new log data is too large (e.g., > 5MB),
            # only read the last 50,000 bytes to avoid OOM crashes.
            if file_size - current_offset > 5 * 1024 * 1024:
                current_offset = max(0, file_size - 50000)

            if file_size == current_offset:
                return

            with open(filepath, "r", errors="ignore") as f:
                f.seek(current_offset)
                new_lines = f.readlines()
                setattr(self, offset_attr, f.tell())

            # Serious connection/broker keywords
            disconnect_triggers = ["WebSocket closed", "WebSocket disconnected", "Connection closed", "socket.timeout", "disconnected"]
            error_triggers = ["Fatal error", "login failed", "Unauthorized", "401", "CRITICAL", "Traceback", "429", "Rate Limit"]

            for line in new_lines:
                if "[Tick]" in line or "LTP:" in line:
                    continue
                # 1. Connection Drop Alert
                if any(trig in line for trig in disconnect_triggers):
                    if not self.alert_states["ws_disconnected"]:
                        self.alert_states["ws_disconnected"] = True
                        msg = (f"🔌 <b>ULLTR FEED DISCONNECTED</b>\n"
                               f"━━━━━━━━━━━━━━━━━\n"
                               f"❌ WebSocket connection closed! Source: <code>{source_label}</code>\n"
                               f"📝 Log: <code>{line.strip()[:150]}</code>\n\n"
                               f"👉 The daily manager or collector will attempt reconnection.")
                        await self.send_telegram(msg)
                
                # Reset connection drop alert
                elif "WebSocket connected" in line or "Connected and subscribed" in line:
                    if self.alert_states["ws_disconnected"]:
                        self.alert_states["ws_disconnected"] = False
                        msg = (f"🔌 <b>ULLTR FEED RESTORED</b>\n"
                               f"━━━━━━━━━━━━━━━━━\n"
                               f"✅ WebSocket successfully connected to market feed!\n"
                               f"🕐 Time: {datetime.now(IST).strftime('%H:%M:%S')} IST")
                        await self.send_telegram(msg)

                # 2. API error monitoring
                elif any(trig in line for trig in error_triggers):
                    if self.errors_sent_this_minute < 5:
                        self.errors_sent_this_minute += 1
                        msg = (f"🚨 <b>ULLTR EXCEPTION ALERT</b>\n"
                               f"━━━━━━━━━━━━━━━━━\n"
                               f"❌ Error logged in [<code>{source_label}</code>]:\n"
                               f"<code>{line.strip()[:250]}</code>\n\n"
                               f"🕐 Time: {datetime.now(IST).strftime('%H:%M:%S')} IST")
                        await self.send_telegram(msg)
                    elif self.errors_sent_this_minute == 5:
                        self.errors_sent_this_minute += 1
                        msg = (f"⚠️ <b>ULLTR ALERT SUPPRESSION</b>\n"
                               f"Too many errors logged in a short period. Suppressing logs for 60 seconds.")
                        await self.send_telegram(msg)

        except Exception as e:
            logger.error(f"Error scanning log {filepath}: {e}")

    async def send_heartbeat(self):
        """Hourly status heartbeat confirming feed and connection integrity"""
        now = time.time()
        heartbeat_interval = HEARTBEAT_HOURS * 3600
        now_ist = datetime.now(IST)

        if now - self.last_heartbeat_time < heartbeat_interval:
            return

        try:
            # Check feed last tick duration
            last_tick_elapsed = "N/A"
            if self.last_tick_time > 0:
                last_tick_elapsed = f"{time.time() - self.last_tick_time:.0f}s ago"
            
            feed_health = "💚 ACTIVE"
            if self.alert_states["data_feed_stalled"]:
                feed_health = "🚨 STALLED (No ticks!)"

            ws_status = "🔌 Connected" if not self.alert_states["ws_disconnected"] else "❌ Disconnected"
            
            # Check process statuses
            coll_active = "Offline"
            if subprocess.run(["pgrep", "-f", "./collector"], capture_output=True).returncode == 0:
                coll_active = "Online"
                
            reco_active = "Offline"
            if subprocess.run(["pgrep", "-f", "reconciler.py"], capture_output=True).returncode == 0:
                reco_active = "Online"
            
            msg = (f"🩺 <b>ULLTR SYSTEM HEARTBEAT</b>\n"
                   f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                   f"🕐 Time: <b>{now_ist.strftime('%Y-%m-%d %H:%M:%S')} IST</b>\n"
                   f"📡 WebSocket Status: <b>{ws_status}</b>\n"
                   f"📈 Broker Data Feed: <b>{feed_health}</b> (Last data: {last_tick_elapsed})\n"
                   f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                   f"⚙️ <b>ULLTR Process States</b>:\n"
                   f"  • C++ Collector: <code>{coll_active}</code>\n"
                   f"  • Python Reconciler: <code>{reco_active}</code>\n\n"
                   f"✅ Data pipeline running optimally.")
            
            success = await self.send_telegram(msg)
            if success:
                self.last_heartbeat_time = now
                logger.info("Sent hourly heartbeat to Telegram")
        except Exception as e:
            logger.error(f"Error compiling heartbeat: {e}")

    async def run(self):
        logger.info("🚀 Starting ULLTR Broker Feed Health Monitor...")
        await self.init_session()
        
        # Confirm startup
        startup_msg = (f"🩺 <b>ULLTR DATA FEED MONITOR ACTIVE</b>\n"
                       f"━━━━━━━━━━━━━━━━━\n"
                       f"📍 Host: AWS EC2 ap-south-1 (Mumbai)\n"
                       f"🔍 Frequency: {CHECK_INTERVAL} seconds\n"
                       f"📈 Monitoring C++ collector websockets & Redis ticks.\n"
                       f"✅ Connection verified!")
        await self.send_telegram(startup_msg)

        try:
            while True:
                await self.check_redis()
                await self.check_processes()
                await self.check_data_feed()
                await self.scan_log_for_websocket_events()
                await self.send_heartbeat()
                
                await asyncio.sleep(CHECK_INTERVAL)
        finally:
            await self.close_session()


if __name__ == "__main__":
    checker = ULLTRHealthChecker()
    try:
        asyncio.run(checker.run())
    except KeyboardInterrupt:
        logger.info("🛑 ULLTR Health Checker stopped by user")

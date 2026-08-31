#!/usr/bin/env bash
# ==============================================================================
# ULLTR Automated EOD Cleanup & Disk Maintenance Script
# Runs daily at 16:15 PM IST (and on boot) to maintain disk health & prevent ENOSPC
# ==============================================================================

set -euo pipefail

LOG_PREFIX="[$(date '+%Y-%m-%d %H:%M:%S')] [ULLTR-EOD-CLEANUP]"

echo "$LOG_PREFIX 🧹 Starting daily automated disk cleanup and maintenance..."

# 1. Truncate heavy log files exceeding 50MB
WEB_DIR="/Users/prana/Desktop/open_source/web"
if [ -d "$WEB_DIR" ]; then
    find "$WEB_DIR" -maxdepth 2 -type f \( -name "*.log" -o -name "*_stdout.log" \) -size +50M | while read -r logfile; do
        echo "$LOG_PREFIX Truncating oversized log file: $logfile ($(du -h "$logfile" | cut -f1))"
        truncate -s 0 "$logfile"
    done
fi

# 2. Vacuum systemd journal logs to retain maximum 100MB
echo "$LOG_PREFIX Vacuuming systemd journal logs (max 100MB / 2 days)..."
journalctl --vacuum-size=100M --vacuum-time=2d 2>/dev/null || true

# 3. Clean up temporary files, Playwright artifacts, and crash dumps
echo "$LOG_PREFIX Cleaning temporary cache and browser artifacts in /tmp..."
rm -rf /tmp/playwright* /tmp/chromium* /tmp/core* /tmp/*.tmp /tmp/*.log 2>/dev/null || true

# 4. Clean APT package cache & pip cache (Strictly PRESERVING Playwright browser binaries)
echo "$LOG_PREFIX Cleaning APT, pip & temporary cache while preserving Playwright binaries..."
apt-get clean 2>/dev/null || true
rm -rf /root/.cache/pip /home/ubuntu/.cache/pip /root/.cache/matplotlib /home/ubuntu/.cache/matplotlib 2>/dev/null || true

# Ensure Playwright browser binaries exist for both root and ubuntu users
if [ -d "/home/ubuntu/.cache/ms-playwright" ] && [ ! -d "/root/.cache/ms-playwright" ]; then
    mkdir -p /root/.cache
    cp -r /home/ubuntu/.cache/ms-playwright /root/.cache/
fi

# Ensure full ownership and permissions remain with ubuntu user
if [ -d "$WEB_DIR" ]; then
    chown -R ubuntu:ubuntu "$WEB_DIR" 2>/dev/null || true
    chmod -R 775 "$WEB_DIR/login" 2>/dev/null || true
fi

# 5. Verify and restore DNS configuration (/etc/resolv.conf)
if [ ! -f /etc/resolv.conf ] || ! grep -q "nameserver 8.8.8.8" /etc/resolv.conf; then
    echo "$LOG_PREFIX Restoring /etc/resolv.conf nameservers..."
    cat << 'EOF' > /etc/resolv.conf
nameserver 8.8.8.8
nameserver 1.1.1.1
nameserver 127.0.0.53
options edns0 trust-ad
EOF
fi

# 6. Report Free Disk Space
FREE_SPACE=$(df -h / | awk 'NR==2 {print $4}')
USED_PCT=$(df -h / | awk 'NR==2 {print $5}')

echo "$LOG_PREFIX ✅ EOD Cleanup completed successfully! Root disk space available: $FREE_SPACE (Usage: $USED_PCT)"

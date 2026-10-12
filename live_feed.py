#!/usr/bin/env python3
"""Convenience launcher for the ULLTR Real-Time WebSocket Feed & Redis Monitor."""
import os
import sys

TARGET_SCRIPT = "/Users/prana/Desktop/open_source/web/scripts/live_websocket_monitor.py"

if __name__ == "__main__":
    if not os.path.exists(TARGET_SCRIPT):
        print(f"Error: Could not locate {TARGET_SCRIPT}")
        sys.exit(1)
    os.execv(sys.executable, [sys.executable, TARGET_SCRIPT] + sys.argv[1:])

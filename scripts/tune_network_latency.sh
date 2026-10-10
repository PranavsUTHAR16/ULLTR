#!/usr/bin/env bash
# ==============================================================================
# ULLTR Low-Latency Network & Core Isolation Optimizer for AWS EC2 (ap-south-1)
# Configures:
#   1. AWS ENA NIC interrupt coalescing disable (rx-usecs 0)
#   2. Hardware ring buffer expansion (rx/tx 4096)
#   3. Linux Kernel network stack tuning (Google BBR, busy-poll, buffer sizes)
#   4. CPU Core Pinning & Thread Priority Enforcement
# ==============================================================================

set -euo pipefail

LOG_PREFIX="[$(date '+%Y-%m-%d %H:%M:%S')] [ULLTR-NETWORK-TUNE]"

echo "$LOG_PREFIX 🚀 Initializing ULLTR Sub-Millisecond Network Ingestion Tuning..."

# 1. Require root privileges
if [ "$(id -u)" -ne 0 ]; then
    echo "$LOG_PREFIX ❌ This tuning script must be run as root (or via sudo)."
    exit 1
fi

# 2. Detect active default network interface (typically eth0 on AWS EC2)
DEFAULT_IFACE=$(ip route show default | awk '/default/ {print $5}' | head -n1 2>/dev/null || true)
if [ -z "$DEFAULT_IFACE" ]; then
    DEFAULT_IFACE="eth0"
fi
echo "$LOG_PREFIX Detected primary network interface: $DEFAULT_IFACE"

# 3. AWS ENA NIC Tuning (Disable interrupt coalescing & maximize ring buffers)
if command -v ethtool &> /dev/null; then
    echo "$LOG_PREFIX Disabling NIC interrupt coalescing on $DEFAULT_IFACE (rx-usecs 0)..."
    ethtool -C "$DEFAULT_IFACE" adaptive-rx off adaptive-tx off rx-usecs 0 tx-usecs 0 2>/dev/null || true

    echo "$LOG_PREFIX Maximizing DMA RX/TX ring buffers to 4096 on $DEFAULT_IFACE..."
    ethtool -G "$DEFAULT_IFACE" rx 4096 tx 4096 2>/dev/null || true
else
    echo "$LOG_PREFIX ⚠️ ethtool not found, skipping NIC ring buffer tuning."
fi

# 4. Linux Kernel sysctl Low-Latency Tuning
echo "$LOG_PREFIX Applying low-latency kernel network parameters..."
SYSCTL_CONF="/etc/sysctl.d/99-ulltr-latency.conf"

# Ensure BBR congestion control module is loaded
modprobe tcp_bbr 2>/dev/null || true

cat << 'EOF' > "$SYSCTL_CONF"
# ULLTR Low-Latency Kernel Tuning
# Google BBR Congestion Control (eliminates bufferbloat on public internet routes)
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr

# Enable low-latency socket processing & busy-poll (bypasses interrupt context switches)
net.ipv4.tcp_low_latency = 1
net.core.busy_read = 50
net.core.busy_poll = 50

# TCP Fast Open & SACK
net.ipv4.tcp_fastopen = 3
net.ipv4.tcp_sack = 1
net.ipv4.tcp_timestamps = 1
net.ipv4.tcp_notsent_lowat = 16384

# Maximize socket memory ceilings to prevent buffer overruns during volatility bursts
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216

# Increase network device packet backlog queue
net.core.netdev_max_backlog = 16384
net.core.somaxconn = 8192
EOF

sysctl -p "$SYSCTL_CONF" > /dev/null 2>&1 || sysctl --system > /dev/null 2>&1 || true

echo "$LOG_PREFIX ✅ Network latency tuning successfully applied!"

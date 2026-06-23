#!/bin/bash
# ==============================================================================
# ULLTR System - AWS VM Bootstrap & Service Installer (ap-south-1 Mumbai)
# ==============================================================================
# Designed for a fresh Ubuntu 22.04 LTS VM on AWS (m7i-flex.large)
# Run as: chmod +x setup_aws_vm.sh && ./setup_aws_vm.sh
# ==============================================================================

set -e

# Output Styling
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}====================================================${NC}"
echo -e "🚀 ULLTR System - AWS ap-south-1 VM Bootstrap Installer"
echo -e "${YELLOW}====================================================${NC}"

# 1. Force Timezone to IST (Crucial for market times)
echo -e "\n${YELLOW}[1/7] Aligning System Timezone to Asia/Kolkata (IST)...${NC}"
sudo timedatectl set-timezone Asia/Kolkata
echo -e "${GREEN}✓ Timezone configured. Current Time: $(date)${NC}"

# 2. Update System Packages
echo -e "\n${YELLOW}[2/7] Updating Ubuntu package repositories...${NC}"
sudo apt update && sudo apt upgrade -y
echo -e "${GREEN}✓ OS packages upgraded.${NC}"

# 3. Install System Dependencies & C++ Compiler Toolchain
echo -e "\n${YELLOW}[3/7] Installing Compilers, Redis Server, and Python dependencies...${NC}"
sudo apt install -y build-essential gcc g++ cmake make redis-server \
    python3-pip python3-venv python3-dev libpq-dev libssl-dev \
    libboost-all-dev libhiredis-dev libprotobuf-dev protobuf-compiler nlohmann-json3-dev
echo -e "${GREEN}✓ System compilers and tools installed.${NC}"

# 4. Compile ULLTR Low-Latency C++ Ingestor
echo -e "\n${YELLOW}[4/7] Compiling C++ Ingestion Collector binary...${NC}"
cd /Users/prana/Desktop/open_source/web/collector
protoc --cpp_out=. proto/MarketDataFeedV3.proto
mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make
echo -e "${GREEN}✓ C++ Collector compiled successfully at $(pwd)/collector${NC}"

# 5. Setup Python Virtual Environment and Libraries
echo -e "\n${YELLOW}[5/7] Setting up Python virtual environment and dependencies...${NC}"
cd /Users/prana/Desktop/open_source/web

if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo -e "${GREEN}✓ Virtual environment 'venv' created.${NC}"
fi

source venv/bin/activate
pip install --upgrade pip

# Install required Python packages for ULLTR (redis, requests, aiohttp, pytz, upstox-python-sdk)
pip install redis requests aiohttp pytz upstox-python-sdk pyotp lightgbm clickhouse-connect pandas numpy scipy playwright tqdm pyarrow
# Install ULLTR package in editable mode
pip install -e .

# Install Playwright browser and dependencies inside venv
playwright install chromium
sudo /Users/prana/Desktop/open_source/web/venv/bin/playwright install-deps

echo -e "${GREEN}✓ Python packages, Playwright Chromium, and ULLTR package installed.${NC}"

# 6. Configure systemd Service for Optimized Redis (Pure In-Memory & Unix Domain Socket)
echo -e "\n${YELLOW}[6/7] Registering systemd Service: ulltr-redis...${NC}"
sudo tee /etc/systemd/system/ulltr-redis.service > /dev/null << EOF
[Unit]
Description=ULLTR High Performance Redis Server (In-Memory & Unix Socket)
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/bin/redis-server /Users/prana/Desktop/open_source/web/redis.conf
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable ulltr-redis
sudo systemctl restart ulltr-redis
echo -e "${GREEN}✓ systemd service ulltr-redis started successfully.${NC}"

# 7. Configure systemd Services for Authentication, Expiry Manager, and Health Check
echo -e "\n${YELLOW}[7/7] Registering systemd Services: ulltr-auth, ulltr-expiry-manager & ulltr-health-check...${NC}"

# A. Boot-Time Authentication Service (Oneshot)
sudo tee /etc/systemd/system/ulltr-auth.service > /dev/null << EOF
[Unit]
Description=ULLTR Upstox Automated Authentication Service
After=network.target ulltr-redis.service
Before=ulltr-expiry-manager.service

[Service]
Type=oneshot
User=ubuntu
WorkingDirectory=/Users/prana/Desktop/open_source/web/login
Environment=PATH=/Users/prana/Desktop/open_source/web/venv/bin:/usr/bin
ExecStart=/Users/prana/Desktop/open_source/web/venv/bin/python auth.py
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

# B. Expiry & Daily Manager Service
sudo tee /etc/systemd/system/ulltr-expiry-manager.service > /dev/null << EOF
[Unit]
Description=ULLTR Expiry & Daily Manager Daemon
After=network.target ulltr-redis.service ulltr-auth.service
Wants=ulltr-auth.service

[Service]
Type=simple
User=root
WorkingDirectory=/Users/prana/Desktop/open_source/web
Environment=PATH=/Users/prana/Desktop/open_source/web/venv/bin:/usr/bin
ExecStart=/Users/prana/Desktop/open_source/web/venv/bin/python -u expiry_manager.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# C. 10s WebSocket & Broker Feed Health Check Service
sudo tee /etc/systemd/system/ulltr-health-check.service > /dev/null << EOF
[Unit]
Description=ULLTR WebSocket & Broker Data Feed Health Check
After=network.target ulltr-expiry-manager.service

[Service]
Type=simple
User=root
WorkingDirectory=/Users/prana/Desktop/open_source/web
Environment=PATH=/Users/prana/Desktop/open_source/web/venv/bin:/usr/bin
ExecStart=/Users/prana/Desktop/open_source/web/venv/bin/python -u health_check.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# D. Daily Strategy Model 7 Service & Timer
sudo tee /etc/systemd/system/ulltr-strategy-model7.service > /dev/null << EOF
[Unit]
Description=ULLTR Model 7 Strategy Live Execution
After=network.target ulltr-redis.service ulltr-expiry-manager.service

[Service]
Type=simple
User=root
WorkingDirectory=/Users/prana/Desktop/open_source/web
Environment=PATH=/Users/prana/Desktop/open_source/web/venv/bin:/usr/bin
ExecStart=/Users/prana/Desktop/open_source/web/venv/bin/python -u forward_tester/run.py

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/ulltr-strategy-model7.timer > /dev/null << EOF
[Unit]
Description=Run ULLTR Model 7 Strategy daily at 09:15 AM

[Timer]
OnCalendar=*-*-* 09:15:00
Unit=ulltr-strategy-model7.service
Persistent=true

[Install]
WantedBy=timers.target
EOF

# E. Daily Strategy TWAP Breakout Service & Timer
sudo tee /etc/systemd/system/ulltr-strategy-twap.service > /dev/null << EOF
[Unit]
Description=ULLTR TWAP Breakout Strategy Live Execution
After=network.target ulltr-redis.service ulltr-expiry-manager.service

[Service]
Type=simple
User=root
WorkingDirectory=/Users/prana/Desktop/open_source/web
Environment=PATH=/Users/prana/Desktop/open_source/web/venv/bin:/usr/bin
ExecStart=/Users/prana/Desktop/open_source/web/venv/bin/python -u forward_tester_twap/run.py

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/ulltr-strategy-twap.timer > /dev/null << EOF
[Unit]
Description=Run ULLTR TWAP Breakout Strategy daily at 09:15 AM

[Timer]
OnCalendar=*-*-* 09:15:00
Unit=ulltr-strategy-twap.service
Persistent=true

[Install]
WantedBy=timers.target
EOF

# F. Daily 10y Bond Yield Scraper Service & Timer
sudo tee /etc/systemd/system/ulltr-yield-scraper.service > /dev/null << EOF
[Unit]
Description=ULLTR India 10-Year Bond Yield Scraper
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/Users/prana/Desktop/open_source/Options_data_upstox
Environment=PATH=/Users/prana/Desktop/open_source/web/venv/bin:/usr/bin
ExecStart=/Users/prana/Desktop/open_source/web/venv/bin/python yield_scraper.py

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/ulltr-yield-scraper.timer > /dev/null << EOF
[Unit]
Description=Run ULLTR India 10-Year Bond Yield Scraper daily at 09:16 AM

[Timer]
OnCalendar=*-*-* 09:16:00
Unit=ulltr-yield-scraper.service
Persistent=true

[Install]
WantedBy=timers.target
EOF

# G. EOD Options Data & Greeks Pipeline Service & Timer
sudo tee /etc/systemd/system/ulltr-eod-pipeline.service > /dev/null << EOF
[Unit]
Description=ULLTR EOD Options Greeks Pipeline
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/Users/prana/Desktop/open_source/Options_data_upstox
Environment=PATH=/Users/prana/Desktop/open_source/web/venv/bin:/usr/bin
ExecStart=/Users/prana/Desktop/open_source/web/venv/bin/python eod_pipeline.py

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/ulltr-eod-pipeline.timer > /dev/null << EOF
[Unit]
Description=Run ULLTR EOD Options Greeks Pipeline daily at 03:35 PM (15:35)

[Timer]
OnCalendar=*-*-* 15:35:00
Unit=ulltr-eod-pipeline.service
Persistent=true

[Install]
WantedBy=timers.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable ulltr-auth
sudo systemctl enable ulltr-expiry-manager
sudo systemctl enable ulltr-health-check
sudo systemctl enable ulltr-strategy-model7.timer
sudo systemctl enable ulltr-strategy-twap.timer
sudo systemctl enable ulltr-yield-scraper.timer
sudo systemctl enable ulltr-eod-pipeline.timer
sudo systemctl start ulltr-strategy-model7.timer
sudo systemctl start ulltr-strategy-twap.timer
sudo systemctl start ulltr-yield-scraper.timer
sudo systemctl start ulltr-eod-pipeline.timer

echo -e "\n${GREEN}================================================================${NC}"
echo -e "✅ ULLTR System VM Bootstrap & Services Installed Successfully!"
echo -e "${GREEN}================================================================${NC}"
echo -e "\nNext steps to run live on AWS:"
echo -e "1. Edit your Upstox credentials in: ${YELLOW}/Users/prana/Desktop/open_source/web/login/access_token.json${NC}"
echo -e "2. Start the ULLTR Core daemons:"
echo -e "   ${YELLOW}sudo systemctl start ulltr-expiry-manager${NC}"
echo -e "   ${YELLOW}sudo systemctl start ulltr-health-check${NC}"
echo -e "3. Monitor execution in real-time:"
echo -e "   ${YELLOW}sudo journalctl -u ulltr-expiry-manager -f${NC}"
echo -e "   ${YELLOW}sudo journalctl -u ulltr-health-check -f${NC}"
echo -e "   ${YELLOW}tail -f /Users/prana/Desktop/open_source/web/collector_bg.log${NC}\n"

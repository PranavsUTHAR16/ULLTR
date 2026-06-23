#!/bin/bash
# ==============================================================================
# ULLTR System & Nifty Strategy - Single-VM AWS Deployment Orchestrator
# ==============================================================================
# Automates Terraform IP lookup, code synchronization (rsync), C++ compilation,
# Python ML library installations, and systemd strategy service registration.
# Run as: chmod +x deploy.sh && ./deploy.sh
# ==============================================================================

set -e

# Configuration
KEY_PATH="/Users/prana/Desktop/open_source/openalgo/openalgo-aws-key.pem"
LOCAL_WEB_DIR="/Users/prana/Desktop/open_source/web/"
LOCAL_BORD_DIR="/Users/prana/Desktop/black_box/bord/"
REMOTE_USER="ubuntu"

# ANSI Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}====================================================${NC}"
echo -e "🚀 ULLTR System AWS Deployer"
echo -e "${YELLOW}====================================================${NC}"

# 1. Fetch Elastic IP from Terraform
echo -e "\n${YELLOW}[1/4] Querying Elastic IP from Terraform outputs...${NC}"
VM_IP=$(terraform output -raw elastic_ip 2>/dev/null || echo "")

if [ -z "$VM_IP" ] || [ "$VM_IP" == "No outputs found" ]; then
    echo -e "${YELLOW}Terraform outputs not found. Attempting dynamic CLI lookup...${NC}"
    VM_IP=$(aws ec2 describe-instances --region ap-south-1 \
        --filters "Name=tag:Name,Values=ULLTR-System-VM" "Name=instance-state-name,Values=running" \
        --query "Reservations[*].Instances[*].PublicIpAddress" --output text | head -n 1)
fi

if [ -z "$VM_IP" ]; then
    echo -e "${RED}❌ Error: Could not retrieve running VM Public IP! Please ensure 'terraform apply' has run successfully.${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Target VM Public IP discovered: ${VM_IP}${NC}"

# Test SSH access
echo -e "\n${YELLOW}Testing SSH connection to VM...${NC}"
ssh -i "$KEY_PATH" -o StrictHostKeyChecking=no -o ConnectTimeout=10 "${REMOTE_USER}@${VM_IP}" "echo 'SSH Connection Successful!'"

# Prep remote directories and permissions
echo -e "\n${YELLOW}Preparing remote workspace directories...${NC}"
ssh -i "$KEY_PATH" "${REMOTE_USER}@${VM_IP}" "
    sudo mkdir -p /Users/prana/Desktop/open_source/web
    sudo mkdir -p /Users/prana/Desktop/black_box/bord
    sudo chown -R ubuntu:ubuntu /Users/prana/Desktop
"

# 2. Synchronize Workspaces via rsync
echo -e "\n${YELLOW}[2/4] Synchronizing code bases to remote VM (rsync)...${NC}"

echo -e "📦 Syncing ULLTR Web Ingestor codebase..."
rsync -avz -e "ssh -i $KEY_PATH" \
    --exclude="venv/" \
    --exclude="*.log" \
    --exclude="collector/build/" \
    "$LOCAL_WEB_DIR" "${REMOTE_USER}@${VM_IP}:/Users/prana/Desktop/open_source/web/"

echo -e "📦 Syncing Bord client modules..."
rsync -avz -e "ssh -i $KEY_PATH" \
    "$LOCAL_BORD_DIR" "${REMOTE_USER}@${VM_IP}:/Users/prana/Desktop/black_box/bord/"

echo -e "${GREEN}✓ All workspaces synchronized successfully!${NC}"

# 3. Bootstrap ULLTR Services on Remote VM
echo -e "\n${YELLOW}[3/4] Bootstrapping remote VM packages & services...${NC}"
ssh -i "$KEY_PATH" "${REMOTE_USER}@${VM_IP}" "chmod +x /Users/prana/Desktop/open_source/web/setup_aws_vm.sh && sudo /Users/prana/Desktop/open_source/web/setup_aws_vm.sh"
echo -e "${GREEN}✓ Remote VM bootstrapped & core ULLTR services configured.${NC}"

# 4. Verification and Status Report
echo -e "\n${YELLOW}[4/4] Compiling runtime verification report...${NC}"
sleep 3 # Wait a couple seconds for services to settle

ssh -i "$KEY_PATH" "${REMOTE_USER}@${VM_IP}" "
    echo '=========================================';
    echo '🔍 System Daemon Status Report:';
    echo '=========================================';
    for service in ulltr-redis ulltr-auth ulltr-expiry-manager ulltr-health-check; do
        status=\$(systemctl is-active \$service);
        if [ \"\$status\" == \"active\" ]; then
            echo -e \"🟢 \$service: \$status\";
        else
            echo -e \"🔴 \$service: \$status (Failed)\";
        fi
    done
    echo '=========================================';
    echo '📝 Active Process Patrol (pgrep):';
    echo '=========================================';
    echo -n 'C++ Collector Ingestor: '; pgrep -f './collector' || echo 'OFFLINE';
    echo '=========================================';
"

echo -e "\n${GREEN}🎉 Deployment successfully completed!${NC}"
echo -e "You can monitor live ULLTR executions in real-time:"
echo -e "👉 ${YELLOW}ssh -i $KEY_PATH ${REMOTE_USER}@${VM_IP} 'tail -f /Users/prana/Desktop/open_source/web/collector_bg.log'${NC}\n"

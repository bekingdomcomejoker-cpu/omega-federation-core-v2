#!/bin/bash
set -euo pipefail

# Omega Federation Core — proot-Ubuntu Bootstrap
# Run inside proot-distro Ubuntu on Termux

echo "========================================"
echo "  Ω Omega Core — proot-Ubuntu Bootstrap"
echo "========================================"

# Install deps
apt-get update
apt-get install -y python3 python3-pip python3-venv git curl

# Create venv
mkdir -p ~/.omega
python3 -m venv ~/.omega/venv
source ~/.omega/venv/bin/activate

# Install Python deps
pip install --upgrade pip
pip install cryptography websockets aiohttp pydantic click rich pyyaml python-dotenv pytest pytest-asyncio

# Clone or update
if [ -d "/root/omega-federation-core" ]; then
    cd /root/omega-federation-core && git pull
else
    cd /root && git clone https://github.com/VrtxOmega/omega-federation-core.git
fi

cd /root/omega-federation-core
pip install -e .

# Create systemd service file if systemd available
if command -v systemctl &> /dev/null; then
    cat > /etc/systemd/system/omega.service << 'SYSTEMD'
[Unit]
Description=Omega Federation Core
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/omega-federation-core
Environment=PYTHONUNBUFFERED=1
ExecStart=/root/.omega/venv/bin/python -m omega start --config omega/config.yaml
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
SYSTEMD

    systemctl daemon-reload
    systemctl enable omega.service
    echo "Systemd service installed: systemctl start omega"
fi

echo ""
echo "========================================"
echo "  ✓ Omega Core ready on proot-Ubuntu"
echo "========================================"
echo ""
echo "Start the runtime:"
echo "  python -m omega start --config omega/config.yaml"
echo ""
echo "Run tests:"
echo "  pytest tests/ -v"

#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

# Omega Federation Core — Termux Bootstrap
# Run inside Termux on Android

echo "========================================"
echo "  Ω Omega Core — Termux Bootstrap"
echo "========================================"

# Update packages
pkg update -y
pkg install -y python git openssh

# Install Python deps
pip install --upgrade pip
pip install cryptography websockets aiohttp pydantic click rich pyyaml python-dotenv

# Create omega data dir
mkdir -p ~/.omega

# Clone or update repo
if [ -d "$HOME/omega-federation-core" ]; then
    cd "$HOME/omega-federation-core" && git pull
else
    cd "$HOME" && git clone https://github.com/VrtxOmega/omega-federation-core.git
fi

cd "$HOME/omega-federation-core"
pip install -e .

echo ""
echo "========================================"
echo "  ✓ Omega Core ready on Termux"
echo "========================================"
echo ""
echo "Start the runtime:"
echo "  python -m omega start --config omega/config.yaml"
echo ""
echo "Or background it:"
echo "  nohup python -m omega start --config omega/config.yaml > ~/.omega/omega.log 2>&1 &"

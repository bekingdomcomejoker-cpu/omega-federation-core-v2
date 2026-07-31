#!/bin/bash
set -euo pipefail

# Omega Federation Core — Git Init & Push
# Usage: ./scripts/git-init-push.sh <github-username> [repo-name]

USERNAME="${1:-VrtxOmega}"
REPO_NAME="${2:-omega-federation-core}"

echo "========================================"
echo "  Ω Git Init & Push"
echo "========================================"
echo ""
echo "  Target owner: $USERNAME"
echo "  Target repo:  $REPO_NAME"
echo ""

cd "$(dirname "$0")/.."

if [ ! -d ".git" ]; then
    echo "→ Initializing git repository..."
    git init
    git branch -M main
fi

# Create remote if not exists
if ! git remote | grep -q "^origin$"; then
    echo "→ Adding remote: https://github.com/$USERNAME/$REPO_NAME.git"
    git remote add origin "https://github.com/$USERNAME/$REPO_NAME.git"
fi

echo "→ Adding files..."
git add .

echo "→ Committing..."
git commit -m "feat: Commit 001–002 — Sovereign Runtime + Connectors + Ingress

- Federation Bus: async pub/sub event backbone
- Event Ledger: immutable SHA-3-256 hash chain
- Checkpoint Engine: state snapshots and recovery
- Permission Engine: capability-based access control
- Event Router: permission-checked dispatch
- Supervisor: service lifecycle with restart policies
- Ingress Authority: unified HTTP/WebSocket transport
- Connectors: filesystem, HTTP client, Git
- Full test suite
- Termux and proot-Ubuntu bootstrap scripts" || echo "Nothing new to commit (or commit failed)"

echo ""
echo "→ Current remotes:"
git remote -v
echo ""
echo "Push requires authentication (token/SSH). To push:"
echo "  git push -u origin main"
echo ""
echo "Or re-run this script after setting credentials."
echo "Repo URL: https://github.com/$USERNAME/$REPO_NAME"

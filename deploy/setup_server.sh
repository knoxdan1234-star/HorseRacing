#!/bin/bash
# =============================================================
# Horse Racing Prediction System - Server Setup Script
# Target: Hostinger KVM2 - Ubuntu 24.04
# =============================================================

set -euo pipefail

APP_USER="horseracing"
APP_DIR="/home/$APP_USER/app"

echo "============================================"
echo "Horse Racing Prediction System - Server Setup"
echo "============================================"

# 1. System update
echo "[1/8] Updating system packages..."
apt update && apt upgrade -y

# 2. Install dependencies
echo "[2/8] Installing system dependencies..."
apt install -y \
    python3.12 \
    python3.12-venv \
    python3-pip \
    git \
    chromium-browser \
    chromium-chromedriver \
    sqlite3

# 3. Create application user
echo "[3/8] Creating application user..."
if ! id "$APP_USER" &>/dev/null; then
    useradd -m -s /bin/bash "$APP_USER"
    echo "User '$APP_USER' created"
else
    echo "User '$APP_USER' already exists"
fi

# 4. Clone/copy application
echo "[4/8] Setting up application directory..."
if [ ! -d "$APP_DIR" ]; then
    echo "Please clone your repository to $APP_DIR"
    echo "  git clone <your-repo-url> $APP_DIR"
    echo "Then re-run this script."
    exit 1
fi

# 5. Create virtual environment and install dependencies
echo "[5/8] Setting up Python environment..."
cd "$APP_DIR"
python3.12 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# 6. Create required directories
echo "[6/8] Creating data directories..."
mkdir -p data/historical data/new data/raw
mkdir -p models output/guide output/predictions output/reports output/backtests
mkdir -p info

# 7. Setup environment file
echo "[7/8] Checking environment configuration..."
if [ ! -f "$APP_DIR/.env" ]; then
    cp "$APP_DIR/.env.example" "$APP_DIR/.env"
    echo ""
    echo "IMPORTANT: Edit $APP_DIR/.env with your Discord webhook URLs:"
    echo "  nano $APP_DIR/.env"
    echo ""
fi

# 8. Fix permissions
echo "[8/8] Setting permissions..."
chown -R "$APP_USER:$APP_USER" "$APP_DIR"
chmod 600 "$APP_DIR/.env"

# Install systemd service
echo "Installing systemd service..."
cp "$APP_DIR/deploy/horseracing.service" /etc/systemd/system/
systemctl daemon-reload
systemctl enable horseracing

echo ""
echo "============================================"
echo "Setup complete!"
echo "============================================"
echo ""
echo "Next steps:"
echo "  1. Edit .env:           nano $APP_DIR/.env"
echo "     (set DISCORD_WEBHOOK_URL and confirm KELLY_FRACTION=0.03)"
echo "  2. Upload SQLite DB:    scp data/horseracing.db root@server:$APP_DIR/data/"
echo "     (already bootstrapped locally — faster than re-scraping 2 seasons)"
echo "  3. Upload trained models: scp models/xgboost_*.joblib root@server:$APP_DIR/models/"
echo "  4. Sanity check:        su - $APP_USER -c 'cd $APP_DIR && source venv/bin/activate && python scripts/run_backtest.py'"
echo "  5. Start service:       systemctl start horseracing"
echo "  6. Check logs:          journalctl -u horseracing -f"
echo ""

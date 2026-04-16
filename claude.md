# Project Overview

HKJC + Macau horse racing prediction system with ML-based betting.
Monolithic Python app with 4 modular agents coordinated by APScheduler.

# About Me

Mathematician and gambler learning horse racing. Zero prior knowledge.
Traditional Chinese is the preferred language for educational content.

# Architecture

- **Main Orchestrator** (`agents/orchestrator.py`) - APScheduler coordinator
- **Sub-agent 1: Data Collector** (`agents/collector/`) - HKJC/MJC scraping
- **Sub-agent 2: Predictor** (`agents/predictor/`) - LightGBM/XGBoost ML
- **Sub-agent 3: Health Monitor** (`agents/monitor/`) - system health checks
- **Discord** (`discord_bot/webhook.py`) - webhook notifications

# Tech Stack

- Python 3.11+, SQLAlchemy (SQLite), APScheduler 3.x
- LightGBM + XGBoost, scikit-learn, pandas
- BeautifulSoup + Selenium for scraping
- Discord webhooks for notifications
- Deployed via systemd on Hostinger KVM2 (Ubuntu 24.04)

# Rules

- Always ask clarifying questions before executing large changes
- Backtest must prove profitability before going live
- Send predictions and weekly Monday P&L to Discord
- Separate historical data from live season data
- Conservative betting: 5% fractional Kelly, 2% max bet, $10K bankroll

# Project Structure

- `config/` - settings (Pydantic) and logging
- `db/` - SQLAlchemy ORM models and database engine
- `agents/collector/hkjc/` - HKJC scrapers (results, racecard, odds, profiles)
- `agents/collector/mjc/` - Macau scrapers
- `agents/predictor/` - feature engine, model trainer, predictor, backtester, P&L tracker
- `agents/monitor/` - health checker, data validator, alerter
- `discord_bot/` - Discord webhook sender
- `output/guide/` - Traditional Chinese horse racing guide
- `output/` - predictions, reports, backtests
- `data/historical/` - historical data
- `data/new/` - current season live data
- `models/` - serialized ML model files
- `scripts/` - bootstrap, backtest, train scripts
- `deploy/` - systemd service, server setup script
- `tests/` - pytest test suite

# Commands

- `python scripts/bootstrap_historical.py --seasons 3` - Load historical data
- `python scripts/train_model.py --target both` - Train win + place models
- `python scripts/run_backtest.py` - Run walk-forward backtest
- `python main.py` - Start the prediction system

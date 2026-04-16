"""
Run Backtest

Manual script to run walk-forward backtesting.

Usage:
    python scripts/run_backtest.py [--start 2023-09-01] [--end 2026-04-01] [--model lightgbm]
"""

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings
from config.logging_config import setup_logging
from db.database import init_database, get_session
from agents.predictor.backtester import Backtester


def main():
    parser = argparse.ArgumentParser(description="Run walk-forward backtest")
    parser.add_argument("--start", type=str, default="2023-09-01", help="Test start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default="2026-04-01", help="Test end date (YYYY-MM-DD)")
    parser.add_argument("--model", type=str, default="lightgbm", choices=["lightgbm", "xgboost"])
    parser.add_argument("--train-months", type=int, default=24, help="Training window months")
    parser.add_argument("--test-months", type=int, default=1, help="Test window months")
    parser.add_argument("--bankroll", type=float, default=10000.0, help="Initial bankroll HKD")
    args = parser.parse_args()

    setup_logging(settings)
    init_database()

    session = get_session()
    backtester = Backtester(session)

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    periods, metrics = backtester.walk_forward_backtest(
        start_date=start,
        end_date=end,
        train_window_months=args.train_months,
        test_window_months=args.test_months,
        model_type=args.model,
        bankroll=args.bankroll,
    )

    backtester.print_summary(metrics)
    report_path = backtester.save_report(periods, metrics)
    print(f"Report saved to: {report_path}")

    session.close()


if __name__ == "__main__":
    main()

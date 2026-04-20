"""
Full walk-forward backtest across available data.

Uses 2024/25 season for initial training, tests on 2025/26 season.
Monthly rolling windows for realistic simulation.

Usage:
    python scripts/run_full_backtest.py
"""
import sys
import logging
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)

from config import settings
from db.database import init_database, get_session
from agents.predictor.backtester import Backtester
from db.models import Race
from sqlalchemy import func


def main():
    init_database()
    session = get_session()

    # Check data availability
    min_date = session.query(func.min(Race.race_date)).scalar()
    max_date = session.query(func.max(Race.race_date)).scalar()
    total_races = session.query(func.count(Race.id)).scalar()

    print(f"Data: {total_races} races from {min_date} to {max_date}")
    print()

    if total_races < 100:
        print("ERROR: Not enough data for backtesting. Need at least 100 races.")
        session.close()
        return

    # Configure backtest windows based on available data
    # Use first 4-6 months as training, rest as test
    backtester = Backtester(session)

    # Strategy 1: Short training window for single-season data
    if min_date >= date(2025, 8, 1):
        # Only have 2025/26 season
        print("=== Backtest: 3-month training window ===")
        periods, metrics = backtester.walk_forward_backtest(
            start_date=date(2025, 12, 1),  # Test starts after 3 months of data
            end_date=max_date,
            train_window_months=3,
            test_window_months=1,
            model_type="lightgbm",
            bankroll=10000.0,
        )
    else:
        # Have 2+ seasons — use proper training window
        print("=== Backtest: 12-month training window ===")
        # Start testing from Sep 2025 (use 2024/25 as training)
        periods, metrics = backtester.walk_forward_backtest(
            start_date=date(2025, 9, 1),
            end_date=max_date,
            train_window_months=12,
            test_window_months=1,
            model_type="lightgbm",
            bankroll=10000.0,
        )

    backtester.print_summary(metrics)
    report_path = backtester.save_report(periods, metrics)
    print(f"Report saved to: {report_path}")

    # Also run XGBoost for comparison
    print("\n\n=== XGBoost comparison ===")
    backtester2 = Backtester(session)

    if min_date >= date(2025, 8, 1):
        periods_xgb, metrics_xgb = backtester2.walk_forward_backtest(
            start_date=date(2025, 12, 1),
            end_date=max_date,
            train_window_months=3,
            test_window_months=1,
            model_type="xgboost",
            bankroll=10000.0,
        )
    else:
        periods_xgb, metrics_xgb = backtester2.walk_forward_backtest(
            start_date=date(2025, 9, 1),
            end_date=max_date,
            train_window_months=12,
            test_window_months=1,
            model_type="xgboost",
            bankroll=10000.0,
        )

    backtester2.print_summary(metrics_xgb)
    report_path_xgb = backtester2.save_report(periods_xgb, metrics_xgb, "backtest_xgboost.json")
    print(f"XGBoost report saved to: {report_path_xgb}")

    session.close()


if __name__ == "__main__":
    main()

"""
Validate the sweet-spot config (odds 4-8, edge 20%, top-2, Kelly 0.03).

Checks:
1. XGBoost with same config — does the edge hold across models?
2. Period-by-period P&L stability — is profit spread or concentrated?
3. Tighter and wider variants — how sensitive are we to the exact odds band?
"""
import sys
import logging
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s: %(message)s")

from db.database import init_database, get_session
from agents.predictor.backtester import Backtester


SWEETSPOT = dict(
    edge_margin=0.20, min_odds=4.0, max_odds=8.0,
    top_rank_only=2, kelly_fraction=0.03,
)

CONFIGS = [
    ("LightGBM sweet-spot (reference)", "lightgbm", SWEETSPOT),
    ("XGBoost sweet-spot (cross-model check)", "xgboost", SWEETSPOT),
    ("LightGBM odds 3.5-9 (slightly wider)", "lightgbm", {**SWEETSPOT, "min_odds": 3.5, "max_odds": 9.0}),
    ("LightGBM odds 4.5-7 (tighter)", "lightgbm", {**SWEETSPOT, "min_odds": 4.5, "max_odds": 7.0}),
    ("LightGBM odds 4-8 edge 25%", "lightgbm", {**SWEETSPOT, "edge_margin": 0.25}),
    ("LightGBM odds 4-8 top-1", "lightgbm", {**SWEETSPOT, "top_rank_only": 1}),
]


def main():
    init_database()
    session = get_session()

    results = []
    for name, model_type, cfg in CONFIGS:
        print(f"\n{'=' * 70}")
        print(f"CONFIG: {name}")
        print(f"{'=' * 70}")

        backtester = Backtester(session)
        periods, metrics = backtester.walk_forward_backtest(
            start_date=date(2025, 9, 1),
            end_date=date(2026, 4, 15),
            train_window_months=12,
            test_window_months=1,
            model_type=model_type,
            bankroll=10000.0,
            **cfg,
        )

        # Period-by-period breakdown
        print(f"\n  Period-by-period P&L:")
        for p in periods:
            print(f"    {p.test_start} to {p.test_end}: {p.num_bets:>3} bets, P&L=${p.total_pnl:>8.2f}")

        backtester.print_summary(metrics)
        results.append((name, metrics, periods))

    # Comparison table
    print("\n" + "=" * 100)
    print("VALIDATION COMPARISON")
    print("=" * 100)
    print(f"{'Config':<50} {'Bets':>5} {'P&L':>10} {'ROI%':>8} {'MaxDD%':>8} {'Sharpe':>7} {'WinRate':>8}")
    print("-" * 100)
    for name, m, _ in results:
        print(f"{name[:49]:<50} {m.total_bets:>5} ${m.total_pnl:>8.0f} "
              f"{m.roi_pct:>7.2f}% {m.max_drawdown_pct:>7.1f}% {m.sharpe_ratio:>6.2f} {m.win_rate:>7.1f}%")
    print("=" * 100)

    # Period-level stability for the two primary configs
    print("\nPERIOD STABILITY (positive P&L periods out of total):")
    for name, m, periods in results[:2]:
        positive = sum(1 for p in periods if p.total_pnl > 0)
        print(f"  {name}: {positive}/{len(periods)} profitable periods")

    session.close()


if __name__ == "__main__":
    main()

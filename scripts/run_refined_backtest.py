"""
Refined backtest sweeping strategy parameters on the full dataset.

Tests multiple configurations to find the best risk-adjusted returns:
- Baseline (current defaults)
- Tighter edge threshold + odds range + lower Kelly
- Top-1 pick only
- Sweet-spot odds range (3-10)
"""
import sys
import logging
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s: %(message)s")

from db.database import init_database, get_session
from agents.predictor.backtester import Backtester


CONFIGS = [
    {
        "name": "Baseline (old: edge>+0.05, no odds filter, Kelly 0.05)",
        "edge_margin": 0.0,
        "min_odds": 1.01,
        "max_odds": 999.0,
        "top_rank_only": None,
        "kelly_fraction": 0.05,
    },
    {
        "name": "Tight edge 15% + odds 2.5-20 + top-3 + Kelly 0.02",
        "edge_margin": 0.15,
        "min_odds": 2.5,
        "max_odds": 20.0,
        "top_rank_only": 3,
        "kelly_fraction": 0.02,
    },
    {
        "name": "Tight edge 20% + odds 3-15 + top-2 + Kelly 0.02",
        "edge_margin": 0.20,
        "min_odds": 3.0,
        "max_odds": 15.0,
        "top_rank_only": 2,
        "kelly_fraction": 0.02,
    },
    {
        "name": "Top-1 only + edge 25% + odds 3-12 + Kelly 0.03",
        "edge_margin": 0.25,
        "min_odds": 3.0,
        "max_odds": 12.0,
        "top_rank_only": 1,
        "kelly_fraction": 0.03,
    },
    {
        "name": "Sweet-spot 4-8 + edge 20% + top-2 + Kelly 0.03",
        "edge_margin": 0.20,
        "min_odds": 4.0,
        "max_odds": 8.0,
        "top_rank_only": 2,
        "kelly_fraction": 0.03,
    },
]


def main():
    init_database()
    session = get_session()

    results = []
    for cfg in CONFIGS:
        print(f"\n{'=' * 70}")
        print(f"CONFIG: {cfg['name']}")
        print(f"{'=' * 70}")

        backtester = Backtester(session)
        periods, metrics = backtester.walk_forward_backtest(
            start_date=date(2025, 9, 1),
            end_date=date(2026, 4, 15),
            train_window_months=12,
            test_window_months=1,
            model_type="lightgbm",
            bankroll=10000.0,
            edge_margin=cfg["edge_margin"],
            min_odds=cfg["min_odds"],
            max_odds=cfg["max_odds"],
            top_rank_only=cfg["top_rank_only"],
            kelly_fraction=cfg["kelly_fraction"],
        )
        backtester.print_summary(metrics)
        results.append((cfg["name"], metrics))

    # Summary comparison table
    print("\n\n" + "=" * 90)
    print("COMPARISON SUMMARY")
    print("=" * 90)
    print(f"{'Config':<55} {'Bets':>6} {'P&L':>10} {'ROI%':>8} {'MaxDD%':>8} {'Sharpe':>8}")
    print("-" * 90)
    for name, m in results:
        print(
            f"{name[:54]:<55} {m.total_bets:>6} ${m.total_pnl:>8.0f} "
            f"{m.roi_pct:>7.2f}% {m.max_drawdown_pct:>7.1f}% {m.sharpe_ratio:>7.2f}"
        )
    print("=" * 90)

    session.close()


if __name__ == "__main__":
    main()

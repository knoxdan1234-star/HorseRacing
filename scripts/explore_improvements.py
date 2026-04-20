"""
Explore strategy improvements beyond the baseline sweet-spot:
1. Add PLA (place) betting at various odds bands
2. Ensemble: require BOTH LightGBM and XGBoost to flag the horse

Best current: XGBoost WIN, odds 4.5-7.0, edge 20%, top-2, Kelly 0.03 -> +17% ROI
"""
import sys
import logging
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s: %(message)s")

import numpy as np
import pandas as pd
from db.database import init_database, get_session
from db.models import Race, Runner, Dividend
from agents.predictor.feature_engine import FeatureEngineer
from agents.predictor.model_trainer import ModelTrainer
from agents.predictor.bet_sizer import BetSizer


def run_backtest(
    session,
    start_date: date,
    end_date: date,
    train_window_months: int = 12,
    bet_type: str = "WIN",
    edge_margin: float = 0.20,
    min_odds: float = 4.5,
    max_odds: float = 7.0,
    top_rank_only: int = 2,
    kelly_fraction: float = 0.03,
    ensemble: bool = False,
):
    """Return (total_bets, total_pnl, total_bet_amount, win_count, max_dd)."""
    bet_sizer = BetSizer(bankroll=10000.0, kelly_fraction=kelly_fraction)
    feature_engine = FeatureEngineer(session)
    feature_cols = FeatureEngineer.get_feature_columns()

    current = start_date
    all_pnls = []
    bets_count = 0
    wins_count = 0
    total_bet_amount = 0.0

    while current < end_date:
        train_start = current - timedelta(days=train_window_months * 30)
        train_end = current - timedelta(days=1)
        test_end = min(current + timedelta(days=30), end_date)

        trainer = ModelTrainer(session)
        xgb_model, _ = trainer.train_win_model(train_start, train_end, "xgboost")
        if xgb_model is None:
            current = test_end + timedelta(days=1)
            continue
        lgbm_model = None
        if ensemble:
            lgbm_model, _ = trainer.train_win_model(train_start, train_end, "lightgbm")

        test_races = (session.query(Race)
                      .filter(Race.race_date >= current, Race.race_date <= test_end)
                      .order_by(Race.race_date, Race.race_no).all())

        for race in test_races:
            df = feature_engine.build_features_for_race(race.id)
            if df.empty:
                continue
            X = df[feature_cols].copy().fillna(df[feature_cols].median())
            xgb_probs = xgb_model.predict_proba(X)[:, 1]
            xgb_norm = xgb_probs / xgb_probs.sum()

            if ensemble and lgbm_model is not None:
                lgbm_probs = lgbm_model.predict_proba(X)[:, 1]
                lgbm_norm = lgbm_probs / lgbm_probs.sum()
                # Average the two models
                model_prob_arr = (xgb_norm + lgbm_norm) / 2
                # Rankings: bet only if BOTH models rank horse in top-N
                xgb_rank = (-xgb_norm).argsort().argsort()
                lgbm_rank = (-lgbm_norm).argsort().argsort()
            else:
                model_prob_arr = xgb_norm
                xgb_rank = (-xgb_norm).argsort().argsort()
                lgbm_rank = xgb_rank  # dummy, not used

            for i, (_, row) in enumerate(df.iterrows()):
                horse_no = int(row["horse_no"])
                model_prob = model_prob_arr[i]
                win_odds = float(row.get("win_odds") or 20.0)
                if win_odds <= 1:
                    continue

                if bet_type == "PLA":
                    # Approximate place odds ~ win_odds / 3.5, and model prob uplift
                    odds = max(win_odds / 3.5, 1.1)
                    effective_prob = min(model_prob * 3.0, 0.95)
                else:
                    odds = win_odds
                    effective_prob = model_prob

                if odds < min_odds or odds > max_odds:
                    continue
                # Top-rank filter on raw win prob ranking
                if xgb_rank[i] >= top_rank_only:
                    continue
                if ensemble and lgbm_rank[i] >= top_rank_only:
                    continue

                implied = 1.0 / odds
                if effective_prob <= implied * (1 + edge_margin):
                    continue

                bet_amount = bet_sizer.size_bet(effective_prob, odds, bet_type)
                if bet_amount <= 0:
                    continue

                # Settle
                runner = (session.query(Runner)
                          .filter_by(race_id=race.id, horse_no=horse_no, scratched=False)
                          .first())
                if not runner or not runner.finish_position:
                    pnl = -bet_amount
                else:
                    won = (bet_type == "WIN" and runner.finish_position == 1) or \
                          (bet_type == "PLA" and runner.finish_position <= 3)
                    if won:
                        # Use actual PLA/WIN dividend
                        horse_str = str(horse_no)
                        div = (session.query(Dividend)
                               .filter_by(race_id=race.id, pool_type=bet_type, combination=horse_str)
                               .first())
                        if not div:
                            div = (session.query(Dividend)
                                   .filter_by(race_id=race.id, pool_type=bet_type).first())
                        if div and div.payout > 0:
                            pnl = (bet_amount / 10) * div.payout - bet_amount
                        else:
                            # Fallback: approx from odds
                            deduction = 0.175
                            if bet_type == "WIN":
                                pnl = bet_amount * (win_odds - 1) * (1 - deduction)
                            else:
                                pnl = bet_amount * (win_odds / 3.5 - 1) * (1 - deduction)
                    else:
                        pnl = -bet_amount

                all_pnls.append(pnl)
                total_bet_amount += bet_amount
                bets_count += 1
                if pnl > 0:
                    wins_count += 1
                bet_sizer.update_bankroll(bet_sizer.bankroll + pnl)

        current = test_end + timedelta(days=1)

    if not all_pnls:
        return 0, 0.0, 0.0, 0, 0.0
    cumulative = np.cumsum(all_pnls)
    peak = np.maximum.accumulate(cumulative)
    max_dd = float((peak - cumulative).max())
    return bets_count, float(sum(all_pnls)), total_bet_amount, wins_count, max_dd


CONFIGS = [
    ("BASELINE: WIN 4.5-7, edge 20%, top-2 (reference)",
     dict(bet_type="WIN", min_odds=4.5, max_odds=7.0, edge_margin=0.20, top_rank_only=2, ensemble=False)),

    ("PLA 2.0-4.0 + edge 15%, top-3",
     dict(bet_type="PLA", min_odds=2.0, max_odds=4.0, edge_margin=0.15, top_rank_only=3, ensemble=False)),
    ("PLA 1.8-3.5 + edge 10%, top-3",
     dict(bet_type="PLA", min_odds=1.8, max_odds=3.5, edge_margin=0.10, top_rank_only=3, ensemble=False)),
    ("PLA 2.0-5.0 + edge 20%, top-2",
     dict(bet_type="PLA", min_odds=2.0, max_odds=5.0, edge_margin=0.20, top_rank_only=2, ensemble=False)),

    ("ENSEMBLE: WIN 4.5-7, edge 20%, top-2, BOTH models agree",
     dict(bet_type="WIN", min_odds=4.5, max_odds=7.0, edge_margin=0.20, top_rank_only=2, ensemble=True)),
    ("ENSEMBLE: WIN 4-8, edge 15%, top-3, BOTH models agree",
     dict(bet_type="WIN", min_odds=4.0, max_odds=8.0, edge_margin=0.15, top_rank_only=3, ensemble=True)),
]


def main():
    init_database()
    session = get_session()

    print(f"{'Config':<58} {'Bets':>5} {'PnL':>9} {'ROI%':>7} {'WinR%':>7} {'MaxDD':>8}")
    print("-" * 100)

    for name, cfg in CONFIGS:
        bets, pnl, bet_amt, wins, mdd = run_backtest(
            session, date(2025, 9, 1), date(2026, 4, 15), **cfg
        )
        roi = (pnl / bet_amt * 100) if bet_amt > 0 else 0
        wr = (wins / bets * 100) if bets > 0 else 0
        print(f"{name[:57]:<58} {bets:>5} ${pnl:>7.0f} {roi:>6.2f}% {wr:>6.1f}% ${mdd:>6.0f}")

    session.close()


if __name__ == "__main__":
    main()

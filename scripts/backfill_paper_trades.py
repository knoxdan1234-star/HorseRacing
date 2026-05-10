"""
Backfill the paper-trade ledger across an OOS period to validate the harness.

Mirrors the walk-forward training schedule used in the original backtest
(one ranker per test month, 24-month trailing window) so the ledger should
reproduce the +46.9% ROI / 28-bet OOS finding for Class 4 + Exp-1 ranker.

Usage:
    python scripts/backfill_paper_trades.py --start 2025-01-01 --end 2026-04-01
"""

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np

from agents.predictor.bet_sizer import BetSizer
from agents.predictor.feature_engine import FeatureEngineer
from agents.predictor.model_trainer import ModelTrainer
from config import settings
from config.logging_config import setup_logging
from db.database import get_session, init_database
from db.models import Dividend, Race, Runner

# Reuse the same ledger + strategy params as the live harness
from scripts.paper_trade import (
    EDGE_MARGIN, KELLY_FRACTION, LEDGER_PATH, MAX_ODDS, MIN_ODDS,
    INITIAL_BANKROLL, TARGET_CLASS, TOP_RANK, TRAIN_WINDOW_MONTHS,
    PaperBet, append_ledger, current_bankroll, load_ledger,
)

logger = logging.getLogger(__name__)


def backfill(start: date, end: date):
    LEDGER_PATH.unlink(missing_ok=True)  # fresh ledger
    LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)

    session = get_session()
    feature_cols = FeatureEngineer.get_feature_columns()
    fe = FeatureEngineer(session)
    trainer = ModelTrainer(session)

    # Walk forward by month
    cur_month_start = start
    total_bets = 0
    while cur_month_start < end:
        # Test window = one calendar month from cur_month_start
        next_month = cur_month_start + timedelta(days=30)
        test_end = min(next_month - timedelta(days=1), end - timedelta(days=1))
        train_end = cur_month_start - timedelta(days=1)
        train_start = train_end - timedelta(days=TRAIN_WINDOW_MONTHS * 30)

        races = (
            session.query(Race)
            .filter(
                Race.race_date >= cur_month_start,
                Race.race_date <= test_end,
                Race.race_class == TARGET_CLASS,
            )
            .order_by(Race.race_date, Race.race_no)
            .all()
        )
        if not races:
            logger.info("No Class-4 races %s..%s; skipping training", cur_month_start, test_end)
            cur_month_start = test_end + timedelta(days=1)
            session.expunge_all()
            continue

        logger.info(
            "Test month %s..%s: %d Class-4 races; training on %s..%s",
            cur_month_start, test_end, len(races), train_start, train_end,
        )
        model, _ = trainer.train_win_ranker(train_start, train_end)
        if model is None:
            logger.warning("Could not train ranker for %s — skipping", cur_month_start)
            cur_month_start = test_end + timedelta(days=1)
            session.expunge_all()
            continue

        for race in races:
            df = fe.build_features_for_race(race.id)
            if df.empty:
                continue
            X = df[feature_cols].copy().fillna(df[feature_cols].median())
            scores = model.predict(X)
            exp_s = np.exp(scores - scores.max())
            win_probs = exp_s / exp_s.sum()
            ranked = np.argsort(-win_probs)
            top_set = set(ranked[:TOP_RANK])

            # Bankroll updates each bet to compound correctly
            ledger = load_ledger()
            sizer = BetSizer(
                bankroll=current_bankroll(ledger),
                kelly_fraction=KELLY_FRACTION,
            )

            for i, (_, row) in enumerate(df.iterrows()):
                if i not in top_set:
                    continue
                win_odds = float(row.get("win_odds") or 20.0)
                if win_odds < MIN_ODDS or win_odds > MAX_ODDS:
                    continue
                model_prob = float(win_probs[i])
                implied = 1.0 / win_odds
                if model_prob <= implied * (1 + EDGE_MARGIN):
                    continue
                bet_amount = sizer.size_bet(model_prob, win_odds, "WIN")
                if bet_amount <= 0:
                    continue

                horse_no = int(row["horse_no"])
                runner = (
                    session.query(Runner)
                    .filter_by(race_id=race.id, horse_no=horse_no)
                    .first()
                )
                horse_name = runner.horse_name if runner and runner.horse_name else f"#{horse_no}"

                # Settle immediately (we're in backfill mode — results are known)
                if not runner or not runner.finish_position:
                    pnl = -bet_amount
                    actual_dividend = None
                    finish_position = runner.finish_position if runner else None
                elif runner.finish_position == 1:
                    div = (
                        session.query(Dividend)
                        .filter_by(race_id=race.id, pool_type="WIN", combination=str(horse_no))
                        .first()
                    )
                    if div and div.payout > 0:
                        pnl = (bet_amount / 10) * div.payout - bet_amount
                        actual_dividend = div.payout
                    else:
                        pnl = (win_odds - 1) * (1 - 0.175) * bet_amount
                        actual_dividend = None
                    finish_position = 1
                else:
                    pnl = -bet_amount
                    actual_dividend = None
                    finish_position = runner.finish_position

                bet = PaperBet(
                    placed_at="backfill",
                    race_date=str(race.race_date),
                    race_id=race.id,
                    racecourse=race.racecourse or "?",
                    race_no=race.race_no,
                    distance=race.distance or 0,
                    race_class=race.race_class or "?",
                    horse_no=horse_no,
                    horse_name=horse_name,
                    model_prob=round(model_prob, 4),
                    win_odds=win_odds,
                    bet_amount=bet_amount,
                    settled=True,
                    finish_position=finish_position,
                    actual_dividend=actual_dividend,
                    pnl=round(pnl, 2),
                )
                append_ledger(bet)
                total_bets += 1
                logger.info(
                    "BACKFILL %s R%d #%d %s @%.1f $%.0f pos=%s pnl=%+.2f",
                    race.racecourse, race.race_no, horse_no, horse_name,
                    win_odds, bet_amount, finish_position, pnl,
                )

        session.expunge_all()
        cur_month_start = test_end + timedelta(days=1)

    session.close()
    logger.info("Backfill complete: %d bets placed", total_bets)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()
    setup_logging(settings)
    init_database()
    backfill(date.fromisoformat(args.start), date.fromisoformat(args.end))


if __name__ == "__main__":
    main()

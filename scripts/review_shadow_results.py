"""
Review shadow-mode results.

Joins predictions -> bet_results -> runners -> races to show:
  - Every value bet signal recorded
  - What the actual race outcome was
  - Running P&L as if bets had been placed

Usage:
    python scripts/review_shadow_results.py [--since 2026-04-22]
"""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import and_

from config import settings
from db.database import init_database, get_session
from db.models import BetResult, Prediction, Race, Runner


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", type=str, default="2026-04-20")
    args = parser.parse_args()
    since = datetime.strptime(args.since, "%Y-%m-%d").date()

    init_database()
    session = get_session()

    # Pull all value-bet predictions since the date
    rows = (
        session.query(Prediction, BetResult, Runner, Race)
        .outerjoin(BetResult, BetResult.prediction_id == Prediction.id)
        .join(Race, Race.id == Prediction.race_id)
        .outerjoin(Runner, Runner.id == Prediction.runner_id)
        .filter(and_(Race.race_date >= since, Prediction.is_value_bet == True))
        .order_by(Race.race_date, Race.race_no, Prediction.predicted_rank)
        .all()
    )

    if not rows:
        print(f"No value-bet predictions recorded since {since}.")
        return 0

    print(f"\n{'Date':<11} {'Course':<3} R# {'Horse':<24} {'Model%':<7} {'Odds':<5} {'Bet$':<5} {'Pos':<4} {'P&L':<9}")
    print("-" * 85)

    total_bet = 0.0
    total_pnl = 0.0
    settled_count = 0
    win_count = 0

    for pred, bet, runner, race in rows:
        horse = runner.horse_name if runner else f"#{pred.horse_no}"
        prob = f"{pred.predicted_win_prob:.1%}" if pred.predicted_win_prob else ""
        odds = runner.win_odds if runner and runner.win_odds else 0
        bet_amt = pred.bet_amount or 0
        pos = bet.actual_position if bet and bet.actual_position else "-"
        pnl = bet.profit_loss if bet else None
        pnl_s = f"${pnl:+.0f}" if pnl is not None else "pending"

        print(
            f"{race.race_date!s:<11} {race.racecourse:<3} {race.race_no:>2} "
            f"{horse[:24]:<24} {prob:<7} {odds:<5} {bet_amt:<5.0f} {str(pos):<4} {pnl_s:<9}"
        )

        total_bet += bet_amt
        if pnl is not None:
            total_pnl += pnl
            settled_count += 1
            if pnl > 0:
                win_count += 1

    print("-" * 85)
    print(f"\nSettled bets: {settled_count}")
    print(f"Total stake: HK${total_bet:.0f}")
    print(f"Total P&L:   HK${total_pnl:+.0f}")
    if total_bet > 0:
        print(f"ROI:         {100 * total_pnl / total_bet:+.2f}%")
    if settled_count > 0:
        print(f"Win rate:    {100 * win_count / settled_count:.1f}%")
    print(f"\nShadow mode: {settings.SHADOW_MODE}")

    session.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

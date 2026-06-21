"""
On-demand prediction sender.

Generates ML predictions for a meeting's races that have NOT yet run (no finish
positions recorded) and sends them to Discord immediately — so you can pull the
remaining card's tips before post time instead of waiting for the 11:00 HKT cron.

Usage:
  python scripts/predict_now.py                 # today's remaining (un-run) races, HK date
  python scripts/predict_now.py --date 2026-06-24
  python scripts/predict_now.py --all           # include races that already ran (testing)
"""
import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.logging_config import setup_logging
from config.settings import Settings
from db.database import get_session
from db.models import Prediction, Race, Runner
from agents.predictor.bet_sizer import BetSizer
from agents.predictor.model_predictor import Predictor
from agents.predictor.pnl_tracker import PnLTracker
from discord_bot.webhook import DiscordWebhook

HK_TZ = ZoneInfo("Asia/Hong_Kong")


def _has_run(session, race_id: int) -> bool:
    """True if any runner already has a finish position (the race has been run)."""
    return (
        session.query(Runner)
        .filter(Runner.race_id == race_id, Runner.finish_position.isnot(None))
        .first()
        is not None
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", help="YYYY-MM-DD (default: today in HK time)")
    ap.add_argument("--all", action="store_true",
                    help="include races that have already run (for testing)")
    args = ap.parse_args()

    setup_logging()
    settings = Settings()
    target = (date.fromisoformat(args.date) if args.date
              else datetime.now(HK_TZ).date())

    session = get_session()
    races = (
        session.query(Race)
        .filter_by(race_date=target)
        .order_by(Race.race_no)
        .all()
    )
    if not races:
        print(f"No races found for {target}.")
        return

    if not args.all:
        upcoming = [r for r in races if not _has_run(session, r.id)]
    else:
        upcoming = races

    print(f"{target}: {len(races)} races on card, {len(upcoming)} to predict"
          + ("" if args.all else " (un-run only)"))
    if not upcoming:
        print("All races on this card have already run — nothing upcoming to send.")
        return

    predictor = Predictor(session)
    predictor.load_models()
    bankroll = PnLTracker(session, settings.INITIAL_BANKROLL).get_bankroll()
    sizer = BetSizer(bankroll=bankroll)
    discord = DiscordWebhook()

    sent = 0
    for race in upcoming:
        preds = predictor.predict_race(race.id)
        if preds.empty:
            print(f"  R{race.race_no}: no predictions (missing features?) — skipped")
            continue

        value_bets = predictor.find_value_bets(race.id)
        for vb in value_bets:
            sizer.size_value_bet(vb)
        predictor.save_predictions(race.id, preds, value_bets)

        predictions = (
            session.query(Prediction)
            .filter_by(race_id=race.id)
            .order_by(Prediction.predicted_rank)
            .all()
        )
        if not predictions:
            continue

        race_info = {
            "race_no": race.race_no,
            "racecourse": race.racecourse,
            "class": race.race_class or "",
            "distance": race.distance,
            "track_type": race.track_type or "",
            "going": race.going or "",
            "field_size": race.field_size,
        }
        pred_dicts, value_dicts = [], []
        for p in predictions:
            runner = (
                session.query(Runner)
                .filter_by(race_id=race.id, horse_no=p.horse_no)
                .first()
            )
            pred_dicts.append({
                "horse_no": p.horse_no,
                "horse_name": runner.horse_name if runner else f"#{p.horse_no}",
                "jockey": runner.jockey.name if runner and runner.jockey else "",
                "trainer": runner.trainer.name if runner and runner.trainer else "",
                "win_prob": p.predicted_win_prob or 0,
                "odds": runner.win_odds if runner else 0,
                "win_rank": p.predicted_rank or 99,
            })
            if p.is_value_bet and p.bet_amount:
                value_dicts.append({
                    "horse_no": p.horse_no,
                    "bet_type": p.bet_type,
                    "bet_amount": p.bet_amount,
                })

        discord.send_prediction(race_info, pred_dicts, value_dicts)
        sent += 1
        print(f"  R{race.race_no}: sent {len(pred_dicts)} predictions, "
              f"{len(value_dicts)} value bets")

    session.close()
    print(f"Done — sent {sent} race(s) to Discord.")


if __name__ == "__main__":
    main()

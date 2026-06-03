"""
Prediction accuracy check (walk-forward, out-of-sample).

Trains the SAME LightGBM win-ranker the live path uses on a trailing window,
then evaluates its picks on every race AFTER the cutoff that has a result.
Reports the model's top-pick win/place rates against the market-favourite
baseline (lowest win odds) — i.e. does the model actually beat the crowd?

Usage:
  python scripts/check_accuracy.py                 # default: train 24m, eval the tail
  python scripts/check_accuracy.py --eval-from 2026-01-01 --train-months 24
  python scripts/check_accuracy.py --class "Class 4"   # restrict eval to one class
"""
import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np

from agents.predictor.feature_engine import FeatureEngineer
from agents.predictor.model_trainer import ModelTrainer
from config.logging_config import setup_logging
from db.database import get_session
from db.models import Race


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--eval-from", help="YYYY-MM-DD; default = last 90 days of data")
    ap.add_argument("--train-months", type=int, default=24)
    ap.add_argument("--class", dest="race_class", default=None,
                    help="restrict evaluation to one race class, e.g. 'Class 4'")
    args = ap.parse_args()

    setup_logging()
    session = get_session()

    data_max = session.query(Race.race_date).order_by(Race.race_date.desc()).first()[0]
    eval_from = (date.fromisoformat(args.eval_from) if args.eval_from
                 else data_max - timedelta(days=90))
    train_end = eval_from - timedelta(days=1)
    train_start = train_end - timedelta(days=args.train_months * 30)

    print(f"\n=== PREDICTION ACCURACY (walk-forward) ===")
    print(f"Train: {train_start} → {train_end}   Eval: {eval_from} → {data_max}"
          + (f"   class={args.race_class}" if args.race_class else ""))

    trainer = ModelTrainer(session)
    model, _ = trainer.train_win_ranker(train_start, train_end)
    if model is None:
        print("Could not train ranker — insufficient data.")
        return

    feature_cols = FeatureEngineer.get_feature_columns()
    fe = FeatureEngineer(session)

    q = session.query(Race).filter(Race.race_date >= eval_from, Race.race_date <= data_max)
    if args.race_class:
        q = q.filter(Race.race_class == args.race_class)
    races = q.order_by(Race.race_date, Race.race_no).all()

    n_races = 0
    model_win = model_place = fav_win = fav_place = 0
    brier_sum = brier_n = 0.0
    rng_skipped = 0

    for race in races:
        df = fe.build_features_for_race(race.id)
        if df.empty or "finish_position" not in df or df["finish_position"].isna().all():
            rng_skipped += 1
            continue
        df = df[df["finish_position"].notna() & (df["finish_position"] > 0)]
        if len(df) < 2:
            rng_skipped += 1
            continue

        X = df[feature_cols].copy().fillna(df[feature_cols].median())
        scores = model.predict(X)
        exp_s = np.exp(scores - scores.max())
        win_probs = exp_s / exp_s.sum()

        pos = df["finish_position"].to_numpy()
        odds = df["win_odds"].to_numpy() if "win_odds" in df else np.full(len(df), np.nan)

        # Model's top pick
        top = int(np.argmax(win_probs))
        model_win += int(pos[top] == 1)
        model_place += int(pos[top] <= 3)

        # Market favourite = lowest win odds (skip baseline if no odds)
        if np.isfinite(odds).any():
            fav = int(np.nanargmin(np.where(np.isfinite(odds), odds, np.inf)))
            fav_win += int(pos[fav] == 1)
            fav_place += int(pos[fav] <= 3)

        # Calibration: Brier score of per-runner win probability
        is_win = (pos == 1).astype(float)
        brier_sum += float(np.sum((win_probs - is_win) ** 2))
        brier_n += len(df)
        n_races += 1

    session.close()

    if n_races == 0:
        print("No evaluable races in the window.")
        return

    def pct(x): return 100.0 * x / n_races
    print(f"\nRaces evaluated: {n_races}   (skipped {rng_skipped} w/o usable data)")
    print(f"\n{'metric':<28}{'MODEL #1 pick':>16}{'market favourite':>20}")
    print(f"{'win rate (pick wins)':<28}{pct(model_win):>15.1f}%{pct(fav_win):>19.1f}%")
    print(f"{'place rate (top-3)':<28}{pct(model_place):>15.1f}%{pct(fav_place):>19.1f}%")
    avg_field = brier_n / n_races
    print(f"\nRandom baseline win rate ≈ {100.0 / avg_field:.1f}% (avg field size {avg_field:.1f})")
    print(f"Brier score (per-runner win prob): {brier_sum / brier_n:.4f}  (lower is better)")
    edge = pct(model_win) - pct(fav_win)
    verdict = "BEATS market favourite" if edge > 0 else "does NOT beat market favourite"
    print(f"\nEdge vs favourite (win): {edge:+.1f} pts → {verdict}")


if __name__ == "__main__":
    main()

"""
Backfill Runner.speed_figure for all historical runs.

Adds the speed_figure column if the DB predates it, then computes a
distance/going-normalized speed figure from each runner's finish time and
stores it. Idempotent — safe to re-run (recomputes from current baselines).

Usage:
  python scripts/backfill_speed_figures.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from sqlalchemy import inspect, text

from agents.predictor.speed_figure import SpeedFigureCalculator
from config.logging_config import setup_logging
from db.database import get_session
from db.models import Race, Runner


def ensure_column(session):
    """ALTER TABLE runners ADD COLUMN speed_figure if it doesn't exist (SQLite)."""
    cols = {c["name"] for c in inspect(session.bind).get_columns("runners")}
    if "speed_figure" not in cols:
        session.execute(text("ALTER TABLE runners ADD COLUMN speed_figure FLOAT"))
        session.commit()
        print("Added column runners.speed_figure")


def main():
    setup_logging()
    session = get_session()
    ensure_column(session)

    calc = SpeedFigureCalculator(session)

    runners = (
        session.query(Runner)
        .join(Race, Runner.race_id == Race.id)
        .filter(Runner.finish_time.isnot(None), Race.distance.isnot(None))
        .all()
    )
    print(f"Computing speed figures for {len(runners)} runs...")

    updated = 0
    for r in runners:
        fig = calc.figure(r.finish_time, r.race.distance, r.race.going)
        r.speed_figure = fig  # always assign (None clears stale/rejected values)
        if fig is not None:
            updated += 1

    session.commit()
    print(f"Stored speed figures for {updated}/{len(runners)} runs.")

    # quick sanity: distribution
    figs = [r.speed_figure for r in runners if r.speed_figure is not None]
    if figs:
        figs.sort()
        n = len(figs)
        print(f"min={figs[0]:.1f}  p25={figs[n//4]:.1f}  median={figs[n//2]:.1f}  "
              f"p75={figs[3*n//4]:.1f}  max={figs[-1]:.1f}")
    session.close()


if __name__ == "__main__":
    main()

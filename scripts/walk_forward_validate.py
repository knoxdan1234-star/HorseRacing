"""
Multi-window walk-forward validation of the MODEL TOP-PICK strategy.

Builds features once for the whole history, then for each monthly test window
trains the LightGBM ranker on the trailing N months and simulates backing the
model's top pick (in an odds band) vs backing the market favourite. Reports
per-window and aggregate ROI, hit rate, how often the model beats the
favourite, and worst window — the gate between "good on one window" and a real,
repeatable edge.

Usage:
  python scripts/walk_forward_validate.py
  python scripts/walk_forward_validate.py --train-months 18 --min-odds 2.5 --max-odds 20
"""
import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import lightgbm as lgb
import numpy as np

from agents.predictor.feature_engine import FeatureEngineer
from config.logging_config import setup_logging
from db.database import get_session
from db.models import Race

RANKER_PARAMS = dict(
    objective="lambdarank", metric="ndcg", ndcg_eval_at=[1, 3],
    n_estimators=500, learning_rate=0.05, num_leaves=63, max_depth=8,
    min_child_samples=50, subsample=0.8, colsample_bytree=0.8,
    reg_alpha=0.1, reg_lambda=1.0, verbose=-1,
    n_jobs=4, random_state=42,  # capped so a run can't oversubscribe all cores
)


def month_windows(first: date, last: date):
    out = []
    y, m = first.year, first.month
    while date(y, m, 1) <= last:
        ws = date(y, m, 1)
        ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
        we = date(ny, nm, 1) - timedelta(days=1)
        out.append((ws, we))
        y, m = ny, nm
    return out


def train_ranker(df, cols, median):
    d = df[df["is_winner"].notna()].sort_values("race_id")
    if d.empty:
        return None
    X = d[cols].fillna(median)
    y = d["is_winner"].astype(int).values
    group = d.groupby("race_id", sort=False).size().values
    r = lgb.LGBMRanker(**RANKER_PARAMS)
    r.fit(X, y, group=group)
    return r


def simulate_window(df_eval, ranker, cols, median, min_odds, max_odds):
    """Returns (model: (bets,hits,pnl), fav: (bets,hits,pnl))."""
    s = h = 0
    pnl = 0.0
    fs = fh = 0
    fpnl = 0.0
    for _, g in df_eval.groupby("race_id"):
        g = g[g["finish_position"].notna() & (g["finish_position"] > 0)]
        if len(g) < 2:
            continue
        odds = g["win_odds"].to_numpy(dtype=float)
        if not np.isfinite(odds).all() or (odds <= 1).any():
            continue
        won = (g["finish_position"].to_numpy() == 1).astype(float)

        scores = ranker.predict(g[cols].fillna(median))
        i = int(np.argmax(scores))
        if min_odds <= odds[i] <= max_odds:
            s += 1
            if won[i] == 1:
                pnl += odds[i] - 1; h += 1
            else:
                pnl -= 1

        f = int(np.argmin(odds))
        fs += 1
        if won[f] == 1:
            fpnl += odds[f] - 1; fh += 1
        else:
            fpnl -= 1
    return (s, h, pnl), (fs, fh, fpnl)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--train-months", type=int, default=18)
    ap.add_argument("--since", help="only build/validate from this date (YYYY-MM-DD) to limit cost")
    ap.add_argument("--min-odds", type=float, default=2.5)
    ap.add_argument("--max-odds", type=float, default=20.0)
    args = ap.parse_args()

    setup_logging()
    session = get_session()
    cols = FeatureEngineer.get_feature_columns()
    fe = FeatureEngineer(session)

    dmax = session.query(Race.race_date).order_by(Race.race_date.desc()).first()[0]
    dmin = session.query(Race.race_date).order_by(Race.race_date.asc()).first()[0]
    if args.since:
        dmin = max(dmin, date.fromisoformat(args.since))

    print("\n=== WALK-FORWARD VALIDATION (MODEL TOP-PICK) ===")
    print(f"Data: {dmin} -> {dmax} | train window {args.train_months}m | "
          f"odds [{args.min_odds}, {args.max_odds}]")
    print("Building features once for the whole history (slow)...")
    full = fe.build_features_for_date_range(dmin, dmax)
    if full.empty:
        print("No features built."); return

    rid_to_date = {r.id: r.race_date for r in session.query(Race.id, Race.race_date).all()}
    full["race_date"] = full["race_id"].map(rid_to_date)

    first_eval = dmin + timedelta(days=args.train_months * 30)
    windows = [(ws, we) for ws, we in month_windows(first_eval, dmax)]

    print(f"\n{'window':<12}{'bets':>6}{'hit%':>7}{'ROI%':>9}{'fav ROI%':>10}")
    rows = []
    for ws, we in windows:
        tr0 = ws - timedelta(days=args.train_months * 30)
        train = full[(full["race_date"] >= tr0) & (full["race_date"] < ws)]
        ev = full[(full["race_date"] >= ws) & (full["race_date"] <= we)]
        if train["race_id"].nunique() < 50 or ev.empty:
            continue
        median = train[cols].median()
        ranker = train_ranker(train, cols, median)
        if ranker is None:
            continue
        (s, h, pnl), (fs, fh, fpnl) = simulate_window(ev, ranker, cols, median,
                                                       args.min_odds, args.max_odds)
        if s == 0:
            continue
        roi = 100 * pnl / s
        fav_roi = 100 * fpnl / fs if fs else 0.0
        rows.append((ws, s, h, pnl, roi, fs, fpnl, fav_roi))
        print(f"{ws.isoformat():<12}{s:>6}{100*h/s:>6.1f}%{roi:>8.1f}%{fav_roi:>9.1f}%")

    if not rows:
        print("No evaluable windows."); return

    tot_bets = sum(r[1] for r in rows)
    tot_pnl = sum(r[3] for r in rows)
    tot_fav_bets = sum(r[5] for r in rows)
    tot_fav_pnl = sum(r[6] for r in rows)
    beats = sum(1 for r in rows if r[4] > r[7])
    mean_roi = float(np.mean([r[4] for r in rows]))

    # worst cumulative drawdown on the per-window pnl sequence (unit stakes)
    cum = np.cumsum([r[3] for r in rows])
    peak = np.maximum.accumulate(cum)
    max_dd = float((peak - cum).max()) if len(cum) else 0.0

    print("\n=== AGGREGATE ===")
    print(f"Windows: {len(rows)}   model beats favourite in {beats}/{len(rows)}")
    print(f"MODEL TOP-PICK : {tot_bets} bets, overall ROI {100*tot_pnl/tot_bets:+.1f}%, "
          f"mean-of-windows {mean_roi:+.1f}%")
    print(f"MARKET FAV     : {tot_fav_bets} bets, overall ROI {100*tot_fav_pnl/tot_fav_bets:+.1f}%")
    print(f"Worst drawdown : {max_dd:.0f} units")
    print("\n(positive overall ROI across many windows = a real, repeatable edge)")


if __name__ == "__main__":
    main()

"""
Two-stage (Benter-style) blend backtest.

Stage 1: the existing LightGBM win-ranker -> a "fundamental" win probability.
Stage 2: a conditional-logit blend of log(fundamental_prob) and log(market_prob)
         fit by maximum likelihood on the actual winners, so the market gets the
         weight it deserves and the model only contributes incremental edge.

Then it simulates flat-stake ROI (1 unit/bet) over an out-of-sample eval window
for three strategies, so we can compare:
  - MARKET FAVOURITE   : back the lowest-odds horse every race (baseline)
  - MODEL-ONLY value   : bet where the ranker's prob beats the market price
  - BLEND value        : bet where the stage-2 blended prob beats the market price

Usage:
  python scripts/backtest_blend.py
  python scripts/backtest_blend.py --eval-from 2026-01-01 --edge 0.05 --min-odds 2.5 --max-odds 20
"""
import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np
from scipy.optimize import minimize

from agents.predictor.feature_engine import FeatureEngineer
from agents.predictor.model_trainer import ModelTrainer
from config.logging_config import setup_logging
from db.database import get_session
from db.models import Race

EPS = 1e-9


def _softmax(x):
    e = np.exp(x - np.max(x))
    return e / e.sum()


def race_table(session, model, feature_cols, fe, races):
    """For each race return per-runner fundamental prob, market prob, win flag,
    odds. Skips races without usable odds or results."""
    rows = []
    for r in races:
        df = fe.build_features_for_race(r.id)
        if df.empty or "finish_position" not in df:
            continue
        df = df[df["finish_position"].notna() & (df["finish_position"] > 0)]
        if len(df) < 2:
            continue
        odds = df["win_odds"].to_numpy(dtype=float)
        if not np.isfinite(odds).all() or (odds <= 1).any():
            continue  # need real odds for every runner to form a market prob
        X = df[feature_cols].copy().fillna(df[feature_cols].median())
        fund = _softmax(model.predict(X))
        inv = 1.0 / odds
        market = inv / inv.sum()           # de-overrounded market prob
        won = (df["finish_position"].to_numpy() == 1).astype(float)
        if won.sum() < 1:
            continue
        rows.append({"fund": fund, "market": market, "won": won, "odds": odds})
    return rows


def fit_blend(tables, l2=0.01):
    """Conditional-logit MLE: P_i ∝ exp(a*log(fund_i) + b*log(market_i)),
    softmax within race. Weights constrained non-negative with an L2 penalty so
    the fit stays a genuine Benter blend (positive market weight) instead of
    overfitting to large offsetting magnitudes. Returns (a, b)."""
    lf = [np.log(np.clip(t["fund"], EPS, 1)) for t in tables]
    lm = [np.log(np.clip(t["market"], EPS, 1)) for t in tables]
    won = [t["won"] for t in tables]

    def nll(w):
        a, b = w
        total = l2 * (a * a + b * b)
        for f, m, y in zip(lf, lm, won):
            p = _softmax(a * f + b * m)
            total -= np.log(p[y == 1] + EPS).sum()
        return total

    res = minimize(nll, x0=np.array([0.5, 1.0]), method="L-BFGS-B",
                   bounds=[(0.0, 10.0), (0.0, 10.0)],
                   options={"maxiter": 2000})
    return float(res.x[0]), float(res.x[1])


def blend_prob(t, a, b):
    score = a * np.log(np.clip(t["fund"], EPS, 1)) + b * np.log(np.clip(t["market"], EPS, 1))
    return _softmax(score)


def simulate_toppick(tables, prob_key, a, b, min_odds, max_odds):
    """Back the model's single highest-probability runner each race, if its odds
    are in band. Tests whether the model's *selection* edge is monetizable."""
    staked = pnl = hits = 0
    for t in tables:
        p = t["fund"] if prob_key == "fund" else blend_prob(t, a, b)
        i = int(np.argmax(p))
        if not (min_odds <= t["odds"][i] <= max_odds):
            continue
        staked += 1
        if t["won"][i] == 1:
            pnl += t["odds"][i] - 1
            hits += 1
        else:
            pnl -= 1
    roi = 100.0 * pnl / staked if staked else 0.0
    return staked, hits, roi


def simulate(tables, prob_key, a, b, edge, min_odds, max_odds, favourite=False):
    """Flat 1-unit stakes. Returns (n_bets, hits, roi_pct)."""
    staked = pnl = hits = 0
    for t in tables:
        if favourite:
            idx = [int(np.argmin(t["odds"]))]
        else:
            p = t["fund"] if prob_key == "fund" else blend_prob(t, a, b)
            idx = [i for i in range(len(t["odds"]))
                   if min_odds <= t["odds"][i] <= max_odds
                   and p[i] > t["market"][i] * (1 + edge)]
        for i in idx:
            staked += 1
            if t["won"][i] == 1:
                pnl += t["odds"][i] - 1
                hits += 1
            else:
                pnl -= 1
    roi = 100.0 * pnl / staked if staked else 0.0
    return staked, hits, roi


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--eval-from", help="YYYY-MM-DD; default = last 90 days of data")
    ap.add_argument("--train-months", type=int, default=24)
    ap.add_argument("--fit-days", type=int, default=120,
                    help="days before eval used to fit the stage-2 blend")
    ap.add_argument("--edge", type=float, default=0.05)
    ap.add_argument("--min-odds", type=float, default=2.5)
    ap.add_argument("--max-odds", type=float, default=20.0)
    args = ap.parse_args()

    setup_logging()
    session = get_session()
    data_max = session.query(Race.race_date).order_by(Race.race_date.desc()).first()[0]
    eval_from = (date.fromisoformat(args.eval_from) if args.eval_from
                 else data_max - timedelta(days=90))
    # Stage-2 must be fit on OUT-OF-SAMPLE ranker probs, so the ranker is trained
    # strictly BEFORE the blend-fit window — otherwise in-sample (overfit) model
    # probs make the blend wrongly trust the model over the market.
    fit_start = eval_from - timedelta(days=args.fit_days)
    ranker_train_end = fit_start - timedelta(days=1)
    train_start = ranker_train_end - timedelta(days=args.train_months * 30)
    fit_end = eval_from - timedelta(days=1)

    print("\n=== TWO-STAGE BLEND BACKTEST ===")
    print(f"Stage-1 train: {train_start} -> {ranker_train_end}")
    print(f"Stage-2 fit:   {fit_start} -> {fit_end}   (OOS for ranker)")
    print(f"Eval (OOS):    {eval_from} -> {data_max}")
    print(f"Filter: edge>={args.edge}, odds [{args.min_odds}, {args.max_odds}]")

    trainer = ModelTrainer(session)
    model, _ = trainer.train_win_ranker(train_start, ranker_train_end)
    if model is None:
        print("Could not train ranker."); return
    feature_cols = FeatureEngineer.get_feature_columns()
    fe = FeatureEngineer(session)

    def races_between(a, b):
        return (session.query(Race)
                .filter(Race.race_date >= a, Race.race_date <= b)
                .order_by(Race.race_date, Race.race_no).all())

    fit_tables = race_table(session, model, feature_cols, fe, races_between(fit_start, fit_end))
    eval_tables = race_table(session, model, feature_cols, fe, races_between(eval_from, data_max))
    if not fit_tables or not eval_tables:
        print("Not enough data with odds to fit/eval."); return

    a, b = fit_blend(fit_tables)
    print(f"\nStage-2 weights:  model a={a:.3f}   market b={b:.3f}   "
          f"(market/model weight ratio {b / a:.2f})" if a else "")
    print(f"Fit races: {len(fit_tables)}   Eval races: {len(eval_tables)}\n")

    rows = [
        ("MARKET FAVOURITE", simulate(eval_tables, "market", a, b, args.edge,
                                      args.min_odds, args.max_odds, favourite=True)),
        ("MODEL-ONLY value", simulate(eval_tables, "fund", a, b, args.edge,
                                      args.min_odds, args.max_odds)),
        ("BLEND value", simulate(eval_tables, "blend", a, b, args.edge,
                                 args.min_odds, args.max_odds)),
        ("MODEL TOP-PICK", simulate_toppick(eval_tables, "fund", a, b,
                                            args.min_odds, args.max_odds)),
        ("BLEND TOP-PICK", simulate_toppick(eval_tables, "blend", a, b,
                                            args.min_odds, args.max_odds)),
    ]
    print(f"{'strategy':<20}{'bets':>7}{'hits':>7}{'hit%':>8}{'ROI%':>9}")
    for name, (n, h, roi) in rows:
        hp = 100.0 * h / n if n else 0.0
        print(f"{name:<20}{n:>7}{h:>7}{hp:>7.1f}%{roi:>8.1f}%")
    print("\n(ROI = flat 1-unit stakes; positive = profitable on this OOS window)")


if __name__ == "__main__":
    main()

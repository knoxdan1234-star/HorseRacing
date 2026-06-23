"""
Fast bet-type comparison: WIN vs PLACE (位置) vs QPL (位置Q).

Same selection every time — the model's top pick(s) when the top pick's win
odds are in [4,12] — settled three ways, on a single train/test split using the
cached walk-forward feature matrix (one ranker fit, ~minutes). Answers "is place
/ 位置Q more profitable than win?" empirically. Flat 1-unit stakes → ROI%.

  WIN   : top pick finishes 1st         (win odds)
  PLACE : top pick finishes top-3       (PLA dividend)
  QPL   : top-2 picks BOTH finish top-3 (QPL dividend)
"""
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import lightgbm as lgb
import numpy as np
import pandas as pd

from agents.predictor.feature_engine import FeatureEngineer
from config.logging_config import setup_logging
from db.database import get_session
from db.models import Dividend, Race

RANKER_PARAMS = dict(
    objective="lambdarank", metric="ndcg", ndcg_eval_at=[1, 3],
    n_estimators=500, learning_rate=0.05, num_leaves=63, max_depth=8,
    min_child_samples=50, subsample=0.8, colsample_bytree=0.8,
    reg_alpha=0.1, reg_lambda=1.0, verbose=-1, n_jobs=4, random_state=42,
)
MIN_ODDS, MAX_ODDS = 4.0, 12.0
TEST_FROM = date(2025, 7, 1)   # train before, test from here (out-of-sample)


def _softmax(x):
    e = np.exp(x - np.max(x))
    return e / e.sum()


def main():
    setup_logging()
    session = get_session()
    cols = FeatureEngineer.get_feature_columns()
    dmin = session.query(Race.race_date).order_by(Race.race_date.asc()).first()[0]
    dmax = session.query(Race.race_date).order_by(Race.race_date.desc()).first()[0]

    cache = ROOT / f"data/.wf_features_{dmin}_{dmax}.pkl"
    if not cache.exists():
        print(f"Feature cache missing ({cache.name}); run walk_forward_validate.py once first.")
        return
    full = pd.read_pickle(cache)

    train = full[full["race_date"] < TEST_FROM]
    test = full[full["race_date"] >= TEST_FROM]
    print(f"Train < {TEST_FROM}: {train['race_id'].nunique()} races | "
          f"Test >= {TEST_FROM}: {test['race_id'].nunique()} races | band [{MIN_ODDS},{MAX_ODDS}]")

    d = train[train["is_winner"].notna()].sort_values("race_id")
    median = train[cols].median()
    ranker = lgb.LGBMRanker(**RANKER_PARAMS)
    ranker.fit(d[cols].fillna(median), d["is_winner"].astype(int).values,
               group=d.groupby("race_id", sort=False).size().values)

    pla = {}
    qpl = {}
    for rid, combo, payout in (session.query(Dividend.race_id, Dividend.combination, Dividend.payout)
                               .filter(Dividend.pool_type == "PLA").all()):
        if combo and str(combo).isdigit():
            pla[(rid, int(combo))] = payout
    for rid, combo, payout in (session.query(Dividend.race_id, Dividend.combination, Dividend.payout)
                               .filter(Dividend.pool_type == "QPL").all()):
        qpl[(rid, str(combo))] = payout

    # [bets, hits, pnl]
    win = [0, 0, 0.0]
    place = [0, 0, 0.0]
    qp = [0, 0, 0.0]

    for rid, g in test.groupby("race_id"):
        g = g[g["finish_position"].notna() & (g["finish_position"] > 0)]
        if len(g) < 2:
            continue
        odds = g["win_odds"].to_numpy(dtype=float)
        if not np.isfinite(odds).all() or (odds <= 1).any():
            continue
        fin = g["finish_position"].to_numpy()
        hno = g["horse_no"].to_numpy().astype(int)
        scores = ranker.predict(g[cols].fillna(median))
        order = np.argsort(scores)
        i = int(order[-1])
        if not (MIN_ODDS <= odds[i] <= MAX_ODDS):
            continue
        rid = int(rid)

        # WIN
        win[0] += 1
        if fin[i] == 1:
            win[1] += 1; win[2] += odds[i] - 1
        else:
            win[2] -= 1

        # PLACE (same pick, top-3)
        place[0] += 1
        if fin[i] <= 3:
            div = pla.get((rid, int(hno[i])))
            place[1] += 1
            place[2] += (div / 10.0 - 1) if div else 0.0
        else:
            place[2] -= 1

        # QPL (top-2 picks both top-3)
        if len(order) >= 2:
            j = int(order[-2])
            qp[0] += 1
            if fin[i] <= 3 and fin[j] <= 3:
                key = f"{min(hno[i], hno[j])},{max(hno[i], hno[j])}"
                div = qpl.get((rid, key))
                qp[1] += 1
                qp[2] += (div / 10.0 - 1) if div else 0.0
            else:
                qp[2] -= 1

    print(f"\n{'bet type':<16}{'bets':>7}{'hit%':>8}{'ROI%':>9}")
    for name, st in [("WIN (獨贏)", win), ("PLACE (位置)", place), ("QPL (位置Q)", qp)]:
        b, h, pnl = st
        if b:
            print(f"{name:<16}{b:>7}{100*h/b:>7.1f}%{100*pnl/b:>8.1f}%")
    print("\n(same selection settled 3 ways; flat 1-unit stakes; OOS test period)")


if __name__ == "__main__":
    main()

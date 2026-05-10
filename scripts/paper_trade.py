"""
Paper-trade the Class-4 + Exp-1 ranker strategy.

This is the strategy validated by the May 2026 walk-forward analysis:
  ranker (LambdaRank), edge=5%, Kelly=1%, odds [2.5,20], top-3, Class 4 only.
Out-of-sample Jan 2025 - Apr 2026: 28 bets, +46.9% ROI on a model trained
without seeing those races.

Modes:
  predict --date YYYY-MM-DD     Generate paper bets for that day's Class 4 races.
  settle --through YYYY-MM-DD   Settle unsettled bets up to a date and update PnL.
  summary                       Print running ledger stats.

Ledger lives at output/paper_trades/ledger.jsonl (one bet per line).
"""

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

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
from discord_bot.webhook import DiscordWebhook

LEDGER_PATH = ROOT / "output" / "paper_trades" / "ledger.jsonl"
TRAIN_WINDOW_MONTHS = 24
INITIAL_BANKROLL = 10000.0

# Exp-1 strategy parameters (the validated config)
EDGE_MARGIN = 0.05
KELLY_FRACTION = 0.01
MIN_ODDS = 2.5
MAX_ODDS = 20.0
TOP_RANK = 3
TARGET_CLASS = "Class 4"

logger = logging.getLogger(__name__)


@dataclass
class PaperBet:
    placed_at: str
    race_date: str
    race_id: int
    racecourse: str
    race_no: int
    distance: int
    race_class: str
    horse_no: int
    horse_name: str
    model_prob: float
    win_odds: float
    bet_amount: float
    settled: bool = False
    finish_position: Optional[int] = None
    actual_dividend: Optional[float] = None
    pnl: Optional[float] = None


def load_ledger() -> list[dict]:
    if not LEDGER_PATH.exists():
        return []
    return [json.loads(line) for line in LEDGER_PATH.read_text().splitlines() if line.strip()]


def append_ledger(bet: PaperBet):
    LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LEDGER_PATH, "a") as f:
        f.write(json.dumps(asdict(bet)) + "\n")


def rewrite_ledger(bets: list[dict]):
    LEDGER_PATH.write_text("\n".join(json.dumps(b) for b in bets) + "\n")


def current_bankroll(ledger: list[dict]) -> float:
    return INITIAL_BANKROLL + sum(b.get("pnl") or 0 for b in ledger if b.get("settled"))


COURSE_TC = {"ST": "沙田", "HV": "跑馬地", "MJC": "澳門"}


def _discord_signal_embed(target: date, bets: list[PaperBet], bankroll: float) -> dict:
    """Build the Traditional-Chinese Discord embed for a day's paper bets."""
    lines = []
    for b in bets:
        course = COURSE_TC.get(b.racecourse, b.racecourse)
        lines.append({
            "name": f"第{b.race_no}場 {course} {b.distance}米 — #{b.horse_no} {b.horse_name}",
            "value": (
                f"模型機率: {b.model_prob*100:.1f}% | 賠率: {b.win_odds:.1f}\n"
                f"建議紙上投注: HK${b.bet_amount:,.0f} | 投注類型: 獨贏 (WIN)"
            ),
            "inline": False,
        })
    total_stake = sum(b.bet_amount for b in bets)
    return {
        "title": f"📋 紙上投注訊號 — {target}",
        "description": (
            f"⚠️ **紙上模式 (PAPER) — 不要實際下注**\n"
            f"策略: Class 4 + Ranker + Exp-1 (edge≥5%, Kelly 1%, 賠率 2.5-20)\n"
            f"今日訊號: {len(bets)} 注 | 總注額: HK${total_stake:,.0f} | 目前本金: HK${bankroll:,.2f}"
        ),
        "fields": lines,
        "color": 3066993,
    }


def _discord_settle_embed(through: date, settled: list[dict], total_pnl: float, bankroll: float) -> dict:
    """Build the Traditional-Chinese settlement embed."""
    fields = []
    for b in settled:
        course = COURSE_TC.get(b["racecourse"], b["racecourse"])
        pnl = b.get("pnl") or 0.0
        status = "✅ 中獎" if pnl > 0 else "❌ 落敗"
        fields.append({
            "name": f"{b['race_date']} 第{b['race_no']}場 {course} #{b['horse_no']} {b['horse_name']}",
            "value": (
                f"{status} | 名次: {b.get('finish_position', '?')} | "
                f"盈虧: HK${pnl:+,.2f}"
            ),
            "inline": False,
        })
    color = 3066993 if total_pnl >= 0 else 15158332
    return {
        "title": f"🏁 賽果結算 — 截至 {through}",
        "description": (
            f"已結算 {len(settled)} 注 | 本批盈虧: HK${total_pnl:+,.2f} | "
            f"目前本金: HK${bankroll:,.2f}"
        ),
        "fields": fields,
        "color": color,
    }


def predict_for_date(target: date) -> int:
    """Generate paper bets for all Class-4 races on `target`. Returns count."""
    session = get_session()
    races = (
        session.query(Race)
        .filter(Race.race_date == target, Race.race_class == TARGET_CLASS)
        .order_by(Race.race_no)
        .all()
    )
    if not races:
        logger.info("No Class 4 races on %s", target)
        session.close()
        return 0

    # Train a fresh ranker on the trailing 24 months
    train_end = target - timedelta(days=1)
    train_start = train_end - timedelta(days=TRAIN_WINDOW_MONTHS * 30)
    logger.info("Training ranker on %s to %s for %d Class-4 races on %s",
                train_start, train_end, len(races), target)

    trainer = ModelTrainer(session)
    model, _ = trainer.train_win_ranker(train_start, train_end)
    if model is None:
        logger.error("Could not train ranker — insufficient data")
        session.close()
        return 0

    feature_cols = FeatureEngineer.get_feature_columns()
    fe = FeatureEngineer(session)

    ledger = load_ledger()
    bankroll = current_bankroll(ledger)
    sizer = BetSizer(bankroll=bankroll, kelly_fraction=KELLY_FRACTION)
    logger.info("Current bankroll: $%.2f", bankroll)

    n_placed = 0
    placed_bets: list[PaperBet] = []
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

            bet = PaperBet(
                placed_at=datetime.now().isoformat(timespec="seconds"),
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
            )
            append_ledger(bet)
            placed_bets.append(bet)
            n_placed += 1
            logger.info(
                "PAPER BET %s R%d #%d %s @%.1f $%.0f (prob=%.3f)",
                race.racecourse, race.race_no, horse_no, horse_name,
                win_odds, bet_amount, model_prob,
            )
    session.close()

    # Discord signal
    if placed_bets:
        try:
            wh = DiscordWebhook()
            embed = _discord_signal_embed(target, placed_bets, current_bankroll(load_ledger()))
            wh._post(wh.webhook_url, {"embeds": [embed]})
            logger.info("Discord signal sent for %d bets on %s", len(placed_bets), target)
        except Exception as e:
            logger.error("Discord signal failed: %s", e)

    return n_placed


def settle_through(through: date) -> int:
    """Settle all unsettled bets with race_date <= `through`. Returns count settled."""
    ledger = load_ledger()
    session = get_session()
    n_settled = 0
    just_settled: list[dict] = []
    batch_pnl = 0.0
    for entry in ledger:
        if entry.get("settled"):
            continue
        if date.fromisoformat(entry["race_date"]) > through:
            continue
        runner = (
            session.query(Runner)
            .filter_by(race_id=entry["race_id"], horse_no=entry["horse_no"])
            .first()
        )
        if not runner or not runner.finish_position:
            # Result not yet recorded — skip until next settle
            continue
        entry["finish_position"] = runner.finish_position
        won = runner.finish_position == 1
        if not won:
            entry["pnl"] = -entry["bet_amount"]
            entry["actual_dividend"] = None
        else:
            div = (
                session.query(Dividend)
                .filter_by(race_id=entry["race_id"], pool_type="WIN", combination=str(entry["horse_no"]))
                .first()
            )
            if not div:
                # Estimate from win odds with 17.5% deduction
                payout = (entry["win_odds"] - 1) * (1 - 0.175) * entry["bet_amount"]
                entry["pnl"] = payout
                entry["actual_dividend"] = None
            else:
                entry["pnl"] = (entry["bet_amount"] / 10) * div.payout - entry["bet_amount"]
                entry["actual_dividend"] = div.payout
        entry["settled"] = True
        n_settled += 1
        just_settled.append(entry)
        batch_pnl += entry.get("pnl") or 0.0
        logger.info("SETTLED race %d #%d pos=%d pnl=%+.2f",
                    entry["race_id"], entry["horse_no"],
                    entry["finish_position"], entry["pnl"])
    rewrite_ledger(ledger)
    session.close()

    if just_settled:
        try:
            wh = DiscordWebhook()
            bankroll = INITIAL_BANKROLL + sum(
                b.get("pnl") or 0 for b in load_ledger() if b.get("settled")
            )
            embed = _discord_settle_embed(through, just_settled, batch_pnl, bankroll)
            wh._post(wh.webhook_url, {"embeds": [embed]})
            logger.info("Discord settlement sent for %d bets through %s", n_settled, through)
        except Exception as e:
            logger.error("Discord settlement notification failed: %s", e)

    return n_settled


def summary():
    ledger = load_ledger()
    if not ledger:
        print("Ledger empty — no paper bets placed yet.")
        return
    settled = [b for b in ledger if b.get("settled")]
    pending = [b for b in ledger if not b.get("settled")]
    n = len(settled)
    wins = sum(1 for b in settled if (b.get("pnl") or 0) > 0)
    total_stake = sum(b["bet_amount"] for b in settled)
    total_pnl = sum(b.get("pnl") or 0 for b in settled)
    bankroll = INITIAL_BANKROLL + total_pnl

    print(f"\n=== PAPER TRADING LEDGER ({LEDGER_PATH.relative_to(ROOT)}) ===")
    print(f"Strategy: ranker + Class 4 + Exp-1 (edge=5%, Kelly=1%, odds [2.5,20], top-3)")
    print(f"Initial bankroll:      ${INITIAL_BANKROLL:,.2f}")
    print(f"Bets placed:           {len(ledger)}")
    print(f"Bets settled:          {n}")
    print(f"Bets pending:          {len(pending)}")
    if n:
        print(f"Hit rate (settled):    {100*wins/n:.1f}% ({wins}/{n})")
        print(f"Total stake:           ${total_stake:,.2f}")
        print(f"Total P&L:             ${total_pnl:+,.2f}")
        print(f"ROI:                   {100*total_pnl/max(total_stake,1):+.2f}%")
        print(f"Current bankroll:      ${bankroll:,.2f}  ({(bankroll/INITIAL_BANKROLL-1)*100:+.2f}%)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)

    p = sub.add_parser("predict", help="generate paper bets for a date")
    p.add_argument("--date", required=True, help="YYYY-MM-DD")

    s = sub.add_parser("settle", help="settle bets up to a date")
    s.add_argument("--through", required=True, help="YYYY-MM-DD")

    sub.add_parser("summary", help="show ledger summary")

    args = parser.parse_args()
    setup_logging(settings)
    init_database()

    if args.mode == "predict":
        n = predict_for_date(date.fromisoformat(args.date))
        print(f"Placed {n} paper bets.")
    elif args.mode == "settle":
        n = settle_through(date.fromisoformat(args.through))
        print(f"Settled {n} paper bets.")
    elif args.mode == "summary":
        summary()


if __name__ == "__main__":
    main()

"""
Backtesting Engine

Walk-forward backtesting to validate profitability before going live.
Simulates actual HKJC pari-mutuel dividends.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from agents.predictor.bet_sizer import BetSizer
from agents.predictor.feature_engine import FeatureEngineer
from agents.predictor.model_trainer import ModelTrainer
from config import settings
from db.models import Dividend, Race, Runner

logger = logging.getLogger(__name__)


@dataclass
class BetRecord:
    race_date: date
    racecourse: str
    race_no: int
    horse_no: int
    bet_type: str
    bet_amount: float
    model_prob: float
    odds: float
    actual_position: int | None = None
    actual_dividend: float | None = None
    profit_loss: float = 0.0


@dataclass
class BacktestPeriod:
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    model_auc: float = 0.0
    num_races: int = 0
    num_bets: int = 0
    total_bet: float = 0.0
    total_pnl: float = 0.0
    bets: list[BetRecord] = field(default_factory=list)


@dataclass
class BacktestMetrics:
    total_periods: int
    total_races: int
    total_bets: int
    total_bet_amount: float
    total_pnl: float
    roi_pct: float
    win_rate: float
    profit_factor: float
    max_drawdown: float
    max_drawdown_pct: float
    sharpe_ratio: float
    avg_bet_pnl: float
    pnl_by_bet_type: dict = field(default_factory=dict)


class Backtester:
    """Walk-forward backtesting engine."""

    def __init__(self, session: Session):
        self.session = session
        self.feature_engine = FeatureEngineer(session)
        self.bet_sizer = BetSizer()

    def walk_forward_backtest(
        self,
        start_date: date,
        end_date: date,
        train_window_months: int = 24,
        test_window_months: int = 1,
        model_type: str = "lightgbm",
        bankroll: float = 10000.0,
        edge_margin: float = 0.15,
        min_odds: float = 2.5,
        max_odds: float = 20.0,
        top_rank_only: int | None = 3,
        kelly_fraction: float | None = None,
    ) -> tuple[list[BacktestPeriod], BacktestMetrics]:
        """
        Run walk-forward backtest.

        For each test window:
        1. Train on preceding train_window_months of data
        2. Generate predictions for the test window
        3. Identify value bets using Kelly criterion
        4. Settle using actual results and dividends
        5. Roll forward
        """
        logger.info(
            "Walk-forward backtest: %s to %s (train=%dm, test=%dm)",
            start_date, end_date, train_window_months, test_window_months,
        )

        self.bet_sizer.update_bankroll(bankroll)
        if kelly_fraction is not None:
            self.bet_sizer.kelly_fraction = kelly_fraction
        self._edge_margin = edge_margin
        self._min_odds = min_odds
        self._max_odds = max_odds
        self._top_rank_only = top_rank_only
        periods = []
        current_test_start = start_date

        while current_test_start < end_date:
            # Define windows
            train_start = current_test_start - timedelta(days=train_window_months * 30)
            train_end = current_test_start - timedelta(days=1)
            test_end = min(
                current_test_start + timedelta(days=test_window_months * 30) - timedelta(days=1),
                end_date,
            )

            logger.info(
                "Period: train %s to %s | test %s to %s",
                train_start, train_end, current_test_start, test_end,
            )

            period = self._run_period(
                train_start, train_end, current_test_start, test_end, model_type
            )
            periods.append(period)

            # Update bankroll
            self.bet_sizer.update_bankroll(
                self.bet_sizer.bankroll + period.total_pnl
            )

            # Roll forward
            current_test_start = test_end + timedelta(days=1)

        metrics = self._calculate_metrics(periods)
        return periods, metrics

    def _run_period(
        self,
        train_start: date,
        train_end: date,
        test_start: date,
        test_end: date,
        model_type: str,
    ) -> BacktestPeriod:
        """Run a single backtest period."""
        period = BacktestPeriod(
            train_start=train_start,
            train_end=train_end,
            test_start=test_start,
            test_end=test_end,
        )

        # Train model
        trainer = ModelTrainer(self.session)
        model, metadata = trainer.train_win_model(train_start, train_end, model_type)
        if model is None:
            logger.warning("Failed to train model for period %s-%s", train_start, train_end)
            return period

        period.model_auc = metadata.get("validation_auc", 0)

        # Get test races
        test_races = (
            self.session.query(Race)
            .filter(Race.race_date >= test_start, Race.race_date <= test_end)
            .order_by(Race.race_date, Race.race_no)
            .all()
        )
        period.num_races = len(test_races)

        feature_cols = FeatureEngineer.get_feature_columns()

        for race in test_races:
            # Distance exclusion filter (1400-1599m bled money, validated OOS)
            if race.distance and (
                settings.BET_EXCLUDE_DISTANCE_MIN
                <= race.distance
                <= settings.BET_EXCLUDE_DISTANCE_MAX
            ):
                continue

            # Build features
            df = self.feature_engine.build_features_for_race(race.id)
            if df.empty:
                continue

            X = df[feature_cols].copy().fillna(df[feature_cols].median())

            # Predict
            win_probs = model.predict_proba(X)[:, 1]
            win_probs_norm = win_probs / win_probs.sum()

            # Rank horses by model probability (for top_rank_only filter)
            ranked_idx = np.argsort(-win_probs_norm)
            top_rank_set = set(ranked_idx[: self._top_rank_only]) if self._top_rank_only else None

            # Find value bets
            for i, (_, row) in enumerate(df.iterrows()):
                horse_no = int(row["horse_no"])
                model_prob = win_probs_norm[i]
                win_odds = row.get("win_odds", 20.0)

                if win_odds <= 1:
                    continue

                # Filter: odds range (skip chalk and longshots)
                if win_odds < self._min_odds or win_odds > self._max_odds:
                    continue

                # Filter: only bet on top-N ranked horses
                if top_rank_set is not None and i not in top_rank_set:
                    continue

                implied_prob = 1.0 / win_odds

                # Multiplicative edge threshold (margin of safety)
                if model_prob > implied_prob * (1 + self._edge_margin):
                    bet_amount = self.bet_sizer.size_bet(model_prob, win_odds, "WIN")
                    if bet_amount > 0:
                        # Settle against actual results
                        pnl = self._settle_simulated_bet(
                            race.id, horse_no, bet_amount, "WIN"
                        )

                        bet_record = BetRecord(
                            race_date=race.race_date,
                            racecourse=race.racecourse,
                            race_no=race.race_no,
                            horse_no=horse_no,
                            bet_type="WIN",
                            bet_amount=bet_amount,
                            model_prob=model_prob,
                            odds=win_odds,
                            profit_loss=pnl,
                        )

                        # Get actual position
                        runner = (
                            self.session.query(Runner)
                            .filter_by(race_id=race.id, horse_no=horse_no)
                            .first()
                        )
                        if runner:
                            bet_record.actual_position = runner.finish_position

                        period.bets.append(bet_record)
                        period.total_bet += bet_amount
                        period.total_pnl += pnl

        period.num_bets = len(period.bets)
        logger.info(
            "Period %s-%s: %d bets, PnL=%.2f, Bankroll=%.2f",
            test_start, test_end, period.num_bets, period.total_pnl,
            self.bet_sizer.bankroll + period.total_pnl,
        )

        return period

    def _settle_simulated_bet(
        self, race_id: int, horse_no: int, bet_amount: float, bet_type: str
    ) -> float:
        """Settle a simulated bet using actual race data."""
        runner = (
            self.session.query(Runner)
            .filter_by(race_id=race_id, horse_no=horse_no, scratched=False)
            .first()
        )

        if not runner or not runner.finish_position:
            return -bet_amount  # Assume lost

        won = False
        if bet_type == "WIN" and runner.finish_position == 1:
            won = True
        elif bet_type == "PLA" and runner.finish_position <= 3:
            won = True

        if not won:
            return -bet_amount

        # Use actual dividend if available, matching the correct horse
        horse_str = str(horse_no)
        div = (
            self.session.query(Dividend)
            .filter_by(race_id=race_id, pool_type=bet_type, combination=horse_str)
            .first()
        )
        if not div:
            # Fallback: for WIN there's usually only one entry
            div = (
                self.session.query(Dividend)
                .filter_by(race_id=race_id, pool_type=bet_type)
                .first()
            )

        if div and div.payout > 0:
            return (bet_amount / 10) * div.payout - bet_amount
        elif runner.win_odds:
            # Fallback: estimate from odds
            deduction = 0.175 if bet_type in ("WIN", "PLA") else 0.20
            if bet_type == "WIN":
                return bet_amount * (runner.win_odds - 1) * (1 - deduction)
            else:
                return bet_amount * (runner.win_odds / 3 - 1) * (1 - deduction)

        return -bet_amount

    def _calculate_metrics(self, periods: list[BacktestPeriod]) -> BacktestMetrics:
        """Calculate aggregate metrics across all backtest periods."""
        all_bets = []
        for p in periods:
            all_bets.extend(p.bets)

        if not all_bets:
            return BacktestMetrics(
                total_periods=len(periods), total_races=sum(p.num_races for p in periods),
                total_bets=0, total_bet_amount=0, total_pnl=0, roi_pct=0,
                win_rate=0, profit_factor=0, max_drawdown=0, max_drawdown_pct=0,
                sharpe_ratio=0, avg_bet_pnl=0,
            )

        total_bet = sum(b.bet_amount for b in all_bets)
        total_pnl = sum(b.profit_loss for b in all_bets)
        winners = [b for b in all_bets if b.profit_loss > 0]
        losers = [b for b in all_bets if b.profit_loss < 0]

        gross_profit = sum(b.profit_loss for b in winners) if winners else 0
        gross_loss = abs(sum(b.profit_loss for b in losers)) if losers else 1

        # Max drawdown
        cumulative = np.cumsum([b.profit_loss for b in all_bets])
        peak = np.maximum.accumulate(cumulative)
        drawdowns = peak - cumulative
        max_dd = float(drawdowns.max()) if len(drawdowns) > 0 else 0

        # Sharpe ratio (daily returns)
        pnl_series = pd.Series([b.profit_loss for b in all_bets])
        sharpe = 0.0
        if len(pnl_series) > 1 and pnl_series.std() > 0:
            sharpe = float(pnl_series.mean() / pnl_series.std() * np.sqrt(252))

        # P&L by bet type
        pnl_by_type = {}
        for b in all_bets:
            if b.bet_type not in pnl_by_type:
                pnl_by_type[b.bet_type] = {"count": 0, "total_pnl": 0, "total_bet": 0}
            pnl_by_type[b.bet_type]["count"] += 1
            pnl_by_type[b.bet_type]["total_pnl"] += b.profit_loss
            pnl_by_type[b.bet_type]["total_bet"] += b.bet_amount

        return BacktestMetrics(
            total_periods=len(periods),
            total_races=sum(p.num_races for p in periods),
            total_bets=len(all_bets),
            total_bet_amount=round(total_bet, 2),
            total_pnl=round(total_pnl, 2),
            roi_pct=round(total_pnl / total_bet * 100, 2) if total_bet > 0 else 0,
            win_rate=round(len(winners) / len(all_bets) * 100, 2),
            profit_factor=round(gross_profit / gross_loss, 2) if gross_loss > 0 else 0,
            max_drawdown=round(max_dd, 2),
            max_drawdown_pct=round(max_dd / self.bet_sizer.bankroll * 100, 2) if self.bet_sizer.bankroll > 0 else 0,
            sharpe_ratio=round(sharpe, 2),
            avg_bet_pnl=round(total_pnl / len(all_bets), 2),
            pnl_by_bet_type=pnl_by_type,
        )

    def save_report(
        self, periods: list[BacktestPeriod], metrics: BacktestMetrics, filename: str | None = None
    ) -> Path:
        """Save backtest results to JSON."""
        output_dir = settings.OUTPUT_DIR / "backtests"
        output_dir.mkdir(parents=True, exist_ok=True)

        if filename is None:
            filename = f"backtest_{periods[0].test_start}_{periods[-1].test_end}.json"

        report = {
            "metrics": {
                "total_periods": metrics.total_periods,
                "total_races": metrics.total_races,
                "total_bets": metrics.total_bets,
                "total_bet_amount": metrics.total_bet_amount,
                "total_pnl": metrics.total_pnl,
                "roi_pct": metrics.roi_pct,
                "win_rate": metrics.win_rate,
                "profit_factor": metrics.profit_factor,
                "max_drawdown": metrics.max_drawdown,
                "max_drawdown_pct": metrics.max_drawdown_pct,
                "sharpe_ratio": metrics.sharpe_ratio,
                "avg_bet_pnl": metrics.avg_bet_pnl,
                "pnl_by_bet_type": metrics.pnl_by_bet_type,
            },
            "periods": [
                {
                    "train": f"{p.train_start} to {p.train_end}",
                    "test": f"{p.test_start} to {p.test_end}",
                    "model_auc": p.model_auc,
                    "num_races": p.num_races,
                    "num_bets": p.num_bets,
                    "total_bet": p.total_bet,
                    "total_pnl": p.total_pnl,
                    "bets": [
                        {
                            "date": str(b.race_date),
                            "course": b.racecourse,
                            "race": b.race_no,
                            "horse": b.horse_no,
                            "type": b.bet_type,
                            "amount": b.bet_amount,
                            "prob": round(b.model_prob, 4),
                            "odds": b.odds,
                            "position": b.actual_position,
                            "pnl": round(b.profit_loss, 2),
                        }
                        for b in p.bets
                    ],
                }
                for p in periods
            ],
        }

        report_path = output_dir / filename
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)

        logger.info("Backtest report saved to %s", report_path)
        return report_path

    def print_summary(self, metrics: BacktestMetrics):
        """Print a formatted backtest summary."""
        print("\n" + "=" * 60)
        print("BACKTEST SUMMARY")
        print("=" * 60)
        print(f"Periods tested:      {metrics.total_periods}")
        print(f"Total races:         {metrics.total_races}")
        print(f"Total bets:          {metrics.total_bets}")
        print(f"Total bet amount:    ${metrics.total_bet_amount:,.2f}")
        print(f"Total P&L:           ${metrics.total_pnl:,.2f}")
        print(f"ROI:                 {metrics.roi_pct:.2f}%")
        print(f"Win rate:            {metrics.win_rate:.1f}%")
        print(f"Profit factor:       {metrics.profit_factor:.2f}")
        print(f"Max drawdown:        ${metrics.max_drawdown:,.2f} ({metrics.max_drawdown_pct:.1f}%)")
        print(f"Sharpe ratio:        {metrics.sharpe_ratio:.2f}")
        print(f"Avg P&L per bet:     ${metrics.avg_bet_pnl:.2f}")

        if metrics.pnl_by_bet_type:
            print("\nP&L by bet type:")
            for bt, data in metrics.pnl_by_bet_type.items():
                roi = data["total_pnl"] / data["total_bet"] * 100 if data["total_bet"] > 0 else 0
                print(f"  {bt}: {data['count']} bets, ${data['total_pnl']:.2f} ({roi:.1f}% ROI)")

        profitable = metrics.total_pnl > 0
        print(f"\nPROFITABLE: {'YES' if profitable else 'NO'}")
        print("=" * 60 + "\n")

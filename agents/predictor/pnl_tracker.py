"""
P&L Tracker

Records betting activity and tracks profit/loss over time.
"""

import logging
from datetime import date, datetime, timedelta
from dataclasses import dataclass

from sqlalchemy import func
from sqlalchemy.orm import Session

from db.models import BetResult, Dividend, Prediction, Race

logger = logging.getLogger(__name__)


@dataclass
class DailyPnL:
    date: date
    num_bets: int
    num_wins: int
    gross_profit: float
    gross_loss: float
    net_pnl: float
    roi_pct: float  # net_pnl / total_bet_amount * 100


@dataclass
class WeeklyPnL:
    week_start: date
    week_end: date
    num_meetings: int
    num_bets: int
    num_wins: int
    gross_profit: float
    gross_loss: float
    net_pnl: float
    roi_pct: float
    best_bet: str  # Description of best bet
    worst_bet: str  # Description of worst bet
    season_total_pnl: float
    current_bankroll: float


class PnLTracker:
    """Tracks betting results and generates P&L reports."""

    def __init__(self, session: Session, initial_bankroll: float = 10000.0):
        self.session = session
        self.initial_bankroll = initial_bankroll

    def record_bet(self, prediction_id: int) -> BetResult | None:
        """Create an unsettled BetResult for a placed bet."""
        prediction = self.session.get(Prediction, prediction_id)
        if not prediction or not prediction.bet_amount:
            return None

        existing = self.session.query(BetResult).filter_by(prediction_id=prediction_id).first()
        if existing:
            return existing

        bet_result = BetResult(
            prediction_id=prediction_id,
            settled=False,
        )
        self.session.add(bet_result)
        self.session.commit()
        return bet_result

    def settle_bet(self, prediction_id: int) -> float | None:
        """
        Settle a bet using actual race results.
        Returns the profit/loss, or None if unable to settle.
        """
        prediction = self.session.get(Prediction, prediction_id)
        if not prediction:
            return None

        bet_result = self.session.query(BetResult).filter_by(prediction_id=prediction_id).first()
        if not bet_result:
            bet_result = BetResult(prediction_id=prediction_id, settled=False)
            self.session.add(bet_result)

        if bet_result.settled:
            return bet_result.profit_loss

        bet_amount = prediction.bet_amount or 0
        if bet_amount <= 0:
            bet_result.profit_loss = 0
            bet_result.settled = True
            bet_result.settled_at = datetime.utcnow()
            self.session.commit()
            return 0

        race = self.session.get(Race, prediction.race_id)
        if not race:
            return None

        # Find the actual dividend for this bet
        pnl = -bet_amount  # Default: lost the bet

        if prediction.bet_type in ("WIN", "PLA"):
            # Check actual finish position
            from db.models import Runner
            runner = (
                self.session.query(Runner)
                .filter_by(race_id=prediction.race_id, horse_no=prediction.horse_no)
                .first()
            )
            if runner:
                bet_result.actual_position = runner.finish_position

                won = False
                if prediction.bet_type == "WIN" and runner.finish_position == 1:
                    won = True
                elif prediction.bet_type == "PLA" and runner.finish_position and runner.finish_position <= 3:
                    won = True

                if won:
                    # Find dividend
                    div = (
                        self.session.query(Dividend)
                        .filter_by(
                            race_id=prediction.race_id,
                            pool_type=prediction.bet_type,
                        )
                        .first()
                    )
                    if div:
                        bet_result.actual_dividend = div.payout
                        # Dividend is per $10 unit
                        pnl = (bet_amount / 10) * div.payout - bet_amount
                    else:
                        # Use odds as fallback
                        if prediction.bet_type == "WIN" and runner.win_odds:
                            pnl = bet_amount * (runner.win_odds - 1) * 0.825  # After deduction
                        elif prediction.bet_type == "PLA" and runner.win_odds:
                            pnl = bet_amount * (runner.win_odds / 3 - 1) * 0.825

        elif prediction.bet_type in ("QIN", "QPL", "FCT", "TCE", "TRI"):
            # Exotic bets: check combination
            combo = prediction.bet_combination or ""
            div = (
                self.session.query(Dividend)
                .filter_by(
                    race_id=prediction.race_id,
                    pool_type=prediction.bet_type,
                    combination=combo,
                )
                .first()
            )
            if div:
                bet_result.actual_dividend = div.payout
                pnl = (bet_amount / 10) * div.payout - bet_amount

        bet_result.profit_loss = round(pnl, 2)
        bet_result.settled = True
        bet_result.settled_at = datetime.utcnow()
        self.session.commit()

        logger.info(
            "Settled bet %d: %s #%d = %.2f HKD",
            prediction_id, prediction.bet_type, prediction.horse_no, pnl,
        )
        return pnl

    def settle_race(self, race_id: int) -> list[float]:
        """Settle all bets for a race. Returns list of P&L values."""
        predictions = (
            self.session.query(Prediction)
            .filter_by(race_id=race_id)
            .filter(Prediction.bet_amount.isnot(None), Prediction.bet_amount > 0)
            .all()
        )

        results = []
        for pred in predictions:
            pnl = self.settle_bet(pred.id)
            if pnl is not None:
                results.append(pnl)

        return results

    def get_daily_pnl(self, target_date: date) -> DailyPnL:
        """Get P&L for a specific day."""
        results = (
            self.session.query(BetResult)
            .join(Prediction)
            .join(Race)
            .filter(Race.race_date == target_date, BetResult.settled == True)
            .all()
        )

        if not results:
            return DailyPnL(date=target_date, num_bets=0, num_wins=0,
                            gross_profit=0, gross_loss=0, net_pnl=0, roi_pct=0)

        total_bet = sum(r.prediction.bet_amount or 0 for r in results)
        profits = [r.profit_loss for r in results if r.profit_loss > 0]
        losses = [r.profit_loss for r in results if r.profit_loss < 0]

        net_pnl = sum(r.profit_loss for r in results)
        roi = (net_pnl / total_bet * 100) if total_bet > 0 else 0

        return DailyPnL(
            date=target_date,
            num_bets=len(results),
            num_wins=len(profits),
            gross_profit=sum(profits),
            gross_loss=sum(losses),
            net_pnl=round(net_pnl, 2),
            roi_pct=round(roi, 2),
        )

    def get_weekly_pnl(self, week_start: date | None = None) -> WeeklyPnL:
        """Get P&L for a week (Monday to Sunday)."""
        if week_start is None:
            # Default to last Monday
            today = date.today()
            week_start = today - timedelta(days=today.weekday())

        week_end = week_start + timedelta(days=6)

        results = (
            self.session.query(BetResult)
            .join(Prediction)
            .join(Race)
            .filter(
                Race.race_date >= week_start,
                Race.race_date <= week_end,
                BetResult.settled == True,
            )
            .all()
        )

        # Count unique meeting dates
        meeting_dates = set()
        for r in results:
            meeting_dates.add(r.prediction.race.race_date)

        total_bet = sum(r.prediction.bet_amount or 0 for r in results)
        profits = [r for r in results if r.profit_loss > 0]
        losses = [r for r in results if r.profit_loss < 0]

        net_pnl = sum(r.profit_loss for r in results)
        roi = (net_pnl / total_bet * 100) if total_bet > 0 else 0

        # Best and worst bets
        best_desc = "N/A"
        worst_desc = "N/A"
        if profits:
            best = max(profits, key=lambda r: r.profit_loss)
            p = best.prediction
            best_desc = f"R{p.race.race_no} {p.race.racecourse} #{p.horse_no} +${best.profit_loss:.0f}"
        if losses:
            worst = min(losses, key=lambda r: r.profit_loss)
            p = worst.prediction
            worst_desc = f"R{p.race.race_no} {p.race.racecourse} #{p.horse_no} -${abs(worst.profit_loss):.0f}"

        # Season totals
        season_pnl = self._get_season_pnl()

        return WeeklyPnL(
            week_start=week_start,
            week_end=week_end,
            num_meetings=len(meeting_dates),
            num_bets=len(results),
            num_wins=len(profits),
            gross_profit=round(sum(r.profit_loss for r in profits), 2),
            gross_loss=round(sum(r.profit_loss for r in losses), 2),
            net_pnl=round(net_pnl, 2),
            roi_pct=round(roi, 2),
            best_bet=best_desc,
            worst_bet=worst_desc,
            season_total_pnl=round(season_pnl, 2),
            current_bankroll=round(self.initial_bankroll + season_pnl, 2),
        )

    def _get_season_pnl(self) -> float:
        """Get total P&L for the current season."""
        total = (
            self.session.query(func.sum(BetResult.profit_loss))
            .filter(BetResult.settled == True)
            .scalar()
        )
        return total or 0.0

    def get_bankroll(self) -> float:
        """Get current bankroll (initial + all settled P&L)."""
        return self.initial_bankroll + self._get_season_pnl()

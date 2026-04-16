"""
Bet Sizer

Implements fractional Kelly criterion for optimal bet sizing.
"""

import logging
from dataclasses import dataclass

from config import settings

logger = logging.getLogger(__name__)

# Pool deduction rates by bet type
POOL_DEDUCTIONS = {
    "WIN": settings.WIN_PLACE_DEDUCTION,
    "PLA": settings.WIN_PLACE_DEDUCTION,
    "QIN": settings.QUINELLA_DEDUCTION,
    "QPL": settings.QUINELLA_DEDUCTION,
    "FCT": settings.FORECAST_DEDUCTION,
    "TCE": settings.TIERCE_TRIO_DEDUCTION,
    "TRI": settings.TIERCE_TRIO_DEDUCTION,
    "F4": settings.FIRST4_QUARTET_DEDUCTION,
    "QTT": settings.FIRST4_QUARTET_DEDUCTION,
}


class BetSizer:
    """Calculates optimal bet sizes using fractional Kelly criterion."""

    def __init__(
        self,
        bankroll: float | None = None,
        kelly_fraction: float | None = None,
        max_bet_pct: float | None = None,
        min_bet_amount: float | None = None,
    ):
        self.bankroll = bankroll or settings.INITIAL_BANKROLL
        self.kelly_fraction = kelly_fraction or settings.KELLY_FRACTION
        self.max_bet_pct = max_bet_pct or settings.MAX_BET_PCT
        self.min_bet_amount = min_bet_amount or settings.MIN_BET_AMOUNT

    def update_bankroll(self, new_bankroll: float):
        """Update the current bankroll."""
        self.bankroll = new_bankroll

    def calculate_kelly(
        self, model_prob: float, odds: float, bet_type: str = "WIN"
    ) -> float:
        """
        Calculate Kelly criterion fraction.

        Kelly formula: f* = (b*p - q) / b
        Where:
          b = net odds (payout per $1 bet, minus 1), adjusted for pool deduction
          p = estimated probability of winning
          q = 1 - p

        Returns the optimal fraction of bankroll to bet (can be negative = don't bet).
        """
        if model_prob <= 0 or model_prob >= 1 or odds <= 1:
            return 0.0

        # Adjust odds for pool deduction
        deduction = POOL_DEDUCTIONS.get(bet_type, 0.175)
        net_odds = (odds - 1) * (1 - deduction)

        if net_odds <= 0:
            return 0.0

        p = model_prob
        q = 1 - p

        kelly = (net_odds * p - q) / net_odds

        return max(0.0, kelly)

    def size_bet(
        self, model_prob: float, odds: float, bet_type: str = "WIN"
    ) -> float:
        """
        Calculate recommended bet size in dollars.

        Applies fractional Kelly (conservative) with caps.
        """
        full_kelly = self.calculate_kelly(model_prob, odds, bet_type)

        if full_kelly <= 0:
            return 0.0

        # Apply fractional Kelly
        fraction = full_kelly * self.kelly_fraction

        # Convert to dollar amount
        bet_amount = self.bankroll * fraction

        # Apply caps
        max_bet = self.bankroll * self.max_bet_pct
        bet_amount = min(bet_amount, max_bet)

        # Check minimum
        if bet_amount < self.min_bet_amount:
            return 0.0

        # Round to nearest $10 (HKJC minimum unit)
        bet_amount = round(bet_amount / 10) * 10

        return bet_amount

    def size_value_bet(self, value_bet) -> float:
        """Size a ValueBet object and set its recommended_bet field."""
        amount = self.size_bet(
            value_bet.model_prob,
            value_bet.odds,
            value_bet.bet_type,
        )
        value_bet.recommended_bet = amount
        return amount

    def calculate_expected_value(
        self, model_prob: float, odds: float, bet_amount: float, bet_type: str = "WIN"
    ) -> float:
        """
        Calculate expected value of a bet.

        EV = (prob * net_payout) - ((1-prob) * bet_amount)
        """
        deduction = POOL_DEDUCTIONS.get(bet_type, 0.175)
        net_payout = bet_amount * (odds - 1) * (1 - deduction)
        ev = (model_prob * net_payout) - ((1 - model_prob) * bet_amount)
        return ev

    def get_portfolio_summary(self, value_bets: list) -> dict:
        """Summarize total exposure and expected value for a set of bets."""
        total_bet = sum(vb.recommended_bet for vb in value_bets)
        total_ev = sum(
            self.calculate_expected_value(
                vb.model_prob, vb.odds, vb.recommended_bet, vb.bet_type
            )
            for vb in value_bets
            if vb.recommended_bet > 0
        )

        return {
            "num_bets": sum(1 for vb in value_bets if vb.recommended_bet > 0),
            "total_bet_amount": total_bet,
            "total_expected_value": total_ev,
            "bankroll_exposure_pct": (total_bet / self.bankroll * 100) if self.bankroll > 0 else 0,
            "current_bankroll": self.bankroll,
        }

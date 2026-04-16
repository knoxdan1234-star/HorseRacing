"""
Model Predictor

Generates predictions and identifies value bets using trained models.
"""

import logging
from dataclasses import dataclass
from itertools import combinations

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from agents.predictor.feature_engine import FeatureEngineer
from agents.predictor.model_trainer import ModelTrainer
from config import settings
from db.models import Prediction, Race, Runner

logger = logging.getLogger(__name__)


@dataclass
class ValueBet:
    race_id: int
    horse_no: int
    horse_name: str
    bet_type: str  # WIN, PLA, QIN, FCT, TCE
    bet_combination: str  # e.g. "3" for WIN, "3,7" for QIN
    model_prob: float
    market_prob: float  # Implied from odds
    edge: float  # model_prob - market_prob
    odds: float
    recommended_bet: float  # From Kelly criterion


class Predictor:
    """Generates predictions and detects value bets."""

    MARGIN_OF_SAFETY = 0.05  # 5% edge required beyond breakeven

    def __init__(self, session: Session):
        self.session = session
        self.feature_engine = FeatureEngineer(session)
        self.trainer = ModelTrainer(session)
        self._win_model = None
        self._place_model = None
        self._model_version = None

    def load_models(self, win_version: str | None = None, place_version: str | None = None):
        """Load trained models."""
        win_ver = win_version or self.trainer.get_latest_model_version("win")
        if win_ver:
            self._win_model = self.trainer.load_model(win_ver)
            self._model_version = win_ver
            logger.info("Loaded win model: %s", win_ver)

        place_ver = place_version or self.trainer.get_latest_model_version("place")
        if place_ver:
            self._place_model = self.trainer.load_model(place_ver)
            logger.info("Loaded place model: %s", place_ver)

    def predict_race(self, race_id: int) -> pd.DataFrame:
        """
        Generate win/place probabilities for all runners in a race.
        Returns DataFrame with columns: horse_no, win_prob, place_prob, win_rank, place_rank
        """
        if not self._win_model:
            logger.error("No win model loaded. Call load_models() first.")
            return pd.DataFrame()

        feature_cols = FeatureEngineer.get_feature_columns()
        df = self.feature_engine.build_features_for_race(race_id)

        if df.empty:
            return pd.DataFrame()

        X = df[feature_cols].copy().fillna(df[feature_cols].median())

        # Win probabilities
        win_probs = self._win_model.predict_proba(X)[:, 1]

        # Normalize probabilities to sum to ~1 within the race
        win_probs_norm = win_probs / win_probs.sum()

        result = df[["horse_no", "runner_id", "win_odds"]].copy()
        result["win_prob"] = win_probs_norm
        result["win_rank"] = result["win_prob"].rank(ascending=False, method="min").astype(int)

        # Place probabilities
        if self._place_model:
            place_probs = self._place_model.predict_proba(X)[:, 1]
            place_probs_norm = place_probs / (place_probs.sum() / 3)  # ~3 horses place
            result["place_prob"] = place_probs_norm
        else:
            result["place_prob"] = win_probs_norm * 3  # Simple approximation

        result["place_rank"] = result["place_prob"].rank(ascending=False, method="min").astype(int)
        result["implied_prob"] = 1.0 / result["win_odds"].clip(lower=1.01)

        return result.sort_values("win_rank")

    def find_value_bets(self, race_id: int) -> list[ValueBet]:
        """Identify value bets where model probability exceeds market probability."""
        predictions = self.predict_race(race_id)
        if predictions.empty:
            return []

        race = self.session.get(Race, race_id)
        value_bets = []

        for _, row in predictions.iterrows():
            runner = (
                self.session.query(Runner)
                .filter_by(race_id=race_id, horse_no=int(row["horse_no"]))
                .first()
            )
            horse_name = runner.horse_name if runner else f"#{int(row['horse_no'])}"

            # --- WIN value bets ---
            win_edge = row["win_prob"] - row["implied_prob"]
            if win_edge > self.MARGIN_OF_SAFETY:
                value_bets.append(ValueBet(
                    race_id=race_id,
                    horse_no=int(row["horse_no"]),
                    horse_name=horse_name,
                    bet_type="WIN",
                    bet_combination=str(int(row["horse_no"])),
                    model_prob=row["win_prob"],
                    market_prob=row["implied_prob"],
                    edge=win_edge,
                    odds=row["win_odds"],
                    recommended_bet=0,  # Filled by BetSizer
                ))

            # --- PLACE value bets ---
            if row["win_odds"] > 0:
                # Approximate place odds as win_odds / 3
                place_odds = max(row["win_odds"] / 3, 1.1)
                place_implied = 1.0 / place_odds
                place_edge = row["place_prob"] - place_implied
                if place_edge > self.MARGIN_OF_SAFETY:
                    value_bets.append(ValueBet(
                        race_id=race_id,
                        horse_no=int(row["horse_no"]),
                        horse_name=horse_name,
                        bet_type="PLA",
                        bet_combination=str(int(row["horse_no"])),
                        model_prob=row["place_prob"],
                        market_prob=place_implied,
                        edge=place_edge,
                        odds=place_odds,
                        recommended_bet=0,
                    ))

        # --- QUINELLA value bets (top pairs) ---
        top_runners = predictions.nsmallest(5, "win_rank")
        for (_, r1), (_, r2) in combinations(top_runners.iterrows(), 2):
            # Quinella prob ≈ P(A wins)*P(B 2nd|A wins) + P(B wins)*P(A 2nd|B wins)
            # Simplified: P(both in top-2) ≈ P(A)*P(B) * field_adjustment
            field = len(predictions)
            qin_prob = (
                r1["win_prob"] * r2["place_prob"] +
                r2["win_prob"] * r1["place_prob"]
            ) * 0.5  # Conservative

            # We don't have quinella odds directly; skip if we can't estimate
            # In live mode, quinella odds would come from the odds scraper

        return value_bets

    def save_predictions(self, race_id: int, predictions: pd.DataFrame, value_bets: list[ValueBet]):
        """Save predictions to the database."""
        for _, row in predictions.iterrows():
            # Check if value bet
            vb = next(
                (b for b in value_bets if b.horse_no == int(row["horse_no"]) and b.bet_type == "WIN"),
                None,
            )

            pred = Prediction(
                race_id=race_id,
                runner_id=int(row["runner_id"]) if pd.notna(row.get("runner_id")) else None,
                horse_no=int(row["horse_no"]),
                predicted_win_prob=row["win_prob"],
                predicted_place_prob=row.get("place_prob"),
                predicted_rank=int(row["win_rank"]),
                is_value_bet=vb is not None,
                bet_type=vb.bet_type if vb else None,
                bet_amount=vb.recommended_bet if vb else None,
                model_version=self._model_version,
            )
            self.session.add(pred)

        self.session.commit()
        logger.info("Saved %d predictions for race %d", len(predictions), race_id)

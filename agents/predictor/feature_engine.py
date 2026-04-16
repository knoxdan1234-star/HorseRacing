"""
Feature Engineering Pipeline

Transforms raw race/horse/jockey/trainer data into ML features.
Each row represents one runner in one race.
"""

import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from db.models import Horse, Jockey, Race, Runner, Trainer

logger = logging.getLogger(__name__)

# Going encoded as ordinal (firmer = lower)
GOING_ENCODING = {
    "Firm": 1,
    "Good to Firm": 2,
    "Good": 3,
    "Good to Yielding": 4,
    "Yielding": 5,
    "Yielding to Soft": 6,
    "Soft": 7,
    "Heavy": 8,
}

# Class encoded as ordinal (higher = better quality)
CLASS_ENCODING = {
    "Class 5": 1,
    "Class 4": 2,
    "Class 3": 3,
    "Class 2": 4,
    "Class 1": 5,
    "Group 3": 6,
    "Group 2": 7,
    "Group 1": 8,
    "Griffin": 0,
}

# Track type
TRACK_ENCODING = {"Turf": 0, "All Weather": 1}


class FeatureEngineer:
    """Builds feature matrices from database records."""

    def __init__(self, session: Session):
        self.session = session

    def build_features_for_race(self, race_id: int) -> pd.DataFrame:
        """Build feature DataFrame for all runners in a given race. One row per runner."""
        race = self.session.get(Race, race_id)
        if not race:
            logger.warning("Race not found: %d", race_id)
            return pd.DataFrame()

        runners = (
            self.session.query(Runner)
            .filter_by(race_id=race_id, scratched=False)
            .all()
        )

        if not runners:
            return pd.DataFrame()

        rows = []
        for runner in runners:
            features = self._build_runner_features(race, runner)
            features["race_id"] = race_id
            features["runner_id"] = runner.id
            features["horse_no"] = runner.horse_no
            # Target (only available for historical data)
            features["finish_position"] = runner.finish_position
            features["is_winner"] = 1 if runner.finish_position == 1 else 0
            features["is_placed"] = 1 if runner.finish_position and runner.finish_position <= 3 else 0
            rows.append(features)

        return pd.DataFrame(rows)

    def build_features_for_date_range(
        self, start_date: date, end_date: date
    ) -> pd.DataFrame:
        """Build features for all races in a date range. Used for training/backtesting."""
        races = (
            self.session.query(Race)
            .filter(Race.race_date >= start_date, Race.race_date <= end_date)
            .order_by(Race.race_date, Race.race_no)
            .all()
        )

        if not races:
            logger.warning("No races found between %s and %s", start_date, end_date)
            return pd.DataFrame()

        all_frames = []
        for race in races:
            df = self.build_features_for_race(race.id)
            if not df.empty:
                all_frames.append(df)

        if not all_frames:
            return pd.DataFrame()

        result = pd.concat(all_frames, ignore_index=True)
        logger.info(
            "Built features: %d runners across %d races (%s to %s)",
            len(result), len(all_frames), start_date, end_date,
        )
        return result

    def _build_runner_features(self, race: Race, runner: Runner) -> dict:
        """Build all features for a single runner."""
        features = {}

        # --- Race context features ---
        features["distance"] = race.distance or 1200
        features["track_type"] = TRACK_ENCODING.get(race.track_type, 0)
        features["going_encoded"] = GOING_ENCODING.get(race.going, 3)
        features["class_encoded"] = CLASS_ENCODING.get(race.race_class, 3)
        features["field_size"] = race.field_size or 14
        features["racecourse_st"] = 1 if race.racecourse == "ST" else 0

        # --- Horse features ---
        features["draw"] = runner.draw or 7
        features["actual_weight"] = runner.actual_weight or 126
        features["rating"] = runner.rating or 60
        features["rating_change"] = runner.rating_change or 0
        features["season_stakes"] = runner.season_stakes or 0.0
        features["win_odds"] = runner.win_odds or 20.0
        features["implied_probability"] = 1.0 / features["win_odds"] if features["win_odds"] > 0 else 0.05

        # Equipment flags
        gear = (runner.gear or "").upper()
        features["has_blinkers"] = 1 if "B" in gear else 0
        features["has_tongue_tie"] = 1 if "TT" in gear else 0
        features["has_visor"] = 1 if "V" in gear else 0

        # Last 6 runs (parse "1/2/3/5/4/1" format)
        last_6 = self._parse_last_6(runner.last_6_runs)
        features["avg_finish_last6"] = np.mean(last_6) if last_6 else 7.0
        features["best_finish_last6"] = min(last_6) if last_6 else 14
        features["worst_finish_last6"] = max(last_6) if last_6 else 14
        features["num_wins_last6"] = sum(1 for p in last_6 if p == 1)
        features["num_places_last6"] = sum(1 for p in last_6 if p <= 3)

        # --- Historical performance features (computed from DB) ---
        if runner.horse_id:
            horse_feats = self._compute_horse_history(runner.horse_id, race.race_date, race)
            features.update(horse_feats)
        else:
            features.update(self._default_horse_history())

        # --- Jockey features ---
        if runner.jockey_id:
            jockey_feats = self._compute_jockey_stats(runner.jockey_id, race.race_date)
            features.update(jockey_feats)
        else:
            features.update(self._default_jockey_stats())

        # --- Trainer features ---
        if runner.trainer_id:
            trainer_feats = self._compute_trainer_stats(runner.trainer_id, race.race_date)
            features.update(trainer_feats)
        else:
            features.update(self._default_trainer_stats())

        # --- Jockey-Trainer combo ---
        if runner.jockey_id and runner.trainer_id:
            features["jt_combo_win_rate"] = self._compute_jt_combo_win_rate(
                runner.jockey_id, runner.trainer_id, race.race_date
            )
        else:
            features["jt_combo_win_rate"] = 0.08

        # --- Draw bias ---
        features["draw_bias_score"] = self._compute_draw_bias(
            runner.draw or 7, race.racecourse, race.distance or 1200
        )

        # --- Odds rank within race ---
        features["odds_rank"] = self._compute_odds_rank(race.id, runner.horse_no)

        return features

    def _parse_last_6(self, last_6_str: str | None) -> list[int]:
        """Parse '1/2/3/5/4/1' or '1-2-3-5-4-1' format."""
        if not last_6_str:
            return []
        parts = last_6_str.replace("-", "/").split("/")
        result = []
        for p in parts:
            p = p.strip()
            if p.isdigit():
                result.append(int(p))
        return result[:6]

    def _compute_horse_history(self, horse_id: int, before_date: date, current_race: Race) -> dict:
        """Compute historical features for a horse from past races."""
        # Single query joining Runner + Race to avoid N+1
        past = (
            self.session.query(
                Runner.finish_position,
                Race.race_date,
                Race.distance,
                Race.racecourse,
                Race.race_class,
            )
            .join(Race, Runner.race_id == Race.id)
            .filter(
                Runner.horse_id == horse_id,
                Race.race_date < before_date,
                Runner.scratched == False,
                Runner.finish_position.isnot(None),
                Runner.finish_position > 0,
            )
            .order_by(Race.race_date.desc())
            .limit(30)
            .all()
        )

        if not past:
            return self._default_horse_history()

        total = len(past)
        wins = sum(1 for r in past if r.finish_position == 1)
        places = sum(1 for r in past if r.finish_position <= 3)

        # Days since last run
        days_since = (before_date - past[0].race_date).days

        # Distance affinity
        current_dist = current_race.distance or 1200
        dist_runs = [r for r in past if abs((r.distance or 1200) - current_dist) <= 200]
        dist_wins = sum(1 for r in dist_runs if r.finish_position == 1)
        dist_win_rate = dist_wins / len(dist_runs) if dist_runs else 0.0

        # Track affinity
        current_course = current_race.racecourse
        track_runs = [r for r in past if r.racecourse == current_course]
        track_wins = sum(1 for r in track_runs if r.finish_position == 1)
        track_win_rate = track_wins / len(track_runs) if track_runs else 0.0

        # Class change (compare current class to last race class)
        last_class = CLASS_ENCODING.get(past[0].race_class, 3)
        curr_class = CLASS_ENCODING.get(current_race.race_class, 3)
        class_change = curr_class - last_class  # Positive = moving up

        return {
            "horse_career_starts": total,
            "horse_win_rate": wins / total,
            "horse_place_rate": places / total,
            "days_since_last_run": days_since,
            "distance_win_rate": dist_win_rate,
            "track_win_rate": track_win_rate,
            "class_change": class_change,
        }

    def _default_horse_history(self) -> dict:
        return {
            "horse_career_starts": 0,
            "horse_win_rate": 0.07,
            "horse_place_rate": 0.21,
            "days_since_last_run": 30,
            "distance_win_rate": 0.07,
            "track_win_rate": 0.07,
            "class_change": 0,
        }

    def _compute_jockey_stats(self, jockey_id: int, before_date: date) -> dict:
        """Compute jockey statistics from past 6 months."""
        cutoff = before_date - timedelta(days=180)
        recent_cutoff = before_date - timedelta(days=30)

        runs = (
            self.session.query(Runner.finish_position, Race.race_date)
            .join(Race, Runner.race_id == Race.id)
            .filter(
                Runner.jockey_id == jockey_id,
                Race.race_date >= cutoff,
                Race.race_date < before_date,
                Runner.scratched == False,
                Runner.finish_position.isnot(None),
                Runner.finish_position > 0,
            )
            .all()
        )

        if not runs:
            return self._default_jockey_stats()

        total = len(runs)
        wins = sum(1 for r in runs if r.finish_position == 1)

        # Recent form (last 30 days) — no subquery needed, we have race_date
        recent_runs = [r for r in runs if r.race_date >= recent_cutoff]
        recent_total = len(recent_runs)
        recent_wins = sum(1 for r in recent_runs if r.finish_position == 1)

        return {
            "jockey_season_win_rate": wins / total,
            "jockey_season_rides": total,
            "jockey_recent_win_rate": recent_wins / recent_total if recent_total else 0.08,
        }

    def _default_jockey_stats(self) -> dict:
        return {
            "jockey_season_win_rate": 0.08,
            "jockey_season_rides": 0,
            "jockey_recent_win_rate": 0.08,
        }

    def _compute_trainer_stats(self, trainer_id: int, before_date: date) -> dict:
        """Compute trainer statistics from past 6 months."""
        cutoff = before_date - timedelta(days=180)

        season_runs = (
            self.session.query(Runner)
            .join(Race)
            .filter(
                Runner.trainer_id == trainer_id,
                Race.race_date >= cutoff,
                Race.race_date < before_date,
                Runner.scratched == False,
                Runner.finish_position.isnot(None),
                Runner.finish_position > 0,
            )
            .all()
        )

        if not season_runs:
            return self._default_trainer_stats()

        total = len(season_runs)
        wins = sum(1 for r in season_runs if r.finish_position == 1)

        return {
            "trainer_season_win_rate": wins / total,
            "trainer_season_runners": total,
        }

    def _default_trainer_stats(self) -> dict:
        return {
            "trainer_season_win_rate": 0.08,
            "trainer_season_runners": 0,
        }

    def _compute_jt_combo_win_rate(
        self, jockey_id: int, trainer_id: int, before_date: date
    ) -> float:
        """Win rate for a specific jockey-trainer combination."""
        cutoff = before_date - timedelta(days=365)
        combo_runs = (
            self.session.query(Runner)
            .join(Race)
            .filter(
                Runner.jockey_id == jockey_id,
                Runner.trainer_id == trainer_id,
                Race.race_date >= cutoff,
                Race.race_date < before_date,
                Runner.scratched == False,
                Runner.finish_position.isnot(None),
                Runner.finish_position > 0,
            )
            .all()
        )

        if len(combo_runs) < 3:
            return 0.08  # Not enough data, use baseline

        wins = sum(1 for r in combo_runs if r.finish_position == 1)
        return wins / len(combo_runs)

    def _compute_draw_bias(self, draw: int, racecourse: str, distance: int) -> float:
        """
        Compute draw bias score from historical data.
        Returns a score 0-1 where higher = more advantaged.
        """
        # Query historical win rate for this draw at this track/distance
        runs = (
            self.session.query(Runner)
            .join(Race)
            .filter(
                Runner.draw == draw,
                Race.racecourse == racecourse,
                Race.distance.between(distance - 100, distance + 100),
                Runner.scratched == False,
                Runner.finish_position.isnot(None),
                Runner.finish_position > 0,
            )
            .limit(200)
            .all()
        )

        if len(runs) < 10:
            return 0.5  # Neutral

        wins = sum(1 for r in runs if r.finish_position == 1)
        return wins / len(runs) * 14  # Normalize: 1/14 baseline = 1.0

    def _compute_odds_rank(self, race_id: int, horse_no: int) -> int:
        """Rank of this horse by odds within the race (1 = favorite)."""
        runners = (
            self.session.query(Runner)
            .filter_by(race_id=race_id, scratched=False)
            .filter(Runner.win_odds.isnot(None))
            .order_by(Runner.win_odds.asc())
            .all()
        )

        for i, r in enumerate(runners, 1):
            if r.horse_no == horse_no:
                return i
        return len(runners) // 2 + 1  # Middle rank if not found

    @staticmethod
    def get_feature_columns() -> list[str]:
        """Return the list of feature column names used by the model."""
        return [
            # Race context
            "distance", "track_type", "going_encoded", "class_encoded",
            "field_size", "racecourse_st",
            # Runner
            "draw", "actual_weight", "rating", "rating_change",
            "season_stakes", "win_odds", "implied_probability",
            "has_blinkers", "has_tongue_tie", "has_visor",
            # Last 6 form
            "avg_finish_last6", "best_finish_last6", "worst_finish_last6",
            "num_wins_last6", "num_places_last6",
            # Horse history
            "horse_career_starts", "horse_win_rate", "horse_place_rate",
            "days_since_last_run", "distance_win_rate", "track_win_rate",
            "class_change",
            # Jockey
            "jockey_season_win_rate", "jockey_season_rides", "jockey_recent_win_rate",
            # Trainer
            "trainer_season_win_rate", "trainer_season_runners",
            # Combo
            "jt_combo_win_rate",
            # Bias
            "draw_bias_score", "odds_rank",
        ]

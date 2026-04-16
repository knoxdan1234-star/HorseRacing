"""
Data Cleaner

Validates, cleans, and normalizes scraped data before database insertion.
Handles edge cases: scratched horses, dead heats, void races.
"""

import logging
import re
from datetime import date

from sqlalchemy.orm import Session

from db.models import Dividend, Horse, Jockey, OddsHistory, Race, Runner, Trainer

logger = logging.getLogger(__name__)

# Normalize going descriptions
GOING_MAP = {
    "GOOD TO FIRM": "Good to Firm",
    "GOOD": "Good",
    "GOOD TO YIELDING": "Good to Yielding",
    "YIELDING": "Yielding",
    "YIELDING TO SOFT": "Yielding to Soft",
    "SOFT": "Soft",
    "HEAVY": "Heavy",
    "FIRM": "Firm",
    "WET FAST": "Wet Fast",
    "WET SLOW": "Wet Slow",
}

# Season boundaries
def get_season(race_date: date) -> str:
    """Determine the racing season (Sep-Jul) for a given date."""
    if race_date.month >= 9:
        return f"{race_date.year}/{str(race_date.year + 1)[-2:]}"
    else:
        return f"{race_date.year - 1}/{str(race_date.year)[-2:]}"


def normalize_going(raw_going: str) -> str:
    """Standardize going descriptions."""
    if not raw_going:
        return ""
    upper = raw_going.strip().upper()
    return GOING_MAP.get(upper, raw_going.strip())


def normalize_racecourse(raw: str) -> str:
    """Normalize racecourse codes."""
    mapping = {
        "SHA TIN": "ST",
        "SHATIN": "ST",
        "ST": "ST",
        "HAPPY VALLEY": "HV",
        "HAPPYVALLEY": "HV",
        "HV": "HV",
        "MACAU": "MJC",
        "MJC": "MJC",
    }
    return mapping.get(raw.strip().upper(), raw.strip().upper())


class DataCleaner:
    """Validates and stores scraped data into the database."""

    def __init__(self, session: Session):
        self.session = session
        self._horse_cache: dict[str, int] = {}  # code -> id
        self._jockey_cache: dict[str, int] = {}  # name -> id
        self._trainer_cache: dict[str, int] = {}  # name -> id

    def store_race_result(self, result) -> Race | None:
        """
        Store a scraped RaceResult into the database.
        Returns the Race ORM object, or None if invalid.
        """
        from agents.collector.hkjc.scraper_results import RaceResult

        if not isinstance(result, RaceResult):
            logger.error("Invalid result type: %s", type(result))
            return None

        if not result.runners:
            logger.warning("Skipping race with no runners: %s R%d", result.race_date, result.race_no)
            return None

        # Check for duplicate
        existing = (
            self.session.query(Race)
            .filter_by(
                race_date=result.race_date,
                racecourse=normalize_racecourse(result.racecourse),
                race_no=result.race_no,
            )
            .first()
        )
        if existing:
            logger.debug("Race already exists: %s %s R%d", result.race_date, result.racecourse, result.race_no)
            return existing

        # Create Race
        race = Race(
            race_date=result.race_date,
            racecourse=normalize_racecourse(result.racecourse),
            race_no=result.race_no,
            race_class=result.race_class or None,
            distance=result.distance,
            track_type=result.track_type or None,
            course_variant=result.course_variant or None,
            going=normalize_going(result.going),
            prize=result.prize,
            race_name=result.race_name or None,
            finish_time=result.finish_time or None,
            sectional_times=result.sectional_times or None,
            field_size=len([r for r in result.runners if not self._is_scratched(r)]),
            source="hkjc",
            season=get_season(result.race_date),
        )
        self.session.add(race)
        self.session.flush()  # Get race.id

        # Store runners
        for runner_data in result.runners:
            runner = self._create_runner(race, runner_data)
            if runner:
                self.session.add(runner)

        # Store dividends
        for div_data in result.dividends:
            if div_data.payout > 0:
                dividend = Dividend(
                    race_id=race.id,
                    pool_type=div_data.pool_type,
                    combination=div_data.combination,
                    payout=div_data.payout,
                )
                self.session.add(dividend)

        try:
            self.session.commit()
            logger.info("Stored race: %s %s R%d (%d runners)", race.race_date, race.racecourse, race.race_no, len(race.runners))
            return race
        except Exception as e:
            self.session.rollback()
            logger.error("Failed to store race: %s", e)
            return None

    def _is_scratched(self, runner_data) -> bool:
        """Check if a runner was scratched/withdrawn."""
        if hasattr(runner_data, "finish_position"):
            return runner_data.finish_position == 0 or runner_data.finish_position is None
        return False

    def _create_runner(self, race: Race, data) -> Runner | None:
        """Create a Runner from scraped data, linking to Horse/Jockey/Trainer."""
        if data.horse_no <= 0:
            return None

        # Get or create Horse
        horse_id = None
        if data.horse_code:
            horse_id = self._get_or_create_horse(data)

        # Get or create Jockey
        jockey_id = None
        if data.jockey:
            jockey_id = self._get_or_create_jockey(data.jockey)

        # Get or create Trainer
        trainer_id = None
        if data.trainer:
            trainer_id = self._get_or_create_trainer(data.trainer)

        runner = Runner(
            race_id=race.id,
            horse_id=horse_id,
            jockey_id=jockey_id,
            trainer_id=trainer_id,
            horse_no=data.horse_no,
            horse_name=data.horse_name or None,
            draw=data.draw,
            actual_weight=getattr(data, "actual_weight", None),
            declared_weight=getattr(data, "declared_weight", None),
            rating=getattr(data, "rating", None),
            rating_change=getattr(data, "rating_change", None),
            gear=getattr(data, "gear", None) or None,
            last_6_runs=getattr(data, "last_6_runs", None) or None,
            season_stakes=getattr(data, "season_stakes", None),
            finish_position=getattr(data, "finish_position", None),
            lbw=getattr(data, "lbw", None) or None,
            finish_time=getattr(data, "finish_time", None) or None,
            win_odds=getattr(data, "win_odds", None),
            running_positions=getattr(data, "running_position", None) or None,
            scratched=(getattr(data, "finish_position", None) == 0),
        )
        return runner

    def _get_or_create_horse(self, data) -> int:
        """Get existing horse ID or create a new horse record."""
        code = data.horse_code
        if code in self._horse_cache:
            return self._horse_cache[code]

        horse = self.session.query(Horse).filter_by(code=code).first()
        if not horse:
            horse = Horse(
                code=code,
                name=data.horse_name or code,
                source="hkjc",
            )
            self.session.add(horse)
            self.session.flush()

        self._horse_cache[code] = horse.id
        return horse.id

    def _get_or_create_jockey(self, name: str) -> int:
        """Get existing jockey ID or create a new jockey record."""
        name = name.strip()
        if name in self._jockey_cache:
            return self._jockey_cache[name]

        # Use name as a simple code
        code = re.sub(r"[^A-Za-z]", "", name)[:10].upper() or name[:10]
        jockey = self.session.query(Jockey).filter_by(name=name).first()
        if not jockey:
            jockey = Jockey(code=code, name=name, source="hkjc")
            self.session.add(jockey)
            self.session.flush()

        self._jockey_cache[name] = jockey.id
        return jockey.id

    def _get_or_create_trainer(self, name: str) -> int:
        """Get existing trainer ID or create a new trainer record."""
        name = name.strip()
        if name in self._trainer_cache:
            return self._trainer_cache[name]

        code = re.sub(r"[^A-Za-z]", "", name)[:10].upper() or name[:10]
        trainer = self.session.query(Trainer).filter_by(name=name).first()
        if not trainer:
            trainer = Trainer(code=code, name=name, source="hkjc")
            self.session.add(trainer)
            self.session.flush()

        self._trainer_cache[name] = trainer.id
        return trainer.id

    def store_odds_snapshot(self, snapshots: list) -> int:
        """Store odds snapshots to the database. Returns count stored."""
        from agents.collector.hkjc.scraper_odds import OddsSnapshot

        count = 0
        for snap in snapshots:
            if not isinstance(snap, OddsSnapshot):
                continue

            # Find the race
            race = (
                self.session.query(Race)
                .filter_by(
                    race_date=snap.race_date,
                    racecourse=normalize_racecourse(snap.racecourse),
                    race_no=snap.race_no,
                )
                .first()
            )
            if not race:
                continue

            odds = OddsHistory(
                race_id=race.id,
                horse_no=snap.horse_no,
                pool_type=snap.pool_type,
                odds_value=snap.odds_value,
                timestamp=snap.timestamp,
            )
            self.session.add(odds)
            count += 1

        try:
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.error("Failed to store odds: %s", e)
            return 0

        return count

"""
Historical Data Bootstrapper

One-time historical data loading for initial system setup.
- Loads Kaggle datasets (if available)
- Scrapes historical HKJC results for the last N seasons
- Backfills horse/jockey/trainer profiles
"""

import csv
import json
import logging
import time
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy.orm import Session

from agents.collector.data_cleaner import DataCleaner, get_season
from agents.collector.hkjc.scraper_profiles import ProfileScraper
from agents.collector.hkjc.scraper_results import ResultsScraper
from config import settings
from db.database import get_session
from db.models import Horse, Race

logger = logging.getLogger(__name__)


class HistoricalBootstrapper:
    """Loads historical racing data into the database."""

    def __init__(self, session: Session | None = None):
        self.session = session or get_session()
        self.cleaner = DataCleaner(self.session)
        self.results_scraper = ResultsScraper()
        self.profile_scraper = ProfileScraper()

    def load_kaggle_csv(self, csv_path: str | Path) -> int:
        """
        Import a Kaggle horse racing CSV dataset.

        Expected columns (flexible): date, racecourse, race_no, class, distance,
        going, horse_no, horse_name, jockey, trainer, draw, weight,
        finish_position, win_odds, lbw, etc.

        Returns the number of races imported.
        """
        csv_path = Path(csv_path)
        if not csv_path.exists():
            logger.error("Kaggle CSV not found: %s", csv_path)
            return 0

        logger.info("Loading Kaggle dataset: %s", csv_path)
        races_imported = 0

        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            # Group rows by (date, racecourse, race_no)
            race_groups: dict[tuple, list[dict]] = {}
            for row in reader:
                key = (
                    row.get("date", row.get("race_date", "")),
                    row.get("racecourse", row.get("course", "ST")),
                    row.get("race_no", row.get("race_number", "1")),
                )
                if key not in race_groups:
                    race_groups[key] = []
                race_groups[key].append(row)

        for (date_str, course, race_no_str), rows in race_groups.items():
            try:
                # Parse date
                race_date = date.fromisoformat(date_str.replace("/", "-"))
                race_no = int(race_no_str)

                # Check duplicate
                existing = (
                    self.session.query(Race)
                    .filter_by(race_date=race_date, racecourse=course, race_no=race_no)
                    .first()
                )
                if existing:
                    continue

                # Create Race
                first_row = rows[0]
                race = Race(
                    race_date=race_date,
                    racecourse=course,
                    race_no=race_no,
                    race_class=first_row.get("class", first_row.get("race_class", "")),
                    distance=self._safe_int(first_row.get("distance")),
                    going=first_row.get("going", ""),
                    track_type=first_row.get("track_type", first_row.get("surface", "")),
                    field_size=len(rows),
                    source="kaggle",
                    season=get_season(race_date),
                )
                self.session.add(race)
                self.session.flush()

                # Create Runners from each row
                from db.models import Runner

                for row in rows:
                    horse_no = self._safe_int(row.get("horse_no", row.get("number")))
                    if not horse_no:
                        continue

                    runner = Runner(
                        race_id=race.id,
                        horse_no=horse_no,
                        horse_name=row.get("horse_name", row.get("horse", "")),
                        draw=self._safe_int(row.get("draw")),
                        actual_weight=self._safe_int(row.get("actual_weight", row.get("weight"))),
                        declared_weight=self._safe_int(row.get("declared_weight")),
                        finish_position=self._safe_int(row.get("finish_position", row.get("placing"))),
                        win_odds=self._safe_float(row.get("win_odds", row.get("odds"))),
                        lbw=row.get("lbw", row.get("lengths_behind", "")),
                        running_positions=row.get("running_position", ""),
                    )
                    self.session.add(runner)

                races_imported += 1

                if races_imported % 100 == 0:
                    self.session.commit()
                    logger.info("Imported %d races from Kaggle...", races_imported)

            except Exception as e:
                logger.warning("Failed to import race %s/%s/R%s: %s", date_str, course, race_no_str, e)
                self.session.rollback()
                continue

        self.session.commit()
        logger.info("Kaggle import complete: %d races", races_imported)
        return races_imported

    def scrape_historical_range(
        self,
        start_date: date,
        end_date: date,
        racecourses: list[str] | None = None,
    ) -> int:
        """
        Scrape HKJC results for a date range.
        Iterates through known meeting days (Wed + Sun pattern) with rate limiting.

        Returns total races scraped.
        """
        if racecourses is None:
            racecourses = ["ST", "HV"]

        logger.info("Scraping historical results from %s to %s", start_date, end_date)
        total_races = 0
        current = start_date

        while current <= end_date:
            # HKJC races are typically Wed + Sun (sometimes Sat for big meetings)
            is_race_day = current.weekday() in (2, 6)  # Wed=2, Sun=6

            if is_race_day:
                for course in racecourses:
                    try:
                        results = self.results_scraper.scrape_meeting(current, course)
                        for result in results:
                            stored = self.cleaner.store_race_result(result)
                            if stored:
                                total_races += 1
                    except Exception as e:
                        logger.error("Failed to scrape %s %s: %s", current, course, e)

                    time.sleep(settings.SCRAPE_DELAY)

                if total_races > 0 and total_races % 50 == 0:
                    logger.info("Progress: %d races scraped (current: %s)", total_races, current)

            current += timedelta(days=1)

        logger.info("Historical scrape complete: %d total races", total_races)
        return total_races

    def backfill_horse_profiles(self, limit: int | None = None) -> int:
        """
        Scrape profiles for all horses in the database that don't have full profiles.
        Returns the number of profiles updated.
        """
        query = self.session.query(Horse).filter(Horse.age.is_(None))
        if limit:
            query = query.limit(limit)

        horses = query.all()
        logger.info("Backfilling %d horse profiles", len(horses))
        updated = 0

        for horse in horses:
            try:
                profile = self.profile_scraper.scrape_horse(horse.code)
                if profile:
                    horse.name = profile.name or horse.name
                    horse.name_tc = profile.name_tc or horse.name_tc
                    horse.age = profile.age
                    horse.sex = profile.sex
                    horse.color = profile.color
                    horse.import_type = profile.import_type
                    horse.current_rating = profile.current_rating
                    horse.season_stakes = profile.season_stakes
                    horse.total_starts = profile.total_starts
                    horse.total_wins = profile.total_wins
                    horse.total_places = profile.total_places
                    horse.sire = profile.sire
                    horse.dam = profile.dam
                    horse.dam_sire = profile.dam_sire
                    horse.country_of_origin = profile.country_of_origin
                    horse.owner = profile.owner
                    updated += 1

                    if updated % 20 == 0:
                        self.session.commit()
                        logger.info("Updated %d horse profiles...", updated)

                time.sleep(settings.SCRAPE_DELAY)

            except Exception as e:
                logger.warning("Failed to scrape profile for %s: %s", horse.code, e)

        self.session.commit()
        logger.info("Profile backfill complete: %d updated", updated)
        return updated

    def generate_bootstrap_report(self) -> dict:
        """Generate a summary report of the bootstrapped data."""
        from sqlalchemy import func

        report = {
            "total_races": self.session.query(func.count(Race.id)).scalar(),
            "total_horses": self.session.query(func.count(Horse.id)).scalar(),
            "races_by_source": {},
            "races_by_season": {},
        }

        # By source
        for source, count in (
            self.session.query(Race.source, func.count(Race.id))
            .group_by(Race.source)
            .all()
        ):
            report["races_by_source"][source] = count

        # By season
        for season, count in (
            self.session.query(Race.season, func.count(Race.id))
            .group_by(Race.season)
            .order_by(Race.season)
            .all()
        ):
            if season:
                report["races_by_season"][season] = count

        # Date range
        min_date = self.session.query(func.min(Race.race_date)).scalar()
        max_date = self.session.query(func.max(Race.race_date)).scalar()
        report["date_range"] = f"{min_date} to {max_date}" if min_date else "empty"

        return report

    @staticmethod
    def _safe_int(value) -> int | None:
        if value is None or value == "":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_float(value) -> float | None:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

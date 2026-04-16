"""
Data Validator

Validates data quality and detects issues like missing data or schema changes.
"""

import logging
from dataclasses import dataclass
from datetime import date, timedelta

from sqlalchemy import func
from sqlalchemy.orm import Session

from db.models import Dividend, Race, Runner

logger = logging.getLogger(__name__)


@dataclass
class DataIssue:
    severity: str  # INFO, WARNING, ERROR
    category: str
    message: str


class DataValidator:
    """Validates race data quality."""

    def __init__(self, session: Session):
        self.session = session

    def validate_all(self) -> list[DataIssue]:
        """Run all validation checks."""
        issues = []
        issues.extend(self.check_recent_data_completeness())
        issues.extend(self.check_for_duplicates())
        issues.extend(self.check_data_consistency())
        return issues

    def check_recent_data_completeness(self, days: int = 14) -> list[DataIssue]:
        """Check that recent meetings have complete data."""
        issues = []
        cutoff = date.today() - timedelta(days=days)

        recent_races = (
            self.session.query(Race)
            .filter(Race.race_date >= cutoff)
            .order_by(Race.race_date, Race.race_no)
            .all()
        )

        # Group by meeting (date + racecourse)
        meetings: dict[tuple, list[Race]] = {}
        for race in recent_races:
            key = (race.race_date, race.racecourse)
            meetings.setdefault(key, []).append(race)

        for (race_date, course), races in meetings.items():
            race_numbers = sorted(r.race_no for r in races)

            # Check for gaps in race numbers
            if race_numbers:
                expected = list(range(1, max(race_numbers) + 1))
                missing = set(expected) - set(race_numbers)
                if missing:
                    issues.append(DataIssue(
                        severity="WARNING",
                        category="completeness",
                        message=f"Missing races {missing} on {race_date} at {course}",
                    ))

            # Check each race has runners
            for race in races:
                runner_count = (
                    self.session.query(func.count(Runner.id))
                    .filter_by(race_id=race.id)
                    .scalar()
                )
                if runner_count == 0:
                    issues.append(DataIssue(
                        severity="ERROR",
                        category="completeness",
                        message=f"Race {race_date} {course} R{race.race_no}: no runners",
                    ))
                elif runner_count < 5:
                    issues.append(DataIssue(
                        severity="WARNING",
                        category="completeness",
                        message=f"Race {race_date} {course} R{race.race_no}: only {runner_count} runners",
                    ))

            # Check dividends exist for completed races
            for race in races:
                has_results = (
                    self.session.query(Runner)
                    .filter_by(race_id=race.id)
                    .filter(Runner.finish_position.isnot(None), Runner.finish_position > 0)
                    .first()
                )
                if has_results:
                    div_count = (
                        self.session.query(func.count(Dividend.id))
                        .filter_by(race_id=race.id)
                        .scalar()
                    )
                    if div_count == 0:
                        issues.append(DataIssue(
                            severity="WARNING",
                            category="dividends",
                            message=f"Race {race_date} {course} R{race.race_no}: has results but no dividends",
                        ))

        return issues

    def check_for_duplicates(self) -> list[DataIssue]:
        """Check for duplicate race records."""
        issues = []

        dupes = (
            self.session.query(
                Race.race_date, Race.racecourse, Race.race_no,
                func.count(Race.id).label("cnt"),
            )
            .group_by(Race.race_date, Race.racecourse, Race.race_no)
            .having(func.count(Race.id) > 1)
            .all()
        )

        for dupe in dupes:
            issues.append(DataIssue(
                severity="ERROR",
                category="duplicates",
                message=f"Duplicate race: {dupe[0]} {dupe[1]} R{dupe[2]} ({dupe[3]} copies)",
            ))

        return issues

    def check_data_consistency(self) -> list[DataIssue]:
        """Check for data consistency issues."""
        issues = []

        # Check races with field_size mismatch
        races_with_mismatch = (
            self.session.query(Race)
            .filter(Race.field_size.isnot(None))
            .all()
        )

        for race in races_with_mismatch[:100]:  # Limit to avoid slow queries
            actual_count = (
                self.session.query(func.count(Runner.id))
                .filter_by(race_id=race.id, scratched=False)
                .scalar()
            )
            if race.field_size and actual_count > 0 and abs(race.field_size - actual_count) > 2:
                issues.append(DataIssue(
                    severity="INFO",
                    category="consistency",
                    message=(
                        f"Race {race.race_date} {race.racecourse} R{race.race_no}: "
                        f"field_size={race.field_size} but {actual_count} runners"
                    ),
                ))

        return issues

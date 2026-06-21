"""
Race-day readiness check.

Verifies the betting pipeline is actually ready BEFORE post time and pings
Discord if anything is off — so silent failures surface while they can still be
fixed. Every failure in the June 2026 debugging saga would have been caught
here: missing racecard, scraper runaway, stale all-one-class card, no odds,
no predictions, or unrecorded results blocking settlement.
"""
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from db.models import Prediction, Race, Runner

logger = logging.getLogger(__name__)

HK_TZ = ZoneInfo("Asia/Hong_Kong")
MAX_RACES_PER_DAY = 24      # mirrors the orchestrator flood guard
MIN_CARD_FOR_VARIETY = 5    # below this, a uniform class is plausible


@dataclass
class Check:
    name: str
    ok: bool
    severity: str  # "OK" | "WARNING" | "CRITICAL"
    detail: str


class RaceDayReadiness:
    """Read-only pre-race checks on the betting pipeline. Never bets or writes."""

    def __init__(self, session):
        self.session = session

    def run(self, target: date | None = None) -> list[Check]:
        target = target or datetime.now(HK_TZ).date()
        checks: list[Check] = []

        races = (
            self.session.query(Race)
            .filter_by(race_date=target)
            .order_by(Race.race_no)
            .all()
        )

        # 1. Racecard present
        if not races:
            checks.append(Check(
                "racecard", False, "CRITICAL",
                f"No races in DB for {target}. If there IS a meeting today the "
                f"racecard scrape failed — no predictions can run.",
            ))
            checks.append(self._results_check(target))  # still surface stuck results
            return checks
        checks.append(Check("racecard", True, "OK", f"{len(races)} races loaded for {target}."))

        # 2. Sane race count (runaway detector)
        if len(races) > MAX_RACES_PER_DAY:
            checks.append(Check(
                "race_count", False, "CRITICAL",
                f"{len(races)} races for {target} (> {MAX_RACES_PER_DAY}) — scraper runaway.",
            ))
        else:
            checks.append(Check("race_count", True, "OK", f"Race count {len(races)} is sane."))

        # 3. Class variety (stale/default-page detector)
        classes = {r.race_class for r in races if r.race_class}
        if len(races) >= MIN_CARD_FOR_VARIETY and len(classes) <= 1:
            only = next(iter(classes), "unknown")
            checks.append(Check(
                "class_variety", False, "WARNING",
                f"All {len(races)} races are '{only}' — likely a stale/default page, "
                f"not the real card.",
            ))
        else:
            checks.append(Check(
                "class_variety", True, "OK", f"{len(classes)} distinct class(es) on the card.",
            ))

        # 4. Odds present on upcoming races
        upcoming = [r for r in races if not self._has_run(r.id)]
        if upcoming:
            with_odds = sum(1 for r in upcoming if self._has_odds(r.id))
            if with_odds == 0:
                checks.append(Check(
                    "odds", False, "WARNING",
                    f"{len(upcoming)} upcoming race(s) but none have win odds yet — "
                    f"value bets will be empty.",
                ))
            else:
                checks.append(Check(
                    "odds", True, "OK", f"{with_odds}/{len(upcoming)} upcoming races have odds.",
                ))
        else:
            checks.append(Check("odds", True, "OK", "No upcoming races (card already run)."))

        # 5. Predictions generated
        npred = (
            self.session.query(Prediction)
            .join(Race, Prediction.race_id == Race.id)
            .filter(Race.race_date == target)
            .count()
        )
        if npred == 0:
            checks.append(Check("predictions", False, "WARNING",
                                f"No predictions generated for {target} yet."))
        else:
            checks.append(Check("predictions", True, "OK",
                                f"{npred} predictions saved for {target}."))

        # 6. Recent results recorded (settlement health)
        checks.append(self._results_check(target))
        return checks

    def _results_check(self, target: date) -> Check:
        """Past meetings (last 3 days) whose races still have no finish positions
        → results scraping/settlement is stuck."""
        cutoff = target - timedelta(days=3)
        past = (
            self.session.query(Race)
            .filter(Race.race_date >= cutoff, Race.race_date < target)
            .all()
        )
        stuck = [r for r in past if not self._has_run(r.id)]
        if stuck:
            dates = ", ".join(sorted({str(r.race_date) for r in stuck}))
            return Check("results", False, "WARNING",
                         f"{len(stuck)} past race(s) without results ({dates}) — settlement blocked.")
        return Check("results", True, "OK", "Recent results recorded.")

    def _has_run(self, race_id: int) -> bool:
        return (
            self.session.query(Runner)
            .filter(Runner.race_id == race_id, Runner.finish_position.isnot(None))
            .first()
            is not None
        )

    def _has_odds(self, race_id: int) -> bool:
        return (
            self.session.query(Runner)
            .filter(Runner.race_id == race_id, Runner.win_odds.isnot(None))
            .first()
            is not None
        )

    @staticmethod
    def worst_severity(checks: list[Check]) -> str:
        if any(c.severity == "CRITICAL" for c in checks):
            return "CRITICAL"
        if any(c.severity == "WARNING" for c in checks):
            return "WARNING"
        return "OK"

    def report(self, target: date | None = None, discord=None) -> list[Check]:
        """Run checks, log them, and (if a Discord webhook is given) send one
        summary embed — green when all pass, yellow/red listing the failures."""
        target = target or datetime.now(HK_TZ).date()
        checks = self.run(target)
        worst = self.worst_severity(checks)

        def icon(c: Check) -> str:
            return "✅" if c.ok else ("🔴" if c.severity == "CRITICAL" else "⚠️")

        for c in checks:
            logger.log(logging.INFO if c.ok else logging.WARNING,
                       "%s [%s] %s: %s", icon(c), c.severity, c.name, c.detail)

        if discord is not None:
            color = {"OK": 3066993, "WARNING": 16776960, "CRITICAL": 16711680}[worst]
            title = {
                "OK": f"✅ Race-day ready — {target}",
                "WARNING": f"⚠️ Race-day readiness — {target}",
                "CRITICAL": f"🔴 Race-day NOT ready — {target}",
            }[worst]
            fields = [{"name": f"{icon(c)} {c.name}", "value": c.detail, "inline": False}
                      for c in checks]
            try:
                discord.send_embed(
                    title=title,
                    description=f"Pipeline check at {datetime.now(HK_TZ).strftime('%H:%M HKT')}",
                    fields=fields,
                    color=color,
                )
            except Exception as e:
                logger.error("Failed to send readiness report: %s", e)

        return checks

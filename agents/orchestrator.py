"""
Main Orchestrator

Coordinates all sub-agents via APScheduler:
- Data Collector (Sub-agent 1)
- Predictor (Sub-agent 2)
- Health Monitor (Sub-agent 3)
- Discord notifications
"""

import logging
import signal
import threading
from datetime import date, datetime, timedelta

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from config.settings import Settings
from db.database import get_session

logger = logging.getLogger(__name__)


class Orchestrator:
    """Central coordinator for all prediction system agents."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.scheduler = BlockingScheduler(
            job_defaults={"coalesce": True, "max_instances": 1},
        )
        self._setup_jobs()

    def _setup_jobs(self):
        """Register all scheduled jobs."""

        # --- Daily: Check fixtures for upcoming meetings ---
        self.scheduler.add_job(
            self._job_check_fixtures,
            CronTrigger(hour=8, minute=0),
            id="check_fixtures",
            name="Check HKJC/MJC fixture calendar",
        )

        # --- Race day morning: Scrape race card ---
        self.scheduler.add_job(
            self._job_scrape_racecard,
            CronTrigger(hour=9, minute=0),
            id="scrape_racecard",
            name="Scrape race card for today",
        )

        # --- Race day: Generate predictions (2hrs before typical start) ---
        self.scheduler.add_job(
            self._job_generate_predictions,
            CronTrigger(hour=10, minute=30),  # Before Wed evening / Sun afternoon
            id="generate_predictions",
            name="Generate predictions for today",
        )

        # --- Race day: Send predictions to Discord ---
        self.scheduler.add_job(
            self._job_send_predictions,
            CronTrigger(hour=11, minute=0),
            id="send_predictions",
            name="Send predictions to Discord",
        )

        # --- Race day: Poll odds (every 5 minutes during active hours) ---
        self.scheduler.add_job(
            self._job_poll_odds,
            IntervalTrigger(minutes=5),
            id="poll_odds",
            name="Poll live odds",
        )

        # --- Race day: Scrape results (every 15 minutes during active hours) ---
        self.scheduler.add_job(
            self._job_scrape_results,
            IntervalTrigger(minutes=15),
            id="scrape_results",
            name="Scrape race results",
        )

        # --- Race day: Settle bets ---
        self.scheduler.add_job(
            self._job_settle_bets,
            CronTrigger(hour=22, minute=0),  # After racing
            id="settle_bets",
            name="Settle bets for today",
        )

        # --- Every Monday 10AM: Send weekly P&L ---
        self.scheduler.add_job(
            self._job_weekly_pnl,
            CronTrigger(day_of_week="mon", hour=10, minute=0),
            id="weekly_pnl",
            name="Send weekly P&L to Discord",
        )

        # --- Every 6 hours: Health checks ---
        self.scheduler.add_job(
            self._job_health_check,
            IntervalTrigger(hours=6),
            id="health_check",
            name="Run system health checks",
        )

        # --- Daily 2AM: Data validation ---
        self.scheduler.add_job(
            self._job_data_validation,
            CronTrigger(hour=2, minute=0),
            id="data_validation",
            name="Validate data quality",
        )

        # --- Monthly 1st at 3AM: Retrain model ---
        self.scheduler.add_job(
            self._job_retrain_model,
            CronTrigger(day=1, hour=3, minute=0),
            id="retrain_model",
            name="Monthly model retraining",
        )

    def start(self):
        """Start the orchestrator (blocking)."""
        logger.info("Orchestrator starting with %d jobs", len(self.scheduler.get_jobs()))
        for job in self.scheduler.get_jobs():
            logger.info("  Job: %s (%s)", job.name, job.trigger)
        self.scheduler.start()

    def stop(self):
        """Gracefully stop the orchestrator."""
        logger.info("Orchestrator shutting down...")
        self.scheduler.shutdown(wait=True)

    # ===== Job implementations =====

    def _is_race_day(self) -> bool:
        """Check if today is a likely race day (Wed or Sun)."""
        today = date.today()
        return today.weekday() in (2, 6)  # Wed=2, Sun=6

    def _job_check_fixtures(self):
        """Check HKJC fixture calendar for upcoming meetings."""
        logger.info("Checking fixture calendar...")
        try:
            from agents.collector.hkjc.scraper_results import ResultsScraper
            scraper = ResultsScraper()
            today = date.today()
            meetings = scraper.get_meeting_dates(today.year, today.month)
            logger.info("Found %d meetings this month", len(meetings))

            upcoming = [m for m in meetings if m[0] >= today]
            if upcoming:
                next_meeting = upcoming[0]
                logger.info("Next meeting: %s at %s", next_meeting[0], next_meeting[1])
        except Exception as e:
            logger.error("Fixture check failed: %s", e)

    def _job_scrape_racecard(self):
        """Scrape today's race card."""
        if not self._is_race_day():
            logger.debug("Not a race day, skipping racecard scrape")
            return

        logger.info("Scraping today's race card...")
        try:
            from agents.collector.hkjc.scraper_racecard import RaceCardScraper
            from agents.collector.data_cleaner import DataCleaner

            session = get_session()
            scraper = RaceCardScraper()
            cleaner = DataCleaner(session)
            today = date.today()

            # Try both courses
            for course in ["ST", "HV"]:
                cards = scraper.scrape_meeting_card(today, course)
                if cards:
                    logger.info("Got %d race cards for %s at %s", len(cards), today, course)
                    break

            session.close()
        except Exception as e:
            logger.error("Racecard scrape failed: %s", e)

    def _job_generate_predictions(self):
        """Generate ML predictions for today's races."""
        if not self._is_race_day():
            return

        logger.info("Generating predictions...")
        try:
            from agents.predictor.model_predictor import Predictor
            from agents.predictor.bet_sizer import BetSizer
            from agents.predictor.pnl_tracker import PnLTracker
            from db.models import Race

            session = get_session()
            predictor = Predictor(session)
            predictor.load_models()

            bet_sizer = BetSizer(bankroll=PnLTracker(session, self.settings.INITIAL_BANKROLL).get_bankroll())

            today = date.today()
            races = session.query(Race).filter_by(race_date=today).all()

            for race in races:
                predictions = predictor.predict_race(race.id)
                if predictions.empty:
                    continue

                value_bets = predictor.find_value_bets(race.id)
                for vb in value_bets:
                    bet_sizer.size_value_bet(vb)

                predictor.save_predictions(race.id, predictions, value_bets)
                logger.info(
                    "Race %d: %d predictions, %d value bets",
                    race.race_no, len(predictions), len(value_bets),
                )

            session.close()
        except Exception as e:
            logger.error("Prediction generation failed: %s", e)

    def _job_send_predictions(self):
        """Send predictions to Discord."""
        if not self._is_race_day():
            return

        logger.info("Sending predictions to Discord...")
        try:
            from discord_bot.webhook import DiscordWebhook
            from db.models import Prediction, Race, Runner

            session = get_session()
            discord = DiscordWebhook()
            today = date.today()

            races = session.query(Race).filter_by(race_date=today).order_by(Race.race_no).all()

            for race in races:
                predictions = (
                    session.query(Prediction)
                    .filter_by(race_id=race.id)
                    .order_by(Prediction.predicted_rank)
                    .all()
                )
                if not predictions:
                    continue

                race_info = {
                    "race_no": race.race_no,
                    "racecourse": race.racecourse,
                    "class": race.race_class or "",
                    "distance": race.distance,
                    "track_type": race.track_type or "",
                    "going": race.going or "",
                    "field_size": race.field_size,
                }

                pred_dicts = []
                value_dicts = []
                for p in predictions:
                    runner = session.query(Runner).filter_by(race_id=race.id, horse_no=p.horse_no).first()
                    pred_dicts.append({
                        "horse_no": p.horse_no,
                        "horse_name": runner.horse_name if runner else f"#{p.horse_no}",
                        "jockey": runner.jockey.name if runner and runner.jockey else "",
                        "trainer": runner.trainer.name if runner and runner.trainer else "",
                        "win_prob": p.predicted_win_prob or 0,
                        "odds": runner.win_odds if runner else 0,
                        "win_rank": p.predicted_rank or 99,
                    })
                    if p.is_value_bet and p.bet_amount:
                        value_dicts.append({
                            "horse_no": p.horse_no,
                            "bet_type": p.bet_type,
                            "bet_amount": p.bet_amount,
                        })

                discord.send_prediction(race_info, pred_dicts, value_dicts)

            session.close()
            logger.info("Predictions sent to Discord")
        except Exception as e:
            logger.error("Failed to send predictions: %s", e)

    def _job_poll_odds(self):
        """Poll live odds."""
        if not self._is_race_day():
            return

        try:
            from agents.collector.hkjc.scraper_odds import OddsScraper
            from agents.collector.data_cleaner import DataCleaner

            session = get_session()
            scraper = OddsScraper()
            cleaner = DataCleaner(session)
            today = date.today()

            for course in ["ST", "HV"]:
                snapshots = scraper.poll_all_races(today, course, 11)
                if snapshots:
                    count = cleaner.store_odds_snapshot(snapshots)
                    logger.debug("Stored %d odds snapshots for %s", count, course)
                    break

            session.close()
        except Exception as e:
            logger.debug("Odds poll failed: %s", e)

    def _job_scrape_results(self):
        """Scrape race results as they become available."""
        if not self._is_race_day():
            return

        try:
            from agents.collector.hkjc.scraper_results import ResultsScraper
            from agents.collector.data_cleaner import DataCleaner

            session = get_session()
            scraper = ResultsScraper()
            cleaner = DataCleaner(session)
            today = date.today()

            for course in ["ST", "HV"]:
                results = scraper.scrape_meeting(today, course)
                for result in results:
                    cleaner.store_race_result(result)

            session.close()
        except Exception as e:
            logger.debug("Results scrape: %s", e)

    def _job_settle_bets(self):
        """Settle all bets for today's races."""
        if not self._is_race_day():
            return

        logger.info("Settling bets...")
        try:
            from agents.predictor.pnl_tracker import PnLTracker
            from db.models import Race

            session = get_session()
            tracker = PnLTracker(session, self.settings.INITIAL_BANKROLL)
            today = date.today()

            races = session.query(Race).filter_by(race_date=today).all()
            total_pnl = 0

            for race in races:
                pnls = tracker.settle_race(race.id)
                total_pnl += sum(pnls)

            logger.info("Today's P&L: $%.2f", total_pnl)

            # Send meeting summary to Discord
            from discord_bot.webhook import DiscordWebhook
            discord = DiscordWebhook()
            color = 3066993 if total_pnl >= 0 else 15158332
            discord.send_embed(
                title=f"Meeting Summary - {today}",
                description=f"Total P&L: ${total_pnl:+,.2f}",
                color=color,
            )

            session.close()
        except Exception as e:
            logger.error("Bet settlement failed: %s", e)

    def _job_weekly_pnl(self):
        """Send weekly P&L report every Monday."""
        logger.info("Generating weekly P&L report...")
        try:
            from agents.predictor.pnl_tracker import PnLTracker
            from discord_bot.webhook import DiscordWebhook

            session = get_session()
            tracker = PnLTracker(session, self.settings.INITIAL_BANKROLL)

            # Last week (Monday to Sunday)
            today = date.today()
            last_monday = today - timedelta(days=7)
            weekly = tracker.get_weekly_pnl(last_monday)

            discord = DiscordWebhook()
            discord.send_weekly_pnl(weekly)

            logger.info("Weekly P&L sent: $%.2f", weekly.net_pnl)
            session.close()
        except Exception as e:
            logger.error("Weekly P&L failed: %s", e)

    def _job_health_check(self):
        """Run system health checks."""
        try:
            from agents.monitor.health_checker import HealthChecker
            from agents.monitor.alerter import Alerter
            from discord_bot.webhook import DiscordWebhook

            session = get_session()
            checker = HealthChecker(session)
            alerter = Alerter()
            alerter.set_discord(DiscordWebhook())

            statuses = checker.run_all_checks()
            alerter.send_health_report(statuses)

            for s in statuses:
                logger.debug("Health: %s - %s: %s", s.status, s.component, s.message)

            session.close()
        except Exception as e:
            logger.error("Health check failed: %s", e)

    def _job_data_validation(self):
        """Run data quality validation."""
        try:
            from agents.monitor.data_validator import DataValidator
            from agents.monitor.alerter import Alerter
            from discord_bot.webhook import DiscordWebhook

            session = get_session()
            validator = DataValidator(session)
            issues = validator.validate_all()

            if issues:
                alerter = Alerter()
                alerter.set_discord(DiscordWebhook())
                for issue in issues:
                    alerter.send_alert(issue.severity, issue.category, issue.message)

            logger.info("Data validation: %d issues found", len(issues))
            session.close()
        except Exception as e:
            logger.error("Data validation failed: %s", e)

    def _job_retrain_model(self):
        """Monthly model retraining."""
        logger.info("Starting monthly model retraining...")
        try:
            from agents.predictor.model_trainer import ModelTrainer
            session = get_session()
            trainer = ModelTrainer(session)

            # Train on last 24 months of data
            today = date.today()
            train_start = today - timedelta(days=730)

            # Train win model
            model, metadata = trainer.train_win_model(train_start, today, "lightgbm")
            if model:
                version = trainer.save_model(model, metadata)
                logger.info("Win model retrained: %s (AUC: %.4f)", version, metadata.get("validation_auc", 0))

            # Train place model
            model, metadata = trainer.train_place_model(train_start, today, "lightgbm")
            if model:
                version = trainer.save_model(model, metadata)
                logger.info("Place model retrained: %s", version)

            # Notify
            from discord_bot.webhook import DiscordWebhook
            discord = DiscordWebhook()
            discord.send_embed(
                title="Model Retrained",
                description=f"Monthly model refresh complete. AUC: {metadata.get('validation_auc', 0):.4f}",
                color=3447003,
            )

            session.close()
        except Exception as e:
            logger.error("Model retraining failed: %s", e)

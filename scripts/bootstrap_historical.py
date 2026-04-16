"""
Bootstrap Historical Data

One-time script to load historical race data into the database.
Run this before starting the prediction system.

Usage:
    python scripts/bootstrap_historical.py [--kaggle path/to/data.csv] [--seasons N]
"""

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings
from config.logging_config import setup_logging
from db.database import init_database, get_session
from agents.collector.bootstrap import HistoricalBootstrapper

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Bootstrap historical horse racing data")
    parser.add_argument(
        "--kaggle",
        type=str,
        help="Path to Kaggle CSV dataset file",
    )
    parser.add_argument(
        "--seasons",
        type=int,
        default=3,
        help="Number of past seasons to scrape (default: 3, each season ~Sep-Jul)",
    )
    parser.add_argument(
        "--skip-scrape",
        action="store_true",
        help="Skip HKJC website scraping (only load Kaggle data)",
    )
    parser.add_argument(
        "--skip-profiles",
        action="store_true",
        help="Skip horse profile backfill",
    )
    parser.add_argument(
        "--profile-limit",
        type=int,
        default=500,
        help="Maximum number of horse profiles to backfill (default: 500)",
    )
    args = parser.parse_args()

    setup_logging(settings)
    logger.info("=" * 60)
    logger.info("HISTORICAL DATA BOOTSTRAP")
    logger.info("=" * 60)

    # Initialize database
    init_database()
    logger.info("Database initialized")

    bootstrapper = HistoricalBootstrapper()

    # Step 1: Load Kaggle data
    if args.kaggle:
        kaggle_path = Path(args.kaggle)
        if kaggle_path.exists():
            logger.info("Step 1: Loading Kaggle dataset from %s", kaggle_path)
            count = bootstrapper.load_kaggle_csv(kaggle_path)
            logger.info("Loaded %d races from Kaggle", count)
        else:
            logger.warning("Kaggle file not found: %s", kaggle_path)
    else:
        logger.info("Step 1: Skipped (no --kaggle path provided)")

    # Step 2: Scrape historical HKJC results
    if not args.skip_scrape:
        today = date.today()
        # Calculate start date based on number of seasons
        # Each season starts in September
        start_year = today.year - args.seasons
        if today.month < 9:
            start_year -= 1
        start_date = date(start_year, 9, 1)

        logger.info(
            "Step 2: Scraping HKJC results from %s to %s (%d seasons)",
            start_date, today, args.seasons,
        )
        total = bootstrapper.scrape_historical_range(start_date, today)
        logger.info("Scraped %d races from HKJC", total)
    else:
        logger.info("Step 2: Skipped (--skip-scrape)")

    # Step 3: Backfill horse profiles
    if not args.skip_profiles:
        logger.info("Step 3: Backfilling horse profiles (limit: %d)", args.profile_limit)
        updated = bootstrapper.backfill_horse_profiles(limit=args.profile_limit)
        logger.info("Updated %d horse profiles", updated)
    else:
        logger.info("Step 3: Skipped (--skip-profiles)")

    # Generate and save report
    report = bootstrapper.generate_bootstrap_report()
    report_path = settings.DATA_DIR / "historical" / "bootstrap_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    logger.info("=" * 60)
    logger.info("BOOTSTRAP COMPLETE")
    logger.info("Total races: %d", report["total_races"])
    logger.info("Total horses: %d", report["total_horses"])
    logger.info("Date range: %s", report["date_range"])
    logger.info("Races by source: %s", report["races_by_source"])
    logger.info("Report saved to: %s", report_path)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

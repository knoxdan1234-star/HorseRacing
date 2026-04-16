"""
HKJC + Macau Horse Racing Prediction System

Entry point for the prediction system. Initializes the database,
sets up logging, and starts the orchestrator with APScheduler.
"""

import logging
import signal
import sys

from config import settings
from config.logging_config import setup_logging
from db.database import init_database

logger = logging.getLogger(__name__)


def check_first_run() -> bool:
    """Check if the database has any historical race data."""
    from sqlalchemy import func

    from db.database import get_session
    from db.models import Race

    session = get_session()
    try:
        count = session.query(func.count(Race.id)).scalar()
        return count == 0
    finally:
        session.close()


def main():
    setup_logging(settings)
    logger.info("Starting Horse Racing Prediction System")
    logger.info("Database: %s", settings.DATABASE_URL)

    # Initialize database tables
    init_database()
    logger.info("Database initialized")

    # Check for first run
    if check_first_run():
        logger.warning(
            "No historical data found. Run 'python scripts/bootstrap_historical.py' first "
            "to load historical race data before starting the prediction system."
        )
        print("\n" + "=" * 60)
        print("FIRST RUN DETECTED")
        print("=" * 60)
        print("No historical race data found in the database.")
        print("Please run the bootstrap script first:")
        print("  python scripts/bootstrap_historical.py")
        print("=" * 60 + "\n")
        sys.exit(1)

    # Start orchestrator
    from agents.orchestrator import Orchestrator

    orchestrator = Orchestrator(settings)

    # Graceful shutdown on SIGTERM/SIGINT
    def shutdown_handler(signum, frame):
        logger.info("Received signal %s, shutting down...", signum)
        orchestrator.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    orchestrator.start()


if __name__ == "__main__":
    main()

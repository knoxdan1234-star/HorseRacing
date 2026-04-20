"""
Shadow test: scrape a race meeting, format entries as Traditional Chinese
embeds, and send to Discord without placing any bets.

Does NOT touch the prediction pipeline — the predictor needs DB-resident
features (horse history, jockey rates, etc.) and historical odds, which
aren't present pre-race. This test validates the scraper + Discord path.

Usage:
    venv/Scripts/python.exe scripts/shadow_test_meeting.py --date 2026-04-22 --course HV
"""

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from agents.collector.hkjc.scraper_racecard import RaceCardScraper
from discord_bot.webhook import DiscordWebhook


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Race date YYYY-MM-DD")
    parser.add_argument("--course", required=True, choices=["ST", "HV"])
    args = parser.parse_args()

    race_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    logger.info("Scraping %s at %s...", race_date, args.course)
    scraper = RaceCardScraper()
    cards = scraper.scrape_meeting_card(race_date, args.course)

    if not cards:
        logger.error("No race cards scraped — abort")
        return 1

    logger.info("Got %d races", len(cards))

    discord = DiscordWebhook()

    for card in cards:
        predictions = []
        for e in card.entries:
            predictions.append({
                "horse_no": e.horse_no,
                "horse_name": e.horse_name,
                "jockey": e.jockey,
                "trainer": e.trainer,
                "win_prob": 0.0,
                "odds": 0.0,
                "win_rank": e.horse_no,
            })

        race_info = {
            "race_no": card.race_no,
            "racecourse": card.racecourse,
            "class": card.race_class,
            "distance": card.distance,
            "track_type": card.track_type,
            "going": card.going,
            "field_size": len(card.entries),
        }

        ok = discord.send_prediction(race_info, predictions, value_bets=[])
        logger.info("R%d sent=%s", card.race_no, ok)

    logger.info("Shadow test complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())

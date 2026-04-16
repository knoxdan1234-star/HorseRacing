"""
HKJC Odds Scraper

Polls live odds on race day and captures final odds before each race.
"""

import logging
import re
import time
from dataclasses import dataclass
from datetime import date, datetime

import requests
from bs4 import BeautifulSoup

from config import settings

logger = logging.getLogger(__name__)


@dataclass
class OddsSnapshot:
    race_date: date
    racecourse: str
    race_no: int
    horse_no: int
    pool_type: str  # WIN, PLA, QIN, QPL
    odds_value: float
    timestamp: datetime


class OddsScraper:
    """Scrapes live and historical odds from HKJC."""

    ODDS_URL = "https://racing.hkjc.com/racing/information/English/Odds/WinPlaceOdds.aspx"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        })
        self.delay = settings.SCRAPE_DELAY

    def poll_win_place_odds(
        self, race_date: date, racecourse: str, race_no: int
    ) -> list[OddsSnapshot]:
        """Fetch current Win/Place odds for a race."""
        date_str = race_date.strftime("%Y/%m/%d")
        url = (
            f"{self.ODDS_URL}"
            f"?RaceDate={date_str}&Racecourse={racecourse}&RaceNo={race_no}"
        )

        try:
            resp = self.session.get(url, timeout=settings.REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Failed to fetch odds: %s", e)
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        now = datetime.utcnow()
        return self._parse_win_place_odds(soup, race_date, racecourse, race_no, now)

    def _parse_win_place_odds(
        self,
        soup: BeautifulSoup,
        race_date: date,
        racecourse: str,
        race_no: int,
        timestamp: datetime,
    ) -> list[OddsSnapshot]:
        """Parse Win/Place odds table."""
        snapshots = []

        table = (
            soup.find("table", class_="table_bd")
            or soup.find("table", {"id": "oddstable"})
        )
        if not table:
            return snapshots

        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            horse_no_text = cells[0].get_text(strip=True)
            if not horse_no_text.isdigit():
                continue

            horse_no = int(horse_no_text)

            # Win odds (typically column 2)
            win_text = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            if win_text and win_text not in ("---", "SCR"):
                try:
                    win_odds = float(win_text)
                    snapshots.append(OddsSnapshot(
                        race_date=race_date,
                        racecourse=racecourse,
                        race_no=race_no,
                        horse_no=horse_no,
                        pool_type="WIN",
                        odds_value=win_odds,
                        timestamp=timestamp,
                    ))
                except (ValueError, TypeError):
                    pass

            # Place odds (typically column 3)
            place_text = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            if place_text and place_text not in ("---", "SCR"):
                try:
                    place_odds = float(place_text)
                    snapshots.append(OddsSnapshot(
                        race_date=race_date,
                        racecourse=racecourse,
                        race_no=race_no,
                        horse_no=horse_no,
                        pool_type="PLA",
                        odds_value=place_odds,
                        timestamp=timestamp,
                    ))
                except (ValueError, TypeError):
                    pass

        return snapshots

    def poll_all_races(
        self, race_date: date, racecourse: str, num_races: int
    ) -> list[OddsSnapshot]:
        """Poll odds for all races in a meeting."""
        all_snapshots = []
        for race_no in range(1, num_races + 1):
            snapshots = self.poll_win_place_odds(race_date, racecourse, race_no)
            all_snapshots.extend(snapshots)
            time.sleep(max(1.0, self.delay / 2))
        return all_snapshots

    def get_final_odds_from_results(
        self, race_date: date, racecourse: str, race_no: int
    ) -> dict[int, float]:
        """
        Extract final win odds from the results page (post-race).
        Returns a dict of horse_no -> final_win_odds.
        """
        date_str = race_date.strftime("%Y/%m/%d")
        url = (
            f"{settings.HKJC_RESULTS_URL}"
            f"?RaceDate={date_str}&Racecourse={racecourse}&RaceNo={race_no}"
        )

        try:
            resp = self.session.get(url, timeout=settings.REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Failed to fetch results for odds: %s", e)
            return {}

        soup = BeautifulSoup(resp.text, "lxml")
        final_odds = {}

        table = soup.find("table", class_="table_bd") or soup.find("table", class_="race_table")
        if not table:
            return final_odds

        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 12:
                continue

            horse_no_text = cells[1].get_text(strip=True)
            if not horse_no_text.isdigit():
                continue

            odds_text = cells[-1].get_text(strip=True)
            try:
                final_odds[int(horse_no_text)] = float(odds_text)
            except (ValueError, TypeError):
                continue

        return final_odds

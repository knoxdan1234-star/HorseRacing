"""
Macau Jockey Club (MJC) Results Scraper

Scrapes race results from the Macau Jockey Club website.
MJC has fewer races and less structured data than HKJC.
This is a secondary data source.
"""

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import date

import requests
from bs4 import BeautifulSoup

from config import settings

logger = logging.getLogger(__name__)


@dataclass
class MJCRunnerResult:
    horse_no: int
    horse_name: str
    jockey: str = ""
    trainer: str = ""
    weight: int | None = None
    draw: int | None = None
    finish_position: int | None = None
    win_odds: float | None = None
    lbw: str = ""


@dataclass
class MJCRaceResult:
    race_date: date
    race_no: int
    race_class: str = ""
    distance: int | None = None
    track_type: str = ""
    going: str = ""
    runners: list[MJCRunnerResult] = field(default_factory=list)


class MJCResultsScraper:
    """
    Macau Jockey Club results scraper.

    NOTE: MJC website structure changes frequently and may require
    Selenium for JavaScript rendering. This implementation provides
    the framework — actual CSS selectors may need adjustment based
    on the current website structure.
    """

    BASE_URL = "https://www.mjc.mo"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        })
        self.delay = settings.SCRAPE_DELAY

    def scrape_meeting(self, race_date: date) -> list[MJCRaceResult]:
        """
        Scrape all races from a Macau meeting.
        Returns empty list if MJC data is unavailable.
        """
        logger.info("Attempting to scrape MJC results for %s", race_date)

        # MJC results page URL pattern — may need updating
        results = []
        race_no = 1
        consecutive_failures = 0

        while consecutive_failures < 2:
            try:
                result = self._scrape_single_race(race_date, race_no)
                if result and result.runners:
                    results.append(result)
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
            except Exception as e:
                logger.warning("MJC scrape failed for race %d: %s", race_no, e)
                consecutive_failures += 1

            race_no += 1
            time.sleep(self.delay)

        logger.info("Scraped %d MJC races for %s", len(results), race_date)
        return results

    def _scrape_single_race(
        self, race_date: date, race_no: int
    ) -> MJCRaceResult | None:
        """Scrape a single MJC race result."""
        # MJC URL pattern — structure depends on current site
        date_str = race_date.strftime("%Y%m%d")
        url = f"{self.BASE_URL}/race/results/{date_str}/{race_no}"

        try:
            resp = self.session.get(url, timeout=settings.REQUEST_TIMEOUT)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.debug("MJC request failed: %s", e)
            return None

        soup = BeautifulSoup(resp.text, "lxml")
        result = MJCRaceResult(race_date=race_date, race_no=race_no)

        # Parse the page — selectors will need to be adapted to MJC's actual structure
        table = soup.find("table")
        if not table:
            return None

        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 4:
                continue

            try:
                no_text = cells[0].get_text(strip=True)
                if not no_text.isdigit():
                    continue

                runner = MJCRunnerResult(
                    horse_no=int(no_text),
                    horse_name=cells[1].get_text(strip=True),
                )

                if len(cells) > 2:
                    runner.jockey = cells[2].get_text(strip=True)
                if len(cells) > 3:
                    runner.trainer = cells[3].get_text(strip=True)

                # Position is usually the placing column
                plc_text = cells[-1].get_text(strip=True) if cells else ""
                if plc_text.isdigit():
                    runner.finish_position = int(plc_text)

                result.runners.append(runner)
            except (IndexError, ValueError):
                continue

        return result if result.runners else None

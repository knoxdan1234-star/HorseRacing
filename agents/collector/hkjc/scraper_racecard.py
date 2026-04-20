"""
HKJC Race Card Scraper

Scrapes pre-race entries and declarations from the HKJC website.
Uses Selenium for JavaScript-rendered content.
URL pattern: racing.hkjc.com/racing/information/English/racing/RaceCard.aspx
  ?racedate=YYYY/MM/DD&Racecourse=ST|HV&RaceNo=N
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
class RaceCardEntry:
    horse_no: int
    horse_name: str
    horse_code: str = ""
    horse_name_tc: str = ""
    draw: int | None = None
    jockey: str = ""
    trainer: str = ""
    rating: int | None = None
    rating_change: int | None = None
    declared_weight: int | None = None
    best_time: str = ""
    last_6_runs: str = ""  # e.g. "1/2/3/5/4/1"
    season_stakes: float | None = None
    age: int | None = None
    gear: str = ""  # Equipment codes: B(linkers), TT, etc.
    priority: int | None = None
    owner: str = ""
    sire: str = ""
    dam: str = ""
    import_type: str = ""


@dataclass
class RaceCardInfo:
    race_date: date
    racecourse: str
    race_no: int
    race_class: str = ""
    distance: int | None = None
    track_type: str = ""
    course_variant: str = ""
    going: str = ""
    prize: float | None = None
    race_name: str = ""
    entries: list[RaceCardEntry] = field(default_factory=list)


class RaceCardScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        })
        self.delay = settings.SCRAPE_DELAY

    def scrape_meeting_card(
        self, race_date: date, racecourse: str
    ) -> list[RaceCardInfo]:
        """Scrape all race cards for a meeting."""
        cards = []
        race_no = 1
        consecutive_failures = 0

        while consecutive_failures < 2:
            try:
                card = self.scrape_racecard(race_date, racecourse, race_no)
                if card and card.entries:
                    cards.append(card)
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
            except Exception as e:
                logger.warning(
                    "Failed to scrape card for race %d on %s at %s: %s",
                    race_no, race_date, racecourse, e,
                )
                consecutive_failures += 1

            race_no += 1
            time.sleep(self.delay)

        logger.info(
            "Scraped %d race cards from %s at %s", len(cards), race_date, racecourse
        )
        return cards

    def scrape_racecard(
        self, race_date: date, racecourse: str, race_no: int
    ) -> RaceCardInfo | None:
        """Scrape a single race card."""
        url = self._build_url(race_date, racecourse, race_no)
        logger.debug("Scraping racecard: %s", url)

        for attempt in range(settings.MAX_RETRIES):
            try:
                resp = self.session.get(url, timeout=settings.REQUEST_TIMEOUT)
                resp.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt < settings.MAX_RETRIES - 1:
                    logger.debug("Retry %d for %s: %s", attempt + 1, url, e)
                    time.sleep(self.delay * (attempt + 1))
                else:
                    logger.error("Failed after %d retries: %s", settings.MAX_RETRIES, url)
                    return None

        soup = BeautifulSoup(resp.text, "lxml")
        return self._parse_racecard_page(soup, race_date, racecourse, race_no)

    def _build_url(self, race_date: date, racecourse: str, race_no: int) -> str:
        # HKJC migrated racecard to /en-us/local/information/racecard in 2026.
        # The old URL (settings.HKJC_RACECARD_URL) redirects here, so go direct.
        date_str = race_date.strftime("%Y/%m/%d")
        return (
            f"https://racing.hkjc.com/en-us/local/information/racecard"
            f"?racedate={date_str}&Racecourse={racecourse}&RaceNo={race_no}"
        )

    def _parse_racecard_page(
        self, soup: BeautifulSoup, race_date: date, racecourse: str, race_no: int
    ) -> RaceCardInfo | None:
        """Parse the race card page."""
        card = RaceCardInfo(
            race_date=race_date,
            racecourse=racecourse,
            race_no=race_no,
        )

        # Parse race info
        self._parse_card_info(soup, card)

        # Parse entries table
        card.entries = self._parse_entries_table(soup)

        if not card.entries:
            return None

        return card

    def _parse_card_info(self, soup: BeautifulSoup, card: RaceCardInfo) -> None:
        """
        Extract race metadata from the race card header.

        New HKJC race card layout (2026) puts race metadata in a `f_fs13` div
        (not a table) with text like:
          "Race 1 - FLAMINGO FLOWER HANDICAP
           Wednesday, April 22, 2026, Happy Valley, 18:40
           Turf, \"B\" Course, 1200M
           Prize Money: $875,000, Rating: 40-0, Class 5"
        Going is sometimes absent on pre-race cards (published race-day morning).
        """
        header_text = ""
        for el in soup.find_all(class_="f_fs13"):
            txt = el.get_text(" ", strip=True)
            if re.search(r"Race\s+\d", txt) and re.search(r"Class\s*\d|Group\s*\d|Griffin|Restricted", txt):
                header_text = txt
                break

        if not header_text:
            return

        text = header_text

        # Race name: between "Race N - " and the next segment (date/venue)
        name_match = re.search(r"Race\s+\d+\s*-\s*(.+?)\s+(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)", text)
        if name_match:
            card.race_name = name_match.group(1).strip()

        # Class
        class_match = re.search(r"Class\s*(\d)", text)
        if class_match:
            card.race_class = f"Class {class_match.group(1)}"
        elif "Group" in text:
            group_match = re.search(r"Group\s*(\d)", text)
            if group_match:
                card.race_class = f"Group {group_match.group(1)}"
        elif "Griffin" in text:
            card.race_class = "Griffin"
        elif "Restricted" in text:
            card.race_class = "Restricted"

        # Distance (e.g. "1200M")
        dist_match = re.search(r"(\d{3,4})\s*M", text)
        if dist_match:
            card.distance = int(dist_match.group(1))

        # Track type
        if "All Weather" in text or "AWT" in text:
            card.track_type = "All Weather"
        elif "Turf" in text:
            card.track_type = "Turf"

        # Course variant (e.g. "B" or "A+3")
        course_match = re.search(r'"([ABC](?:\+\d)?)"', text)
        if course_match:
            card.course_variant = course_match.group(1)

        # Going (optional — may be missing pre-race)
        going_match = re.search(
            r"(Good\s*(?:to\s*(?:Firm|Yielding))?|Firm|Yielding(?:\s*to\s*Soft)?|Soft|Heavy|Wet\s*(?:Fast|Slow))",
            text,
        )
        if going_match:
            card.going = going_match.group(1).strip()

        # Prize money
        prize_match = re.search(r"Prize\s+Money:\s*\$\s*([\d,]+)", text)
        if prize_match:
            try:
                card.prize = float(prize_match.group(1).replace(",", ""))
            except ValueError:
                pass

    def _parse_entries_table(self, soup: BeautifulSoup) -> list[RaceCardEntry]:
        """Extract race card entries from the `starter draggable` table."""
        entries = []

        # New HKJC layout: starter table has class 'starter' and 27 columns.
        table = soup.find("table", class_="starter")
        if not table:
            return entries

        rows = table.find_all("tr")
        if len(rows) < 2:
            return entries

        # Skip the header row
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 15:
                continue
            entry = self._parse_entry_row(cells)
            if entry:
                entries.append(entry)

        return entries

    # Column indices for the new HKJC starter table (27 cols).
    COL_HORSE_NO = 0
    COL_LAST_6 = 1
    COL_HORSE = 3
    COL_BRAND = 4
    COL_WEIGHT = 5
    COL_JOCKEY = 6
    COL_DRAW = 8
    COL_TRAINER = 9
    COL_INT_RATING = 10
    COL_RATING = 11
    COL_RATING_CHANGE = 12
    COL_HORSE_WT_DECL = 13
    COL_BEST_TIME = 15
    COL_AGE = 16
    COL_SEASON_STAKES = 19
    COL_PRIORITY = 20
    COL_DAYS_LAST_RUN = 21
    COL_GEAR = 22
    COL_OWNER = 23
    COL_SIRE = 24
    COL_DAM = 25

    def _parse_entry_row(self, cells: list) -> RaceCardEntry | None:
        """Parse a single horse entry from the 27-column starter table."""

        def txt(i: int) -> str:
            if i >= len(cells):
                return ""
            return cells[i].get_text(" ", strip=True)

        def as_int(i: int) -> int | None:
            s = txt(i).replace(",", "")
            if s.lstrip("+-").isdigit():
                return int(s)
            return None

        def as_float(i: int) -> float | None:
            s = txt(i).replace(",", "").replace("$", "")
            try:
                return float(s)
            except (ValueError, TypeError):
                return None

        try:
            horse_no_text = txt(self.COL_HORSE_NO)
            if not horse_no_text.isdigit():
                return None

            entry = RaceCardEntry(horse_no=int(horse_no_text), horse_name="")
            entry.last_6_runs = txt(self.COL_LAST_6)
            entry.horse_name = txt(self.COL_HORSE)

            # Extract horse code from link: /en-us/local/information/horse?horseid=HK_2024_K316
            horse_cell = cells[self.COL_HORSE] if self.COL_HORSE < len(cells) else None
            if horse_cell:
                a = horse_cell.find("a")
                if a and a.get("href"):
                    m = re.search(r"horseid=([A-Za-z0-9_]+)", a["href"], re.IGNORECASE)
                    if m:
                        entry.horse_code = m.group(1)

            entry.jockey = txt(self.COL_JOCKEY)
            entry.trainer = txt(self.COL_TRAINER)
            entry.declared_weight = as_int(self.COL_WEIGHT)
            entry.draw = as_int(self.COL_DRAW)
            entry.rating = as_int(self.COL_RATING)
            entry.rating_change = as_int(self.COL_RATING_CHANGE)
            entry.best_time = txt(self.COL_BEST_TIME)
            entry.age = as_int(self.COL_AGE)
            entry.season_stakes = as_float(self.COL_SEASON_STAKES)
            entry.priority = as_int(self.COL_PRIORITY)
            entry.gear = txt(self.COL_GEAR)
            entry.owner = txt(self.COL_OWNER)
            entry.sire = txt(self.COL_SIRE)
            entry.dam = txt(self.COL_DAM)

            return entry

        except (IndexError, ValueError) as e:
            logger.debug("Failed to parse racecard row: %s", e)
            return None

    def scrape_entries_page(self) -> list[dict]:
        """Scrape the upcoming entries page for next meeting's declarations."""
        url = settings.HKJC_ENTRIES_URL
        logger.debug("Scraping entries: %s", url)

        try:
            resp = self.session.get(url, timeout=settings.REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Failed to scrape entries: %s", e)
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        entries_data = []

        # Parse the entries page for upcoming race declarations
        for table in soup.find_all("table"):
            header = table.get_text(strip=True).lower()
            if "horse" in header and "entry" in header:
                # This table contains entry data
                for row in table.find_all("tr"):
                    cells = row.find_all("td")
                    if cells:
                        entries_data.append({
                            "text": [c.get_text(strip=True) for c in cells]
                        })

        return entries_data

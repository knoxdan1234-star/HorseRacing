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
        date_str = race_date.strftime("%Y/%m/%d")
        return (
            f"{settings.HKJC_RACECARD_URL}"
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
        """Extract race metadata from the race card header."""
        info_section = soup.find("div", class_="race_head") or soup.find("div", class_="raceInfo")
        if not info_section:
            info_section = soup.find("table", class_="race_head")

        if info_section:
            text = info_section.get_text(separator=" ", strip=True)

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

            # Distance
            dist_match = re.search(r"(\d{3,4})\s*M", text, re.IGNORECASE)
            if dist_match:
                card.distance = int(dist_match.group(1))

            # Track type
            if "All Weather" in text or "AWT" in text:
                card.track_type = "All Weather"
            elif "Turf" in text:
                card.track_type = "Turf"

            # Course variant
            course_match = re.search(r'"([ABC](?:\+\d)?)"', text)
            if course_match:
                card.course_variant = course_match.group(1)

            # Going
            going_match = re.search(
                r"(Good\s*(?:to\s*(?:Firm|Yielding))?|Firm|Yielding(?:\s*to\s*Soft)?|Soft|Heavy)",
                text, re.IGNORECASE,
            )
            if going_match:
                card.going = going_match.group(1).strip()

            # Prize
            prize_match = re.search(r"\$\s*([\d,]+)", text)
            if prize_match:
                try:
                    card.prize = float(prize_match.group(1).replace(",", ""))
                except ValueError:
                    pass

        # Race name
        race_name_el = soup.find("span", class_="race_name")
        if race_name_el:
            card.race_name = race_name_el.get_text(strip=True)

    def _parse_entries_table(self, soup: BeautifulSoup) -> list[RaceCardEntry]:
        """Extract race card entries from the entries table."""
        entries = []

        table = (
            soup.find("table", class_="table_bd")
            or soup.find("table", class_="race_table")
            or soup.find("table", {"id": "racecard"})
        )

        if not table:
            for t in soup.find_all("table"):
                header_text = t.get_text(strip=True).lower()
                if "horse" in header_text and ("draw" in header_text or "jockey" in header_text):
                    table = t
                    break

        if not table:
            return entries

        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 5:
                continue

            entry = self._parse_entry_row(cells)
            if entry:
                entries.append(entry)

        return entries

    def _parse_entry_row(self, cells: list) -> RaceCardEntry | None:
        """Parse a single row of the race card entries table."""
        try:
            # Typical columns: No, Last 6, Horse (Name), Jockey, Trainer,
            # Act.Wt, Decl.Wt, Draw, Rating, +/-, Gear, Best Time, Season Stakes
            horse_no_text = cells[0].get_text(strip=True)
            if not horse_no_text.isdigit():
                return None

            entry = RaceCardEntry(horse_no=int(horse_no_text), horse_name="")

            # Last 6 runs (index 1)
            if len(cells) > 1:
                entry.last_6_runs = cells[1].get_text(strip=True)

            # Horse name and code (index 2)
            if len(cells) > 2:
                horse_cell = cells[2]
                entry.horse_name = horse_cell.get_text(strip=True)
                horse_link = horse_cell.find("a")
                if horse_link and horse_link.get("href"):
                    code_match = re.search(r"HorseId=(\w+)", horse_link["href"])
                    if code_match:
                        entry.horse_code = code_match.group(1)

            # Jockey (index 3)
            if len(cells) > 3:
                entry.jockey = cells[3].get_text(strip=True)

            # Trainer (index 4)
            if len(cells) > 4:
                entry.trainer = cells[4].get_text(strip=True)

            # Actual weight (index 5)
            if len(cells) > 5:
                wt = cells[5].get_text(strip=True)
                if wt.isdigit():
                    entry.declared_weight = int(wt)

            # Draw (index 6)
            if len(cells) > 6:
                draw_text = cells[6].get_text(strip=True)
                if draw_text.isdigit():
                    entry.draw = int(draw_text)

            # Rating (index 7)
            if len(cells) > 7:
                rating_text = cells[7].get_text(strip=True)
                if rating_text.isdigit():
                    entry.rating = int(rating_text)

            # Rating change (index 8)
            if len(cells) > 8:
                change_text = cells[8].get_text(strip=True)
                if change_text.lstrip("+-").isdigit():
                    entry.rating_change = int(change_text)

            # Gear (index 9)
            if len(cells) > 9:
                entry.gear = cells[9].get_text(strip=True)

            # Season stakes (index 10+)
            for i in range(10, len(cells)):
                stakes_text = cells[i].get_text(strip=True).replace(",", "").replace("$", "")
                try:
                    entry.season_stakes = float(stakes_text)
                    break
                except (ValueError, TypeError):
                    continue

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

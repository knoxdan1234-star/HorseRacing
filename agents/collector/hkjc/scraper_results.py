"""
HKJC Race Results Scraper

Scrapes completed race results from the HKJC website.
URL pattern: racing.hkjc.com/racing/information/English/Racing/LocalResults.aspx
  ?RaceDate=YYYY/MM/DD&Racecourse=HV|ST&RaceNo=N
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

RACECOURSES = {"ST": "Sha Tin", "HV": "Happy Valley"}


@dataclass
class RunnerResult:
    horse_no: int
    horse_name: str
    horse_code: str = ""
    jockey: str = ""
    trainer: str = ""
    actual_weight: int | None = None
    declared_weight: int | None = None
    draw: int | None = None
    lbw: str = ""
    running_position: str = ""
    finish_time: str = ""
    win_odds: float | None = None
    finish_position: int | None = None
    gear: str = ""


@dataclass
class DividendResult:
    pool_type: str  # WIN, PLA, QIN, QPL, FCT, TCE, TRI, F4, QTT
    combination: str  # e.g. "3", "3,7", "3,7,11"
    payout: float  # Per $10 unit


@dataclass
class RaceResult:
    race_date: date
    racecourse: str  # ST or HV
    race_no: int
    race_class: str = ""
    distance: int | None = None
    track_type: str = ""
    course_variant: str = ""
    going: str = ""
    prize: float | None = None
    race_name: str = ""
    finish_time: str = ""
    sectional_times: dict = field(default_factory=dict)
    runners: list[RunnerResult] = field(default_factory=list)
    dividends: list[DividendResult] = field(default_factory=list)


class ResultsScraper:
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

    def scrape_meeting(self, race_date: date, racecourse: str) -> list[RaceResult]:
        """Scrape all races from a given meeting date and racecourse."""
        results = []
        race_no = 1
        consecutive_failures = 0

        while consecutive_failures < 2:
            try:
                result = self.scrape_race(race_date, racecourse, race_no)
                if result and result.runners:
                    results.append(result)
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
            except Exception as e:
                logger.warning(
                    "Failed to scrape race %d on %s at %s: %s",
                    race_no, race_date, racecourse, e,
                )
                consecutive_failures += 1

            race_no += 1
            time.sleep(self.delay)

        logger.info(
            "Scraped %d races from %s at %s", len(results), race_date, racecourse
        )
        return results

    def scrape_race(
        self, race_date: date, racecourse: str, race_no: int
    ) -> RaceResult | None:
        """Scrape a single race result."""
        url = self._build_url(race_date, racecourse, race_no)
        logger.debug("Scraping: %s", url)

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
        return self._parse_race_page(soup, race_date, racecourse, race_no)

    def _build_url(self, race_date: date, racecourse: str, race_no: int) -> str:
        date_str = race_date.strftime("%Y/%m/%d")
        return (
            f"{settings.HKJC_RESULTS_URL}"
            f"?RaceDate={date_str}&Racecourse={racecourse}&RaceNo={race_no}"
        )

    def _parse_race_page(
        self, soup: BeautifulSoup, race_date: date, racecourse: str, race_no: int
    ) -> RaceResult | None:
        """Parse the full race result page."""
        result = RaceResult(
            race_date=race_date,
            racecourse=racecourse,
            race_no=race_no,
        )

        # Parse race info header
        self._parse_race_info(soup, result)

        # Parse results table
        result.runners = self._parse_results_table(soup)

        if not result.runners:
            return None

        # Parse dividends
        result.dividends = self._parse_dividends(soup)

        return result

    def _parse_race_info(self, soup: BeautifulSoup, result: RaceResult) -> None:
        """Extract race metadata from header section.

        HKJC page structure (confirmed):
        - Table 1 (no class) has rows like:
          Row 0: "RACE 1 (574)"
          Row 2: "Class 4 - 1000M - (60-40)", "Going :", "GOOD"
        """
        # Find all tables and look for the race info table
        for table in soup.find_all("table"):
            text = table.get_text(separator=" ", strip=True)

            # The info table contains "Class" or "Group" and distance
            if re.search(r"Class\s*\d|Group\s*\d|Griffin", text) and re.search(r"\d{3,4}M", text):
                # Extract class
                class_match = re.search(r"Class\s*(\d)", text)
                if class_match:
                    result.race_class = f"Class {class_match.group(1)}"
                elif "Group" in text:
                    group_match = re.search(r"Group\s*(\d)", text)
                    if group_match:
                        result.race_class = f"Group {group_match.group(1)}"
                elif "Griffin" in text:
                    result.race_class = "Griffin"

                # Extract distance (e.g. "1000M", "1200M", "2400M")
                dist_match = re.search(r"(\d{3,4})\s*M", text, re.IGNORECASE)
                if dist_match:
                    result.distance = int(dist_match.group(1))

                # Extract going (from "Going : GOOD" pattern)
                going_match = re.search(
                    r"Going\s*:?\s*(GOOD TO FIRM|GOOD TO YIELDING|GOOD|FIRM|YIELDING TO SOFT|YIELDING|SOFT|HEAVY|WET FAST|WET SLOW)",
                    text, re.IGNORECASE,
                )
                if going_match:
                    result.going = going_match.group(1).strip().title()

                # Track type from course info
                if "All Weather" in text or "AWT" in text:
                    result.track_type = "All Weather"
                else:
                    result.track_type = "Turf"

                # Course variant (e.g. "A", "A+3", "B", "C")
                course_match = re.search(r'"([ABC](?:\+\d)?)"', text)
                if course_match:
                    result.course_variant = course_match.group(1)

                break

    def _parse_results_table(self, soup: BeautifulSoup) -> list[RunnerResult]:
        """Extract runner results from the results table.

        HKJC page structure (confirmed):
        - Results table has classes: f_tac table_bd draggable
        - Header row: Pla. | Horse No. | Horse | Jockey | Trainer | Act. Wt. |
                       Declar. Horse Wt. | Dr. | LBW | RunningPosition | Finish Time | Win Odds
        - Horse names include code in parentheses: "ZOUPER FELLOW(K284)"
        """
        runners = []

        # Find the results table by its unique class combination
        table = soup.find("table", class_="draggable")

        if not table:
            # Fallback: find table with runner header keywords
            for t in soup.find_all("table"):
                header_text = t.get_text(strip=True).lower()
                if "horse no" in header_text and "jockey" in header_text and "finish time" in header_text:
                    table = t
                    break

        if not table:
            return runners

        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 6:
                continue

            runner = self._parse_runner_row(cells)
            if runner:
                runners.append(runner)

        return runners

    def _parse_runner_row(self, cells: list) -> RunnerResult | None:
        """Parse a single row of the results table.

        Column order (confirmed from live HTML):
        0: Pla.  1: Horse No.  2: Horse  3: Jockey  4: Trainer
        5: Act. Wt.  6: Declar. Horse Wt.  7: Dr.  8: LBW
        9: RunningPosition  10: Finish Time  11: Win Odds
        """
        try:
            position_text = cells[0].get_text(strip=True)

            # Skip header rows or non-result rows
            if not position_text or position_text.lower() in (
                "pla", "pla.", "plc", "placing", "---",
            ):
                return None

            # Handle DNF, WV (withdrawn), DSQ (disqualified)
            finish_position = None
            if position_text.isdigit():
                finish_position = int(position_text)
            elif position_text in ("WV", "DISQ", "DNF", "PU", "UR", "FE"):
                finish_position = 0  # 0 indicates did not finish / scratched

            horse_no_text = cells[1].get_text(strip=True)
            horse_no = int(horse_no_text) if horse_no_text.isdigit() else 0

            # Horse name and code — HKJC format: "HORSE NAME(CODE)"
            horse_cell = cells[2]
            horse_raw = horse_cell.get_text(strip=True)
            horse_code = ""

            # Extract code from parentheses: "ZOUPER FELLOW(K284)" -> name="ZOUPER FELLOW", code="K284"
            name_code_match = re.match(r"^(.+?)\((\w+)\)$", horse_raw)
            if name_code_match:
                horse_name = name_code_match.group(1).strip()
                horse_code = name_code_match.group(2)
            else:
                horse_name = horse_raw

            # Also try extracting horse code from link href
            if not horse_code:
                horse_link = horse_cell.find("a")
                if horse_link and horse_link.get("href"):
                    code_match = re.search(r"HorseId=(\w+)", horse_link["href"])
                    if code_match:
                        horse_code = code_match.group(1)

            runner = RunnerResult(
                horse_no=horse_no,
                horse_name=horse_name,
                horse_code=horse_code,
                finish_position=finish_position,
            )

            # Jockey (index 3)
            if len(cells) > 3:
                runner.jockey = cells[3].get_text(strip=True)

            # Trainer (index 4)
            if len(cells) > 4:
                runner.trainer = cells[4].get_text(strip=True)

            # Actual weight (index 5)
            if len(cells) > 5:
                wt_text = cells[5].get_text(strip=True)
                if wt_text.isdigit():
                    runner.actual_weight = int(wt_text)

            # Declared horse weight (index 6)
            if len(cells) > 6:
                dw_text = cells[6].get_text(strip=True)
                if dw_text.isdigit():
                    runner.declared_weight = int(dw_text)

            # Draw (index 7)
            if len(cells) > 7:
                draw_text = cells[7].get_text(strip=True)
                if draw_text.isdigit():
                    runner.draw = int(draw_text)

            # LBW (index 8) — "-" for winner, distances like "1", "1-1/4", "N", "SH", "HD"
            if len(cells) > 8:
                runner.lbw = cells[8].get_text(strip=True)

            # Running position (index 9) — concatenated like "121" meaning 1st-2nd-1st
            if len(cells) > 9:
                runner.running_position = cells[9].get_text(strip=True)

            # Finish time (index 10) — format "M:SS.DD" e.g. "0:55.44"
            if len(cells) > 10:
                runner.finish_time = cells[10].get_text(strip=True)

            # Win odds (index 11)
            if len(cells) > 11:
                odds_text = cells[11].get_text(strip=True)
                try:
                    runner.win_odds = float(odds_text)
                except (ValueError, TypeError):
                    pass

            return runner

        except (IndexError, ValueError) as e:
            logger.debug("Failed to parse runner row: %s", e)
            return None

    def _parse_dividends(self, soup: BeautifulSoup) -> list[DividendResult]:
        """Extract dividend/payout information.

        HKJC page structure (confirmed):
        - Dividend table has classes: table_bd f_tac f_fs13 f_fl
        - Header rows: "Dividend" then "Pool | Winning Combination | Dividend (HK$)"
        - Data rows: "WIN | 12 | 231.50", "PLACE | 12 | 42.50", etc.
        - Some pools have multiple rows (e.g. PLACE has 3 rows for 1st/2nd/3rd)
        - Pool name only appears in first row; subsequent rows for same pool have empty first cell
        """
        dividends = []

        # Find the dividend table — it has "f_fs13" which is unique to it
        div_table = soup.find("table", class_="f_fs13")

        if not div_table:
            # Fallback: find table containing "Dividend" header
            for table in soup.find_all("table"):
                text = table.get_text(strip=True)
                if "Dividend" in text and "Winning Combination" in text:
                    div_table = table
                    break

        if not div_table:
            return dividends

        # Order matters: longer names first so "QUINELLA PLACE" matches before "QUINELLA"
        pool_map = [
            ("QUINELLA PLACE", "QPL"),
            ("QUINELLA", "QIN"),
            ("DOUBLE TRIO", "DT"),
            ("TRIPLE TRIO", "TT"),
            ("JOCKEY CHALLENGE", "JKC"),
            ("FIRST 4", "F4"),
            ("SIX UP", "6UP"),
            ("WIN", "WIN"),
            ("PLACE", "PLA"),
            ("FORECAST", "FCT"),
            ("TIERCE", "TCE"),
            ("TRIO", "TRI"),
            ("QUARTET", "QTT"),
            ("DOUBLE", "DBL"),
            ("TREBLE", "TBL"),
        ]

        rows = div_table.find_all("tr")
        current_pool = ""

        for row in rows:
            cells = row.find_all("td")
            if not cells:
                continue

            row_text = [c.get_text(strip=True) for c in cells]
            first_upper = row_text[0].upper() if row_text else ""

            # Skip header rows
            if first_upper in ("DIVIDEND", "POOL"):
                continue

            # Check if first cell contains a pool name (3-cell rows)
            matched_pool = False
            for key, code in pool_map:
                if first_upper == key or first_upper.startswith(key):
                    current_pool = code
                    matched_pool = True
                    break

            if not current_pool:
                continue

            # Two formats observed:
            # 3 cells (named row):   ["WIN", "12", "231.50"]
            # 2 cells (continuation): ["9", "29.00"]  (combo, payout for same pool)
            combo = ""
            payout_text = ""

            if len(row_text) == 3 and matched_pool:
                # Named row: Pool | Combination | Payout
                combo = row_text[1].strip()
                payout_text = row_text[2].strip()
            elif len(row_text) == 2:
                # Continuation row: Combination | Payout
                combo = row_text[0].strip()
                payout_text = row_text[1].strip()
            elif len(row_text) >= 3 and not matched_pool:
                # Unexpected 3-cell continuation (shouldn't happen, but be safe)
                combo = row_text[-2].strip()
                payout_text = row_text[-1].strip()

            if not payout_text:
                continue

            # Parse payout value — may have commas: "3,616.00", "$1,234.50"
            payout_match = re.search(r"\$?([\d,]+\.?\d*)", payout_text)
            if payout_match:
                try:
                    payout = float(payout_match.group(1).replace(",", ""))
                    dividends.append(DividendResult(
                        pool_type=current_pool,
                        combination=combo,
                        payout=payout,
                    ))
                except (ValueError, TypeError):
                    continue

        return dividends

    def get_meeting_dates(self, year: int, month: int) -> list[tuple[date, str]]:
        """Get meeting dates and racecourses from the fixture calendar."""
        url = f"{settings.HKJC_FIXTURE_URL}?CalYear={year}&CalMonth={month}"

        try:
            resp = self.session.get(url, timeout=settings.REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Failed to fetch fixtures for %d/%d: %s", year, month, e)
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        meetings = []

        # Parse fixture calendar links
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "LocalResults" in href or "RaceCard" in href:
                date_match = re.search(r"RaceDate=(\d{4}/\d{2}/\d{2})", href)
                course_match = re.search(r"Racecourse=(\w+)", href)
                if date_match and course_match:
                    d = date.fromisoformat(date_match.group(1).replace("/", "-"))
                    rc = course_match.group(1)
                    if (d, rc) not in meetings:
                        meetings.append((d, rc))

        return meetings
